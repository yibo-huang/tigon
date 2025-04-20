/*
 * See: https://github.com/qemu/qemu/blob/f9d58e0ca53b3f470b84725a7b5e47fcf446a2ea/contrib/ivshmem-server/ivshmem-server.c
 *
 * Copyright 6WIND S.A., 2014
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or
 * (at your option) any later version.  See the COPYING file in the
 * top-level directory.
 */

use std::fs;
use std::fs::File;
use std::io;
use std::os::fd::AsFd as _;
use std::os::fd::AsRawFd as _;
use std::os::fd::BorrowedFd;
use std::path::Path;
use std::path::PathBuf;

use anyhow::anyhow;
use anyhow::Context;
use clap::Parser;
use ivshmem::cmsg;
use ivshmem::EventNotifier;
use ivshmem::IVSHMEM_PROTOCOL_VERSION;
use mio::net::UnixListener;
use mio::net::UnixStream;
use mio::Interest;
use mio::Token;

#[derive(Parser)]
struct Command {
    #[arg(short = 'S', long, default_value = "/tmp/ivshmem_socket")]
    socket_path: PathBuf,

    /// Use pipe instead of eventfd for interrupt emulation
    #[arg(short, long)]
    pipe: bool,

    #[arg(short = 'm', long, default_value = "ivshmem")]
    memory_path: PathBuf,

    #[arg(short = 'l', long, default_value = "4M")]
    memory_size: ivshmem::Size,

    /// Number of MSI interrupt vectors per client
    #[arg(short = 'n', long, default_value_t = 1)]
    vector_count: u16,

    /// Total number of clients that will share this server
    #[arg(short = 'v', long, default_value_t = 2)]
    vm_count: u16,

    /// Offset VM IDs by one
    #[arg(short = 'o', long)]
    vm_offset: bool,
}

fn main() -> anyhow::Result<()> {
    pretty_env_logger::init_timed();
    ivshmem::handle_signals()?;

    let command = Command::parse();

    let mut server = Server::new(
        &command.socket_path,
        command.pipe,
        &command.memory_path,
        command.memory_size.0,
        command.vector_count,
        command.vm_count,
        command.vm_offset,
    )?;

    server.poll_events()?;

    Ok(())
}

struct Server {
    memory: File,
    peers: Vec<(UnixListener, Option<Peer>)>,
    pipe: bool,
    poll: mio::Poll,
    vector_count: u16,
    vm_offset: bool,
}

impl Server {
    fn new(
        socket_path: &Path,
        pipe: bool,
        memory_path: &Path,
        memory_size: usize,
        vector_count: u16,
        vm_count: u16,
        vm_offset: bool,
    ) -> anyhow::Result<Self> {
        log::info!("Using file-backed shared memory: {}", memory_path.display());

        let memory = File::options()
            .read(true)
            .write(true)
            .open(memory_path)
            .with_context(|| anyhow!("Failed to open {}", memory_path.display()))?;

        ftruncate(&memory, memory_size)?;

        let mut peers = Vec::new();
        let poll = mio::Poll::new()?;

        for index in 0..vm_count {
            let id = index + vm_offset as u16;
            let path = socket_path.with_extension(id.to_string());

            match fs::remove_file(&path) {
                Ok(()) => (),
                Err(error) if error.kind() == io::ErrorKind::NotFound => (),
                Err(error) => {
                    return Err(error)
                        .with_context(|| anyhow!("Failed to remove {}", path.display()));
                }
            }

            let mut listener = UnixListener::bind(&path)
                .with_context(|| anyhow!("Cannot bind to {}", path.display()))?;

            poll.registry()
                .register(&mut listener, Token(index as usize), Interest::READABLE)?;

            peers.push((listener, None));
        }

        Ok(Server {
            memory,
            peers,
            pipe,
            poll,
            vector_count,
            vm_offset,
        })
    }

    fn poll_events(&mut self) -> anyhow::Result<()> {
        let mut events = mio::Events::with_capacity(16);

        while !ivshmem::quit() {
            match self.poll.poll(&mut events, None) {
                Ok(()) => (),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error).context("Failed to poll"),
            }

            for event in &events {
                match event.token().0.checked_sub(self.peers.len()) {
                    None => self
                        .handle_new_conn(event.token().0)
                        .context("Failed to handle new connection")?,
                    Some(index) => {
                        assert!(event.is_read_closed());
                        assert!(event.is_write_closed());

                        let mut peer = self.peers[index].1.take().expect("Closed peer twice");
                        let id = index as u16 + self.vm_offset as u16;
                        log::debug!("Dropping peer {} fd={}...", id, peer.socket.as_raw_fd());

                        self.poll.registry().deregister(&mut peer.socket)?;

                        // advertise the deletion to other peers
                        for (other_id, other) in self.peers() {
                            other.send(Some(id), None).with_context(|| {
                                anyhow!("Failed to send deletion of {} to peer {}", id, other_id)
                            })?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    // Handle message on listening unix socket (new client connection).
    fn handle_new_conn(&mut self, index: usize) -> anyhow::Result<()> {
        let listener = &self.peers[index];
        let (socket, _) = listener.0.accept()?;

        let mut peer = Peer {
            socket,
            vectors: Vec::new(),
        };

        // create eventfd, one per vector
        for _ in 0..self.vector_count {
            peer.vectors.push(EventNotifier::new(!self.pipe)?);
        }

        let id = index as u16 + self.vm_offset as u16;

        // send our protocol version first
        peer.send(Some(IVSHMEM_PROTOCOL_VERSION), None)
            .context("Failed to send version")?;

        // send the peer id to the client
        peer.send(Some(id), None)
            .context("Failed to send peer ID")?;

        // send the shm_fd
        peer.send(None, Some(self.memory.as_fd()))
            .context("Failed to send shared memory file descriptor")?;

        // advertise the new peer to others
        for (_, other) in self.peers() {
            for i in 0..peer.vectors.len() {
                other.send(Some(id), Some(peer.vectors[i].wfd()))?;
            }
        }

        // advertise the other peers to the new one
        for (other_id, other) in self.peers() {
            for i in 0..peer.vectors.len() {
                peer.send(Some(other_id), Some(other.vectors[i].wfd()))?;
            }
        }

        // advertise the new peer to itself
        for i in 0..peer.vectors.len() {
            peer.send(Some(id), Some(peer.vectors[i].rfd()))?;
        }

        self.poll.registry().register(
            &mut peer.socket,
            Token(index + self.peers.len()),
            Interest::READABLE,
        )?;

        log::info!("new peer id = {}", id);
        match self.peers[index].1.replace(peer) {
            None => Ok(()),
            Some(_) => panic!("Created peer with same ID twice: {}", id),
        }
    }

    fn peers(&self) -> impl Iterator<Item = (u16, &Peer)> {
        self.peers
            .iter()
            .enumerate()
            .filter_map(|(id, (_, peer))| Some((id as u16, peer.as_ref()?)))
    }
}

struct Peer {
    socket: UnixStream,
    vectors: Vec<EventNotifier>,
}

impl Peer {
    fn send(&self, peer_id: Option<u16>, fd: Option<BorrowedFd>) -> anyhow::Result<()> {
        cmsg::send(&self.socket, peer_id, fd)
    }
}

// disable the roundup size operation and respect the actual size
fn ftruncate(memory: &File, mut size: usize) -> anyhow::Result<()> {
    size = size.next_power_of_two();

    if memory.metadata()?.len() == size as u64 {
        return Ok(());
    }

    memory
        .set_len(size as u64)
        .context("ivshmem_server_ftruncate failed")
}
