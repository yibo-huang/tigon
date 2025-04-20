/*
 * See: https://github.com/qemu/qemu/blob/f9d58e0ca53b3f470b84725a7b5e47fcf446a2ea/contrib/ivshmem-client/ivshmem-client.c
 *
 * Copyright 6WIND S.A., 2014
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or
 * (at your option) any later version.  See the COPYING file in the
 * top-level directory.
 */

use std::ffi;
use std::io;
use std::mem;
use std::os::fd::AsRawFd;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::path::PathBuf;
use std::ptr;

use anyhow::anyhow;
use anyhow::Context as _;
use clap::Parser;
use ivshmem::cmsg;
use ivshmem::IVSHMEM_PROTOCOL_VERSION;

#[derive(Parser)]
struct Command {
    #[arg(short = 'S', default_value = "/tmp/ivshmem_socket")]
    unix_sock_path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    pretty_env_logger::init_timed();

    let command = Command::parse();
    let mut client = Client::new(&command.unix_sock_path)?;

    ivshmem::handle_signals()?;

    while !ivshmem::quit() {
        let (mut fds, maxfd) = client.get_fds();

        unsafe {
            libc::FD_SET(libc::STDIN_FILENO, &mut fds);
        }

        match unsafe {
            libc::select(
                maxfd,
                &mut fds,
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null_mut(),
            )
        } {
            0 => continue,
            -1 if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted => continue,
            -1 => return Err(io::Error::last_os_error()).context("Select error"),
            _ => (),
        }

        if unsafe { libc::FD_ISSET(0, &fds) } {
            client.handle_stdin_command()?;
        }

        client.handle_fds(&fds, maxfd)?;
    }

    Ok(())
}

struct Client {
    sock: UnixStream,
    peer_list: Vec<Peer>,
    local: Peer,
}

impl Client {
    fn new(unix_sock_path: &Path) -> anyhow::Result<Self> {
        log::info!("connecting to client {}", unix_sock_path.display());

        let sock = UnixStream::connect(unix_sock_path)
            .with_context(|| anyhow!("Could not connect to {}", unix_sock_path.display()))?;

        match cmsg::recv(&sock)? {
            (Some(protocol), None) if protocol == IVSHMEM_PROTOCOL_VERSION => (),
            _ => return Err(anyhow!("Cannot read protocol from server")),
        }

        let local = match cmsg::recv(&sock)? {
            (Some(id), None) => Peer {
                id,
                vectors: Vec::new(),
            },
            _ => return Err(anyhow!("Cannot read local ID from server")),
        };

        log::info!("our_id={}", local.id);

        let shm_fd = match cmsg::recv(&sock)? {
            (None, Some(fd)) => fd,
            _ => {
                return Err(anyhow!(
                    "Cannot read shared memory file descriptor from server"
                ))
            }
        };

        log::info!("shm_fd={}", shm_fd.as_raw_fd());

        Ok(Client {
            sock,
            peer_list: Vec::new(),
            local,
        })
    }

    fn handle_stdin_command(&self) -> anyhow::Result<()> {
        let mut stdin = String::new();
        io::stdin().read_line(&mut stdin)?;
        match stdin.trim() {
            "dump" => {
                self.dump();
                Ok(())
            }
            "int all" => self.notify_broadcast(),
            command => {
                let Some(suffix) = command.strip_prefix("int ") else { return Ok(()) };
                let Some((peer_id, vector)) = suffix.split_once(' ') else { return Ok(()) };
                let Some(peer_id) = peer_id.parse::<u16>().ok() else { return Ok(()) };
                let Some(peer) = self.peer_list.iter().chain([&self.local]).find(|peer| peer.id == peer_id) else { return Ok(()) };
                match vector.parse::<u16>() {
                    Ok(vector) => peer.notify(vector),
                    Err(_) if vector == "all" => peer.notify_all_vects(),
                    Err(_) => Ok(()),
                }
            }
        }
    }

    fn handle_fds(&mut self, fds: &libc::fd_set, maxfd: ffi::c_int) -> anyhow::Result<()> {
        match if self.sock.as_raw_fd() < maxfd
            && unsafe { libc::FD_ISSET(self.sock.as_raw_fd(), fds) }
        {
            self.handle_server_msg()
                .context("ivshmem_client_handle_server_msg failed")
        } else {
            self.handle_event(fds, maxfd)
                .context("ivshmem_client_handle_event failed")
        } {
            Ok(()) => Ok(()),
            Err(error)
                if error
                    .downcast_ref::<io::Error>()
                    .map_or(false, |error| error.kind() == io::ErrorKind::Interrupted) =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn handle_server_msg(&mut self) -> anyhow::Result<()> {
        let (peer_id, fd) = cmsg::recv(&self.sock)?;
        let peer_id = peer_id.ok_or_else(|| anyhow!("Missing peer ID from server message"))?;
        let peer_index = self.peer_list.iter().position(|peer| peer.id == peer_id);

        let fd = match (fd, peer_index) {
            (Some(fd), _) => fd,
            (None, None) => {
                return Err(anyhow!("Received delete for invalid peer {}", peer_id));
            }
            (None, Some(index)) => {
                log::debug!("delete peer id = {}", peer_id);
                self.peer_list.remove(index);
                return Ok(());
            }
        };

        let peer = match peer_index {
            Some(index) => &mut self.peer_list[index],
            None if peer_id == self.local.id => &mut self.local,
            None => {
                log::debug!("new peer id = {}", peer_id);
                self.peer_list.push(Peer {
                    id: peer_id,
                    vectors: Vec::new(),
                });
                self.peer_list.last_mut().unwrap()
            }
        };

        log::debug!(
            "new vector {} (fd={}) for peer id {}",
            peer.vectors.len(),
            fd.as_raw_fd(),
            peer_id
        );
        peer.vectors.push(fd);
        Ok(())
    }

    fn handle_event(&self, cur: &libc::fd_set, maxfd: ffi::c_int) -> anyhow::Result<()> {
        let mut kick = [0u8; 8];

        for (i, fd) in self.local.vectors.iter().enumerate() {
            if fd.as_raw_fd() >= maxfd || unsafe { !libc::FD_ISSET(fd.as_raw_fd(), cur) } {
                continue;
            }

            let ret = unsafe { libc::read(fd.as_raw_fd(), kick.as_mut_ptr().cast(), kick.len()) };
            if ret < 0 || ret as usize != kick.len() {
                return Err(anyhow!("Invalid read size = {}", ret));
            }

            log::info!(
                "received event on fd {} vector {}: {}",
                fd.as_raw_fd(),
                i,
                u64::from_le_bytes(kick),
            );
        }

        Ok(())
    }

    fn get_fds(&self) -> (libc::fd_set, libc::c_int) {
        let mut fds = unsafe {
            let mut fds = mem::zeroed::<libc::fd_set>();
            libc::FD_ZERO(&mut fds);
            fds
        };

        let max = self
            .local
            .vectors
            .iter()
            .map(|fd| fd.as_raw_fd())
            .chain([self.sock.as_raw_fd()])
            .inspect(|fd| unsafe { libc::FD_SET(*fd, &mut fds) })
            .max()
            .unwrap_or_default()
            + 1;

        (fds, max)
    }

    /// Dump our info, the list of peers their vectors on stdout.
    fn dump(&self) {
        println!("our_id = {}", self.local.id);
        for (vector, fd) in self.local.vectors.iter().enumerate() {
            println!("  vector {} is enabled (fd={})", vector, fd.as_raw_fd());
        }

        for peer in &self.peer_list {
            println!("peer_id = {}", peer.id);
            for (vector, fd) in peer.vectors.iter().enumerate() {
                println!("  vector {} is enabled (fd={})", vector, fd.as_raw_fd());
            }
        }
    }

    /// Send a notification to all peers.
    fn notify_broadcast(&self) -> anyhow::Result<()> {
        for peer in &self.peer_list {
            peer.notify_all_vects()?;
        }
        Ok(())
    }
}

struct Peer {
    id: u16,
    vectors: Vec<OwnedFd>,
}

impl Peer {
    /// Send a notification to all vectors of a peer.
    fn notify_all_vects(&self) -> anyhow::Result<()> {
        for vector in 0..self.vectors.len() {
            self.notify(vector as u16)?;
        }
        Ok(())
    }

    /// Send a notification on a vector of a peer.
    fn notify(&self, vector: u16) -> anyhow::Result<()> {
        if vector as usize >= self.vectors.len() {
            return Err(anyhow!("Invalid vector {} on peer {}", vector, self.id));
        }

        log::debug!(
            "notify peer {} on vector {}, fd {}",
            self.id,
            vector,
            self.vectors[vector as usize].as_raw_fd(),
        );

        let kick = 1u64.to_le_bytes();

        unsafe {
            if libc::write(
                self.vectors[vector as usize].as_raw_fd(),
                kick.as_ptr().cast(),
                kick.len(),
            ) != kick.len() as isize
            {
                return Err(anyhow!(
                    "Could not write to {}",
                    self.vectors[vector as usize].as_raw_fd()
                ));
            }
        }

        Ok(())
    }
}
