use std::fs;
use std::io;
use std::mem;
use std::os::fd::AsFd;
use std::os::fd::AsRawFd as _;
use std::os::fd::BorrowedFd;
use std::os::fd::FromRawFd as _;
use std::os::fd::OwnedFd;
use std::os::unix::net::UnixListener;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::ptr;
use std::time::Instant;

use anyhow::Context as _;
use clap::Parser;
use ivshmem::cmsg;
use ivshmem::EventNotifier;

#[derive(Parser)]
struct Command {
    #[arg(short, long)]
    path: PathBuf,

    #[arg(short, long)]
    pipe: bool,

    #[arg(short, long)]
    poll: PollMode,

    #[command(subcommand)]
    mode: Mode,
}

#[derive(Parser)]
enum Mode {
    Echo,
    Bench {
        #[arg(short, long, default_value = "100000")]
        warmup: usize,
        #[arg(short, long, default_value = "1000000")]
        iterations: usize,
    },
}

#[derive(Clone, clap::ValueEnum)]
enum PollMode {
    Block,
    Select,
    Spin,
    Epoll,
}

fn main() -> anyhow::Result<()> {
    let command = Command::parse();

    match command.mode {
        Mode::Echo => {
            match fs::remove_file(&command.path) {
                Ok(()) => (),
                Err(error) if error.kind() == io::ErrorKind::NotFound => (),
                Err(error) => return Err(anyhow::Error::from(error)),
            }

            let listener = UnixListener::bind(&command.path)?;

            let (stream, _) = listener.accept()?;
            let mut event = EventNotifier::new(!command.pipe)?;

            let (_, rfd) = cmsg::recv(&stream)?;
            let rfd = rfd.unwrap();
            cmsg::send(&stream, 0, Some(event.rfd()))?;

            let mut poll = Poll::new(command.poll, rfd.as_fd())?;

            loop {
                poll.wait();
                event.set()?;
            }
        }
        Mode::Bench { warmup, iterations } => {
            let stream = UnixStream::connect(&command.path)?;
            let mut event = EventNotifier::new(!command.pipe)?;

            cmsg::send(&stream, 0, Some(event.rfd()))?;
            let (_, rfd) = cmsg::recv(&stream)?;
            let rfd = rfd.unwrap();

            let mut poll = Poll::new(command.poll, rfd.as_fd())?;

            for i in 0..warmup + iterations {
                let start = Instant::now();
                event.set()?;
                poll.wait();
                let end = Instant::now();
                if i >= warmup {
                    println!("{}", (end - start).as_nanos());
                }
            }
        }
    }

    Ok(())
}

enum Poll<'fd> {
    Block(BorrowedFd<'fd>),
    Spin(BorrowedFd<'fd>),
    Select(BorrowedFd<'fd>),
    Epoll {
        epollfd: OwnedFd,
        rfd: BorrowedFd<'fd>,
    },
}

impl<'fd> Poll<'fd> {
    fn new(mode: PollMode, rfd: BorrowedFd<'fd>) -> anyhow::Result<Self> {
        match mode {
            PollMode::Block => {
                let flags = match unsafe { libc::fcntl(rfd.as_raw_fd(), libc::F_GETFL) } {
                    -1 => return Err(io::Error::last_os_error()).context("fcntl(GETFD) failed"),
                    flags => flags,
                };

                match unsafe {
                    libc::fcntl(rfd.as_raw_fd(), libc::F_SETFL, flags & !libc::O_NONBLOCK)
                } {
                    -1 => Err(io::Error::last_os_error()).context("fcntl(SETFD) failed"),
                    _ => Ok(Poll::Block(rfd)),
                }
            }
            PollMode::Select => Ok(Poll::Select(rfd)),
            PollMode::Spin => Ok(Poll::Spin(rfd)),
            PollMode::Epoll => unsafe {
                let epollfd = match libc::epoll_create1(0) {
                    -1 => return Err(anyhow::Error::from(io::Error::last_os_error())),
                    fd => OwnedFd::from_raw_fd(fd),
                };

                let mut ev = mem::zeroed::<libc::epoll_event>();
                ev.events = libc::EPOLLIN as u32;
                ev.u64 = rfd.as_raw_fd() as u64;

                if libc::epoll_ctl(
                    epollfd.as_raw_fd(),
                    libc::EPOLL_CTL_ADD,
                    rfd.as_raw_fd(),
                    &mut ev,
                ) == -1
                {
                    return Err(anyhow::Error::from(io::Error::last_os_error()));
                }

                Ok(Poll::Epoll { epollfd, rfd })
            },
        }
    }

    fn wait(&mut self) {
        match self {
            Poll::Block(rfd) => {
                assert_eq!(read(*rfd), 8);
            }
            Poll::Spin(rfd) => {
                let mut len = -1;
                while len == -1 {
                    len = read(*rfd);
                }
            }
            Poll::Select(rfd) => unsafe {
                loop {
                    let mut fds = mem::zeroed::<libc::fd_set>();
                    libc::FD_ZERO(&mut fds);
                    libc::FD_SET(rfd.as_raw_fd(), &mut fds);
                    match libc::select(
                        rfd.as_raw_fd() + 1,
                        &mut fds,
                        ptr::null_mut(),
                        ptr::null_mut(),
                        ptr::null_mut(),
                    ) {
                        0 => continue,
                        -1 if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted => {
                            continue
                        }
                        -1 => panic!("select error: {}", io::Error::last_os_error()),
                        _ => (),
                    }

                    assert_eq!(read(*rfd), 8);
                    return;
                }
            },
            Poll::Epoll { epollfd, rfd } => unsafe {
                let mut events = [mem::zeroed::<libc::epoll_event>(); 1];
                loop {
                    match libc::epoll_wait(epollfd.as_raw_fd(), events.as_mut_ptr(), 1, -1) {
                        0 => continue,
                        -1 => panic!("epoll_wait error: {}", io::Error::last_os_error()),
                        1 => (),
                        _ => unreachable!(),
                    }

                    assert_eq!(read(*rfd), 8);
                    return;
                }
            },
        }
    }
}

fn read(rfd: BorrowedFd) -> isize {
    let mut buffer = [0u8; 512];
    unsafe { libc::read(rfd.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) }
}
