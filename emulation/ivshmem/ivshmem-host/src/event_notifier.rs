/*
 * See: https://github.com/qemu/qemu/blob/f9d58e0ca53b3f470b84725a7b5e47fcf446a2ea/util/event_notifier-posix.c#L27
 *
 * event notifier support
 *
 * Copyright Red Hat, Inc. 2010
 *
 * Authors:
 *  Michael S. Tsirkin <mst@redhat.com>
 *
 * This work is licensed under the terms of the GNU GPL, version 2 or later.
 * See the COPYING file in the top-level directory.
 */

use std::ffi;
use std::io;
use std::mem;
use std::os::fd::AsFd as _;
use std::os::fd::AsRawFd as _;
use std::os::fd::BorrowedFd;
use std::os::fd::FromRawFd as _;
use std::os::fd::OwnedFd;

use anyhow::anyhow;
use anyhow::Context as _;

pub enum EventNotifier {
    Event(OwnedFd),
    Pipe { read: OwnedFd, write: OwnedFd },
}

impl EventNotifier {
    pub fn new(eventfd: bool) -> anyhow::Result<Self> {
        unsafe {
            if eventfd {
                match libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) {
                    fd if fd >= 0 => Ok(EventNotifier::Event(OwnedFd::from_raw_fd(fd))),
                    error => Err(io::Error::last_os_error())
                        .with_context(|| anyhow!("eventfd returned error: {}", error)),
                }
            } else {
                let mut fds = [0; 2];

                match libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) {
                    0 => Ok(EventNotifier::Pipe {
                        read: OwnedFd::from_raw_fd(fds[0]),
                        write: OwnedFd::from_raw_fd(fds[1]),
                    }),
                    error => Err(io::Error::last_os_error())
                        .with_context(|| anyhow!("pipe2 returned error: {}", error)),
                }
            }
        }
    }

    pub fn set(&mut self) -> anyhow::Result<()> {
        let value = 1u64;
        let mut ret = -1;
        let mut error = io::Error::from_raw_os_error(libc::EINTR);

        while ret < 0 && error.kind() == io::ErrorKind::Interrupted {
            ret = unsafe {
                libc::write(
                    self.wfd().as_raw_fd(),
                    &value as *const u64 as *const ffi::c_void,
                    mem::size_of_val(&value),
                )
            };
            error = io::Error::last_os_error();
        }

        if ret < 0 && error.kind() != io::ErrorKind::Interrupted {
            Err(error).context("event_notifier_set failed")
        } else {
            Ok(())
        }
    }

    pub fn test_and_clear(&mut self) -> bool {
        let mut value = false;
        let mut len = -1;
        let mut buffer = [0u8; 512];
        let mut error = io::Error::from_raw_os_error(libc::EINTR);

        while len == -1 && error.kind() == io::ErrorKind::Interrupted {
            len = unsafe {
                libc::read(
                    self.rfd().as_raw_fd(),
                    buffer.as_mut_ptr() as *mut libc::c_void,
                    buffer.len(),
                )
            };
            value |= len > 0;
            error = io::Error::last_os_error();
        }

        value
    }

    pub fn rfd(&self) -> BorrowedFd {
        match self {
            EventNotifier::Event(fd) => fd.as_fd(),
            EventNotifier::Pipe { read, .. } => read.as_fd(),
        }
    }

    pub fn wfd(&self) -> BorrowedFd {
        match self {
            EventNotifier::Event(fd) => fd.as_fd(),
            EventNotifier::Pipe { write, .. } => write.as_fd(),
        }
    }
}
