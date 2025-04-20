use std::ffi;
use std::mem;
use std::os::fd::AsRawFd;
use std::os::fd::BorrowedFd;
use std::os::fd::FromRawFd as _;
use std::os::fd::OwnedFd;
use std::os::fd::RawFd;

use anyhow::anyhow;
use anyhow::Context;

#[repr(C)]
union msg_control {
    cmsg: libc::cmsghdr,
    align: [u8; unsafe { libc::CMSG_SPACE(mem::size_of::<RawFd>() as u32) } as usize],
}

/// Send message to a client unix socket.
/// See: https://copyconstruct.medium.com/file-descriptor-transfer-over-unix-domain-sockets-dcbbf5b3b6ec
pub fn send<S: AsRawFd>(
    sock: &S,
    peer_id: Option<u16>,
    fd: Option<BorrowedFd>,
) -> anyhow::Result<()> {
    let mut msg = unsafe { mem::zeroed::<libc::msghdr>() };
    let mut msg_control = unsafe { mem::zeroed::<msg_control>() };

    let mut id = peer_id
        .map(|peer_id| peer_id as i64)
        .unwrap_or(-1)
        .to_le_bytes();

    let mut iov = [libc::iovec {
        iov_base: id.as_mut_ptr().cast(),
        iov_len: id.len(),
    }];

    msg.msg_iov = iov.as_mut_ptr();
    msg.msg_iovlen = 1;

    // if fd is specified, add it in a cmsg
    if let Some(fd) = fd {
        msg.msg_control = unsafe { msg_control.align.as_mut_ptr().cast() };
        msg.msg_controllen = mem::size_of::<msg_control>();

        let cmsg = unsafe { &mut *libc::CMSG_FIRSTHDR(&msg) };
        cmsg.cmsg_level = libc::SOL_SOCKET;
        cmsg.cmsg_type = libc::SCM_RIGHTS;
        cmsg.cmsg_len = unsafe { libc::CMSG_LEN(mem::size_of::<BorrowedFd>() as u32) } as usize;

        let fd = fd.as_raw_fd().to_le_bytes();

        unsafe {
            libc::memcpy(
                libc::CMSG_DATA(cmsg).cast(),
                fd.as_ptr().cast(),
                mem::size_of_val(&fd),
            );
        }
    }

    match unsafe { libc::sendmsg(sock.as_raw_fd(), &msg, 0) } {
        error if error <= 0 => Err(anyhow!("ivshmem_server_send_one_msg: {}", error)),
        _ => Ok(()),
    }
}

/// Read message from the unix socket.
/// See: https://copyconstruct.medium.com/file-descriptor-transfer-over-unix-domain-sockets-dcbbf5b3b6ec
pub fn recv<S: AsRawFd>(sock: &S) -> anyhow::Result<(Option<u16>, Option<OwnedFd>)> {
    let mut buffer = [0u8; 8];
    let mut msg = unsafe { mem::zeroed::<libc::msghdr>() };
    let mut msg_control = unsafe { mem::zeroed::<msg_control>() };

    let mut iov = [libc::iovec {
        iov_base: buffer.as_mut_ptr() as *mut ffi::c_void,
        iov_len: mem::size_of_val(&buffer),
    }];

    msg.msg_iov = iov.as_mut_ptr();
    msg.msg_iovlen = 1;
    msg.msg_control = &mut msg_control as *mut msg_control as *mut ffi::c_void;
    msg.msg_controllen = mem::size_of::<msg_control>();

    let ret = unsafe { libc::recvmsg(sock.as_raw_fd(), &mut msg, 0) };
    if (ret as usize) < mem::size_of_val(&buffer) {
        return Err(anyhow!("Cannot read message"));
    }

    if ret == 0 {
        return Err(anyhow!("Lost connection to server"));
    }

    let peer_id = match i64::from_le_bytes(buffer) {
        -1 => None,
        id => u16::try_from(id)
            .context("Received invalid peer ID")
            .map(Option::Some)?,
    };

    let mut fd = [0; mem::size_of::<RawFd>()];

    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);

        while !cmsg.is_null() {
            if (*cmsg).cmsg_len == libc::CMSG_LEN(mem::size_of::<RawFd>() as u32) as usize
                && (*cmsg).cmsg_level == libc::SOL_SOCKET
                && (*cmsg).cmsg_type == libc::SCM_RIGHTS
            {
                libc::memcpy(
                    fd.as_mut_ptr().cast(),
                    libc::CMSG_DATA(cmsg) as *const libc::c_void,
                    mem::size_of_val(&fd),
                );
                return Ok((
                    peer_id,
                    Some(OwnedFd::from_raw_fd(RawFd::from_le_bytes(fd))),
                ));
            }

            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
    }

    Ok((peer_id, None))
}
