use std::ffi;
use std::mem;
use std::ptr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use anyhow::anyhow;

static QUIT: AtomicBool = AtomicBool::new(false);

extern "C" fn quit_callback(_: ffi::c_int) {
    QUIT.store(true, Ordering::Release);
}

pub fn quit() -> bool {
    QUIT.load(Ordering::Acquire)
}

pub fn handle_signals() -> anyhow::Result<()> {
    unsafe {
        let mut sa = mem::zeroed::<libc::sigaction>();
        sa.sa_sigaction = libc::SIG_IGN;

        if libc::sigemptyset(&mut sa.sa_mask) == -1
            || libc::sigaction(libc::SIGPIPE, &sa, ptr::null_mut()) == -1
        {
            return Err(anyhow!("Failed to ignore SIGPIPE"));
        }

        let mut sa_quit = mem::zeroed::<libc::sigaction>();
        sa_quit.sa_sigaction = quit_callback as usize;

        if libc::sigemptyset(&mut sa_quit.sa_mask) == -1
            || libc::sigaction(libc::SIGTERM, &sa_quit, ptr::null_mut()) == -1
            || libc::sigaction(libc::SIGINT, &sa_quit, ptr::null_mut()) == -1
        {
            return Err(anyhow!("Failed to handle SIGTERM, SIGINT"));
        }

        Ok(())
    }
}
