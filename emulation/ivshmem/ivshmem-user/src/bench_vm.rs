use std::fs;
use std::fs::File;
use std::io;
use std::mem;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::ptr;
use std::time::Instant;

use anyhow::anyhow;
use anyhow::Context as _;
use clap::Parser;

#[derive(Parser)]
struct Command {
    #[arg(short, long)]
    device: PathBuf,

    #[arg(short, long)]
    ivposition: u16,

    #[arg(short, long, default_value = "0")]
    vector: u16,

    #[command(subcommand)]
    mode: Mode,
}

#[derive(Parser)]
enum Mode {
    Echo,
    Bench {
        #[arg(short, long)]
        warmup: usize,

        #[arg(short, long)]
        iterations: usize,
    },
}

fn main() -> anyhow::Result<()> {
    let command = Command::parse();
    let file = fs::File::options()
        .read(true)
        .write(true)
        .open(&command.device)
        .with_context(|| anyhow!("Failed to open {}", command.device.display()))?;

    match command.mode {
        Mode::Echo => loop {
            read(&file)?;
            write(&file, command.ivposition, command.vector)?;
        },
        Mode::Bench { warmup, iterations } => {
            for i in 0..warmup + iterations {
                let start = Instant::now();
                write(&file, command.ivposition, command.vector)?;
                read(&file)?;
                let end = Instant::now();
                if i >= warmup {
                    println!("{}", (end - start).as_nanos());
                }
            }
        }
    }

    Ok(())
}

fn read(file: &File) -> anyhow::Result<()> {
    match unsafe { libc::read(file.as_raw_fd(), ptr::null_mut(), 0) } {
        0 => Ok(()),
        _ => Err(anyhow::Error::from(io::Error::last_os_error())),
    }
}

fn write(file: &File, ivposition: u16, vector: u16) -> anyhow::Result<()> {
    let ivposition = ivposition.to_ne_bytes();
    match unsafe {
        libc::pwrite(
            file.as_raw_fd(),
            ivposition.as_ptr().cast(),
            mem::size_of::<u16>(),
            vector as i64,
        )
    } {
        2 => Ok(()),
        _ => Err(anyhow::Error::from(io::Error::last_os_error())),
    }
}
