use std::fs;
use std::io;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::ptr;

use anyhow::anyhow;
use anyhow::Context as _;
use clap::Parser;

#[derive(Parser)]
struct Command {
    path: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let command = Command::parse();
    let file = fs::File::options()
        .read(true)
        .write(true)
        .open(&command.path)
        .with_context(|| anyhow!("Failed to open file: {}", command.path.display()))?;
}
