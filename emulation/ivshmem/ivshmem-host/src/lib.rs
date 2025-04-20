pub mod cmsg;
mod event_notifier;
mod signal;

pub use event_notifier::EventNotifier;
pub use signal::handle_signals;
pub use signal::quit;

pub const IVSHMEM_PROTOCOL_VERSION: u16 = 0;

#[derive(Copy, Clone)]
pub struct Size(pub usize);

impl std::str::FromStr for Size {
    type Err = anyhow::Error;
    fn from_str(string: &str) -> anyhow::Result<Self> {
        let (value, unit) = if let Some(prefix) = string.strip_suffix('K') {
            (prefix, 1024)
        } else if let Some(prefix) = string.strip_suffix('M') {
            (prefix, 1024 * 1024)
        } else if let Some(prefix) = string.strip_suffix('G') {
            (prefix, 1024 * 1024 * 1024)
        } else {
            (string, 1)
        };

        Ok(Size(value.parse::<usize>()? * unit))
    }
}
