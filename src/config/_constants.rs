use std::time::Duration;

pub const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);

pub const ELECTION_TICK_TIMEOUT: u64 = 100;
pub const TICK_PERIOD: Duration = Duration::from_millis(50);
pub const RECONSTRUCTION_WAIT_TIMEOUT: u64 = 1000;
pub const OUTGOING_MESSAGE_PERIOD: Duration = Duration::from_millis(5);
pub const BUFFER_SIZE: usize = 1024 * 1024; // 1 MB
