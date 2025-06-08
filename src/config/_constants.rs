use std::time::Duration;

pub const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);

pub const ELECTION_TICK_TIMEOUT: u64 = 100;
pub const TICK_PERIOD: Duration = Duration::from_millis(50);
pub const OUTGOING_MESSAGE_PERIOD: Duration = Duration::from_millis(5);
