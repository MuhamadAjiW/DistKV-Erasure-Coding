use std::time::Duration;

pub const STOP_INTERVAL: Duration = Duration::from_secs(1); // How long to wait before trying to stop the node
pub const RECONNECT_INTERVAL: Duration = Duration::from_secs(1); // How long to wait before trying to reconnect
pub const ACK_TIMEOUT: Duration = Duration::from_secs(10); // Max time to wait for acknowledgment
