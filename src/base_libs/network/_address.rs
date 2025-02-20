use core::fmt;
use std::net::SocketAddr;

// ---Address---
pub struct Address {
    pub ip: String,
    pub port: u16,
}
impl Address {
    pub fn new(ip: &str, port: u16) -> Self {
        return Address {
            ip: ip.to_string(),
            port: port,
        };
    }

    pub fn from_string(ip: &str) -> Option<Self> {
        if let Ok(socket_addr) = ip.parse::<SocketAddr>() {
            let ip = socket_addr.ip().to_string();
            let port = socket_addr.port();
            Some(Address { ip, port })
        } else {
            None
        }
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.ip, self.port)
    }
}
