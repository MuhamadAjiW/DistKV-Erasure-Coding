use core::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OperationType {
    BAD,
    PING,
    GET,
    SET,
    DELETE,
}

// ---RequestType---
impl fmt::Display for OperationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let str;
        match self {
            OperationType::BAD => str = "BAD",
            OperationType::PING => str = "PING",
            OperationType::GET => str = "GET",
            OperationType::SET => str = "SET",
            OperationType::DELETE => str = "DEL",
        }

        write!(f, "{}", str)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub op_type: OperationType,
    pub kv: BinKV,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinKV {
    pub key: String,
    pub value: Vec<u8>,
}

// ---Operation---
impl Operation {
    pub fn parse(payload: &Vec<u8>) -> Option<Self> {
        let payload_str = String::from_utf8(payload.clone()).ok()?;
        let command = payload_str.trim();
        let parts: Vec<&str> = command.split_whitespace().collect();

        if parts.is_empty() {
            return None;
        }

        match parts.as_slice() {
            ["PING"] => {
                return Some(Operation {
                    op_type: OperationType::PING,
                    kv: BinKV {
                        key: "".to_string(),
                        value: vec![],
                    },
                })
            }
            ["GET", key] => {
                return Some(Operation {
                    op_type: OperationType::GET,
                    kv: BinKV {
                        key: key.to_string(),
                        value: vec![],
                    },
                })
            }
            ["SET", key, val] => {
                return Some(Operation {
                    op_type: OperationType::SET,
                    kv: BinKV {
                        key: key.to_string(),
                        value: val.as_bytes().to_vec(),
                    },
                })
            }
            ["DEL", key] => {
                return Some(Operation {
                    op_type: OperationType::DELETE,
                    kv: BinKV {
                        key: key.to_string(),
                        value: vec![0; 1],
                    },
                })
            }
            _ => {
                return Some(Operation {
                    op_type: OperationType::BAD,
                    kv: BinKV {
                        key: "".to_string(),
                        value: vec![],
                    },
                })
            }
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {} {:?}", self.op_type, self.kv.key, self.kv.value)
    }
}
