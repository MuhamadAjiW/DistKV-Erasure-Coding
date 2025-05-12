use memcache::Client;
use tracing::instrument;

use crate::base_libs::_operation::{Operation, OperationType};

pub struct KvMemory {
    memcached: Client,
}

impl KvMemory {
    pub fn new(memcached_url: &str) -> Self {
        return KvMemory {
            memcached: memcache::connect(memcached_url).unwrap(),
        };
    }

    #[instrument(skip_all)]
    pub fn set(&self, key: &str, value: &str) {
        self.memcached
            .set(key, value, 0)
            .expect("Failed to set memcached");
    }

    #[instrument(skip_all)]
    pub fn get(&self, key: &str) -> Option<String> {
        if let Ok(Some(value)) = self.memcached.get(key) {
            return Some(value);
        } else {
            return None;
        }
    }

    #[instrument(skip_all)]
    pub fn remove(&self, key: &str) -> () {
        self.memcached
            .delete(key)
            .expect("Failed to delete from memcached");
    }

    #[instrument(skip_all)]
    pub fn process_request(&self, request: &Operation) -> Option<String> {
        let mut response: Option<String> = None;

        match request.op_type {
            OperationType::GET => {
                response = self.get(&request.kv.key);
            }
            OperationType::SET => {
                self.set(
                    &request.kv.key,
                    &String::from_utf8(request.kv.value.clone()).expect("Invalid UTF-8 in value"),
                );
            }
            OperationType::DELETE => {
                self.remove(&request.kv.key);
            }
            _ => {}
        }

        response
    }
}
