use crate::{
    base_libs::_operation::{Operation, OperationType},
    classes::node::_node::Node,
};

use super::{_memory_store::MemoryStore, _persistent_store::PersistentStore};

// _NOTE: Storage Controller should only be used inside node that owns it
// It may cause memory problems otherwise
pub struct StorageController<'a> {
    persistent: PersistentStore,
    memory: MemoryStore,
    node: Option<&'a Node>, // Dependency injection 'a
}

impl<'a> StorageController<'a> {
    pub fn new(db_path: &str, memcached_url: &str, node: Option<&'a Node>) -> Self {
        StorageController {
            persistent: PersistentStore::new(db_path),
            memory: MemoryStore::new(memcached_url),
            node,
        }
    }

    pub fn assign_node(&mut self, node: &'a Node) {
        self.node = Some(node);
    }

    pub fn process_request(&mut self, request: &Operation) -> Option<String> {
        let mut response: Option<String> = None;

        match request.op_type {
            OperationType::GET => {
                response = self.memory.get(&request.kv.key);
                if response.is_none() {
                    // _TODO: Reconstruction logic
                    response = self.persistent.get(&request.kv.key);
                }
            }
            OperationType::SET => {
                self.memory.set(
                    &request.kv.key,
                    &String::from_utf8(request.kv.value.clone()).expect("Invalid UTF-8 in value"),
                );
                //
                self.persistent.set(&request.kv.key, &request.kv.value);
            }
            OperationType::DELETE => {
                self.persistent.remove(&request.kv.key);
                self.memory.remove(&request.kv.key);
            }
            _ => {}
        }

        response
    }
}
