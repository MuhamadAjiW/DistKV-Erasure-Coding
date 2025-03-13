use crate::{base_libs::_operation::Operation, classes::node::_node::Node};

impl Node {
    // _TODO: handle false leader
    pub async fn leader_handle_leader_request(&self, _src_addr: &String, _request_id: u64) {}
    pub async fn leader_handle_leader_accepted(
        &self,
        _src_addr: &String,
        _request_id: u64,
        _operation: &Operation,
    ) {
    }

    // _TODO: handle false message
    pub async fn leader_handle_follower_ack(&self, _src_addr: &String, _request_id: u64) {}
}
