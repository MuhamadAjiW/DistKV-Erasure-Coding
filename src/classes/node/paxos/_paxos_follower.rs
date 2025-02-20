use std::{io, thread::sleep, time::Duration};

use crate::{
    base_libs::{
        _operation::{Operation, OperationType},
        _paxos_types::{FollowerRegistrationReply, FollowerRegistrationRequest, PaxosMessage},
        network::_messages::send_message,
    },
    classes::node::_node::Node,
};

impl Node {
    // ---Active Commands---
    pub async fn follower_send_register(&self) -> bool {
        let leader_addr = &self.leader_address.to_string() as &str;
        let follower_addr = &self.address.to_string() as &str;
        let load_balancer_addr = &self.load_balancer_address.to_string() as &str;

        // Register with the leader
        let registration_message =
            PaxosMessage::FollowerRegisterRequest(FollowerRegistrationRequest {
                follower_addr: follower_addr.to_string(),
            });
        send_message(&self.socket, registration_message, leader_addr)
            .await
            .unwrap();
        println!("Follower registered with leader: {}", leader_addr);

        // Retry logic for registering with load balancer
        let lb_registration_message = format!("register:{}", follower_addr);
        let mut registered = false;
        while !registered {
            match self
                .socket
                .send_to(lb_registration_message.as_bytes(), load_balancer_addr)
                .await
            {
                Ok(_) => {
                    println!(
                        "Follower registered with load balancer: {}",
                        load_balancer_addr
                    );
                    registered = true; // Registration successful
                }
                Err(e) => {
                    println!(
                        "Failed to register with load balancer, retrying in 2 seconds: {}",
                        e
                    );
                    sleep(Duration::from_secs(2)); // Retry after 2 seconds
                }
            }
        }

        return registered;
    }

    // ---Handlers---
    pub async fn follower_handle_leader_request(&self, src_addr: &String, request_id: u64) {
        let leader_addr = &self.leader_address.to_string() as &str;
        if src_addr != leader_addr {
            println!("Follower received request message from not a leader");
            return;
        }

        println!("Follower received request message from leader");
        let ack = PaxosMessage::FollowerAck { request_id };
        send_message(&self.socket, ack, &leader_addr).await.unwrap();
        println!("Follower acknowledged request ID: {}", request_id);
    }
    pub async fn follower_handle_leader_accepted(
        &mut self,
        src_addr: &String,
        request_id: u64,
        operation: &Operation,
    ) -> Result<(), io::Error> {
        let leader_addr = &self.leader_address.to_string() as &str;
        if src_addr != leader_addr {
            println!("Follower received request message from not a leader");
            return Ok(());
        }

        self.request_id = request_id;
        self.store
            .persist_request_ec(
                &("".to_string()
                    + &operation.kv.key
                    + ".."
                    + &self.address.ip.to_string()
                    + ".."
                    + &self.address.port.to_string()
                    + ".log"),
                operation,
            )
            .await?;

        if !self.store.get(&operation.kv.key).is_empty() {
            self.store.remove(&operation.kv.key);
        }

        println!(
            "Follower received accept message from leader:\nKey: {}, Shard: {:?}",
            operation.kv.key, operation.kv.value
        );
        let ack = PaxosMessage::FollowerAck { request_id };
        send_message(&self.socket, ack, &leader_addr).await.unwrap();
        println!("Follower acknowledged request ID: {}", request_id);

        Ok(())
    }
    pub async fn follower_handle_client_request(
        &mut self,
        src_addr: &String,
        request_id: u64,
        payload: &Vec<u8>,
    ) -> Result<(), io::Error> {
        let leader_addr = &self.leader_address.to_string() as &str;
        let mut result: String;
        let message: &str;

        println!("Follower received client request.");
        let req = Operation::parse(payload);
        if matches!(req, None) {
            println!("Request was invalid, dropping request");
            return Ok(());
        }
        let operation = req.unwrap();

        match operation.op_type {
            OperationType::SET | OperationType::DELETE => {
                println!("Forwarding to leader.");
                send_message(
                    &self.socket,
                    PaxosMessage::ClientRequest {
                        request_id,
                        payload: payload.clone(),
                    },
                    leader_addr,
                )
                .await
                .unwrap();

                return Ok(());
            }
            _ => {
                message = "Request is handled by follower";
                result = self.store.process_request(&operation);
                if result.is_empty() {
                    println!("Fetching data from cluster");
                    result = self
                        .get_from_cluster(
                            &("".to_string()
                                + &operation.kv.key
                                + ".."
                                + &self.address.ip.to_string()
                                + ".."
                                + &self.address.port.to_string()
                                + ".log"),
                            &operation.kv.key,
                        )
                        .await
                        .expect("Failed to get data from cluster");
                    println!("Fetching data from done");
                }

                let response = format!(
                    "Request ID: {}\nMessage: {}\nReply: {}.",
                    self.request_id, message, result
                );
                self.socket
                    .send_to(response.as_bytes(), src_addr)
                    .await
                    .unwrap();

                Ok(())
            }
        }
    }

    pub async fn follower_handle_follower_register_reply(
        &mut self,
        src_addr: &String,
        follower: &FollowerRegistrationReply,
    ) {
        let leader_addr = &self.leader_address.to_string() as &str;
        println!("Follower received leader data {}", src_addr);

        let mut followers_guard = self.cluster_list.lock().unwrap();
        *followers_guard = follower.follower_list.clone();

        if self.cluster_index == std::usize::MAX {
            self.cluster_index = follower.index;
        }

        let ack = PaxosMessage::FollowerAck {
            request_id: self.cluster_index as u64,
        };
        send_message(&self.socket, ack, &leader_addr).await.unwrap();
        println!("Acknowledged follower with given index: {}", follower.index);
    }

    // _TODO: handle false message
    pub async fn follower_handle_follower_ack(&self, _src_addr: &String, _request_id: u64) {}
    pub async fn follower_handle_follower_register_request(
        &self,
        _src_addr: &String,
        _follower: &FollowerRegistrationRequest,
    ) {
    }
}
