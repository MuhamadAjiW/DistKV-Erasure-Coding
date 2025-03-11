use std::{sync::Arc, time::Duration};

use tokio::{task::JoinSet, time::timeout};

use crate::{
    base_libs::{
        _operation::Operation,
        _paxos_types::{FollowerRegistrationReply, FollowerRegistrationRequest, PaxosMessage},
        network::_messages::{receive_message, send_message},
    },
    classes::node::_node::Node,
};

impl Node {
    // ---Active Commands---
    pub async fn leader_broadcast_membership(
        &self,
        follower_list: &Vec<String>,
        index: usize,
    ) -> usize {
        if follower_list.is_empty() {
            println!("No followers registered. Cannot proceed.");
            return 0;
        }

        let mut acks = 1;
        let mut tasks = JoinSet::new();

        for follower_addr in follower_list.iter() {
            if follower_addr == self.address.to_string().as_str() {
                continue;
            }

            let socket = Arc::clone(&self.socket);
            let index = index.clone();
            let follower_addr = follower_addr.clone();
            let follower_list = follower_list.clone();

            tasks.spawn(async move {
                // Send the request to the follower
                if let Err(_e) = send_message(
                    PaxosMessage::FollowerRegisterReply(FollowerRegistrationReply {
                        follower_list,
                        index,
                    }),
                    follower_addr.as_str(),
                )
                .await
                {
                    println!(
                        "Failed to broadcast request to follower at {}",
                        follower_addr
                    );
                    return None;
                }
                // println!("Broadcasted request to follower at {}", follower_addr);

                // Wait for acknowledgment with timeout (ex. 2 seconds)
                match timeout(Duration::from_secs(2), receive_message(&socket)).await {
                    Ok(Ok((ack, _))) => {
                        if let PaxosMessage::FollowerAck { .. } = ack {
                            // println!(
                            //     "Leader received acknowledgment from follower at {}",
                            //     follower_addr
                            // );
                            return Some(1);
                        }
                    }
                    Ok(Err(e)) => {
                        println!(
                            "Error receiving acknowledgment from follower at {}: {}",
                            follower_addr, e
                        );
                    }
                    Err(_) => {
                        println!(
                            "Timeout waiting for acknowledgment from follower at {}",
                            follower_addr
                        );
                    }
                }

                return Some(0);
            });
        }

        while let Some(response) = tasks.join_next().await {
            match response {
                Ok(Some(value)) => {
                    acks += value;
                }
                _ => {} // Handle errors or None responses if needed
            }
        }

        acks
    }

    // ---Handlers---
    pub async fn leader_handle_follower_register_request(
        &self,
        follower: &FollowerRegistrationRequest,
    ) {
        let mut followers_guard = self.cluster_list.lock().unwrap();
        let index = followers_guard.len();
        followers_guard.push(follower.follower_addr.clone());
        println!("Follower registered: {}", follower.follower_addr);

        self.leader_broadcast_membership(&followers_guard, index)
            .await;
    }

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
    pub async fn leader_handle_follower_register_reply(
        &self,
        _src_addr: &String,
        _follower: &FollowerRegistrationReply,
    ) {
    }
}
