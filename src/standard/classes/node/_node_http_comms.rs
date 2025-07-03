use crate::standard::classes::_entry::KeyValue;
use crate::standard::classes::node::_node::OmniPaxosRequest;
use actix_web::{web, HttpResponse, Responder};
use omnipaxos::erasure::log_entry::OperationType;
use omnipaxos::util::LogEntry;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, instrument};

use super::_node::Node;

#[derive(Deserialize)]
pub struct GetBody {
    pub key: String,
}

#[derive(Deserialize, Serialize)]
pub struct PostBody {
    pub key: String,
    pub value: String,
}

#[derive(Deserialize, Serialize)]
pub struct DeleteBody {
    pub key: String,
}

#[derive(Serialize)]
struct BaseResponse {
    key: String,
    response: String,
}

#[derive(Serialize)]
struct ClusterStateResponse {
    node_id: u64,
    leader_id: Option<u64>,
    commit_index: u64,
    peers: Vec<String>,
    status: String,
}

#[derive(Deserialize, Serialize)]
pub struct Operation {
    pub op_type: OperationType,
    pub key: String,
    pub value: Option<String>,
}

#[derive(Deserialize)]
pub struct BulkOperationBody {
    pub operations: Vec<Operation>,
}

#[derive(Serialize)]
struct BulkOperationResponse {
    results: Vec<BaseResponse>,
}

#[derive(Serialize)]
struct StatusResponse {
    node_id: u64,
    http_address: String,
    tcp_address: String,
    uptime_seconds: u64,
    decided_entries: u64,
    peer_count: usize,
    is_leader: bool,
    current_leader: Option<u64>,
}

#[derive(Serialize)]
struct ApiDocumentation {
    endpoints: Vec<EndpointDoc>,
}

#[derive(Serialize)]
struct EndpointDoc {
    path: String,
    method: String,
    description: String,
    request_body: Option<String>,
    response_example: String,
}

impl Node {
    // Utility function to send a request to OmniPaxos and get the response
    #[instrument(level = "debug", skip_all)]
    pub async fn send_omnipaxos_request(&self, entry: KeyValue) -> String {
        let (tx, rx) = tokio::sync::oneshot::channel();

        match self
            .omnipaxos_sender
            .send(OmniPaxosRequest::Client {
                entry,
                response: tx,
            })
            .await
        {
            Ok(_) => match rx.await {
                Ok(response) => response,
                Err(e) => {
                    let error_msg = format!("Failed to receive response from OmniPaxos: {}", e);
                    error!("[OMNIPAXOS] {}", error_msg);
                    error_msg
                }
            },
            Err(e) => {
                let error_msg = format!("Failed to send request to OmniPaxos: {}", e);
                error!("[OMNIPAXOS] {}", error_msg);
                error_msg
            }
        }
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn http_healthcheck(node_data: web::Data<Arc<RwLock<Node>>>) -> impl Responder {
        info!("[REQUEST] Received healthcheck requesst");

        let node = node_data.read().await;
        node.print_info().await;

        HttpResponse::Ok().body("HTTP server running just fine")
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn http_get(
        node_data: web::Data<Arc<RwLock<Node>>>,
        body: web::Json<GetBody>,
    ) -> impl Responder {
        info!("[REQUEST] GET request received for key: {}", body.key);

        // Validate input data
        if body.key.trim().is_empty() {
            return HttpResponse::BadRequest().json(BaseResponse {
                key: "error".to_string(),
                response: "Key cannot be empty".to_string(),
            });
        }

        let entry = KeyValue {
            key: body.key.clone(),
            value: vec![],
            op: OperationType::GET,
            version: 0, // Version not needed for GET operations
        };

        let node = node_data.read().await;
        let result = node.send_omnipaxos_request(entry).await;

        let response = BaseResponse {
            key: body.key.clone(),
            response: result,
        };

        HttpResponse::Ok().json(response)
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn http_put(
        node_data: web::Data<Arc<RwLock<Node>>>,
        body: web::Json<PostBody>,
    ) -> impl Responder {
        info!("[REQUEST] PUT request received for key: {}", body.key);

        // Validate input data
        if body.key.trim().is_empty() {
            return HttpResponse::BadRequest().json(BaseResponse {
                key: "error".to_string(),
                response: "Key cannot be empty".to_string(),
            });
        }

        let entry = KeyValue {
            key: body.key.clone(),
            value: body.value.clone().into_bytes(),
            op: OperationType::SET,
            version: 0, // Version will be determined by the leader before consensus
        };

        let node = node_data.read().await;
        let result = node.send_omnipaxos_request(entry).await;

        let response = BaseResponse {
            key: body.key.clone(),
            response: result,
        };

        HttpResponse::Ok().json(response)
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn http_delete(
        node_data: web::Data<Arc<RwLock<Node>>>,
        body: web::Json<DeleteBody>,
    ) -> impl Responder {
        info!("[REQUEST] DELETE request received for key: {}", body.key);

        // Validate input data
        if body.key.trim().is_empty() {
            return HttpResponse::BadRequest().json(BaseResponse {
                key: "error".to_string(),
                response: "Key cannot be empty".to_string(),
            });
        }

        let entry = KeyValue {
            key: body.key.clone(),
            value: vec![],
            op: OperationType::DELETE,
            version: 0, // Version not needed for DELETE operations
        };

        let node = node_data.read().await;
        let result = node.send_omnipaxos_request(entry).await;

        let response = BaseResponse {
            key: body.key.clone(),
            response: result,
        };

        HttpResponse::Ok().json(response)
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn http_bulk_operation(
        node_data: web::Data<Arc<RwLock<Node>>>,
        body: web::Json<BulkOperationBody>,
    ) -> impl Responder {
        info!(
            "[REQUEST] Bulk operation received with {} operations",
            body.operations.len()
        );

        if body.operations.is_empty() {
            return HttpResponse::BadRequest().json(BaseResponse {
                key: "error".to_string(),
                response: "No operations provided".to_string(),
            });
        }

        // Validate max number of operations to prevent abuse
        if body.operations.len() > 100 {
            return HttpResponse::BadRequest().json(BaseResponse {
                key: "error".to_string(),
                response: "Too many operations. Maximum allowed is 100.".to_string(),
            });
        }

        let node = node_data.read().await;
        let mut results = Vec::with_capacity(body.operations.len());

        for op in body.operations.iter() {
            if op.key.trim().is_empty() {
                results.push(BaseResponse {
                    key: "error".to_string(),
                    response: "Key cannot be empty".to_string(),
                });
                continue;
            }

            let op_type = match op.op_type {
                OperationType::GET => OperationType::GET,
                OperationType::SET => OperationType::SET,
                OperationType::DELETE => OperationType::DELETE,
                _ => {
                    results.push(BaseResponse {
                        key: op.key.clone(),
                        response: format!(
                            "Unsupported operation type: {}. Must be GET, SET, or DELETE.",
                            op.op_type
                        ),
                    });
                    continue;
                }
            };

            let entry = KeyValue {
                key: op.key.clone(),
                value: match op_type {
                    OperationType::SET => op.value.clone().unwrap().into_bytes(),
                    _ => vec![],
                },
                op: op_type,
                version: 0, // Version will be determined by the leader before consensus
            };

            let result = node.send_omnipaxos_request(entry).await;
            results.push(BaseResponse {
                key: op.key.clone(),
                response: result,
            });
        }

        HttpResponse::Ok().json(BulkOperationResponse { results })
    }

    pub async fn http_cluster_state(node_data: web::Data<Arc<RwLock<Node>>>) -> impl Responder {
        info!("[REQUEST] Cluster state request received");

        let node = node_data.read().await;
        let node_id = (node.cluster_index + 1) as u64;
        let peers = node.cluster_list.read().await.clone();

        let mut leader_id = None;
        let commit_index;
        let status;

        {
            let omni = node.omnipaxos.lock().await;

            // Get current leader
            if let Some((leader, term)) = omni.get_current_leader() {
                leader_id = Some(leader);
                status = format!("Term: {}", term);
            } else {
                status = "No leader elected".to_string();
            }

            // Get commit index directly
            commit_index = omni.get_decided_idx() as u64;
        }

        let response = ClusterStateResponse {
            node_id,
            leader_id,
            commit_index,
            peers,
            status,
        };

        HttpResponse::Ok().json(response)
    }

    pub async fn http_status(node_data: web::Data<Arc<RwLock<Node>>>) -> impl Responder {
        info!("[REQUEST] Status request received");

        let node = node_data.read().await;
        let node_id = (node.cluster_index + 1) as u64;
        let http_address = node.http_address.to_string();
        let tcp_address = node.address.to_string();
        let peer_count = node.cluster_list.read().await.len();

        // Calculate uptime
        let uptime_seconds = node.start_time.elapsed().as_secs();

        let decided_entries: u64;
        let mut is_leader: bool = false;
        let mut current_leader: Option<u64> = None;
        {
            let omni = node.omnipaxos.lock().await;
            decided_entries = omni.get_decided_idx() as u64;
            // Get leadership info
            if let Some((leader, _is_accepted)) = omni.get_current_leader() {
                current_leader = Some(leader);
                is_leader = leader == node_id;
            }
        }

        let response = StatusResponse {
            node_id,
            http_address,
            tcp_address,
            uptime_seconds,
            decided_entries,
            peer_count,
            is_leader,
            current_leader,
        };

        HttpResponse::Ok().json(response)
    }

    pub async fn http_api_docs() -> impl Responder {
        info!("[REQUEST] API documentation request received");

        let endpoints = vec![
            EndpointDoc {
                path: "/health".to_string(),
                method: "GET".to_string(),
                description: "Check if the HTTP server is running".to_string(),
                request_body: None,
                response_example: "HTTP server running just fine".to_string(),
            },
            EndpointDoc {
                path: "/get".to_string(),
                method: "POST".to_string(),
                description: "Retrieve a value by key".to_string(),
                request_body: Some(r#"{"key": "my-key"}"#.to_string()),
                response_example: r#"{"key": "my-key", "response": "my-value"}"#.to_string(),
            },
            EndpointDoc {
                path: "/put".to_string(),
                method: "POST".to_string(),
                description: "Store a key-value pair".to_string(),
                request_body: Some(r#"{"key": "my-key", "value": "my-value"}"#.to_string()),
                response_example: r#"{"key": "my-key", "response": "Value set successfully"}"#.to_string(),
            },
            EndpointDoc {
                path: "/delete".to_string(),
                method: "POST".to_string(),
                description: "Delete a key-value pair".to_string(),
                request_body: Some(r#"{"key": "my-key"}"#.to_string()),
                response_example: r#"{"key": "my-key", "response": "Value deleted successfully"}"#.to_string(),
            },
            EndpointDoc {
                path: "/bulk".to_string(),
                method: "POST".to_string(),
                description: "Perform multiple operations in a single request (maximum 100 operations)".to_string(),
                request_body: Some(r#"{"operations": [{"op_type": "SET", "key": "key1", "value": "value1"}, {"op_type": "GET", "key": "key2"}]}"#.to_string()),
                response_example: r#"{"results": [{"key": "key1", "response": "Value set successfully"}, {"key": "key2", "response": "value2"}]}"#.to_string(),
            },
            EndpointDoc {
                path: "/status".to_string(),
                method: "GET".to_string(),
                description: "Get the status of this node".to_string(),
                request_body: None,
                response_example: r#"{"node_id": 1, "http_address": "127.0.0.1:8080", "tcp_address": "127.0.0.1:7000", "uptime_seconds": 3600, "decided_entries": 42, "peer_count": 3, "is_leader": false, "current_leader": 2}"#.to_string(),
            },
            EndpointDoc {
                path: "/cluster".to_string(),
                method: "GET".to_string(),
                description: "Get the status of the cluster".to_string(),
                request_body: None,
                response_example: r#"{"node_id": 1, "leader_id": 2, "commit_index": 42, "peers": ["127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"], "status": "Term: 3"}"#.to_string(),
            },
            EndpointDoc {
                path: "/docs".to_string(),
                method: "GET".to_string(),
                description: "Get the API documentation".to_string(),
                request_body: None,
                response_example: "This documentation".to_string(),
            },
        ];

        let api_docs = ApiDocumentation { endpoints };

        HttpResponse::Ok().json(api_docs)
    }

    pub async fn http_logs(node_data: web::Data<Arc<RwLock<Node>>>) -> impl Responder {
        info!("[REQUEST] Logs request received");
        let node = node_data.read().await;
        let mut logs = Vec::new();
        {
            let omni = node.omnipaxos.lock().await;
            if let Some(entries) = omni.read_decided_suffix(0) {
                for (idx, entry) in entries.iter().enumerate() {
                    match entry {
                        LogEntry::Decided(e) => {
                            logs.push(serde_json::json!({
                                "index": idx,
                                "type": "Decided",
                                "entry": e
                            }));
                        }
                        LogEntry::Undecided(e) => {
                            logs.push(serde_json::json!({
                                "index": idx,
                                "type": "Undecided",
                                "entry": e
                            }));
                        }
                        LogEntry::Trimmed(idx_trimmed) => {
                            logs.push(serde_json::json!({
                                "index": idx,
                                "type": "Trimmed",
                                "trimmed_idx": idx_trimmed
                            }));
                        }
                        LogEntry::Snapshotted(snap) => {
                            logs.push(serde_json::json!({
                                "index": idx,
                                "type": "Snapshotted",
                                "trimmed_idx": snap.trimmed_idx,
                                "snapshot": snap.snapshot
                            }));
                        }
                        LogEntry::StopSign(ss, decided) => {
                            logs.push(serde_json::json!({
                                "index": idx,
                                "type": "StopSign",
                                "stopsign": ss,
                                "decided": decided
                            }));
                        }
                    }
                }
            }
        }
        HttpResponse::Ok().json(logs)
    }

    pub fn register_services(cfg: &mut web::ServiceConfig) {
        cfg.service(web::resource("/health").route(web::get().to(Self::http_healthcheck)))
            .service(web::resource("/get").route(web::post().to(Self::http_get)))
            .service(web::resource("/put").route(web::post().to(Self::http_put)))
            .service(web::resource("/delete").route(web::post().to(Self::http_delete)))
            .service(web::resource("/bulk").route(web::post().to(Self::http_bulk_operation)))
            .service(web::resource("/status").route(web::get().to(Self::http_status)))
            .service(web::resource("/cluster").route(web::get().to(Self::http_cluster_state)))
            .service(web::resource("/docs").route(web::get().to(Self::http_api_docs)))
            .service(web::resource("/logs").route(web::get().to(Self::http_logs)));
    }
}
