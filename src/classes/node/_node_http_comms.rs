use std::sync::Arc;

use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{info, instrument};

use crate::base_libs::_operation::{BinKV, Operation, OperationType};

use super::_node::Node;

#[derive(Deserialize)]
pub struct GetBody {
    key: String,
}

#[derive(Deserialize, Serialize)]
pub struct PostBody {
    key: String,
    value: String,
}

#[derive(Deserialize, Serialize)]
pub struct DeleteBody {
    key: String,
}

#[derive(Serialize)]
struct BaseResponse {
    key: String,
    response: String,
}

impl Node {
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
        info!("[REQUEST] GET request received");
        let key = body.key.clone();
        let request_id = {
            let node = node_data.read().await;
            node.request_id
        };
        // Only pass the data needed, drop the lock before processing
        let result = Node::process_request_arc(
            node_data.get_ref().clone(),
            Operation {
                op_type: OperationType::GET,
                kv: BinKV {
                    key: key.clone(),
                    value: vec![],
                },
            },
            request_id,
        )
        .await;
        let response = BaseResponse {
            key,
            response: result,
        };
        HttpResponse::Ok().json(response)
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn http_put(
        node_data: web::Data<Arc<RwLock<Node>>>,
        body: web::Json<PostBody>,
    ) -> impl Responder {
        info!("[REQUEST] POST request received");
        let key = body.key.clone();
        let value = body.value.clone();
        let request_id = {
            let node = node_data.read().await;
            node.request_id
        };
        let result = Node::process_request_arc(
            node_data.get_ref().clone(),
            Operation {
                op_type: OperationType::SET,
                kv: BinKV {
                    key: key.clone(),
                    value: value.into_bytes(),
                },
            },
            request_id,
        )
        .await;
        let response = BaseResponse {
            key,
            response: result,
        };
        HttpResponse::Ok().json(response)
    }

    #[instrument(level = "debug", skip_all)]
    pub async fn http_delete(
        node_data: web::Data<Arc<RwLock<Node>>>,
        body: web::Json<DeleteBody>,
    ) -> impl Responder {
        info!("[REQUEST] DELETE request received");
        let key = body.key.clone();
        let request_id = {
            let node = node_data.read().await;
            node.request_id
        };
        let result = Node::process_request_arc(
            node_data.get_ref().clone(),
            Operation {
                op_type: OperationType::DELETE,
                kv: BinKV {
                    key: key.clone(),
                    value: vec![],
                },
            },
            request_id,
        )
        .await;
        let response = BaseResponse {
            key,
            response: result,
        };
        HttpResponse::Ok().json(response)
    }

    // Call process_request on the node, but do not hold the lock across await
    pub async fn process_request_arc(
        node_data: Arc<RwLock<Node>>,
        operation: Operation,
        request_id: u64,
    ) -> String {
        let node = node_data.read().await;
        node.process_request(&operation, request_id)
            .await
            .unwrap_or_default()
    }
}
