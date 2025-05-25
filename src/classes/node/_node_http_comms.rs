use std::sync::Arc;

use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, time::Instant};
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
    #[instrument(level = "trace", skip_all)]
    pub async fn http_healthcheck(node_data: web::Data<Arc<Mutex<Node>>>) -> impl Responder {
        info!("[REQUEST] Received healthcheck requesst");

        let node = node_data.lock().await;
        node.print_info().await;

        HttpResponse::Ok().body("HTTP server running just fine")
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn http_get(
        node_data: web::Data<Arc<Mutex<Node>>>,
        body: web::Json<GetBody>,
    ) -> impl Responder {
        info!("[REQUEST] GET request received");
        let mut node = node_data.lock().await;

        let operation = Operation {
            op_type: OperationType::GET,
            kv: BinKV {
                key: body.key.clone(),
                value: vec![],
            },
        };
        let request_id = node.request_id;

        let result = node
            .process_request(&operation, request_id)
            .await
            .unwrap_or_default();

        let response = BaseResponse {
            key: body.key.clone(),
            response: result,
        };

        let mut last_heartbeat_mut = node.last_heartbeat.write().await;
        *last_heartbeat_mut = Instant::now();

        HttpResponse::Ok().json(response)
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn http_put(
        node_data: web::Data<Arc<Mutex<Node>>>,
        body: web::Json<PostBody>,
    ) -> impl Responder {
        info!("[REQUEST] POST request received");
        let mut node = node_data.lock().await;
        node.request_id += 1;

        let operation = Operation {
            op_type: OperationType::SET,
            kv: BinKV {
                key: body.key.clone(),
                value: body.value.clone().into_bytes(),
            },
        };
        let request_id = node.request_id;

        let result = node
            .process_request(&operation, request_id)
            .await
            .unwrap_or_default();

        let response = BaseResponse {
            key: body.key.clone(),
            response: result,
        };

        let mut last_heartbeat_mut = node.last_heartbeat.write().await;
        *last_heartbeat_mut = Instant::now();

        HttpResponse::Ok().json(response)
    }

    #[instrument(level = "trace", skip_all)]
    pub async fn http_delete(
        node_data: web::Data<Arc<Mutex<Node>>>,
        body: web::Json<DeleteBody>,
    ) -> impl Responder {
        info!("[REQUEST] DELETE request received");
        let mut node = node_data.lock().await;
        node.request_id += 1;

        let operation = Operation {
            op_type: OperationType::DELETE,
            kv: BinKV {
                key: body.key.clone(),
                value: vec![],
            },
        };
        let request_id = node.request_id;

        info!("[REQUEST] Processing DELETE request");
        let result = node
            .process_request(&operation, request_id)
            .await
            .unwrap_or_default();

        let response = BaseResponse {
            key: body.key.clone(),
            response: result,
        };

        let mut last_heartbeat_mut = node.last_heartbeat.write().await;
        *last_heartbeat_mut = Instant::now();

        HttpResponse::Ok().json(response)
    }
}
