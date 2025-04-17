use std::sync::Arc;

use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, time::Instant};

use crate::base_libs::_operation::{BinKV, Operation, OperationType};

use super::_node::Node;

#[derive(Deserialize)]
pub struct GetQuery {
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
    pub async fn http_healthcheck(node_data: web::Data<Arc<Mutex<Node>>>) -> impl Responder {
        let node = node_data.lock().await;
        node.print_info().await;
        HttpResponse::Ok().body("HTTP server running just fine")
    }

    pub async fn http_get(
        node_data: web::Data<Arc<Mutex<Node>>>,
        query: web::Query<GetQuery>,
    ) -> impl Responder {
        println!("[REQUEST] get request received for key: {}", query.key);
        let node = node_data.lock().await;
        let operation = Operation {
            op_type: OperationType::GET,
            kv: BinKV {
                key: query.key.clone(),
                value: vec![],
            },
        };
        let result = node
            .store
            .process_request(&operation, &node)
            .await
            .unwrap_or_default();

        let response = BaseResponse {
            key: query.key.clone(),
            response: result,
        };

        let mut last_heartbeat_mut = node.last_heartbeat.write().await;
        *last_heartbeat_mut = Instant::now();

        HttpResponse::Ok().json(response)
    }

    pub async fn http_post(
        node_data: web::Data<Arc<Mutex<Node>>>,
        body: web::Json<PostBody>,
    ) -> impl Responder {
        println!(
            "[REQUEST] post request received for key: {}, value {}",
            body.key, body.value
        );
        let mut node = node_data.lock().await;
        node.request_id += 1;

        let operation = Operation {
            op_type: OperationType::SET,
            kv: BinKV {
                key: body.key.clone(),
                value: body.value.clone().into_bytes(),
            },
        };
        let result = node
            .store
            .process_request(&operation, &node)
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

    pub async fn http_delete(
        node_data: web::Data<Arc<Mutex<Node>>>,
        body: web::Json<DeleteBody>,
    ) -> impl Responder {
        println!("[REQUEST] delete request received for key: {}", body.key);
        let mut node = node_data.lock().await;
        node.request_id += 1;

        let operation = Operation {
            op_type: OperationType::DELETE,
            kv: BinKV {
                key: body.key.clone(),
                value: vec![],
            },
        };
        let result = node
            .store
            .process_request(&operation, &node)
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
