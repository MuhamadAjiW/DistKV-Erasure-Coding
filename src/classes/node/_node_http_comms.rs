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

        let operation = Operation {
            op_type: OperationType::GET,
            kv: BinKV {
                key: body.key.clone(),
                value: vec![],
            },
        };

        let result = {
            let node = node_data.read().await;
            let result = node.process_request(&operation).await.unwrap_or_default();

            result
        };

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
        info!("[REQUEST] POST request received");

        let operation = Operation {
            op_type: OperationType::SET,
            kv: BinKV {
                key: body.key.clone(),
                value: body.value.clone().into_bytes(),
            },
        };

        let result = {
            let node = node_data.read().await;
            let result = node.process_request(&operation).await.unwrap_or_default();

            result
        };

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
        info!("[REQUEST] DELETE request received");

        let operation = Operation {
            op_type: OperationType::DELETE,
            kv: BinKV {
                key: body.key.clone(),
                value: vec![],
            },
        };

        let result = {
            let node = node_data.read().await;
            let result = node.process_request(&operation).await.unwrap_or_default();

            result
        };

        let response = BaseResponse {
            key: body.key.clone(),
            response: result,
        };

        HttpResponse::Ok().json(response)
    }
}
