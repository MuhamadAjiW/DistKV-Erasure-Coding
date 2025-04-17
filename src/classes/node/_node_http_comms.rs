use std::sync::Arc;

use actix_web::{web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

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
        let node = node_data.lock().await;
        HttpResponse::Ok().body(format!("GET: key = {}", query.key))
    }

    pub async fn http_post(
        node_data: web::Data<Arc<Mutex<Node>>>,
        body: web::Json<PostBody>,
    ) -> impl Responder {
        let mut node = node_data.lock().await;
        let op = Operation {
            op_type: OperationType::SET,
            kv: BinKV {
                key: body.key.clone(),
                value: body.value.clone().into_bytes(),
            },
        };
        HttpResponse::Ok().body(format!("POST: key = {}, value = {}", body.key, body.value))
    }

    pub async fn http_delete(
        node_data: web::Data<Arc<Mutex<Node>>>,
        body: web::Json<DeleteBody>,
    ) -> impl Responder {
        let mut node = node_data.lock().await;
        let op = Operation {
            op_type: OperationType::DELETE,
            kv: BinKV {
                key: body.key.clone(),
                value: vec![],
            },
        };
        HttpResponse::Ok().body(format!("Delete: key = {}", body.key))
    }
}
