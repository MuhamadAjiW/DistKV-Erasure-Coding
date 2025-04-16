use std::sync::Arc;

use actix_web::{web, HttpResponse, Responder};
use tokio::sync::Mutex;

use super::_node::Node;

impl Node {
    pub async fn http_healthcheck(node_data: web::Data<Arc<Mutex<Node>>>) -> impl Responder {
        let node = node_data.lock().await;
        node.print_info().await;
        HttpResponse::Ok().body("HTTP server running just fine")
    }

    pub async fn http_get(node_data: web::Data<Arc<Mutex<Node>>>) -> impl Responder {
        let node = node_data.lock().await;
        HttpResponse::Ok().body(format!("GET: store state"))
    }

    pub async fn http_post(node_data: web::Data<Arc<Mutex<Node>>>) -> impl Responder {
        let mut node = node_data.lock().await;
        HttpResponse::Ok().body("POST: updated store")
    }
}
