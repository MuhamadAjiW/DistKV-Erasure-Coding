use actix_web::{HttpResponse, Responder};

use super::_node::Node;

impl Node {
    pub async fn http_healthcheck() -> impl Responder {
        HttpResponse::Ok().body("HTTP server running just fine")
    }
    pub async fn http_get() -> impl Responder {
        HttpResponse::Ok().body("GET: returning current store state")
    }
    pub async fn http_post() -> impl Responder {
        HttpResponse::Ok().body("GET: returning current store state")
    }
}
