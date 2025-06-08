use crate::base_libs::_ec::ECKeyValue;
use crate::base_libs::_types::OmniPaxosECMessage;
use crate::classes::node::_node::OmniPaxosRequest;
use actix_web::{web, HttpResponse, Responder};
use omnipaxos::erasure::ec_service::EntryFragment;
use omnipaxos::erasure::log_entry::OperationType;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, instrument};

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
        let ec_entry = ECKeyValue {
            key: body.key.clone(),
            fragment: EntryFragment::default(),
            op: OperationType::GET,
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        let node = node_data.read().await;
        let _ = node
            .omnipaxos_sender
            .send(OmniPaxosRequest::Client {
                entry: ec_entry,
                response: tx,
            })
            .await;
        let result = rx.await.unwrap_or_else(|_| "No response".to_string());
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
        let ec_entry = ECKeyValue {
            key: body.key.clone(),
            fragment: EntryFragment::for_request(body.value.clone().into_bytes()),
            op: OperationType::SET,
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        let node = node_data.read().await;
        let _ = node
            .omnipaxos_sender
            .send(OmniPaxosRequest::Client {
                entry: ec_entry,
                response: tx,
            })
            .await;
        let result = rx.await.unwrap_or_else(|_| "No response".to_string());
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
        let ec_entry = ECKeyValue {
            key: body.key.clone(),
            fragment: EntryFragment::default(),
            op: OperationType::DELETE,
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        let node = node_data.read().await;
        let _ = node
            .omnipaxos_sender
            .send(OmniPaxosRequest::Client {
                entry: ec_entry,
                response: tx,
            })
            .await;
        let result = rx.await.unwrap_or_else(|_| "No response".to_string());
        let response = BaseResponse {
            key: body.key.clone(),
            response: result,
        };
        HttpResponse::Ok().json(response)
    }
}
