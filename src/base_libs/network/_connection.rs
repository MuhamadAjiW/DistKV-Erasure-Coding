use dashmap::DashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::base_libs::network::_transaction::TransactionManager;

pub struct ConnectionManager {
    pool: Arc<DashMap<String, Arc<Mutex<TcpStream>>>>,
    pub transaction_manager: Arc<TransactionManager>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            pool: Arc::new(DashMap::new()),
            transaction_manager: Arc::new(TransactionManager::new()),
        }
    }
    pub async fn get_or_connect(&self, addr: &str) -> std::io::Result<Arc<Mutex<TcpStream>>> {
        if let Some(stream) = self.pool.get(addr) {
            return Ok(stream.clone());
        }
        let stream = TcpStream::connect(addr).await?;
        let arc_stream = Arc::new(Mutex::new(stream));
        self.pool.insert(addr.to_string(), arc_stream.clone());
        Ok(arc_stream)
    }

    pub async fn remove(&self, addr: &str) {
        if let Some(entry) = self.pool.remove(addr) {
            if let Ok(mut stream) = entry.1.try_lock() {
                let _ = stream.shutdown().await;
            }
        }
    }

    pub async fn insert_incoming(&self, addr: &str, stream: Arc<Mutex<TcpStream>>) {
        self.pool.insert(addr.to_string(), stream);
    }

    pub async fn get_stream(&self, addr: &str) -> Option<Arc<Mutex<TcpStream>>> {
        self.pool.get(addr).map(|stream| stream.clone())
    }

    pub async fn close_all(&self) {
        for item in self.pool.iter() {
            let stream_arc = item.value();
            if let Ok(mut stream) = stream_arc.try_lock() {
                let _ = stream.shutdown().await;
            }
        }
        self.pool.clear();
    }
}
