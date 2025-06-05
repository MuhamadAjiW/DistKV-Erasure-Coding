use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};

pub struct ConnectionManager {
    pool: Arc<RwLock<HashMap<String, Arc<Mutex<TcpStream>>>>>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            pool: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    pub async fn get_or_connect(&self, addr: &str) -> std::io::Result<Arc<Mutex<TcpStream>>> {
        let maybe_stream = {
            let pool = self.pool.read().await;
            pool.get(addr).cloned()
        };
        if let Some(stream) = maybe_stream {
            return Ok(stream);
        }
        let stream = TcpStream::connect(addr).await?;
        let arc_stream = Arc::new(Mutex::new(stream));
        self.pool
            .write()
            .await
            .insert(addr.to_string(), arc_stream.clone());
        Ok(arc_stream)
    }

    pub async fn remove(&self, addr: &str) {
        if let Some(stream_arc) = self.pool.read().await.get(addr).cloned() {
            if let Ok(mut stream) = stream_arc.try_lock() {
                let _ = stream.shutdown().await;
            }
        }
        self.pool.write().await.remove(addr);
    }

    pub async fn insert_incoming(&self, addr: &str, stream: Arc<Mutex<TcpStream>>) {
        self.pool.write().await.insert(addr.to_string(), stream);
    }

    pub async fn get_stream(&self, addr: &str) -> Option<Arc<Mutex<TcpStream>>> {
        let pool = self.pool.read().await;
        pool.get(addr).cloned()
    }

    pub async fn close_all(&self) {
        let mut pool = self.pool.write().await;
        for (_addr, stream_arc) in pool.drain() {
            if let Ok(mut stream) = stream_arc.try_lock() {
                let _ = stream.shutdown().await;
            }
        }
    }
}
