use std::collections::HashMap;
use std::sync::Arc;
use tokio::io;
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
    pub async fn get_or_connect(&self, addr: &str) -> io::Result<Arc<Mutex<TcpStream>>> {
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
        self.pool.write().await.remove(addr);
    }

    pub async fn insert_incoming(&self, addr: &str, stream: Arc<Mutex<TcpStream>>) {
        self.pool.write().await.insert(addr.to_string(), stream);
    }

    pub async fn get_stream(&self, addr: &str) -> Option<Arc<Mutex<TcpStream>>> {
        let pool = self.pool.read().await;
        pool.get(addr).cloned()
    }
}
