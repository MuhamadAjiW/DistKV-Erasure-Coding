use std::path::Path;

use tokio::{
    fs::File,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
};

use crate::base_libs::_operation::Operation;

pub struct KvTransactionLog {
    pub file_path: String,
    pub transaction: Vec<Operation>,
}

impl KvTransactionLog {
    pub fn new(file_path: &str) -> Self {
        KvTransactionLog {
            file_path: file_path.to_string(),
            transaction: [].to_vec(),
        }
    }

    pub async fn initialize(&mut self) {
        if !Path::new(&self.file_path).exists() {
            _ = File::create(self.file_path.clone()).await;
            return;
        }

        let file = File::open(&self.file_path)
            .await
            .expect("Failed to open transaction log file");
        let reader = BufReader::new(file);

        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await.unwrap() {
            let operation = Operation::from_string(&line);
            self.transaction.push(operation);
        }
    }

    pub async fn append(&mut self, operation: &Operation) {
        let mut file = File::options()
            .append(true)
            .open(&self.file_path)
            .await
            .expect("Failed to open transaction log file");

        let operation_str = operation.to_string();
        file.write_all(operation_str.as_bytes())
            .await
            .expect("Failed to write to transaction log file");
    }

    pub async fn synchronize(&mut self, length: usize) {
        let file = File::create(&self.file_path)
            .await
            .expect("Failed to open transaction log file");
        file.set_len(0)
            .await
            .expect("Failed to truncate transaction log file");

        let mut file = File::options()
            .append(true)
            .open(&self.file_path)
            .await
            .expect("Failed to open transaction log file");

        for i in 0..length {
            let operation_str = self.transaction[i].to_string();
            file.write_all(operation_str.as_bytes())
                .await
                .expect("Failed to write to transaction log file");
        }
        file.flush()
            .await
            .expect("Failed to flush transaction log file");
        self.transaction.truncate(length);
        self.transaction
            .iter()
            .for_each(|op| println!("Truncated operation: {}", op.to_string()));
    }
}
