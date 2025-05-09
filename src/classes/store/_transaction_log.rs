use crate::base_libs::_operation::Operation;

pub struct TransactionLog {
    pub path: String,
    pub transaction: Vec<Operation>,
}

impl TransactionLog {
    pub fn new(file_path: &str) -> Self {
        TransactionLog {
            path: file_path.to_string(),
            transaction: [].to_vec(),
        }
    }
}
