use reed_solomon_erasure::{galois_8::Field, ReedSolomon};

#[derive(Clone)]
pub struct ECService {
    pub active: bool,
    pub shard_count: usize,
    pub parity_count: usize,
    reed_solomon: ReedSolomon<Field>,
}

impl ECService {
    pub fn new(active: bool, shard_count: usize, parity_count: usize) -> Self {
        let rs = ReedSolomon::new(shard_count, parity_count).unwrap();

        ECService {
            active,
            shard_count,
            parity_count,
            reed_solomon: rs,
        }
    }

    pub fn encode(&self, value: &Vec<u8>) -> Vec<Vec<u8>> {
        let payload_len = value.len();
        let shard_size = (payload_len + self.shard_count - 1) / self.shard_count;
        let padded_len = shard_size * self.shard_count;

        let mut padded_payload = value.clone();
        padded_payload.resize(padded_len, 0);

        let mut shards: Vec<Vec<u8>> = Vec::with_capacity(self.shard_count + self.parity_count);
        for i in 0..self.shard_count {
            let start = i * shard_size;
            let end = start + shard_size;
            shards.push(padded_payload[start..end].to_vec());
        }
        for _ in 0..self.parity_count {
            shards.push(vec![0; shard_size]);
        }
        self.reed_solomon
            .encode(&mut shards)
            .expect("Failed to encode message");

        shards
    }

    pub fn reconstruct(
        &self,
        data: &mut Vec<Option<Vec<u8>>>,
    ) -> Result<(), reed_solomon_erasure::Error> {
        self.reed_solomon.reconstruct(data)?;
        Ok(())
    }
}
