use reed_solomon_erasure::{galois_8::Field, ReedSolomon};

#[derive(Clone)]
pub struct ECService {
    pub active: bool,
    pub data_shard_count: usize,
    pub parity_shard_count: usize,
    reed_solomon: ReedSolomon<Field>,
}

impl ECService {
    pub const LENGTH_PREFIX_BYTES: usize = std::mem::size_of::<usize>(); // Limited to system usize, should be good enough for most cases

    pub fn new(active: bool, shard_count: usize, parity_count: usize) -> Self {
        let rs = ReedSolomon::new(shard_count, parity_count).unwrap();

        ECService {
            active,
            data_shard_count: shard_count,
            parity_shard_count: parity_count,
            reed_solomon: rs,
        }
    }

    pub fn encode(&self, value: &Vec<u8>) -> Vec<Vec<u8>> {
        let original_payload_len = value.len();
        let mut data_with_len_prefix: Vec<u8> =
            Vec::with_capacity(ECService::LENGTH_PREFIX_BYTES + value.len());

        data_with_len_prefix.extend_from_slice(&original_payload_len.to_be_bytes());
        data_with_len_prefix.extend_from_slice(value);

        let payload_len = data_with_len_prefix.len();

        let shard_size = (payload_len + self.data_shard_count - 1) / self.data_shard_count;
        let padded_len = shard_size * self.data_shard_count;

        let mut padded_payload = data_with_len_prefix.clone();
        padded_payload.resize(padded_len, 0);

        let mut shards: Vec<Vec<u8>> =
            Vec::with_capacity(self.data_shard_count + self.parity_shard_count);
        for i in 0..self.data_shard_count {
            let start = i * shard_size;
            let end = start + shard_size;
            shards.push(padded_payload[start..end].to_vec());
        }
        for _ in 0..self.parity_shard_count {
            shards.push(vec![0; shard_size]);
        }
        self.reed_solomon
            .encode(&mut shards)
            .expect("Failed to encode message");

        shards
    }

    pub fn reconstruct(
        &self,
        recovered_data: &mut Vec<Option<Vec<u8>>>,
    ) -> Result<Vec<u8>, reed_solomon_erasure::Error> {
        self.reed_solomon.reconstruct(recovered_data)?;

        let mut reconstructed_padded_with_len = Vec::new();
        for i in 0..self.data_shard_count {
            reconstructed_padded_with_len.extend_from_slice(recovered_data[i].as_ref().unwrap());
        }

        if reconstructed_padded_with_len.len() < Self::LENGTH_PREFIX_BYTES {
            println!(
                "Reconstructed data length {} is less than length prefix bytes {}",
                reconstructed_padded_with_len.len(),
                Self::LENGTH_PREFIX_BYTES
            );
            return Err(reed_solomon_erasure::Error::IncorrectShardSize);
        }

        let len_bytes: [u8; Self::LENGTH_PREFIX_BYTES] = reconstructed_padded_with_len
            [0..Self::LENGTH_PREFIX_BYTES]
            .try_into()
            .expect("Slice length does not match array length for length prefix");

        let original_len_u64 = usize::from_be_bytes(len_bytes);
        let original_len = original_len_u64 as usize; // Just in case for 32 bit systems

        let actual_data_start_index = Self::LENGTH_PREFIX_BYTES;

        if original_len + actual_data_start_index > reconstructed_padded_with_len.len() {
            println!(
                "Original length {} + actual data start index {} exceeds reconstructed data length {}",
                original_len,
                actual_data_start_index,
                reconstructed_padded_with_len.len()
            );
            return Err(reed_solomon_erasure::Error::IncorrectShardSize);
        }

        let reconstructed_data = reconstructed_padded_with_len[actual_data_start_index..]
            .to_vec()
            .into_iter()
            .take(original_len)
            .collect();

        Ok(reconstructed_data)
    }
}
