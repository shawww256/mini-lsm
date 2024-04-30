use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}
///计算两个键的共有前缀长度
/// 这有助于优化存储，因为共有前缀不需要重复存储多次
fn compute_overlap(first_key: KeySlice, key: KeySlice) -> usize {
    let mut i = 0; //i 是一个计数器，用于记录共有前缀的长度
    loop {
        if i >= first_key.len() || i >= key.len() {
            break;
        }
        if first_key.raw_ref()[i] != key.raw_ref()[i] {
            break;
            //first_key.raw_ref()[i] 和 key.raw_ref()[i] 分别获取 first_key 和 key 在索引 i 处的字节
        }
        i += 1;
    } //如果在某个索引处字节不同，循环结束，返回当前的 i 作为共有前缀的长度
    i
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }
    /// 估计当前区块的已使用大小
    fn estimated_size(&self) -> usize {
        SIZEOF_U16 /* number of key-value pairs in the block */ +  self.offsets.len() * SIZEOF_U16 /* offsets */ + self.data.len()
        // key-value pairs
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        if self.estimated_size() + key.len() + value.len() + SIZEOF_U16 * 3 /* key_len, value_len and offset */ > self.block_size
            && !self.is_empty()
        {
            return false;
        }
        // Add the offset of the data into the offset array.
        self.offsets.push(self.data.len() as u16);
        let overlap = compute_overlap(self.first_key.as_key_slice(), key);
        // Encode key overlap.
        self.data.put_u16(overlap as u16);
        // Encode key length.
        self.data.put_u16((key.len() - overlap) as u16);
        // Encode key content.
        self.data.put(&key.raw_ref()[overlap..]);
        // Encode value length.
        self.data.put_u16(value.len() as u16);
        // Encode value content.
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        true
    }

    /// Check if there are no key-value pairs in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    /// 构建并返回一个 Block 实例。
    /// 如果尝试构建一个空区块，则会触发一个 panic
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
