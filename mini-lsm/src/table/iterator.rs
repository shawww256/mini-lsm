use std::sync::Arc;// 使用 Arc 来提供线程间的数据同步访问

use anyhow::Result;// 使用 anyhow 库提供的 Result 类型，用于错误处理

use super::SsTable;
use crate::block::BlockIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,// 一个通过 Arc 包装的 SsTable 实例，允许多线程访问
    blk_iter: BlockIterator,// 当前块的迭代器
    blk_idx: usize,// 当前正在遍历的块的索引
}

impl SsTableIterator {
     // 在给定的表中寻找第一个数据块，并创建一个指向它的迭代器
    fn seek_to_first_inner(table: &Arc<SsTable>) -> Result<(usize, BlockIterator)> {
        Ok((
            0,// 第一个数据块的索引通常是 0
            BlockIterator::create_and_seek_to_first(table.read_block_cached(0)?),
            // 创建并定位到第一个块的第一个键值对
        ))
    }

    /// Create a new iterator and seek to the first key-value pair.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let (blk_idx, blk_iter) = Self::seek_to_first_inner(&table)?;
        let iter = Self {
            blk_iter,
            table,
            blk_idx,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let (blk_idx, blk_iter) = Self::seek_to_first_inner(&self.table)?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }

    //在给定的表中寻找给定键所在的块，并创建一个指向它的迭代器
    fn seek_to_key_inner(table: &Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        let mut blk_idx = table.find_block_idx(key);// 查找包含给定键的块的索引
        let mut blk_iter =
            BlockIterator::create_and_seek_to_key(table.read_block_cached(blk_idx)?, key);
        if !blk_iter.is_valid() {
            // 如果在该块中找不到有效的键，则尝试下一个块
            blk_idx += 1;
            if blk_idx < table.num_of_blocks() {
                blk_iter =
                    BlockIterator::create_and_seek_to_first(table.read_block_cached(blk_idx)?);
            }
        }
        Ok((blk_idx, blk_iter))
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&table, key)?;
        let iter = Self {
            blk_iter,
            table,
            blk_idx,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&self.table, key)?;
        self.blk_iter = blk_iter;
        self.blk_idx = blk_idx;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.num_of_blocks() {
                self.blk_iter = BlockIterator::create_and_seek_to_first(
                    self.table.read_block_cached(self.blk_idx)?,
                );
            }
        }
        Ok(())
    }
}
