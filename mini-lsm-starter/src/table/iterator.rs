#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc; // 使用 Arc 来提供线程间的数据同步访问

use anyhow::Result; // 使用 anyhow 库提供的 Result 类型，用于错误处理

use super::SsTable;
use crate::block::BlockIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,     // 一个通过 Arc 包装的 SsTable 实例，允许多线程访问
    blk_iter: BlockIterator, // 当前块的迭代器
    blk_idx: usize,          // 当前正在遍历的块的索引
}

impl SsTableIterator {
    // 在给定的表中寻找第一个数据块，并创建一个指向它的迭代器
    fn seek_to_first_inner(table: &Arc<SsTable>) -> Result<(usize, BlockIterator)> {
        Ok((
            0, // 第一个数据块的索引通常是 0
            BlockIterator::create_and_seek_to_first(table.read_block_cached(0)?),
            // 创建并定位到第一个块的第一个键值对
        ))
    }
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        //unimplemented!()
        let (blk_idx, blk_iter) = Self::seek_to_first_inner(&table)?;
        let iter = Self {
            blk_iter,
            table,
            blk_idx,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        //unimplemented!()
        let (blk_idx, blk_iter) = Self::seek_to_first_inner(&self.table)?;
        self.blk_idx = blk_idx;
        self.blk_iter = blk_iter;
        Ok(())
    }

    //在给定的表中寻找给定键所在的块，并创建一个指向它的迭代器
    fn seek_to_key_inner(table: &Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        let mut blk_idx = table.find_block_idx(key);
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
        //unimplemented!()
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&table, key)?;
        let iter = Self {
            blk_iter,
            table,
            blk_idx,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        //unimplemented!()
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&self.table, key)?;
        self.blk_iter = blk_iter;
        self.blk_idx = blk_idx;
        Ok(())
    }
}

// 为 SsTableIterator 实现 StorageIterator 特质
//使其可以作为存储迭代器使用
impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;
    // 关联类型，指定了迭代器的键类型
    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        //unimplemented!()
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        //unimplemented!()
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        //unimplemented!()
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        //unimplemented!()
        self.blk_iter.next(); // 移动到下一个键值对
        if !self.blk_iter.is_valid() {
            // 如果当前迭代器不再有效
            self.blk_idx += 1; // 移动到下一个块的索引
            if self.blk_idx < self.table.num_of_blocks() {
                self.blk_iter = BlockIterator::create_and_seek_to_first(
                    self.table.read_block_cached(self.blk_idx)?,
                    // 创建一个新的块迭代器并定位到第一个键值对
                );
            }
        }
        Ok(())
    }
}
