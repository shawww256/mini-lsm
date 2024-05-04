#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc; // 使用标准库中的Arc，用于线程间共享所有权

use anyhow::Result; // 使用anyhow库中的Result类型，用于简化错误处理

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap.
/// We do not want to create the iterators when initializing this iterator to reduce the overhead of seeking.
/// `SstConcatIterator` 用于串联多个有序且键范围不重叠的SSTable迭代器。
/// 我们不想在初始化迭代器时就创建所有的迭代器，以减少查找时的开销。
pub struct SstConcatIterator {
    current: Option<SsTableIterator>, // 当前活动的SSTable迭代器
    next_sst_idx: usize,              // 下一个要检查的SSTable的索引
    sstables: Vec<Arc<SsTable>>,      // 存储SSTable对象的向量，这些对象将被串联迭代
}

impl SstConcatIterator {
    /// 检查提供的SSTable列表是否有效，即每个SSTable的键范围不重叠且有序
    fn check_sst_valid(sstables: &[Arc<SsTable>]) {
        for sst in sstables {
            assert!(sst.first_key() <= sst.last_key());
        }
        if !sstables.is_empty() {
            // 如果列表不为空，确保连续的SSTable之间键的范围不重叠
            for i in 0..(sstables.len() - 1) {
                assert!(sstables[i].last_key() < sstables[i + 1].first_key());
            }
        }
    }
    /// 移动当前迭代器直到它有效（即直到它指向一个有效的键值对）
    /// 或者直到没有更多的迭代器可以移动。
    fn move_until_valid(&mut self) -> Result<()> {
        // 如果当前迭代器有效，则退出循环
        while let Some(iter) = self.current.as_mut() {
            if iter.is_valid() {
                break;
            }
            // 如果所有迭代器都已检查完毕，且当前迭代器无效，则设置当前迭代器为 `None`
            if self.next_sst_idx >= self.sstables.len() {
                self.current = None;
            } else {
                // 否则，创建一个新的迭代器并定位到下一个SSTable的第一个键值对
                self.current = Some(SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                )?);
                self.next_sst_idx += 1;
            }
        }
        Ok(())
    }

    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        //unimplemented!()
        // 检查SSTable列表是否有效
        Self::check_sst_valid(&sstables);
        // 如果SSTable列表为空，则直接返回一个没有活动迭代器的迭代器
        if sstables.is_empty() {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }
        // 创建迭代器，定位到第一个SSTable的第一个键值对，并确保迭代器有效
        let mut iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_first(
                sstables[0].clone(),
            )?),
            next_sst_idx: 1,
            sstables,
        };
        iter.move_until_valid()?;
        Ok(iter)
    }

    // 创建一个 `SstConcatIterator` 并定位到给定键的键值对
    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        //unimplemented!()
        Self::check_sst_valid(&sstables);
        // 找到第一个键大于或等于给定键的SSTable的索引
        let idx: usize = sstables
            .partition_point(|table| table.first_key().as_key_slice() <= key)
            .saturating_sub(1);
        // 如果索引超出范围，返回一个没有当前迭代器的迭代器
        if idx >= sstables.len() {
            return Ok(Self {
                current: None,
                next_sst_idx: sstables.len(),
                sstables,
            });
        }
        // 创建迭代器，定位到选定SSTable的给定键，并确保迭代器有效
        let mut iter = Self {
            current: Some(SsTableIterator::create_and_seek_to_key(
                sstables[idx].clone(),
                key,
            )?),
            next_sst_idx: idx + 1,
            sstables,
        };
        iter.move_until_valid()?;
        Ok(iter)
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        //unimplemented!()
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        //unimplemented!()
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        //unimplemented!()
        if let Some(current) = &self.current {
            assert!(current.is_valid());
            true
        } else {
            false
        }
    }

    fn next(&mut self) -> Result<()> {
        //unimplemented!()
        self.current.as_mut().unwrap().next()?;
        self.move_until_valid()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
