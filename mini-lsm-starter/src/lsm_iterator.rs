#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{bail, Result};
use bytes::Bytes;
use std::ops::Bound;

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::mem_table::MemTableIterator;
use crate::table::SsTableIterator;

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
//type LsmIteratorInner = MergeIterator<MemTableIterator>;
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
}

impl LsmIterator {
    // 函数返回一个 `Result<Self, _>` 类型，这意味着它要么成功返回一个 `LsmIterator` 实例，要么返回一个错误。
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound,
        };
        // 调用 `move_to_non_delete` 方法，确保迭代器不指向一个删除的项
        //（如果 `value().is_empty()` 为 `true`，则表示该项被删除）
        // 这个方法可能会失败，因此使用 `?` 操作符来传播错误。
        iter.move_to_non_delete()?;
        Ok(iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        self.inner.next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        match self.end_bound.as_ref() {
            Bound::Unbounded => {}
            Bound::Included(key) => self.is_valid = self.inner.key().raw_ref() <= key.as_ref(),
            Bound::Excluded(key) => self.is_valid = self.inner.key().raw_ref() < key.as_ref(),
        }
        Ok(())
    }
    // 将迭代器移动到下一个非删除的项上。
    // 这个方法返回一个 `Result<()>` 类型，表示它可能失败，但成功时不返回任何有价值的数据。
    fn move_to_non_delete(&mut self) -> Result<()> {
        while self.is_valid() && self.inner.value().is_empty() {
            self.next_inner()?;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        //unimplemented!()
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        //unimplemented!()
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        //unimplemented!()
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        //unimplemented!()
        self.next_inner()?;
        self.move_to_non_delete()?;
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        //unimplemented!()
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        //unimplemented!()
        if self.has_errored || !self.iter.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        //unimplemented!()
        if self.has_errored || !self.iter.is_valid() {
            panic!("invalid access to the underlying iterator");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        //unimplemented!()
        // only move when the iterator is valid and not errored
        if self.has_errored {
            bail!("the iterator is tainted");
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }
}
