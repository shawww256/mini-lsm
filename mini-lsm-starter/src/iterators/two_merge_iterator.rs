#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    choose_a: bool, // 一个布尔值，指示当前应该选择哪个迭代器的键和值
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    fn choose_a(a: &A, b: &B) -> bool {
        if !a.is_valid() {
            return false; // 如果A无效，则选择B
        }
        if !b.is_valid() {
            return true; // 如果B无效，则选择A
        }
        a.key() < b.key() // 如果两者都有效，比较键值，选择较小的键对应的迭代器
    }

    // 跳过迭代器B中的当前键，如果它与迭代器A中的当前键相同
    fn skip_b(&mut self) -> Result<()> {
        if self.a.is_valid() && self.b.is_valid() && self.b.key() == self.a.key() {
            self.b.next()?;
        }
        Ok(())
    }

    pub fn create(a: A, b: B) -> Result<Self> {
        //unimplemented!()
        let mut iter = Self {
            choose_a: false,
            a,
            b,
        };
        iter.skip_b()?;
        iter.choose_a = Self::choose_a(&iter.a, &iter.b);
        Ok(iter)
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        //unimplemented!()
        if self.choose_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        //unimplemented!()
        if self.choose_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        //unimplemented!()
        if self.choose_a {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        //unimplemented!()
        if self.choose_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.skip_b()?;
        self.choose_a = Self::choose_a(&self.a, &self.b);
        Ok(())
    }

    // 获取当前迭代器中活跃的迭代器数量
    //（即至少有一个元素可以迭代的迭代器数量）
    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
