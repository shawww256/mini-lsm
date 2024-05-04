#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    //返回一个包含新创建的SSTable的向量或者一个错误
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        //unimplemented!()
        // 读取当前状态的快照，用于在压缩过程中保持一致性
        let snapshot = {
            // 使用锁机制读取当前状态的不可变引用
            let state = self.state.read();
            state.clone()
        };
        // 创建一个迭代器，用于遍历需要被压缩的SSTable
        let mut iter = match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                // 创建一个迭代器列表，用于遍历L0层的所有SSTable
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                //遍历L0层的所有SSTable的ID
                for id in l0_sstables.iter() {
                    // 对每个SSTable创建一个迭代器，并将其添加到迭代器列表中
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables.get(id).unwrap().clone(),
                    )?)); // 获取对应的SSTable并克隆它
                }
                // 创建一个迭代器列表，用于遍历L1层的所有SSTable
                let mut l1_iters = Vec::with_capacity(l1_sstables.len());
                // 遍历L1层的所有SSTable的ID
                for id in l1_sstables.iter() {
                    // 获取对应的SSTable并克隆它，然后添加到迭代器列表中
                    l1_iters.push(snapshot.sstables.get(id).unwrap().clone());
                }
                // 使用L0层的迭代器列表和L1层的迭代器列表，创建一个合并迭代器
                // 这个迭代器将用于生成压缩过程中的键值对
                TwoMergeIterator::create(
                    // 使用L0层的迭代器列表创建一个合并迭代器
                    MergeIterator::create(l0_iters),
                    // 使用L1层的迭代器列表创建一个串联迭代器，并将迭代器的指针移动到第一个元素
                    SstConcatIterator::create_and_seek_to_first(l1_iters)?,
                )?
            }
            _ => unimplemented!(),
        };
        // 初始化一个可选的SsTable构建器，用于构建新的SSTable
        let mut builder = None;
        // 初始化一个向量，用于存储新创建的SSTable
        let mut new_sst = Vec::new();
        // 从压缩任务中获取是否需要将数据压缩到最底层的标志
        let compact_to_bottom_level = task.compact_to_bottom_level();
        // 使用迭代器遍历所有需要被压缩的键值对
        while iter.is_valid() {
            // 检查构建器是否存在，如果不存在则创建一个新的构建器
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            // 获取构建器的可变引用
            let builder_inner = builder.as_mut().unwrap();
            // 根据是否需要压缩到最底层，决定是否将当前迭代器的键值对添加到构建器中
            if compact_to_bottom_level {
                // 如果需要压缩到最底层，并且当前迭代器的值不为空(说明未被删除)，则添加到构建器中
                if !iter.value().is_empty() {
                    builder_inner.add(iter.key(), iter.value());
                }
            } else {
                // 如果不需要压缩到最底层，则直接添加键值对到构建器中
                builder_inner.add(iter.key(), iter.value());
            }
            // 移动迭代器到下一个键值对
            iter.next()?;
            // 检查构建器估算的大小是否达到了目标SSTable大小
            if builder_inner.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id();
                // 从Some中取出构建器，因为不再需要添加更多的键值对
                let builder = builder.take().unwrap();
                // 使用构建器构建一个新的SSTable，并将其添加到新SSTable的列表
                let sst = Arc::new(builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?);
                new_sst.push(sst);
            }
        }
        // 检查是否存在剩余的构建器，如果存在，则构建最后的SSTable并添加到列表中
        // 最后的SSTable可能小于目标大小
        if let Some(builder) = builder {
            let sst_id = self.next_sst_id(); // lock dropped here
            let sst = Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?);
            new_sst.push(sst);
        }
        // 返回包含所有新创建的SSTable的向量
        Ok(new_sst)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        //unimplemented!()
        // 检查当前的压缩选项是否允许执行全压缩操作。
        // 如果不允许（即 `self.options.compaction_options` 不是 `NoCompaction`），则程序会 panic。
        // 这个检查确保了只有在预期的配置下才会执行全压缩操作。
        let CompactionOptions::NoCompaction = self.options.compaction_options else {
            panic!("full compaction can only be called with compaction is not enabled")
        };

        // 读取当前状态的快照，用于在压缩过程中保持状态的一致性。
        // 这里使用了 `read` 方法，它可能涉及到获取一个锁，以保证读取状态时的线程安全。
        let snapshot = {
            let state = self.state.read();
            state.clone() // 克隆状态，以便在压缩过程中使用。
        };

        // 从快照中获取L0层的所有SSTable的引用。
        let l0_sstables = snapshot.l0_sstables.clone();
        // 从快照中获取第一层（通常是L1层）的所有SSTable的引用。
        let l1_sstables = snapshot.levels[0].1.clone();

        // 创建一个压缩任务，指定需要进行全压缩操作，包括L0和L1层的SSTable。
        // 这个任务将被用于实际的压缩过程。
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(), // 需要压缩的L0层的SSTable列表。
            l1_sstables: l1_sstables.clone(), // 需要压缩的L1层的SSTable列表。
        };
        println!("force full compaction: {:?}", compaction_task);
        // 使用压缩任务执行压缩操作，并将结果存储在 `sstables` 中。
        // 这个压缩操作可能会生成一些新的SSTable，这些SSTable将替换旧的SSTable。
        let sstables = self.compact(&compaction_task)?;
        // 准备一个新ID的列表，用于存储新生成的SSTable的ID。
        let mut ids = Vec::with_capacity(sstables.len());
        // 使用 `state_lock` 来保证在修改状态信息时的线程安全。
        // `_state_lock` 将保持锁定状态直到它的作用域结束。
        {
            let state_lock = self.state_lock.lock();
            // 克隆当前的状态，以便在其中进行修改。
            let mut state = self.state.read().as_ref().clone();

            // 从状态中移除所有旧的L0和L1层的SSTable。
            // 这里使用了迭代器链（`chain`），来遍历L0和L1层的所有SSTable。
            for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
                // 移除旧的SSTable，并确保它们确实存在于状态中。
                let result = state.sstables.remove(sst);
                assert!(result.is_some());
            }
            // 将新生成的SSTable添加到状态中。
            for new_sst in sstables {
                // 获取新SSTable的ID。
                ids.push(new_sst.sst_id());
                // 将新SSTable添加到状态的sstables映射中。
                let result = state.sstables.insert(new_sst.sst_id(), new_sst);
                // 确保没有插入失败（即ID不重复）。
                assert!(result.is_none());
            }
            // 更新L1层的SSTable列表为新生成的SSTable的ID列表。
            assert_eq!(l1_sstables, state.levels[0].1); // 断言旧的L1层SSTable列表与快照中的一致。
            state.levels[0].1 = ids.clone(); // 更新L1层的SSTable列表。

            // 更新L0层的SSTable列表，移除已经被合并的SSTable。
            let mut l0_sstables_map = l0_sstables.iter().copied().collect::<HashSet<_>>();
            state.l0_sstables = state
                .l0_sstables
                .iter()
                // 过滤掉已经被合并的L0层的SSTable。
                .filter(|x| !l0_sstables_map.remove(x))
                .copied()
                .collect::<Vec<_>>();
            // 断言所有被合并的L0层的SSTable都已经被移除。
            assert!(l0_sstables_map.is_empty());

            // 将修改后的状态写回，这里使用了 `write` 方法，可能涉及到写入操作和释放锁。
            *self.state.write() = Arc::new(state);
            //self.sync_dir()?;
        }

        // 删除旧的SSTable文件，因为它们已经被新的SSTable所取代。
        // 这里使用了 `iter.chain` 来遍历L0和L1层的所有SSTable。
        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            // 使用 `std::fs::remove_file` 删除每个旧的SSTable文件。
            // `?` 用于错误传播，如果文件删除失败，则整个 `force_full_compaction` 将返回错误。
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }
        println!("force full compaction done, new SSTs: {:?}", ids);
        // 所有步骤完成后，返回Ok，表示全压缩操作成功完成。
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let res = {
            let state = self.state.read();
            state.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if res {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
