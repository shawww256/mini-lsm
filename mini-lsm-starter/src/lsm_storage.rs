#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{map_bound, MemTable};
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

fn range_overlap(
    user_begin: Bound<&[u8]>,
    user_end: Bound<&[u8]>,
    table_begin: KeySlice,
    table_end: KeySlice,
) -> bool {
    match user_end {
        Bound::Excluded(key) if key <= table_begin.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key < table_begin.raw_ref() => {
            return false;
        }
        _ => {}
    }
    match user_begin {
        Bound::Excluded(key) if key >= table_end.raw_ref() => {
            return false;
        }
        Bound::Included(key) if key > table_end.raw_ref() => {
            return false;
        }
        _ => {}
    }
    true
}

fn key_within(user_key: &[u8], table_begin: KeySlice, table_end: KeySlice) -> bool {
    table_begin.raw_ref() <= user_key && user_key <= table_end.raw_ref()
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    // pub(crate) mvcc: Option<LsmMvccInner>,
    // pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    //在关闭存储引擎之前执行一系列同步和清理操作
    pub fn close(&self) -> Result<()> {
        // 调用 sync_dir 方法同步存储目录，确保所有更改都被写入磁盘。
        self.inner.sync_dir()?;

        // 发送通知给压缩线程，告知其可以停止工作。
        self.compaction_notifier.send(()).ok();

        // 发送通知给刷新线程，告知其可以停止工作。
        self.flush_notifier.send(()).ok();

        // 加锁并获取压缩线程句柄。
        let mut compaction_thread = self.compaction_thread.lock();
        // 如果存在压缩线程，等待其完成。
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join() // 等待线程结束。
                .map_err(|e| anyhow::anyhow!("{:?}", e))?; // 将线程结束的错误转换为 anyhow::Error。
        }

        // 加锁并获取刷新线程句柄。
        let mut flush_thread = self.flush_thread.lock();
        // 如果存在刷新线程，等待其完成。
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        // 检查是否启用了 WAL（Write-Ahead Logging，预写日志）。
        if self.inner.options.enable_wal {
            // 如果启用 WAL，则同步 WAL 文件和目录。
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(()); // 如果 WAL 已启用，直接返回成功。
        }

        // 如果 WAL 未启用，并且内存表(memtable)不为空，则创建一个新的 SST。
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .freeze_memtable_with_memtable(Arc::new(MemTable::create(
                    self.inner.next_sst_id(), // 使用当前的下一个 SST ID 创建新的内存表。
                )))?;
        }

        // 循环直到所有的非持久化内存表(imm_memtables)都被刷新到磁盘。
        while {
            let snapshot = self.inner.state.read();
            !snapshot.imm_memtables.is_empty() // 检查是否有待刷新的非持久化内存表。
        } {
            self.inner.force_flush_next_imm_memtable()?; // 强制刷新下一个非持久化内存表。
        }

        // 再次同步存储目录，确保所有更改都被写入磁盘。
        self.inner.sync_dir()?;

        // 如果所有操作都成功完成，返回 Ok。
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let mut state = LsmStorageState::create(&options);
        let path = path.as_ref();
        let mut next_sst_id = 1;
        let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4GB block cache,
        let manifest;

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        if !path.exists() {
            std::fs::create_dir_all(path).context("failed to create DB dir")?;
        }
        let manifest_path = path.join("MANIFEST");
        if !manifest_path.exists() {
            manifest = Manifest::create(&manifest_path).context("failed to create manifest")?;
        } else {
            let (m, records) = Manifest::recover(&manifest_path)?;
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::NewMemtable(_) => unimplemented!(),
                    ManifestRecord::Compaction(task, output) => {
                        let (new_state, _) =
                            compaction_controller.apply_compaction_result(&state, &task, &output);
                        // TODO: apply remove again
                        state = new_state;
                        next_sst_id =
                            next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                }
            }
            // recover SSTs
            for table_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().map(|(_, files)| files).flatten())
            {
                let table_id = *table_id;
                let sst = SsTable::open(
                    table_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, table_id))
                        .context("failed to open SST")?,
                )?;
                state.sstables.insert(table_id, Arc::new(sst));
            }

            next_sst_id += 1;
            state.memtable = Arc::new(MemTable::create(next_sst_id));

            next_sst_id += 1;
            manifest = m;
        };
        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
        };
        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
        //self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        unimplemented!()
        // let mut compaction_filters = self.compaction_filters.lock();
        // compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        //unimplemented!()
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        // Search on the current memtable.
        if let Some(value) = snapshot.memtable.get(key) {
            if value.is_empty() {
                // found tomestone, return key not exists
                return Ok(None);
            }
            return Ok(Some(value));
        }
        // Search on immutable memtables.
        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(key) {
                if value.is_empty() {
                    // found tomestone, return key not exists
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        let keep_table = |key: &[u8], table: &SsTable| {
            if key_within(
                key,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                if let Some(bloom) = &table.bloom {
                    if bloom.may_contain(farmhash::fingerprint32(key)) {
                        return true;
                    }
                } else {
                    return true;
                }
            }
            false
        };

        for table in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table].clone();
            if keep_table(key, &table) {
                l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    table,
                    KeySlice::from_slice(key),
                )?));
            }
        }
        let l0_iter = MergeIterator::create(l0_iters);
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(snapshot.levels[0].1.len());
            for table in level_sst_ids {
                let table = snapshot.sstables[table].clone();
                if keep_table(key, &table) {
                    level_ssts.push(table);
                }
            }
            let level_iter =
                SstConcatIterator::create_and_seek_to_key(level_ssts, KeySlice::from_slice(key))?;
            level_iters.push(Box::new(level_iter));
        }
        let iter = TwoMergeIterator::create(l0_iter, MergeIterator::create(level_iters))?;

        if iter.is_valid() && iter.key().raw_ref() == key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        //unimplemented!()
        for record in batch {
            match record {
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    let size;
                    {
                        let guard = self.state.read();
                        guard.memtable.put(key, b"")?;
                        size = guard.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    assert!(!value.is_empty(), "value cannot be empty");
                    let size;
                    {
                        let guard = self.state.read();
                        guard.memtable.put(key, value)?;
                        size = guard.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
            }
        }
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        //unimplemented!()
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        //unimplemented!()
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            // the memtable could have already been frozen, check again to ensure we really need to freeze
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        //unimplemented!()
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        //unimplemented!()
        let mut guard = self.state.write();
        // Swap the current memtable with a new one.
        let mut snapshot = guard.as_ref().clone();
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        // Add the memtable to the immutable memtables.
        snapshot.imm_memtables.insert(0, old_memtable.clone());
        // Update the snapshot.
        *guard = Arc::new(snapshot);

        // drop(guard);
        // old_memtable.sync_wal()?;

        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        //unimplemented!()
        let memtable_id = self.next_sst_id();
        let memtable = Arc::new(MemTable::create(memtable_id));
        self.freeze_memtable_with_memtable(memtable)?;
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        //unimplemented!()
        let state_lock = self.state_lock.lock();

        let flush_memtable;

        {
            let guard = self.state.read();
            flush_memtable = guard
                .imm_memtables
                .last()
                .expect("no imm memtables!")
                .clone();
        }

        let mut builder = SsTableBuilder::new(self.options.block_size);
        flush_memtable.flush(&mut builder)?;
        let sst_id = flush_memtable.id();
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        // Add the flushed L0 table to the list.
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            // Remove the memtable from the immutable memtables.
            let mem = snapshot.imm_memtables.pop().unwrap();
            assert_eq!(mem.id(), sst_id);
            // Add L0 table
            if self.compaction_controller.flush_to_l0() {
                // In leveled compaction or no compaction, simply flush to L0
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                // In tiered compaction, create a new tier
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }
            println!("flushed {}.sst with size={}", sst_id, sst.table_size());
            snapshot.sstables.insert(sst_id, sst);
            // Update the snapshot.
            *guard = Arc::new(snapshot);
        }
        self.sync_dir()?;

        self.manifest
            .as_ref()
            .unwrap()
            .add_record(&state_lock, ManifestRecord::Flush(sst_id))?;

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        //unimplemented!()
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // drop global lock here

        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(lower, upper)));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);
        let mut table_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            if range_overlap(
                lower,
                upper,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                let iter = match lower {
                    Bound::Included(key) => {
                        SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?
                    }
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(
                            table,
                            KeySlice::from_slice(key),
                        )?;
                        if iter.is_valid() && iter.key().raw_ref() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
                };

                table_iters.push(Box::new(iter));
            }
        }
        // 创建一个合并迭代器 `l0_iter`，它将用于迭代属于L0层级的多个SSTable。
        let l0_iter = MergeIterator::create(table_iters);
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for table in level_sst_ids {
                let table = snapshot.sstables[table].clone();
                if range_overlap(
                    lower,
                    upper,
                    table.first_key().as_key_slice(),
                    table.last_key().as_key_slice(),
                ) {
                    level_ssts.push(table);
                }
            }

            let level_iter = match lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    level_ssts,
                    KeySlice::from_slice(key),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        level_ssts,
                        KeySlice::from_slice(key),
                    )?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(level_ssts)?,
            };
            level_iters.push(Box::new(level_iter));
        }

        // 创建一个两级合并迭代器 `iter`，首先合并内存表 `memtable_iter` 和 L0层级的迭代器 `l0_iter`。
        let iter = TwoMergeIterator::create(memtable_iter, l0_iter)?;
        // 再次使用两级合并迭代器，将上一步得到的迭代器与L1层级的迭代器 `l1_iter` 合并。
        let iter = TwoMergeIterator::create(iter, MergeIterator::create(level_iters))?;
        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(upper),
        )?))
    }
}
