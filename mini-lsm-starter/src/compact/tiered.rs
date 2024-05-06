use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
// TieredCompactionTask 结构体定义了一个层级压缩任务，包含要合并的层级（tiers）和是否包含最底层的标记
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>, // 每个元素是一个元组，包含层级 ID 和该层级中 SSTable 的列表
    pub bottom_tier_included: bool,      // 指示是否包括最底层的层级
}

#[derive(Debug, Clone)]
// TieredCompactionOptions 结构体定义了层级压缩的配置选项
pub struct TieredCompactionOptions {
    pub num_tiers: usize,                      // 期望的层级数量
    pub max_size_amplification_percent: usize, // 最大空间放大比率的百分比
    pub size_ratio: usize,                     // 层级间 SSTable 大小比例的触发值
    pub min_merge_width: usize,                // 最小合并宽度，即一次压缩至少要涉及的层级数
}

// TieredCompactionController 结构体管理层级压缩的逻辑
pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }
    // 生成压缩任务的函数
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState, // 当前 LSM 存储状态快照
    ) -> Option<TieredCompactionTask> {
        //unimplemented!()
        // 断言 L0 层的 SSTable 列表必须为空，因为在层级压缩策略中
        // L0 层 SSTable 应该直接被刷新到更高的层级，而不是参与压缩
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        // 如果当前的层级数量小于配置的层级数量，则不满足压缩触发条件，直接返回 None。
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }
        // compaction triggered by space amplification ratio
        // 初始化一个变量 size 用于计算除了最后一个层级之外所有层级的 SSTable 数量总和
        let mut size = 0;
        // 遍历 snapshot.levels 除了最后一个元素的所有元素以计算 SSTable 的数量总和
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
        }
        // 计算空间放大比率，如果这个比率大于或等于配置的 max_size_amplification_percent，
        // 则认为触发了压缩
        let space_amp_ratio =
            (size as f64) / (snapshot.levels.last().unwrap().1.len() as f64) * 100.0;
        // 检查是否因为空间放大比率触发压缩
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction triggered by space amplification ratio: {}",
                space_amp_ratio
            );
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(), // 克隆 snapshot.levels 作为压缩任务的层级列表
                bottom_tier_included: true,
            });
        }
        // 计算 size_ratio_trigger，这个值用于判断层级间的大小比例是否触发压缩
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        // compaction triggered by size ratio
        let mut size = 0;
        // 根据层级间的大小比例触发压缩
        for id in 0..(snapshot.levels.len() - 1) {
            // 累加至当前层级的总SSTable 数量
            size += snapshot.levels[id].1.len();
            // 获取下一层的 SSTable 数量
            let next_level_size = snapshot.levels[id + 1].1.len();
            // 计算当前层及之前所有层的 SSTable 数量与下一层 SSTable 数量的比例
            let current_size_ratio = size as f64 / next_level_size as f64;
            // 判断当前层及之前所有层与下一层的大小比例是否达到触发条件，
            // 并且当前索引加 2 是否满足最小合并宽度的要求（只有满足最小合并宽度的情况下才能进行压缩）
            if current_size_ratio >= size_ratio_trigger && id + 2 >= self.options.min_merge_width {
                println!(
                    "compaction triggered by size ratio: {}",
                    current_size_ratio * 100.0
                );
                println!(
                    "next level id is: {}",
                    id+1
                );
                println!(
                    "next level size is: {}",
                    next_level_size
                );
                // 返回一个压缩任务，包括从第一层到 id + 2 层的所有层级
                // 最底层是否包括取决于 id + 2 是否大于或等于 snapshot.levels.len()
                return Some(TieredCompactionTask {
                    tiers: snapshot
                        .levels
                        .iter() // 遍历 snapshot.levels
                        .take(id + 2) // 取从开始到 id + 2 的所有层级
                        .cloned() // 克隆这些层级
                        .collect::<Vec<_>>(), // 收集到一个 Vec 中
                    bottom_tier_included: id + 2 >= snapshot.levels.len(), // 判断是否包括最底层
                });
            }
        }
        // trying to reduce sorted runs without respecting size ratio
        // 如果没有通过空间放大比率或大小比例触发压缩，但是需要减少排序运行集的数量，
        // 则取前面的若干层级进行压缩。这里取的层级数量为 snapshot.levels.len() - self.options.num_tiers + 2
        let num_tiers_to_take = snapshot.levels.len() - self.options.num_tiers + 2;
        println!("compaction triggered by reducing sorted runs");
        return Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter() // 遍历 snapshot.levels
                .take(num_tiers_to_take) // 取从开始到 num_tiers_to_take 的元素
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: snapshot.levels.len() >= num_tiers_to_take,
        });
    }

    // apply_compaction_result 方法将压缩任务的输出结果应用到 LSM 存储状态的快照中，
    // 并返回更新后的 LSM 存储状态和一个包含需要移除的文件的向量。
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,  // 当前 LSM 存储状态的快照
        task: &TieredCompactionTask, // 压缩任务的引用
        output: &[usize],            // 压缩操作产生的输出，通常是新生成的 SST 文件的 ID 列表
    ) -> (LsmStorageState, Vec<usize>) {
        // 返回值是一个元组，包含更新后的 LSM 存储状态和需要移除的 SST 文件的 ID 列表
        //unimplemented!()
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        // 克隆当前的 LSM 存储状态快照，以便在其中应用压缩结果
        let mut snapshot = snapshot.clone();
        // 从压缩任务的 tiers 中创建一个 HashMap，映射每个层级 ID 到该层级的 SST 文件列表
        let mut tier_to_remove = task
            .tiers
            .iter() // 遍历压缩任务中的层级
            .map(|(x, y)| (*x, y)) // 转换为元组，解构每个层级的 ID 和 SST 文件列表
            .collect::<HashMap<_, _>>(); // 收集到 HashMap 中
                                         // 初始化一个空的 levels 向量，用于存储更新后的层级信息
        let mut levels = Vec::new();
        // 初始化一个标志变量 new_tier_added，用于指示是否已将压缩后的层级添加到 LSM Tree
        let mut new_tier_added = false;
        // 初始化一个空的 files_to_remove 向量，用于存储需要移除的 SST 文件的 ID
        let mut files_to_remove = Vec::new();
        for (tier_id, files) in &snapshot.levels {
            // 检查当前层级 ID 是否存在于 tier_to_remove HashMap 中
            if let Some(ffiles) = tier_to_remove.remove(tier_id) {
                // the tier should be removed
                // 如果存在，表示这个层级应该被移除。
                // 同时断言移除的文件列表与当前快照中的文件列表相同，确保压缩操作的一致性
                assert_eq!(ffiles, files, "file changed after issuing compaction task");
                // 将这些文件的 ID 添加到 files_to_remove 列表中，以便后续移除
                files_to_remove.extend(ffiles.iter().copied());
            } else {
                // retain the tier
                // 如果当前层级 ID 不在 tier_to_remove 中，表示这个层级应该被保留
                // 将这个层级添加到 levels 向量中
                levels.push((*tier_id, files.clone()));
            }
            // 检查如果 tier_to_remove 已空且新的层级还尚未添加，则将压缩输出的层级添加到 LSM Tree
            if tier_to_remove.is_empty() && !new_tier_added {
                // add the compacted tier to the LSM tree
                new_tier_added = true;
                levels.push((output[0], output.to_vec()));
            }
        }
        // 如果 tier_to_remove 非空，说明有些层级没有在 snapshot.levels 中找到，这是一个不应该发生的情况
        if !tier_to_remove.is_empty() {
            unreachable!("some tiers not found??");
        }
        // 更新 snapshot 的 levels 为新的 levels 向量
        snapshot.levels = levels;
        // 返回更新后的 LSM 存储状态和需要移除的 SST 文件的 ID 列表
        (snapshot, files_to_remove)
    }
}
