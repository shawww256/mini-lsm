use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    // level_size_multiplier: the size ratio between two adjacent levels
    pub level_size_multiplier: usize,
    // level0_file_num_compaction_trigger: the number of sstables in L0 to trigger compaction
    pub level0_file_num_compaction_trigger: usize,
    // max_levels: the maximum number of levels
    pub max_levels: usize,
    // base_level_size_mb: the size of the base level in MB
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState, // 一个 LSM 存储状态的快照
        sst_ids: &[usize],          // 当前层级中要检查的 SSTable 的 ID 列表
        in_level: usize,            // 当前层级的索引
    ) -> Vec<usize> {
        //unimplemented!()
        // 找到 sst_ids 列表中所有 SSTable 的最小开始键（first_key），这将是重叠查找的开始边界
        let begin_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();
        // 找到 sst_ids 列表中所有 SSTable 的最大结束键（last_key），这将是重叠查找的结束边界
        let end_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();
        let mut overlap_ssts = Vec::new(); // 用于存储找到的重叠 SSTable 的 ID
                                           // 遍历当前层级下一层的所有 SSTable
        for sst_id in &snapshot.levels[in_level - 1].1 {
            let sst = &snapshot.sstables[sst_id];
            let first_key = sst.first_key(); // 下一层 SSTable 的开始键
            let last_key = sst.last_key(); // 下一层 SSTable 的结束键
                                           // 如果下一层的 SSTable 与当前层级的 SSTable 有重叠，则添加到重叠列表中
            if !(last_key < &begin_key || first_key > &end_key) {
                overlap_ssts.push(*sst_id);
            }
        }
        overlap_ssts // 返回找到的重叠 SSTable 的 ID 列表
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // 计算每个层级的目标大小。
        let mut target_level_size = (0..self.options.max_levels).map(|_| 0).collect::<Vec<_>>(); // 从1到max_levels，排除L0层。

        let mut real_level_size = Vec::with_capacity(self.options.max_levels);
        let mut base_level = self.options.max_levels;
        for i in 0..self.options.max_levels {
            // 计算每个层级的实际大小。
            real_level_size.push(
                snapshot.levels[i]
                    .1
                    .iter()
                    .map(|x| snapshot.sstables.get(x).unwrap().table_size())
                    .sum::<u64>() as usize,
            );
        }
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;

        // 选择基础层级并计算目标层级大小。
        target_level_size[self.options.max_levels - 1] =
            // 最后一层的目标大小是实际大小和基础层级大小中较大的一个。
            real_level_size[self.options.max_levels - 1].max(base_level_size_bytes);
        for i in (0..(self.options.max_levels - 1)).rev() {
            // 从第二底层开始向上计算每一层的目标大小。
            let next_level_size = target_level_size[i + 1];
            let this_level_size = next_level_size / self.options.level_size_multiplier;
            if next_level_size > base_level_size_bytes {
                target_level_size[i] = this_level_size;
            }
            if target_level_size[i] > 0 {
                base_level = i + 1;
            }
        }

        // 优先处理 L0 层的压缩。
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!("flush L0 SST to base level {}", base_level);
            return Some(LeveledCompactionTask {
                upper_level: None, // L0 压缩，没有上层。
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        // 计算每个层级的压缩优先级。
        let mut priorities = Vec::with_capacity(self.options.max_levels);
        for level in 0..self.options.max_levels {
            let prio = real_level_size[level] as f64 / target_level_size[level] as f64;
            if prio > 1.0 {
                priorities.push((prio, level + 1));
            }
        }
        // 根据优先级排序，优先级最高的在前。
        priorities.sort_by(|a, b| a.partial_cmp(b).unwrap().reverse());
        let priority = priorities.first();
        if let Some((_, level)) = priority {
            println!(
                "target level sizes: {:?}, real level sizes: {:?}, base_level: {}, priority: {:?}",
                target_level_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                real_level_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                base_level,
                priority,
            );

            let level = *level;
            // 选择要压缩的 SSTable，这里是选择每个层级中最老的 SSTable。
            let selected_sst = snapshot.levels[level - 1].1.iter().min().copied().unwrap();
            println!(
                "compaction triggered by priority: {level} out of {:?}, select {selected_sst} for compaction",
                priorities
            );
            return Some(LeveledCompactionTask {
                upper_level: Some(level), // 指定上层。
                upper_level_sst_ids: vec![selected_sst],
                lower_level: level + 1, // 下层是上层的下一个层级。
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &[selected_sst],
                    level + 1,
                ),
                is_lower_level_bottom_level: level + 1 == self.options.max_levels,
            });
        }
        None // 如果没有压缩任务，则返回None。
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        // 创建快照的可变克隆
        let mut snapshot = snapshot.clone();
        // 准备一个列表，用于存储需要移除的文件 ID
        let mut files_to_remove = Vec::new();
        // 将上层的 SSTable ID 放入一个 HashSet 中，以便快速查找和移除
        let mut upper_level_sst_ids_set = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        // 将下层的 SSTable ID 也放入一个 HashSet 中
        let mut lower_level_sst_ids_set = task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        if let Some(upper_level) = task.upper_level {
            // 如果压缩任务包含上层（非 L0），则从上层移除参与压缩的 SSTable
            let new_upper_level_ssts = snapshot.levels[upper_level - 1]
                .1
                .iter()
                .filter_map(|x| {
                    if upper_level_sst_ids_set.remove(x) {
                        // 如果在压缩 SSTable 列表中，则移除
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
            // 确保所有参与压缩的 SSTable 已被移除
            assert!(upper_level_sst_ids_set.is_empty());
            // 更新上层的 SSTable 列表
            snapshot.levels[upper_level - 1].1 = new_upper_level_ssts;
        } else {
            // 如果是 L0 压缩，更新 L0 层的 SSTable 列表
            let new_l0_ssts = snapshot
                .l0_sstables
                .iter()
                .filter_map(|x| {
                    if upper_level_sst_ids_set.remove(x) {
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
            assert!(upper_level_sst_ids_set.is_empty());
            snapshot.l0_sstables = new_l0_ssts;
        }

        // 将参与压缩的 SSTable ID 添加到需要移除的文件列表中
        files_to_remove.extend(&task.upper_level_sst_ids);
        files_to_remove.extend(&task.lower_level_sst_ids);

        // 更新下层的 SSTable 列表，移除参与压缩的 SSTable 并添加新的 SSTable
        let mut new_lower_level_ssts = snapshot.levels[task.lower_level - 1]
            .1
            .iter()
            .filter_map(|x| {
                if lower_level_sst_ids_set.remove(x) {
                    return None;
                }
                Some(*x)
            })
            .collect::<Vec<_>>();
        assert!(lower_level_sst_ids_set.is_empty());
        // 添加新的 SSTable 到下层的 SSTable 列表中
        new_lower_level_ssts.extend(output);
        // 对新的 SSTable 列表进行排序，以确保键的范围是连续的
        new_lower_level_ssts.sort_by(|x, y| {
            snapshot
                .sstables
                .get(x)
                .unwrap()
                .first_key()
                .cmp(snapshot.sstables.get(y).unwrap().first_key())
        });
        // 更新下层的 SSTable 列表
        snapshot.levels[task.lower_level - 1].1 = new_lower_level_ssts;

        // 返回更新后的快照和需要移除的 SSTable 文件 ID 列表
        (snapshot, files_to_remove)
    }
}
