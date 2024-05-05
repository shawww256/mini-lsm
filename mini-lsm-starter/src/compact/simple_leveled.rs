use std::collections::HashSet;
// 引入serde库，用于序列化和反序列化数据结构。
use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize, // 当前层与下一层文件大小比的百分比阈值
    pub level0_file_num_compaction_trigger: usize, // 触发压缩的L0层文件数量
    pub max_levels: usize,         // LSM树中的最大层数
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>, // 要压缩的上层SSTable的ID列表
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>, // 要合并到的下层SSTable的ID列表
    pub is_lower_level_bottom_level: bool, // 下层是否是LSM树的最底层
}

//用于生成和管理压缩任务的控制器
pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    // 创建一个新的压缩控制器实例
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    // 生成压缩任务。
    // 如果不需要安排压缩，则返回None。压缩任务中SSTable的顺序很重要。
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState, // 借用当前LSM树的状态快照
    ) -> Option<SimpleLeveledCompactionTask> {
        // 返回一个压缩任务的Option枚举，None表示不需要压缩
        //unimplemented!()
        // 初始化一个向量来存储每层的大小（以文件数量计算）
        let mut level_sizes = Vec::new();
        // 将L0层的SSTable数量作为第一层的大小
        level_sizes.push(snapshot.l0_sstables.len());
        // 遍历snapshot.levels，为每个层级添加其对应SSTable的数量
        for (_, files) in &snapshot.levels {
            level_sizes.push(files.len());
        }
        // 从0层开始，一直到max_levels-1层，检查是否需要压缩
        for i in 0..self.options.max_levels {
            // 如果是0层（L0），并且文件数量小于触发值，则跳过不处理
            if i == 0
                && snapshot.l0_sstables.len() < self.options.level0_file_num_compaction_trigger
            {
                continue;
            }

            let lower_level = i + 1;
            let size_ratio = level_sizes[lower_level] as f64 / level_sizes[i] as f64;
            // 如果下一层与当前层的文件大小比例小于设定的百分比阈值，则触发压缩
            if size_ratio < self.options.size_ratio_percent as f64 / 100.0 {
                println!(
                    "compaction triggered at level {} and {} with size ratio {}",
                    i, lower_level, size_ratio
                );
                return Some(SimpleLeveledCompactionTask {
                    // 判断是否是L0层的压缩，若是，则upper_level为None
                    upper_level: if i == 0 { None } else { Some(i) },
                    // 复制对应的SSTable ID列表
                    upper_level_sst_ids: if i == 0 {
                        snapshot.l0_sstables.clone()
                    } else {
                        snapshot.levels[i - 1].1.clone()
                    },
                    lower_level,
                    lower_level_sst_ids: snapshot.levels[lower_level - 1].1.clone(),
                    is_lower_level_bottom_level: lower_level == self.options.max_levels,
                });
            }
        }
        // 如果所有层级都不需要压缩，则返回None
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    /// 将压缩操作的结果应用到LSM树的状态中。
    /// 它接收一个压缩任务、压缩生成的SSTable ID列表，以及当前的LSM树状态快照。
    /// 函数的目的是更新LSM树状态，移除旧的SSTable，并添加新的SSTable。
    pub fn apply_compaction_result(
        &self,                              // 借用当前SimpleLeveledCompactionController实例
        snapshot: &LsmStorageState,         // 借用当前LSM树的状态快照
        task: &SimpleLeveledCompactionTask, // 借用压缩任务
        output: &[usize],                   // 借用压缩生成的SSTable ID列表
    ) -> (LsmStorageState, Vec<usize>) {
        // 返回新的LSM状态和需要移除的SSTable ID列表
        //unimplemented!()
        // 克隆当前的LSM状态快照
        let mut snapshot = snapshot.clone();
        // 初始化一个向量来存储需要移除的文件ID
        let mut files_to_remove = Vec::new();
        // 如果压缩任务指定了上层，则处理非L0层的压缩
        if let Some(upper_level) = task.upper_level {
            // 断言压缩任务中的上层SSTable ID列表与当前状态中的上层SSTable ID列表是否匹配
            assert_eq!(
                task.upper_level_sst_ids,
                snapshot.levels[upper_level - 1].1,
                "sst mismatched"
            );
            // 将上层的SSTable ID添加到需要移除的列表中
            files_to_remove.extend(&snapshot.levels[upper_level - 1].1);
            // 清空上层的SSTable ID列表
            snapshot.levels[upper_level - 1].1.clear();
        } else {
            // 如果压缩任务没有指定上层，即为L0层的压缩
            // 将L0层的SSTable ID添加到需要移除的列表中
            files_to_remove.extend(&task.upper_level_sst_ids);
            // 创建一个HashSet来记录已经被压缩的L0层的SSTable ID
            let mut l0_ssts_compacted = task
                .upper_level_sst_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            // 创建一个新的L0层SSTable ID列表，排除那些被压缩的SSTable
            let new_l0_sstables = snapshot
                .l0_sstables
                .iter()
                .copied()
                .filter(|x| !l0_ssts_compacted.remove(x))
                .collect::<Vec<_>>();
            // 断言所有被压缩的L0层SSTable ID都已被移除
            assert!(l0_ssts_compacted.is_empty());
            // 更新L0层的SSTable ID列表为新的列表
            snapshot.l0_sstables = new_l0_sstables;
        }
        // 断言压缩任务中的下层SSTable ID列表与当前状态中的下层SSTable ID列表是否匹配
        assert_eq!(
            task.lower_level_sst_ids,
            snapshot.levels[task.lower_level - 1].1,
            "sst mismatched"
        );
        // 将下层的SSTable ID添加到需要移除的列表中
        files_to_remove.extend(&snapshot.levels[task.lower_level - 1].1);
        // 更新下层的SSTable ID列表为压缩输出的新SSTable ID列表
        snapshot.levels[task.lower_level - 1].1 = output.to_vec();
        // 返回更新后的LSM状态和需要移除的SSTable ID列表
        (snapshot, files_to_remove)
    }
}
