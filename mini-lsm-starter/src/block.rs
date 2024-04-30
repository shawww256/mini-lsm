mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;
// 定义一个常量，表示 `u16` 类型的大小（以字节为单位）
pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    /// 存储序列化后的键值对数据
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// 将内部数据编码为教程中展示的数据布局
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        //unimplemented!()
        // 创建一个缓冲区，初始内容为 `data` 字段的副本
        //键值对正常存储在 `data` 字段中，不变动
        let mut buf = self.data.clone();
        let offsets_len = self.offsets.len();
        // 将每个偏移量添加到缓冲区中
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        // Adds number of elements at the end of the block
        // 在区块的末尾添加元素的数量
        buf.put_u16(offsets_len as u16);
        // 将缓冲区转换为 `Bytes` 类型并返回
        buf.into()
    }

    /// 从数据布局解码，将输入的 `data` 转换为单个 `Block` 实例
    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        //unimplemented!()
        // get number of elements in the block
        let entry_offsets_len = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        // 计算数据部分的结束位置
        let data_end = data.len() - SIZEOF_U16 - entry_offsets_len * SIZEOF_U16;
        // 获取原始的偏移量数组
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16];
        // get offset array
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        // retrieve data
        // 从数据中提取键值对数据部分
        let data = data[0..data_end].to_vec();
        // 创建并返回 `Block` 实例
        Self { data, offsets }
    }
}
