#include "rocksdb/cache.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "delta/cuid_partitioner.h"

namespace ROCKSDB_NAMESPACE {

// 专门为 Delta 表 + OBS 场景调优 Options
void OptimizeOptionsForDeltaOBS(Options& options) {
  
  // =========================================================
  // 1. 物理对齐 (Physical Alignment)
  // =========================================================
  // 启用自定义切分器，保证 L1 文件尽量按 CUID 隔离，且最小粒度为 4MB
  options.sst_partitioner_factory = std::make_shared<CuidPartitionerFactory>(4 * 1024 * 1024);

  // =========================================================
  // 2. 内存与缓存策略 (Memory & Pinning)
  // =========================================================
  BlockBasedTableOptions table_options;
  
  // [关键] 创建一个带高优先级池的 LRU Cache
  // 8GB 总大小，high_pri_pool_ratio = 0.5 (50% 给 L1 索引)
  // 这样 L1 的元数据会进入 High Pri 池，不易被 L0/L2 的数据挤出
  std::shared_ptr<Cache> cache = NewLRUCache(
      8ull * 1024 * 1024 * 1024, // capacity: 8GB
      -1,                        // num_shard_bits: auto
      false,                     // strict_capacity_limit
      0.5                        // high_pri_pool_ratio: 50%
  );
  table_options.block_cache = cache;

  // [关键] 开启 Index/Filter Block 的缓存
  table_options.cache_index_and_filter_blocks = true;
  
  // [关键] 标记 L1/L0 的元数据为高优先级，使其常驻内存
  // RocksDB 内部逻辑：L0 和 L1 属于 Top Level
  table_options.cache_index_and_filter_blocks_with_high_priority = true;
  table_options.pin_l0_filter_and_index_blocks_in_cache = true;
  table_options.pin_top_level_index_and_filter = true; // Pin L1 if LevelCompaction

  // 使用 Bloom Filter 进一步减少读 IO
  table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
  
  // 增大 Block Size 以适应 OBS 高带宽 (默认 4KB -> 16KB 或 32KB)
  table_options.block_size = 16 * 1024; 

  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  // =========================================================
  // 3. 写策略 (Write Strategy)
  // =========================================================
  // 增大 L1 目标文件大小，配合 SstPartitioner 使用
  // 让没切分的文件尽量长，利用 OBS 大吞吐
  options.target_file_size_base = 64 * 1024 * 1024; // 64MB
  options.max_bytes_for_level_base = 512 * 1024 * 1024; // L1 总大小
  
  // 关闭压缩 (OBS 场景通常 CPU 是瓶颈，且 Delta 表数据更新快，压缩收益低且耗 CPU)
  // 或者只在 L2+ 使用压缩
  options.compression = kNoCompression;
}

// 获取针对 L1 扫描优化的 ReadOptions
ReadOptions GetReadOptionsForDeltaScan() {
    ReadOptions ro;
    // [关键] 开启大包预取
    // 4MB 是 OBS 延迟和带宽的平衡点 (60ms * 300MB/s ≈ 18MB，保守取 4-8MB)
    // 这会让 Iterator 在读 L1 文件时发起异步大 IO
    ro.readahead_size = 4 * 1024 * 1024; 
    return ro;
}

} // namespace ROCKSDB_NAMESPACE

/*
int main() {
    Options options;
    options.create_if_missing = true;
    
    // 1. 初始化 HotspotManager (之前已有的逻辑)
    // ...
    
    // 2. 应用 Delta+OBS 专属优化配置
    OptimizeOptionsForDeltaOBS(options);

    // 3. 打开 DB
    DB* db;
    DB::Open(options, kDBPath, &db);

    // 4. 进行扫描时
    // 使用优化过的 ReadOptions 进行大包预取
    Iterator* iter = db->NewIterator(GetReadOptionsForDeltaScan());
    // ...
}
*/