#pragma once
#include "rocksdb/cache.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "delta/cuid_partitioner.h"
#include "delta/cuid_prefix_extractor.h"

namespace ROCKSDB_NAMESPACE {

// 专门为 Delta 表 + OBS 场景调优 Options
void OptimizeOptionsForDeltaOBS(rocksdb::ColumnFamilyOptions& options) {
    // 1. 严格两层结构 (LSM-Tree Levels)
    // 强制限制只有 L0 和 L1。L1 大小设为极高以模拟“无限”
    options.num_levels = 2;
    
    // [优化 8 & 负载特征适应]：宽扇出批量合并
    // 调高触发阈值，让 L0 在本地/内存积攒更多文件后再下沉 L1，从而摊薄重写成本。
    options.level0_file_num_compaction_trigger = 10; 
    
    // 目标文件大小设为 64MB 或 128MB，适合 OBS 大块顺序 IO
    options.target_file_size_base = 64 * 1024 * 1024;
    
    // L1 的基准大小，设为极高（如 100TB），确保 RocksDB 不会因空间压力自动推数据到更深层
    options.max_bytes_for_level_base = 100ULL * 1024 * 1024 * 1024 * 1024;

    // 2. 物理对齐与切分 (优化 1 & 5 & 7)
    // 利用 CuidPartitioner 实现 CUID 边界对齐，减少 L0->L1 的重叠
    // 最小切分阈值设为 8MB，防止单个 CUID 数据太少导致 OBS 小文件过多
    options.sst_partitioner_factory = std::make_shared<CuidPartitionerFactory>(64 * 1024 * 1024);

    // 3. 读路精确制导与前缀过滤 (优化 10)
    // 针对 Delta Merge 的长扫描负载进行优化
    // 配置 24 字节 CUID 前缀提取器 (16字节 pad + 8字节 CUID)
    options.prefix_extractor.reset(new CuidPrefixExtractor());

    BlockBasedTableOptions table_options;
    
    // [关键] 禁用全量 Key 过滤，仅开启前缀过滤。
    // 这可以让 Bloom Filter 变得非常小（仅索引 CUID），且能快速判断 CUID 在不在某文件中。
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    table_options.whole_key_filtering = false; 

    // 4. 元数据全内存化 (优化 10 & 2)
    // 消除二分查找时的 OBS 网络往返，将延迟降至内存级
    table_options.cache_index_and_filter_blocks = true;
    
    // 强制将 L0 和 L1（Top Level）的索引和过滤器 Pin 在内存中
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.pin_top_level_index_and_filter = true;

    // 提高索引加载的缓存优先级
    table_options.cache_index_and_filter_blocks_with_high_priority = true;

    // 针对 OBS 的带宽优化：增大 Data Block 大小到 16KB (默认 4KB)
    // 减少网络请求次数，提升顺序扫描吞吐量
    table_options.block_size = 16 * 1024;

    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    // 5. 垃圾回收与定期维护 (优化 9)
    // 定期重写 L1 文件（即使 L0 没撞击），物理擦除 GDCT 标记的死数据
    // 设置定期合并时间，例如 2 天 (172800秒)，作为“顺风车”GC 的保底
    options.periodic_compaction_seconds = 172800;

    // 6. 写放大与性能权衡
    // 允许我们在 Picker 里进行“宽窗口”评估的最大体积限制
    options.max_compaction_bytes = 1024 * 1024 * 1024; // 1GB
}

void OptimizeOptionsForDeltaNVME(rocksdb::ColumnFamilyOptions& options) {
    // 1. 严格两层结构 (LSM-Tree Levels)
    options.num_levels = 2;
    
    // NVMe 调优：触发阈值设为 8。
    // 在 NVMe 上，我们可以容忍更频繁的 Compaction 以换取更低的 L0 扫描开销
    options.level0_file_num_compaction_trigger = 8; 
    options.level0_slowdown_writes_trigger = 40;
    options.level0_stop_writes_trigger = 72;
    options.soft_pending_compaction_bytes_limit = 0;
    // options.level0_file_num_compaction_trigger = 1000000000; 
    // options.level0_slowdown_writes_trigger = 1000000000;
    // options.level0_stop_writes_trigger = 1000000000;

    // NVMe 调优：目标文件大小 64MB
    // 兼顾 L1 元数据规模和 Compaction 并行度。
    options.write_buffer_size = 128 * 1024 * 1024;
    options.target_file_size_base = 128 * 1024 * 1024;
    
    // L1 的基准大小，设为极高 (100TB)
    options.max_bytes_for_level_base = 100ULL * 1024 * 1024 * 1024 * 1024;

    // 2. 物理对齐与切分 (优化 1 & 5 & 7)
    // NVMe 调优：最小切分阈值设为 16MB。
    // NVMe 处理小文件元数据的开销较小，更细的切分有助于 Picker 精准定位“牙签”
    options.sst_partitioner_factory = std::make_shared<CuidPartitionerFactory>(16 * 1024 * 1024);

    // 3. 读路精确制导与前缀过滤 (优化 10)
    options.prefix_extractor.reset(new CuidPrefixExtractor());
    options.memtable_prefix_bloom_size_ratio = 0.1;

    BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    table_options.whole_key_filtering = false; 

    // 4. 元数据全内存化 (优化 10 & 2)
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = false;
    table_options.pin_top_level_index_and_filter = true;
    table_options.cache_index_and_filter_blocks_with_high_priority = true;

    // NVMe 调优：Block Size 设为 8KB。
    // 相比 OBS 用的 16KB，8KB 在 NVMe 上能提供更好的随机读性能
    table_options.block_size = 8 * 1024;

    options.table_factory.reset(NewBlockBasedTableFactory(table_options));

    // 5. 并行度与硬件优化 (NVMe 特有)
    // 使用 LZ4，这是 NVMe 存储的最佳搭档：极速压缩，CPU 开销低
    // options.compression = kLZ4Compression;

    // 6. 垃圾回收与定期维护
    options.periodic_compaction_seconds = 172800;
    options.max_compaction_bytes = 1 * 1024 * 1024 * 1024; // 1GB
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