#include "rocksdb/cache.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "delta/cuid_partitioner.h"
#include "delta/cuid_prefix_extractor.h"

namespace ROCKSDB_NAMESPACE {

// 专门为 Delta 表 + OBS 场景调优 Options
void OptimizeOptionsForDeltaOBS(Options& options) {
    //[优化 1] 强制 L0/L1 文件按 CUID 边界对齐 (最小 4MB)
    options.sst_partitioner_factory = std::make_shared<CuidPartitionerFactory>(4 * 1024 * 1024);
    
    // [优化 10] 配置 CUID 级别前缀提取器
    options.prefix_extractor.reset(new CuidPrefixExtractor());

    BlockBasedTableOptions table_options;
    // 使用 Bloom Filter，并且不全量过滤，只过滤 Prefix (CUID)
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    table_options.whole_key_filtering = false; 

    //[优化 10] L1 元数据全内存化 (Pinning)
    table_options.cache_index_and_filter_blocks = true;
    table_options.cache_index_and_filter_blocks_with_high_priority = true;
    table_options.pin_top_level_index_and_filter = true; // 强制 L1 的 Index 留在内存
    
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
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