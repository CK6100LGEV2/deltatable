// CUID 前缀提取器,利用 Bloom Filter 过滤掉不包含该 CUID 的 SST 文件
#include "rocksdb/slice_transform.h"

namespace ROCKSDB_NAMESPACE {

// 定义提取器
class CuidPrefixExtractor : public SliceTransform {
public:
  const char* Name() const override { return "CuidPrefixExtractor"; }

  Slice Transform(const Slice& key) const override {
    // 16 字节 pad + 8 字节 CUID = 24 字节
    if (key.size() < 24) return key;
    return Slice(key.data(), 24);
  }

  bool InDomain(const Slice& key) const override { return key.size() >= 24; }
  bool InRange(const Slice& dst) const override { return dst.size() == 24; }
};

}

/*
// 在 DBImpl::DBImpl 构造函数中，或者在 Open 时处理 Options
// 假设你是在 SetDBOptions 或类似地方，确保 immutable_db_options_ 包含它

// 最佳位置是在你的 OptimizeOptionsForDeltaOBS 函数中（test/delta_test_options.h）
// 或者是生产环境的 Options 初始化代码中：

// 1. 引入头文件或类定义
// 2. 配置 Options
options.prefix_extractor.reset(new CuidPrefixExtractor());
// 3. 开启全量过滤，确保 L0 也能用 Bloom Filter
BlockBasedTableOptions table_options;
table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
table_options.whole_key_filtering = true; // 或 false 配合 prefix_extractor
*/