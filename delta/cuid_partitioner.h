#include <memory>
#include <string>
#include <vector>

#include "rocksdb/sst_partitioner.h"
#include "rocksdb/slice.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// 具体的切分器实现类
class CuidPartitioner : public SstPartitioner {
 public:
  explicit CuidPartitioner(uint64_t min_size) : min_file_size_(min_size) {}

  ~CuidPartitioner() override {}

  const char* Name() const override { return "CuidPartitioner"; }

  // 核心切分逻辑：决定当前是否需要结束旧文件、开启新文件
  PartitionerResult ShouldPartition(const PartitionerRequest& request) override;

  // 即使文件跨越了 CUID，是否允许 Trivial Move？
  // 我们返回 true，将具体的脏数据拦截逻辑交给 Compaction Picker 处理
  bool CanDoTrivialMove(const Slice& smallest_user_key,
                        const Slice& largest_user_key) override;

 private:
  uint64_t min_file_size_;
  
  // 辅助函数：从 Key 中提取 CUID
  uint64_t ExtractCUID(const Slice& key);
};

// 工厂类：用于在 Options 中创建 Partitioner 实例
class CuidPartitionerFactory : public SstPartitionerFactory {
 public:
  // min_size: 最小切分阈值，建议设置在 4MB - 8MB 之间
  explicit CuidPartitionerFactory(uint64_t min_size = 4 * 1024 * 1024) 
      : min_file_size_(min_size) {}

  ~CuidPartitionerFactory() override {}

  const char* Name() const override { return "CuidPartitionerFactory"; }

  // 创建切分器实例
  std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& /*context*/) const override {
    return std::make_unique<CuidPartitioner>(min_file_size_);
  }

 private:
  uint64_t min_file_size_;
};

}  // namespace ROCKSDB_NAMESPACE