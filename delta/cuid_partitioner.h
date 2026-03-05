#include <memory>
#include <string>
#include "rocksdb/sst_partitioner.h"
#include "rocksdb/slice.h"

namespace ROCKSDB_NAMESPACE {

class CuidPartitioner : public SstPartitioner {
public:
  explicit CuidPartitioner(uint64_t min_size) : min_file_size_(min_size) {}

  const char* Name() const override { return "CuidPartitioner"; }

  PartitionerResult ShouldPartition(const PartitionerRequest& request) override;

  bool CanDoTrivialMove(const Slice& smallest_user_key,
                        const Slice& largest_user_key) override;

private:
  uint64_t min_file_size_;
  uint64_t ExtractCUID(const Slice& key);
};

class CuidPartitionerFactory : public SstPartitionerFactory {
public:
  // min_size: 最小切分阈值，建议 4MB - 8MB，防止 OBS 小文件爆炸
  explicit CuidPartitionerFactory(uint64_t min_size = 4 * 1024 * 1024) 
      : min_file_size_(min_size) {}

  const char* Name() const override { return "CuidPartitionerFactory"; }

  std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& /*context*/) const override {
    return std::make_unique<CuidPartitioner>(min_file_size_);
  }

private:
  uint64_t min_file_size_;
};

}  // namespace ROCKSDB_NAMESPACE