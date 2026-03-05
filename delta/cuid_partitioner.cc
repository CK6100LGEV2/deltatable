#include "delta/cuid_partitioner.h"
#include "util/coding.h" 

namespace ROCKSDB_NAMESPACE {

uint64_t CuidPartitioner::ExtractCUID(const Slice& key) {
  // DWS 格式：16字节 pad + 8字节 CUID
  if (key.size() < 24) return 0;
  const unsigned char* p = reinterpret_cast<const unsigned char*>(key.data()) + 16;
  
  return (static_cast<uint64_t>(p[0]) << 56) |
         (static_cast<uint64_t>(p[1]) << 48) |
         (static_cast<uint64_t>(p[2]) << 40) |
         (static_cast<uint64_t>(p[3]) << 32) |
         (static_cast<uint64_t>(p[4]) << 24) |
         (static_cast<uint64_t>(p[5]) << 16) |
         (static_cast<uint64_t>(p[6]) << 8)  |
         (static_cast<uint64_t>(p[7]));
}

PartitionerResult CuidPartitioner::ShouldPartition(const PartitionerRequest& request) {
  // 1. 最小文件大小检查 (保护 OBS 不产生极小文件)
  if (request.current_output_file_size < min_file_size_) {
    return PartitionerResult::kNotRequired;
  }

  // 2. 检查指针是否为空 (防御性编程)
  if (request.prev_user_key == nullptr || request.current_user_key == nullptr) {
    return PartitionerResult::kNotRequired;
  }

  if (request.prev_user_key->compare(*request.current_user_key) == 0) {
    return PartitionerResult::kNotRequired;
  }

  // 3. 提取 CUID（注意：prev_user_key 是指针，需要解引用）
  uint64_t prev_cuid = ExtractCUID(*request.prev_user_key);
  uint64_t curr_cuid = ExtractCUID(*request.current_user_key);

  // 4. 如果 CUID 发生变化，则切分 SST
  if (prev_cuid != curr_cuid) {
    return PartitionerResult::kRequired;
  }

  return PartitionerResult::kNotRequired;
}

bool CuidPartitioner::CanDoTrivialMove(const Slice& /*smallest*/,
                                       const Slice& /*largest*/) {
  // 允许 Trivial Move，由 Picker 逻辑进一步决定是否拦截（拦截逻辑在你的 Smart Picker 中）
  return true;
}

}  // namespace ROCKSDB_NAMESPACE