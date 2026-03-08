#include "delta/cuid_partitioner.h"

#include <cstdint>
#include <cstring>

namespace ROCKSDB_NAMESPACE {

// 从 User Key 中提取 CUID
// 假设 Key 格式：[16 bytes Padding] [8 bytes CUID (Big Endian)] [...]
uint64_t CuidPartitioner::ExtractCUID(const Slice& key) {
  // 如果 Key 长度不足 24 字节，说明不是标准的 Delta Key，返回 0 归为一类
  if (key.size() < 24) {
    return 0;
  }

  // 跳过前 16 字节 padding
  const unsigned char* p = reinterpret_cast<const unsigned char*>(key.data()) + 16;

  // Big-Endian 解码 8 字节 CUID
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
  // 1. 防碎片化保护 (Anti-Fragmentation)
  // 如果当前文件已经写入的大小小于最小阈值 (如 4MB)，
  // 即使 CUID 变了也不切分，优先保证 OBS 文件粒度，避免元数据爆炸。
  if (request.current_output_file_size < min_file_size_) {
    return PartitionerResult::kNotRequired;
  }

  // 2. 指针检查 (防御性编程)
  if (request.prev_user_key == nullptr || request.current_user_key == nullptr) {
    return PartitionerResult::kNotRequired;
  }

  // 3. 提取前一个 Key 和当前 Key 的 CUID
  // 注意：request 中的 key 是指针，需要解引用
  uint64_t prev_cuid = ExtractCUID(*request.prev_user_key);
  uint64_t curr_cuid = ExtractCUID(*request.current_user_key);

  // 4. 边界判断
  // 如果 CUID 发生跳变，强制要求 RocksDB 结束当前 SST 文件并开启新文件
  if (prev_cuid != curr_cuid) {
    return PartitionerResult::kRequired;
  }

  return PartitionerResult::kNotRequired;
}

bool CuidPartitioner::CanDoTrivialMove(const Slice& /*smallest_user_key*/,
                                       const Slice& /*largest_user_key*/) {
  // 始终允许 Trivial Move。
  // 注意：虽然这里返回 true，但在我们的架构中，Smart Picker 会有一层额外的
  // "Dirty Trivial Move Veto" (脏数据拦截) 逻辑。
  // 如果 Picker 判定文件太脏，Picker 会主动拦截，不会走到这里。
  // 如果 Picker 判定文件干净，这里放行，允许高效下沉。
  return true;
}

}  // namespace ROCKSDB_NAMESPACE