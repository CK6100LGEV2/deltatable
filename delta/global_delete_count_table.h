#pragma once
#include <vector>
#include <map>
#include <unordered_set>
#include <shared_mutex>
#include "rocksdb/rocksdb_namespace.h"
#include "parallel_hashmap/phmap.h"

namespace ROCKSDB_NAMESPACE {

struct GDCTEntry {
  std::unordered_set<uint64_t> tracked_phys_ids;
  bool is_deleted = false;

  int GetRefCount() const { return static_cast<int>(tracked_phys_ids.size()); }
};

// 定义高并发的分片 Map
// 2^8 = 256 个分片锁，轻松应对 32 线程甚至 128 线程的极高并发
using GDCTMap = phmap::parallel_flat_hash_map<
    uint64_t, GDCTEntry,
    std::hash<uint64_t>,
    std::equal_to<uint64_t>,
    std::allocator<std::pair<const uint64_t, GDCTEntry>>,
    8, // submaps = 2^8
    std::shared_mutex
>;

class GlobalDeleteCountTable {
 public:
  GlobalDeleteCountTable() = default;

  bool TrackPhysicalUnit(uint64_t cuid, uint64_t phys_id);
  bool UntrackPhysicalUnit(uint64_t cuid, uint64_t phys_id);
  void UntrackFiles(uint64_t cuid, const std::vector<uint64_t>& file_ids);

  bool IsTracked(uint64_t cuid) const;
  bool MarkDeleted(uint64_t cuid);
  bool IsDeleted(uint64_t cuid) const;

  int GetRefCount(uint64_t cuid) const;
  std::vector<uint64_t> GetTrackedFiles(uint64_t cuid) const;

  std::vector<uint64_t> AtomicCompactionUpdate(
    const std::unordered_set<uint64_t>& involved_cuids,
    const std::vector<uint64_t>& input_files,
    const std::map<uint64_t, std::unordered_set<uint64_t>>& output_file_to_cuids);

 private:
  // 不再需要全局 mutex_
  GDCTMap table_;
};

} // namespace ROCKSDB_NAMESPACE