#pragma once

#include <string>
#include <memory>
#include <vector>
#include <atomic>
#include <shared_mutex>
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/rocksdb_namespace.h"
#include "delta/global_delete_count_table.h"
#include "parallel_hashmap/phmap.h"

namespace ROCKSDB_NAMESPACE {
class DB; 

// === 污染账本的高并发 Map (64个分片) ===
using TaintMap = phmap::parallel_flat_hash_map<
    uint64_t, uint64_t,
    std::hash<uint64_t>,
    std::equal_to<uint64_t>,
    std::allocator<std::pair<const uint64_t, uint64_t>>,
    6, std::shared_mutex
>;

// === L0 双向索引的高并发 Map (64个分片) ===
using L0IndexMap = phmap::parallel_flat_hash_map<
    uint64_t, std::vector<uint64_t>,
    std::hash<uint64_t>,
    std::equal_to<uint64_t>,
    std::allocator<std::pair<const uint64_t, std::vector<uint64_t>>>,
    6, std::shared_mutex
>;

class HotspotManager {
 public:
  HotspotManager(const Options& db_options, const std::string& data_dir);
  ~HotspotManager() = default;

  GlobalDeleteCountTable& GetDeleteTable() { return delete_table_; }

  bool IsCuidDeleted(uint64_t cuid) { return delete_table_.IsDeleted(cuid); }
  bool InterceptDelete(const Slice& key, DB* db, ColumnFamilyHandle* cfh);
  uint64_t ExtractCUID(const Slice& key);

  void RegisterFileRefs(uint64_t file_number, uint64_t file_size, const std::unordered_set<uint64_t>& cuids);
  void UntrackMemTableRef(uint64_t cuid, uint64_t mem_id);
  void ApplyCompactionResult(
    const std::unordered_set<uint64_t>& involved_cuids,
    const std::vector<uint64_t>& input_files,
    const std::map<uint64_t, std::unordered_set<uint64_t>>& output_file_to_cuids);

  void AddL0Tracking(uint64_t file_num, const std::vector<uint64_t>& cuids);
  void RemoveL0Tracking(uint64_t file_num);
  std::vector<uint64_t> GetL0FilesForCuid(uint64_t cuid);
  double GetL0FileGarbageRatio(uint64_t file_num);

  void ClearL1Taint(uint64_t file_num);
  double GetL1FileTaintRatio(uint64_t file_num, uint64_t file_size);

 private:
  Options db_options_;
  std::string data_dir_;

  GlobalDeleteCountTable delete_table_;

  // Taint 账本 (自带并发锁，移除 taint_mutex_)
  TaintMap l1_file_garbage_bytes_;
  std::atomic<uint64_t> avg_cuid_size_{4194304};

  // L0 双向全索引 (自带并发锁，移除 l0_index_mutex_)
  L0IndexMap l0_file_to_cuids_;
  L0IndexMap cuid_to_l0_files_;

  void TaintL1Files(DB* db, ColumnFamilyHandle* cfh, uint64_t cuid, const Slice& start_user_key, const Slice& end_user_key);
};

}  // namespace ROCKSDB_NAMESPACE