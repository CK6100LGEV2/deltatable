#pragma once

#include <string>
#include <memory>
#include <queue>
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/rocksdb_namespace.h"
#include "delta/global_delete_count_table.h"
#include "db/dbformat.h"

namespace ROCKSDB_NAMESPACE {

struct ScanContext {
    uint64_t current_cuid = 0;
    // 当前 cuid 已访问的文件 ID (FileNumber...)
    std::unordered_set<uint64_t> visited_phys_units; 
};
struct CompactionHint {
    uint64_t cuid;
    std::string start_key;
    std::string end_key;
    bool is_l0_to_l0; // 新增: 指示是 L0 内部聚合还是下沉 L1
    bool force_purge; // 新增：指示是否是由 Delete 触发的强制垃圾清理
};

class HotspotManager {
 public:
  // db_options: 用于初始化 SstFileWriter
  // data_dir: 生成 SST 文件存放的目录路径
  HotspotManager(const Options& db_options, const std::string& data_dir);

  ~HotspotManager() = default;

  // 返回数据给用户前调用 【暂时不要了】
  //   void OnUserScan(const Slice& key, const Slice& value);
  
  // 返回 true 表示该 CUID 是热点
  bool RegisterScan(uint64_t cuid);

  // 收集数据 (只有 RegisterScan 返回 true 时才调用此函数)
  bool BufferHotData(uint64_t cuid, const Slice& key, const Slice& value);

  void TriggerBufferFlush();

  GlobalDeleteCountTable& GetDeleteTable() { return delete_table_; }

  bool ShouldTriggerScanAsCompaction(uint64_t cuid);

  void FinalizeScanAsCompaction(uint64_t cuid);

  bool IsCuidDeleted(uint64_t cuid) {
      return delete_table_.IsDeleted(cuid);
  }

  bool IsHot(uint64_t cuid);

  // 拦截 Delete 操作?
  bool InterceptDelete(const Slice& key, DB* db);

  void ClearL1Taint(uint64_t file_num);

  uint64_t ExtractCUID(const Slice& key);

  void UpdateCompactionDelta(uint64_t cuid, 
                                           const std::vector<uint64_t>& input_files,
                                           uint64_t output_file_number,
                                           uint64_t offset,
                                           uint64_t length);
  
  // CompactionIterator 使用 否应该跳过
  bool ShouldSkipObsoleteDelta(uint64_t cuid, const std::vector<uint64_t>& input_files);

  // CompactionJob cleanup：obsolete
  void CleanUpMetadataAfterCompaction(const std::unordered_set<uint64_t>& involved_cuids,
                                      const std::vector<uint64_t>& input_files);

  std::string GenerateSstFileName(uint64_t cuid);

    // Flush 注册接口
  void RegisterFileRefs(uint64_t file_number, uint64_t file_size, const std::unordered_set<uint64_t>& cuids);

  // [新增] 封装 MemTable 的销账操作，用于 FlushJob
  void UntrackMemTableRef(uint64_t cuid, uint64_t mem_id);
  
  // Compaction 闭环接口
  void ApplyCompactionResult(
    const std::unordered_set<uint64_t>& involved_cuids,
    const std::vector<uint64_t>& input_files,
    const std::map<uint64_t, std::unordered_set<uint64_t>>& output_file_to_cuids);
  
  // L0 双向全索引
  
  // 【写接口】: 注册 L0 文件及其包含的 CUIDs (Flush 或 Intra-L0 生成新文件时调用)
  void AddL0Tracking(uint64_t file_num, const std::vector<uint64_t>& cuids);

  // 【写接口】: 移除 L0 文件追踪 (Compaction 消耗掉 L0 文件时调用)
  void RemoveL0Tracking(uint64_t file_num);

  // 【读接口】: 获取某个 CUID 分布在哪些 L0 文件中 (用于 Scan 精确制导)
  std::vector<uint64_t> GetL0FilesForCuid(uint64_t cuid);

  // 【读接口】: 计算 L0 文件的真实垃圾密度 (用于 Picker 垃圾清洗)
  // 返回值 0.0 ~ 1.0 (1.0 表示纯垃圾)
  double GetL0FileGarbageRatio(uint64_t file_num);

 private:
  Options db_options_;
  std::string data_dir_;

  GlobalDeleteCountTable delete_table_;

  std::mutex pending_mutex_;
  std::unordered_set<uint64_t> active_buffered_cuids_;
  std::mutex buffered_cuids_mutex_; // 保护上述集合

  // 污染账本
  std::mutex taint_mutex_;
  std::unordered_map<uint64_t, uint64_t> l1_file_garbage_bytes_;
  std::atomic<uint64_t> avg_cuid_size_{4194304};

  // [优化 2] L0 索引数据结构
  mutable std::shared_mutex l0_index_mutex_;

  // 正向索引: FileID -> List of CUIDs
  // 作用：计算文件垃圾密度，以及 Remove 时反查 CUID
  std::unordered_map<uint64_t, std::vector<uint64_t>> l0_file_to_cuids_;

  // 反向索引: CUID -> List of FileIDs
  // 作用：读请求路由，闭包计算
  std::unordered_map<uint64_t, std::vector<uint64_t>> cuid_to_l0_files_;

  void TaintL1Files(DB* db, uint64_t cuid, const InternalKey& start, const InternalKey& end);
};

}  // namespace ROCKSDB_NAMESPACE