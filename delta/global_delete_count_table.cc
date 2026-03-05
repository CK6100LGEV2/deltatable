// delta/global_delete_count_table.cc

#include "delta/global_delete_count_table.h"

namespace ROCKSDB_NAMESPACE {

bool GlobalDeleteCountTable::TrackPhysicalUnit(uint64_t cuid, uint64_t phys_id) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto& entry = table_[cuid]; // Lazy Init
  
  if (entry.tracked_phys_ids.find(phys_id) == entry.tracked_phys_ids.end()) {
    entry.tracked_phys_ids.insert(phys_id);
    return true; // 新文件，Ref++
  }
  return false;
}

bool GlobalDeleteCountTable::UntrackPhysicalUnit(uint64_t cuid, uint64_t phys_id) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it != table_.end()) {
    it->second.tracked_phys_ids.erase(phys_id);
    
    // 物理引用归零且逻辑已删除 -> 执行 GC
    if (it->second.is_deleted && it->second.tracked_phys_ids.empty()) {
        table_.erase(it);
        return true; // [核心修复] 返回信号：我被删掉了
    }
  }
  return false;
}

// 用于 L0Compaction 对 delete cuid 的清理
void GlobalDeleteCountTable::UntrackFiles(uint64_t cuid, const std::vector<uint64_t>& file_ids) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it != table_.end()) {
    // 遍历本次 Compaction 的所有输入文件
    for (uint64_t fid : file_ids) {
      it->second.tracked_phys_ids.erase(fid);
    }
    // 检查是否归零且标记删除，如果是则清理条目
    if (it->second.tracked_phys_ids.empty() && it->second.is_deleted) {
      table_.erase(it);
    }
  }
}

bool GlobalDeleteCountTable::MarkDeleted(uint64_t cuid) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it != table_.end()) {
    it->second.is_deleted = true;
    return true; 
  }
  return false;
}

bool GlobalDeleteCountTable::IsDeleted(uint64_t cuid) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);  
  if (it != table_.end()) {
    return it->second.is_deleted;
  }
  return false;
}

int GlobalDeleteCountTable::GetRefCount(uint64_t cuid) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it != table_.end()) {
    return it->second.GetRefCount();
  }
  return 0;
}

bool GlobalDeleteCountTable::IsTracked(uint64_t cuid) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return table_.find(cuid) != table_.end();
}

void GlobalDeleteCountTable::ClearDeletedFlag(uint64_t cuid) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = table_.find(cuid);
    if (it != table_.end()) {
        it->second.is_deleted = false; // 撤销删除标记，给予新生
    }
}

// 获取该 CUID 目前分布在哪些文件里
std::vector<uint64_t> GlobalDeleteCountTable::GetTrackedFiles(uint64_t cuid) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = table_.find(cuid);
    if (it != table_.end()) {
        return std::vector<uint64_t>(it->second.tracked_phys_ids.begin(), 
                                      it->second.tracked_phys_ids.end());
    }
    return {};
}

std::vector<uint64_t> GlobalDeleteCountTable::AtomicCompactionUpdate(
    const std::unordered_set<uint64_t>& involved_cuids,
    const std::vector<uint64_t>& input_files,
    const std::map<uint64_t, std::unordered_set<uint64_t>>& output_file_to_cuids) {

    std::unique_lock<std::shared_mutex> lock(mutex_);
    std::vector<uint64_t> gc_cuids;

    // 1. 加新账 (不变)
    for (const auto& pair : output_file_to_cuids) {
        for (uint64_t cuid : pair.second) {
            table_[cuid].tracked_phys_ids.insert(pair.first);
        }
    }

    // 2. 销旧账
    for (uint64_t cuid : involved_cuids) {
        auto it = table_.find(cuid);
        if (it == table_.end()) continue;

        for (uint64_t old_fid : input_files) {
            it->second.tracked_phys_ids.erase(old_fid);
        }

        // 3. 垃圾回收检查
        if (it->second.tracked_phys_ids.empty() && it->second.is_deleted) {
            table_.erase(it);
            gc_cuids.push_back(cuid);
        }
    }
    return gc_cuids;
}

} // namespace ROCKSDB_NAMESPACE