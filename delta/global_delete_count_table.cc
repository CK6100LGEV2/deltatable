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

void GlobalDeleteCountTable::UntrackPhysicalUnit(uint64_t cuid, uint64_t phys_id) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  auto it = table_.find(cuid);
  if (it != table_.end()) {
    it->second.tracked_phys_ids.erase(phys_id);
    // 如果计数归零且已标记删除的清理？
    if (it->second.is_deleted && it->second.tracked_phys_ids.empty()) {
        table_.erase(it);
    }
  }
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

/*void GlobalDeleteCountTable::AtomicCompactionUpdate(
    const std::unordered_set<uint64_t>& involved_cuids,
    const std::vector<uint64_t>& input_files,
    uint64_t output_file,
    const std::unordered_set<uint64_t>& survivor_cuids) {

    std::unique_lock<std::shared_mutex> lock(mutex_);

    // 遍历所有卷入 Compaction 的 CUID (这些 CUID 的 Input 引用必须移除)
    for (uint64_t cuid : involved_cuids) {
        auto it = table_.find(cuid);
        if (it == table_.end()) continue;

        auto& entry = it->second;

        // 1. 移除旧文件(Input)的引用 (Ref Count --)
        for (uint64_t old_fid : input_files) {
            entry.tracked_phys_ids.erase(old_fid);
        }

        // 2. 添加新文件(Output)的引用 (Ref Count ++)
        // 只有当该 CUID 确实被写入了 Output 文件时才添加 (幸存者)
        if (output_file > 0 && survivor_cuids.count(cuid)) {
            entry.tracked_phys_ids.insert(output_file);
        }

        // 3. 垃圾回收检查 (The GC)
        // 如果引用归零，且已被逻辑删除，说明物理数据已彻底清除，回收内存条目
        if (entry.tracked_phys_ids.empty() && entry.is_deleted) {
            table_.erase(it);
        }
    }
}*/

void GlobalDeleteCountTable::AtomicCompactionUpdate(
    const std::unordered_set<uint64_t>& involved_cuids,
    const std::vector<uint64_t>& input_files,
    const std::map<uint64_t, std::unordered_set<uint64_t>>& output_file_to_cuids) {

    std::unique_lock<std::shared_mutex> lock(mutex_);

    // 1. 处理输出 (加新账)：完美解决文件分裂！
    for (const auto& pair : output_file_to_cuids) {
        uint64_t out_id = pair.first;
        std::cout << "[GDCT-TRACE] Adding Ref: CUID=" << *pair.second.begin() 
                  << " -> File=" << pair.first << std::endl;
        for (uint64_t cuid : pair.second) {
            table_[cuid].tracked_phys_ids.insert(out_id);
        }
    }

    // 2. 处理输入 (消旧账)：大道至简，安全移除所有被销毁的文件
    for (uint64_t cuid : involved_cuids) {
        auto it = table_.find(cuid);
        if (it == table_.end()) continue;

        // 尝试移除本次 Compaction 销毁的所有 Input 文件
        for (uint64_t old_fid : input_files) {
            std::cout << "[GDCT-TRACE] Removing Ref: CUID=" << cuid 
                          << " -> File=" << old_fid << std::endl;
            it->second.tracked_phys_ids.erase(old_fid);
        }

        // 3. 垃圾回收检查
        if (it->second.tracked_phys_ids.empty() && it->second.is_deleted) {
            table_.erase(it);
        }
    }
}

} // namespace ROCKSDB_NAMESPACE