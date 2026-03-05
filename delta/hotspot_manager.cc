//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "delta/hotspot_manager.h"

#include <chrono>
#include <sstream>
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/env.h"
#include "port/port.h"
#include "db/dbformat.h"

namespace ROCKSDB_NAMESPACE {

HotspotManager::HotspotManager(const Options& db_options, const std::string& data_dir)
    : db_options_(db_options), 
      data_dir_(data_dir){ 
  db_options_.env->CreateDirIfMissing(data_dir_);
}

uint64_t HotspotManager::ExtractCUID(const Slice& key) {
  // TODO: 根据实际的 Key Schema提取 cuid，这里先假设一波
  if (key.size() < 24) {
    return 0; 
  }

  const unsigned char* p = reinterpret_cast<const unsigned char*>(key.data()) + 16;

  // Big-Endian Decoding
  uint64_t cuid = (static_cast<uint64_t>(p[0]) << 56) |
                  (static_cast<uint64_t>(p[1]) << 48) |
                  (static_cast<uint64_t>(p[2]) << 40) |
                  (static_cast<uint64_t>(p[3]) << 32) |
                  (static_cast<uint64_t>(p[4]) << 24) |
                  (static_cast<uint64_t>(p[5]) << 16) |
                  (static_cast<uint64_t>(p[6]) << 8)  |
                  (static_cast<uint64_t>(p[7]));

  return cuid;
}

bool HotspotManager::InterceptDelete(const Slice& key) {
  uint64_t cuid = ExtractCUID(key);
  if (cuid == 0) return false;

  // 在 GDCT 中查询是否该 cuid 被标记为删除
  bool marked = delete_table_.MarkDeleted(cuid);
  
  if (marked) {
    // fprintf(stderr, "[HotspotManager] Intercepted Delete for CUID: %lu\n", cuid);
    return true;
  }

  // CUID 不在热点管理范围内
  return false;
}

std::string HotspotManager::GenerateSstFileName(uint64_t cuid) {
  auto now = std::chrono::system_clock::now();
  auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
                       now.time_since_epoch())
                       .count();
  
  std::stringstream ss;
  ss << data_dir_ << "/hot_" << cuid << "_" << timestamp << ".sst";
  return ss.str();
}

// [Delta Fix] 实现转发逻辑
void HotspotManager::RegisterFileRefs(uint64_t file_number, const std::unordered_set<uint64_t>& cuids) {
    std::lock_guard<std::mutex> lock(file_meta_mutex_);
    file_to_cuids_[file_number] = std::vector<uint64_t>(cuids.begin(), cuids.end());
    for (uint64_t cuid : cuids) {
        delete_table_.TrackPhysicalUnit(cuid, file_number);
    }
}

void HotspotManager::ApplyCompactionResult(
    const std::unordered_set<uint64_t>& involved_cuids,
    const std::vector<uint64_t>& input_files,
    const std::map<uint64_t, std::unordered_set<uint64_t>>& output_file_to_cuids) {
    
    delete_table_.AtomicCompactionUpdate(involved_cuids, input_files, output_file_to_cuids);

    // 新增：清理被合并/删除文件的元数据
    std::lock_guard<std::mutex> lock(file_meta_mutex_);
    for (uint64_t fid : input_files) {
        file_to_cuids_.erase(fid);
    }
}

bool HotspotManager::HasHighPriorityHints() {
    std::lock_guard<std::mutex> lock(hint_mutex_);
    return !priority_hints_.empty();
}

void HotspotManager::AddHint(uint64_t cuid) {
    std::lock_guard<std::mutex> lock(hint_mutex_);
    if (priority_hints_.size() >= 100) return; // 防 OOM

    // 精确构造 CUID 边界的 UserKey
    std::string start_ukey(24, '\0');
    std::string end_ukey(24, '\xFF');
    for (int i = 0; i < 8; ++i) {
        unsigned char c = (cuid >> (56 - 8 * i)) & 0xFF;
        start_ukey[16 + i] = c;
        end_ukey[16 + i] = c;
    }

    // 构造 InternalKey，覆盖该 CUID 的所有 SeqNum 和 Type
    InternalKey start_ikey(start_ukey, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey end_ikey(end_ukey, 0, kTypeDeletion);

    priority_hints_.push({cuid, start_ikey.Encode().ToString(), end_ikey.Encode().ToString()});
}

CompactionHint HotspotManager::PopHint() {
    std::lock_guard<std::mutex> lock(hint_mutex_);
    CompactionHint h = priority_hints_.front();
    priority_hints_.pop();
    return h;
}

void HotspotManager::RegisterFileMetadata(uint64_t file_num, const std::unordered_set<uint64_t>& cuids) {
    std::lock_guard<std::mutex> lock(file_meta_mutex_);
    file_to_cuids_[file_num] = std::vector<uint64_t>(cuids.begin(), cuids.end());
}

double HotspotManager::GetFileGarbageRatio(uint64_t file_num) {
    std::lock_guard<std::mutex> lock(file_meta_mutex_);
    auto it = file_to_cuids_.find(file_num);
    if (it == file_to_cuids_.end() || it->second.empty()) return 0.0;

    int deleted = 0;
    for (uint64_t cuid : it->second) {
        if (delete_table_.IsDeleted(cuid)) deleted++;
    }
    return static_cast<double>(deleted) / it->second.size();
}
}  // namespace ROCKSDB_NAMESPACE