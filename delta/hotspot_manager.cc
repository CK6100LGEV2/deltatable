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
#include "db/db_impl/db_impl.h"
#include "logging/logging.h"

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

bool HotspotManager::InterceptDelete(const Slice& key, DB* db) {
    uint64_t cuid = ExtractCUID(key);
    if (cuid == 0) return false;

    if (delete_table_.MarkDeleted(cuid)) {
        // 2. 构造该 CUID 的全量 UserKey 区间
        // DWS Key Schema: [16 bytes Prefix] [8 bytes CUID] [Suffix...]
        std::string start_ukey(24, '\0');
        std::string end_ukey(24, '\0');

        // A. 复制原始 Key 的前 16 字节前缀 (dbid_tableid)，保证在同一个表空间内
        if (key.size() >= 16) {
            memcpy(&start_ukey[0], key.data(), 16);
            memcpy(&end_ukey[0], key.data(), 16);
        }

        // B. 写入 CUID (Big-Endian 编码) 到 offset 16-23
        for (int i = 0; i < 8; ++i) {
            unsigned char byte = (cuid >> (56 - 8 * i)) & 0xFF;
            start_ukey[16 + i] = byte;
            end_ukey[16 + i] = byte;
        }

        // C. 处理边界扩展
        // start_ukey 保持 24 字节（最小长度）
        // end_ukey 追加足够多的 0xFF，确保覆盖该 CUID 下的所有 row_id
        end_ukey.append(8, '\xFF'); 

        // 4. 评估 L1 污染并定点爆破
        TaintL1Files(db, cuid, Slice(start_ukey), Slice(end_ukey));
        
        return true;
    }
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
void HotspotManager::RegisterFileRefs(uint64_t file_number, uint64_t file_size, 
                                      const std::unordered_set<uint64_t>& cuids) {
    if (!cuids.empty()) {
        // 1. 计算本次批次的平均大小
        uint64_t this_batch_avg = file_size / cuids.size();

        // 2. 使用指数加权移动平均 (EMA) 更新全局平均值
        // 公式：new_avg = old_avg * 0.9 + this_batch_avg * 0.1
        // 这样可以平滑异常值，同时快速适应负载变化
        uint64_t old_avg = avg_cuid_size_.load();
        uint64_t updated_avg = (old_avg * 9 + this_batch_avg) / 10;
        avg_cuid_size_.store(updated_avg);

        // 3. 执行 L0 索引维护逻辑
        // std::vector<uint64_t> cuid_vec(cuids.begin(), cuids.end());
        // AddL0Tracking(file_number, cuid_vec); // 建立 L0 双向索引
    }

    // 更新 GDCT 账本
    for (uint64_t cuid : cuids) {
        delete_table_.TrackPhysicalUnit(cuid, file_number);
    }
}

// 修改后的 Compaction 结果应用
void HotspotManager::ApplyCompactionResult(
    const std::unordered_set<uint64_t>& involved_cuids,
    const std::vector<uint64_t>& input_files,
    const std::map<uint64_t, std::unordered_set<uint64_t>>& output_file_to_cuids) {
    delete_table_.AtomicCompactionUpdate(
        involved_cuids, input_files, output_file_to_cuids);
}

// 封装的 MemTable 销账 (供 FlushJob 使用)
void HotspotManager::UntrackMemTableRef(uint64_t cuid, uint64_t mem_id) {
    bool erased = delete_table_.UntrackPhysicalUnit(cuid, mem_id);
}

// L0 双向全索引实现
void HotspotManager::AddL0Tracking(uint64_t file_num, const std::vector<uint64_t>& cuids) {
    std::unique_lock<std::shared_mutex> lock(l0_index_mutex_);
    
    fprintf(stderr, "[L0-INDEX-ACTION] ADD File: %lu | CUID Count: %zu\n", file_num, cuids.size());
    
    // 1. 更新正向索引
    l0_file_to_cuids_[file_num] = cuids;

    // 2. 更新反向索引
    for (uint64_t cuid : cuids) {
        cuid_to_l0_files_[cuid].push_back(file_num);
    }
}

void HotspotManager::RemoveL0Tracking(uint64_t file_num) {
    std::unique_lock<std::shared_mutex> lock(l0_index_mutex_);
    
    // 1. 查找该文件包含哪些 CUID
    auto it = l0_file_to_cuids_.find(file_num);
    if (it == l0_file_to_cuids_.end()) {
        fprintf(stderr, "[L0-INDEX-ACTION] REMOVE IGNORED (Not Found): %lu\n", file_num);
        return; // 文件未被追踪，直接返回
    }

    
    fprintf(stderr, "[L0-INDEX-ACTION] REMOVE File: %lu\n", file_num);
    // 2. 从反向索引中移除该文件 ID
    const auto& cuids = it->second;
    for (uint64_t cuid : cuids) {
        auto& files = cuid_to_l0_files_[cuid];
        // Erase-Remove idiom
        files.erase(std::remove(files.begin(), files.end(), file_num), files.end());
        
        // 如果该 CUID 在 L0 没有任何文件了，清理 Key 以节省内存
        if (files.empty()) {
            cuid_to_l0_files_.erase(cuid);
        }
    }

    // 3. 删除正向索引
    l0_file_to_cuids_.erase(it);
}

std::vector<uint64_t> HotspotManager::GetL0FilesForCuid(uint64_t cuid) {
    std::shared_lock<std::shared_mutex> lock(l0_index_mutex_);
    auto it = cuid_to_l0_files_.find(cuid);
    if (it != cuid_to_l0_files_.end()) {
        return it->second;
    }
    return {};
}

double HotspotManager::GetL0FileGarbageRatio(uint64_t file_num) {
    std::shared_lock<std::shared_mutex> lock(l0_index_mutex_);
    auto it = l0_file_to_cuids_.find(file_num);
    
    // 如果文件未被索引，或者是空的，认为垃圾率为 0 (不清理)
    if (it == l0_file_to_cuids_.end() || it->second.empty()) {
        return 0.0;
    }

    int deleted_count = 0;
    for (uint64_t cuid : it->second) {
        // 结合 GDCT 查询逻辑删除状态
        if (delete_table_.IsDeleted(cuid)) {
            deleted_count++;
        }
    }

    return static_cast<double>(deleted_count) / it->second.size();
}

void HotspotManager::TaintL1Files(DB* db, uint64_t cuid, const Slice& start_user_key, const Slice& end_user_key) {
    DBImpl* db_impl = static_cast<DBImpl*>(db);
    std::vector<FileMetaData*> overlaps;
    auto cfd = db_impl->GetVersionSet()->GetColumnFamilySet()->GetDefault();
    auto current_version = cfd->current();
    auto* vstorage = current_version->storage_info();
    
    // StartKey: 序列号设为最大，类型设为 Seek，确保包含该 UserKey 的所有版本
    InternalKey start_ikey(start_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    // EndKey: 序列号设为 0，类型设为 Deletion，确保封住该 UserKey 的末尾
    InternalKey end_ikey(end_user_key, 0, kTypeDeletion);
    vstorage->GetOverlappingInputs(1, &start_ikey, &end_ikey, &overlaps);

    if (overlaps.empty()) return;

    std::lock_guard<std::mutex> lock(taint_mutex_);
    uint64_t estimated_garbage = avg_cuid_size_.load(); // 假设单次 Deltas 积攒了 2MB 垃圾

    for (auto* f : overlaps) {
        uint64_t fid = f->fd.GetNumber();
        l1_file_garbage_bytes_[fid] += estimated_garbage;
        
        // 污染度 = 预估垃圾量 / 文件总大小
        double taint_ratio = (double)l1_file_garbage_bytes_[fid] / f->fd.GetFileSize();

        // 如果文件里 40% 都是死数据，触发 "陨石坑" 定点爆破
        if (taint_ratio > 0.40) {
             Slice s = f->smallest.user_key();
             Slice e = f->largest.user_key();
             
             // 交给 RocksDB 原生后台线程执行 L1->L1 垃圾回收
             db->SuggestCompactRange(db->DefaultColumnFamily(), &s, &e);
             l1_file_garbage_bytes_[fid] = 0; // 重置账本
             ROCKS_LOG_INFO(db->GetOptions().info_log, "[Delta-GC] Suggesting L1->L1 Purge for File %lu", fid);
        }
    }
}

void HotspotManager::ClearL1Taint(uint64_t file_num) {
    std::lock_guard<std::mutex> lock(taint_mutex_);
    l1_file_garbage_bytes_.erase(file_num);
    // fprintf(stderr, "[Delta-GC] Cleared Taint for File %lu\n", file_num);
}
}  // namespace ROCKSDB_NAMESPACE