#include "delta/hotspot_manager.h"
#include <chrono>
#include <sstream>
#include "rocksdb/env.h"
#include "db/db_impl/db_impl.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

HotspotManager::HotspotManager(const Options& db_options, const std::string& data_dir)
    : db_options_(db_options), data_dir_(data_dir) { 
  db_options_.env->CreateDirIfMissing(data_dir_);
}

uint64_t HotspotManager::ExtractCUID(const Slice& key) {
  if (key.size() < 24) return 0; 
  const unsigned char* p = reinterpret_cast<const unsigned char*>(key.data()) + 16;
  return (static_cast<uint64_t>(p[0]) << 56) | (static_cast<uint64_t>(p[1]) << 48) |
         (static_cast<uint64_t>(p[2]) << 40) | (static_cast<uint64_t>(p[3]) << 32) |
         (static_cast<uint64_t>(p[4]) << 24) | (static_cast<uint64_t>(p[5]) << 16) |
         (static_cast<uint64_t>(p[6]) << 8)  | (static_cast<uint64_t>(p[7]));
}

bool HotspotManager::InterceptDelete(const Slice& key, DB* db, ColumnFamilyHandle* cfh) {
    uint64_t cuid = ExtractCUID(key);
    if (cuid == 0) return false;

    if (delete_table_.MarkDeleted(cuid)) {
        std::string start_ukey(24, '\0');
        std::string end_ukey(24, '\0');
        if (key.size() >= 16) {
            memcpy(&start_ukey[0], key.data(), 16);
            memcpy(&end_ukey[0], key.data(), 16);
        }
        for (int i = 0; i < 8; ++i) {
            unsigned char byte = (cuid >> (56 - 8 * i)) & 0xFF;
            start_ukey[16 + i] = byte;
            end_ukey[16 + i] = byte;
        }
        end_ukey.append(8, '\xFF'); 
        TaintL1Files(db, cfh, cuid, Slice(start_ukey), Slice(end_ukey));
        return true;
    }
    return false;
}

void HotspotManager::RegisterFileRefs(uint64_t file_number, uint64_t file_size, 
                                      const std::unordered_set<uint64_t>& cuids) {
    if (!cuids.empty()) {
        uint64_t this_batch_avg = file_size / cuids.size();
        uint64_t old_avg = avg_cuid_size_.load();
        uint64_t updated_avg = (old_avg * 9 + this_batch_avg) / 10;
        avg_cuid_size_.store(updated_avg);
    }
    for (uint64_t cuid : cuids) {
        delete_table_.TrackPhysicalUnit(cuid, file_number);
    }
}

void HotspotManager::ApplyCompactionResult(
    const std::unordered_set<uint64_t>& involved_cuids,
    const std::vector<uint64_t>& input_files,
    const std::map<uint64_t, std::unordered_set<uint64_t>>& output_file_to_cuids) {
    delete_table_.AtomicCompactionUpdate(involved_cuids, input_files, output_file_to_cuids);
}

void HotspotManager::UntrackMemTableRef(uint64_t cuid, uint64_t mem_id) {
    delete_table_.UntrackPhysicalUnit(cuid, mem_id);
}

// === L0 索引高并发实现 ===
void HotspotManager::AddL0Tracking(uint64_t file_num, const std::vector<uint64_t>& cuids) {
    l0_file_to_cuids_.insert_or_assign(file_num, cuids);

    for (uint64_t cuid : cuids) {
        cuid_to_l0_files_.lazy_emplace_l(cuid,
            [&](auto& v) { 
                if (std::find(v.second.begin(), v.second.end(), file_num) == v.second.end()) {
                    v.second.push_back(file_num);
                }
            },
            [&](const auto& ctor) { ctor(cuid, std::vector<uint64_t>{file_num}); }
        );
    }
}

void HotspotManager::RemoveL0Tracking(uint64_t file_num) {
    std::vector<uint64_t> cuids;
    if (!l0_file_to_cuids_.if_contains(file_num, [&](const auto& v) { cuids = v.second; })) {
        return; 
    }

    for (uint64_t cuid : cuids) {
        bool is_empty = false;
        cuid_to_l0_files_.modify_if(cuid, [&](auto& v) {
            v.second.erase(std::remove(v.second.begin(), v.second.end(), file_num), v.second.end());
            is_empty = v.second.empty();
        });
        if (is_empty) {
            cuid_to_l0_files_.erase(cuid);
        }
    }
    l0_file_to_cuids_.erase(file_num);
}

std::vector<uint64_t> HotspotManager::GetL0FilesForCuid(uint64_t cuid) {
    std::vector<uint64_t> res;
    cuid_to_l0_files_.if_contains(cuid, [&](const auto& v) { res = v.second; });
    return res;
}

double HotspotManager::GetL0FileGarbageRatio(uint64_t file_num) {
    std::vector<uint64_t> cuids;
    if (!l0_file_to_cuids_.if_contains(file_num, [&](const auto& v) { cuids = v.second; }) || cuids.empty()) {
        return 0.0;
    }
    int deleted_count = 0;
    for (uint64_t cuid : cuids) {
        if (delete_table_.IsDeleted(cuid)) deleted_count++;
    }
    return static_cast<double>(deleted_count) / cuids.size();
}

// === L1 Taint 污染追踪高并发实现 ===
void HotspotManager::TaintL1Files(DB* db, ColumnFamilyHandle* cfh, uint64_t cuid, const Slice& start_user_key, const Slice& end_user_key) {
    DBImpl* db_impl = static_cast<DBImpl*>(db);
    std::vector<FileMetaData*> overlaps;
    auto* cfh_impl = static_cast<ColumnFamilyHandleImpl*>(cfh);
    auto cfd = cfh_impl->cfd();
    auto current_version = cfd->current();
    auto* vstorage = current_version->storage_info();

    InternalKey start_ikey(start_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey end_ikey(end_user_key, 0, kTypeDeletion);
    vstorage->GetOverlappingInputs(1, &start_ikey, &end_ikey, &overlaps);

    if (overlaps.empty()) return;
    uint64_t estimated_garbage = avg_cuid_size_.load();

    for (auto* f : overlaps) {
        uint64_t fid = f->fd.GetNumber();
        uint64_t current_garbage = 0;

        l1_file_garbage_bytes_.lazy_emplace_l(fid,
            [&](auto& v) { v.second += estimated_garbage; current_garbage = v.second; },
            [&](const auto& ctor) { ctor(fid, estimated_garbage); current_garbage = estimated_garbage; }
        );

        double taint_ratio = (double)current_garbage / f->fd.GetFileSize();
        if (taint_ratio > 0.40) {
             Slice s = f->smallest.user_key();
             Slice e = f->largest.user_key();
             db->SuggestCompactRange(cfh, &s, &e);
             l1_file_garbage_bytes_.erase(fid); // 重置账本
             ROCKS_LOG_INFO(db->GetOptions().info_log, "[Delta-GC] Suggesting L1->L1 Purge for File %lu", fid);
        }
    }
}

void HotspotManager::ClearL1Taint(uint64_t file_num) {
    l1_file_garbage_bytes_.erase(file_num);
}

double HotspotManager::GetL1FileTaintRatio(uint64_t file_num, uint64_t file_size) {
    if (file_size == 0) return 0.0;
    uint64_t garbage = 0;
    l1_file_garbage_bytes_.if_contains(file_num, [&](const auto& v) { garbage = v.second; });
    return static_cast<double>(garbage) / file_size;
}

}  // namespace ROCKSDB_NAMESPACE