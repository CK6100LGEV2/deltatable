#include "delta/global_delete_count_table.h"

namespace ROCKSDB_NAMESPACE {

bool GlobalDeleteCountTable::TrackPhysicalUnit(uint64_t cuid, uint64_t phys_id) {
    bool inserted = false;
    table_.lazy_emplace_l(cuid,[&](auto& v) { // Key 已存在，获取分片写锁
            if (v.second.tracked_phys_ids.find(phys_id) == v.second.tracked_phys_ids.end()) {
                v.second.tracked_phys_ids.insert(phys_id);
                inserted = true;
            }
        },
        [&](const auto& ctor) { // Key 不存在，获取分片写锁进行初始化
            GDCTEntry entry;
            entry.tracked_phys_ids.insert(phys_id);
            ctor(cuid, std::move(entry));
            inserted = true;
        }
    );
    return inserted;
}

bool GlobalDeleteCountTable::UntrackPhysicalUnit(uint64_t cuid, uint64_t phys_id) {
    bool do_erase = false;
    table_.modify_if(cuid, [&](auto& v) { // 获取分片写锁
        v.second.tracked_phys_ids.erase(phys_id);
        if (v.second.tracked_phys_ids.empty() && v.second.is_deleted) {
            do_erase = true;
        }
    });
    if (do_erase) {
        table_.erase(cuid);
        return true;
    }
    return false;
}

void GlobalDeleteCountTable::UntrackFiles(uint64_t cuid, const std::vector<uint64_t>& file_ids) {
    bool do_erase = false;
    table_.modify_if(cuid, [&](auto& v) { // 获取分片写锁
        for (uint64_t fid : file_ids) {
            v.second.tracked_phys_ids.erase(fid);
        }
        if (v.second.tracked_phys_ids.empty() && v.second.is_deleted) {
            do_erase = true;
        }
    });
    if (do_erase) {
        table_.erase(cuid);
    }
}

bool GlobalDeleteCountTable::MarkDeleted(uint64_t cuid) {
    bool marked = false;
    table_.lazy_emplace_l(cuid,
        [&](auto& v) { v.second.is_deleted = true; marked = true; },
        [&](const auto& ctor) {
            GDCTEntry entry;
            entry.is_deleted = true;
            ctor(cuid, std::move(entry));
            marked = true;
        }
    );
    return marked;
}

bool GlobalDeleteCountTable::IsDeleted(uint64_t cuid) const {
    bool deleted = false;
    table_.if_contains(cuid, [&](const auto& v) { // 获取分片读锁 (并发极为高效)
        deleted = v.second.is_deleted;
    });
    return deleted;
}

int GlobalDeleteCountTable::GetRefCount(uint64_t cuid) const {
    int ref = 0;
    table_.if_contains(cuid, [&](const auto& v) {
        ref = v.second.GetRefCount();
    });
    return ref;
}

bool GlobalDeleteCountTable::IsTracked(uint64_t cuid) const {
    return table_.contains(cuid);
}

std::vector<uint64_t> GlobalDeleteCountTable::GetTrackedFiles(uint64_t cuid) const {
    std::vector<uint64_t> res;
    table_.if_contains(cuid, [&](const auto& v) { // 获取分片读锁，内部安全拷贝
        res.assign(v.second.tracked_phys_ids.begin(), v.second.tracked_phys_ids.end());
    });
    return res;
}

std::vector<uint64_t> GlobalDeleteCountTable::AtomicCompactionUpdate(
    const std::unordered_set<uint64_t>& involved_cuids,
    const std::vector<uint64_t>& input_files,
    const std::map<uint64_t, std::unordered_set<uint64_t>>& output_file_to_cuids) {

    std::vector<uint64_t> gc_cuids;

    // 1. 加新账 (安全插入)
    for (const auto& pair : output_file_to_cuids) {
        for (uint64_t cuid : pair.second) {
            TrackPhysicalUnit(cuid, pair.first);
        }
    }

    // 2. 销旧账
    for (uint64_t cuid : involved_cuids) {
        bool do_erase = false;
        table_.modify_if(cuid, [&](auto& v) {
            for (uint64_t old_fid : input_files) {
                v.second.tracked_phys_ids.erase(old_fid);
            }
            if (v.second.tracked_phys_ids.empty() && v.second.is_deleted) {
                do_erase = true;
            }
        });
        if (do_erase) {
            table_.erase(cuid);
            gc_cuids.push_back(cuid);
        }
    }
    return gc_cuids;
}

} // namespace ROCKSDB_NAMESPACE