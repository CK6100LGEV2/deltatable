/**
 * delta_snapshot_test.cc
 * 
 * 专项验证 Delta 架构下的 MVCC (多版本并发控制) 与快照隔离
 */

#include <cassert>
#include <iostream>
#include <vector>
#include <thread>
#include <cstring>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "db/db_impl/db_impl.h"
#include "delta/hotspot_manager.h"
#include "rocksdb/types.h"

using namespace ROCKSDB_NAMESPACE;

const std::string kDBPath = "/tmp/rocksdb_delta_snapshot_test";

// ======================= 工具函数 =======================

std::string Key(uint64_t cuid, int suffix) {
    char buf[24];
    memset(buf, 0, 24);
    snprintf(buf, 16, "pad_%010d", 0); 
    
    unsigned char* p = reinterpret_cast<unsigned char*>(buf) + 16;
    for(int i=0; i<8; ++i) p[i] = (cuid >> (56 - 8*i)) & 0xFF;
    
    char suffix_buf[16];
    snprintf(suffix_buf, sizeof(suffix_buf), "%010d", suffix);
    
    std::string key(buf, 24);
    key.append(suffix_buf, 10);
    return key;
}

void Check(bool cond, const std::string& msg) {
    if (cond) std::cout << "[PASS] " << msg << std::endl;
    else {
        std::cerr << "[FAIL] " << msg << std::endl;
        exit(1);
    }
}

bool GetDeleted(DB* db, uint64_t cuid) {
    auto mgr = static_cast<DBImpl*>(db)->GetHotspotManager();
    return mgr->GetDeleteTable().IsDeleted(cuid, kMaxSequenceNumber, 0);
}

// 辅助 Scan 测试
int CountKeysInSnapshot(DB* db, uint64_t cuid, const Snapshot* snap) {
    ReadOptions ro;
    ro.snapshot = snap;
    
    std::string start_key = Key(cuid, -100);
    std::string end_key = Key(cuid, 2000000);
    Slice upper_bound(end_key);
    ro.iterate_upper_bound = &upper_bound;

    Iterator* it = db->NewIterator(ro);
    int count = 0;
    for (it->Seek(start_key); it->Valid(); it->Next()) {
        auto mgr = static_cast<DBImpl*>(db)->GetHotspotManager();
        if (mgr->ExtractCUID(it->key()) == cuid) {
            count++;
        }
    }
    delete it;
    return count;
}


int main() {
    std::cout << "=========================================" << std::endl;
    std::cout << "Delta MVCC & Snapshot Isolation Test" << std::endl;
    std::cout << "=========================================" << std::endl;

    Options options;
    options.create_if_missing = true;
    DestroyDB(kDBPath, options);

    options.disable_auto_compactions = true; 
    options.num_levels = 3;
    options.target_file_size_base = 64 * 1024; // 64KB
    options.compression = kNoCompression;
    
    DB* db = nullptr;
    Status s = DB::Open(options, kDBPath, &db);
    Check(s.ok(), "DB Open");

    // ==========================================================
    // 场景 1: 时间旅行 (Time Travel Read)
    // 验证：Delete 发生后，旧的 Snapshot 依然能无损读取数据
    // ==========================================================
    std::cout << "\n--- Scenario 1: Time Travel Read ---" << std::endl;
    const uint64_t CUID_TIME = 100;

    // 1. 写入原始数据 1000 条
    WriteBatch b1;
    for(int i=0; i<1000; ++i) b1.Put(Key(CUID_TIME, i), "v_original");
    db->Write(WriteOptions(), &b1);
    db->Flush(FlushOptions());

    // 2. 拍下快照 A (历史的见证)
    const Snapshot* snap_A = db->GetSnapshot();
    std::cout << "Snapshot A created at Seq: " << snap_A->GetSequenceNumber() << std::endl;

    // 3. 执行残忍的逻辑删除
    db->Delete(WriteOptions(), Key(CUID_TIME, 0)); // GDCT 标记
    Check(GetDeleted(db, CUID_TIME), "CUID_TIME logically deleted in GDCT");

    // 4. 当前视图验证：什么都看不见
    int current_count = CountKeysInSnapshot(db, CUID_TIME, nullptr);
    Check(current_count == 0, "Current View: Keys should be invisible (0 found)");

    std::string val;
    s = db->Get(ReadOptions(), Key(CUID_TIME, 500), &val);
    Check(s.IsNotFound(), "Current View: Point Get should return NotFound");

    // 5. 【核心验证】历史视图验证：必须看到全部 1000 条！
    int history_count = CountKeysInSnapshot(db, CUID_TIME, snap_A);
    std::cout << "Snapshot View Count: " << history_count << std::endl;
    Check(history_count == 1000, "Time Travel: Snapshot MUST see all 1000 keys!");

    ReadOptions ro_snap; ro_snap.snapshot = snap_A;
    s = db->Get(ro_snap, Key(CUID_TIME, 500), &val);
    Check(s.ok() && val == "v_original", "Time Travel: Point Get MUST succeed with old value");

    db->ReleaseSnapshot(snap_A);


    // ==========================================================
    // 场景 2: 删后重插存活战 (Re-insertion Survival)
    // 验证：删除后立刻插入新数据，Compaction 绝不能误杀新数据！
    // ==========================================================
    std::cout << "\n--- Scenario 2: Re-insertion Survival ---" << std::endl;
    const uint64_t CUID_RE = 200;

    // 1. 写旧数据 -> 删数据
    WriteBatch b2;
    for(int i=0; i<500; ++i) b2.Put(Key(CUID_RE, i), "old_ghost");
    db->Write(WriteOptions(), &b2);
    db->Flush(FlushOptions());
    
    db->Delete(WriteOptions(), Key(CUID_RE, 0));
    Check(GetDeleted(db, CUID_RE), "CUID_RE deleted");

    // 2. 重插新数据 (Re-insertion)
    WriteBatch b3;
    for(int i=0; i<500; ++i) b3.Put(Key(CUID_RE, i), "new_hero");
    db->Write(WriteOptions(), &b3);
    db->Flush(FlushOptions());

    // 3. 当前读取：必须读到新数据
    s = db->Get(ReadOptions(), Key(CUID_RE, 100), &val);
    Check(s.ok() && val == "new_hero", "Re-insertion: Should read 'new_hero' before Compaction");

    // 4. 触发强力 GC (Bottommost Force)
    std::cout << "Triggering Force GC Compaction..." << std::endl;
    CompactRangeOptions cro_re;
    cro_re.change_level = true;
    cro_re.target_level = 1;
    cro_re.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    db->CompactRange(cro_re, nullptr, nullptr);

    // 5. 【核心验证】GC 之后，新数据活下来了吗？
    // Iterator 必须聪明地跳过 "old_ghost"，但保留 "new_hero"！
    s = db->Get(ReadOptions(), Key(CUID_RE, 100), &val);
    if (!s.ok()) {
        std::cerr << "[FATAL] Re-inserted data was MURDERED by Compaction GC!" << std::endl;
        exit(1);
    }
    Check(val == "new_hero", "Survival Confirmed: New data survived Force GC");
    Check(CountKeysInSnapshot(db, CUID_RE, nullptr) == 500, "Survival Confirmed: All 500 new keys exist");


    // ==========================================================
    // 场景 3: 多版本混沌交叉 (Multi-Version Chaos)
    // 验证：极度复杂的版本交错下，各 Snapshot 能否各司其职
    // ==========================================================
    std::cout << "\n--- Scenario 3: Multi-Version Chaos ---" << std::endl;
    const uint64_t CUID_CHAOS = 300;

    db->Put(WriteOptions(), Key(CUID_CHAOS, 1), "V1");
    const Snapshot* snap_V1 = db->GetSnapshot();

    db->Put(WriteOptions(), Key(CUID_CHAOS, 1), "V2");
    const Snapshot* snap_V2 = db->GetSnapshot();

    db->Delete(WriteOptions(), Key(CUID_CHAOS, 1));
    const Snapshot* snap_V3 = db->GetSnapshot(); // V3 应该看到 NotFound

    db->Put(WriteOptions(), Key(CUID_CHAOS, 1), "V4");
    const Snapshot* snap_V4 = db->GetSnapshot();

    // 强制 Flush 落盘并合并，制造物理混乱
    db->Flush(FlushOptions());
    db->CompactRange(cro_re, nullptr, nullptr);

    // 严苛验证 4 个快照
    ReadOptions ro;
    
    ro.snapshot = snap_V1;
    s = db->Get(ro, Key(CUID_CHAOS, 1), &val);
    Check(s.ok() && val == "V1", "Chaos V1 Match");

    ro.snapshot = snap_V2;
    s = db->Get(ro, Key(CUID_CHAOS, 1), &val);
    Check(s.ok() && val == "V2", "Chaos V2 Match");

    ro.snapshot = snap_V3;
    s = db->Get(ro, Key(CUID_CHAOS, 1), &val);
    Check(s.IsNotFound(), "Chaos V3 Match (Deleted)");

    ro.snapshot = snap_V4;
    s = db->Get(ro, Key(CUID_CHAOS, 1), &val);
    Check(s.ok() && val == "V4", "Chaos V4 Match");

    db->ReleaseSnapshot(snap_V1);
    db->ReleaseSnapshot(snap_V2);
    db->ReleaseSnapshot(snap_V3);
    db->ReleaseSnapshot(snap_V4);

    std::cout << "\n=========================================" << std::endl;
    std::cout << "SNAPSHOT ISOLATION TEST PASSED PERFECTLY!" << std::endl;
    std::cout << "=========================================" << std::endl;

    delete db;
    return 0;
}