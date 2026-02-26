#include <cassert>
#include <iostream>
#include <vector>
#include <thread>
#include <cstring>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "db/db_impl/db_impl.h"
#include "delta/hotspot_manager.h"

using namespace ROCKSDB_NAMESPACE;

const std::string kDBPath = "/tmp/rocksdb_delta_test";

// ======================= 工具函数 =======================

// 构造 Key (24字节: 16字节前缀 + 8字节 CUID)
// 只有符合这个格式，HotspotManager 才能提取出 CUID
std::string Key(uint64_t cuid, int suffix) {
    char buf[24];
    memset(buf, 0, 24);
    // 模拟前缀 padding
    snprintf(buf, 16, "pad_%010d", 0); 
    
    // Big-Endian 写入 CUID (位于 [16-23])
    unsigned char* p = reinterpret_cast<unsigned char*>(buf) + 16;
    for(int i=0; i<8; ++i) p[i] = (cuid >> (56 - 8*i)) & 0xFF;
    
    // 把 suffix 拼接到最后或者作为 value，这里 key 长度定死 24 方便提取
    // 为了区分同一个 CUID 下的不同 key，我们可以稍作修改，让 key 变长一点
    // 或者简单点，同一个 CUID 我们测试只写一个 key，或者覆盖写
    std::string key(buf, 24);
    key += std::to_string(suffix); // 简单的 suffix
    return key;
}

void Check(bool cond, const std::string& msg) {
    if (cond) std::cout << "[PASS] " << msg << std::endl;
    else {
        std::cerr << "[FAIL] " << msg << std::endl;
        exit(1);
    }
}

// 获取 GDCT 引用计数
int GetRef(DB* db, uint64_t cuid) {
    auto mgr = static_cast<DBImpl*>(db)->GetHotspotManager();
    if (!mgr->GetDeleteTable().IsTracked(cuid)) return -1; // 不存在返回 -1
    return mgr->GetDeleteTable().GetRefCount(cuid);
}

// 获取是否逻辑删除
bool GetDeleted(DB* db, uint64_t cuid) {
    auto mgr = static_cast<DBImpl*>(db)->GetHotspotManager();
    return mgr->GetDeleteTable().IsDeleted(cuid);
}

// 获取 L0 文件数量
int NumL0(DB* db) {
    std::string val;
    db->GetProperty("rocksdb.num-files-at-level0", &val);
    return std::stoi(val);
}

// 获取 L1 文件数量
int NumL1(DB* db) {
    std::string val;
    db->GetProperty("rocksdb.num-files-at-level1", &val);
    return std::stoi(val);
}

int main() {
    // 0. 清理环境
    Options options;
    options.create_if_missing = true;
    DestroyDB(kDBPath, options);

    // 1. 配置参数
    options.disable_auto_compactions = true; // 手动控制
    options.level0_file_num_compaction_trigger = 4;
    options.num_levels = 3;
    
    DB* db = nullptr;
    Status s = DB::Open(options, kDBPath, &db);
    Check(s.ok(), "DB Open");

    const uint64_t CUID_A = 100; // 测试 GC
    const uint64_t CUID_B = 200; // 测试 Trivial Move

    // ==========================================================
    // 场景 1: 验证 Flush 注册 (Ref Count 0 -> 1)
    // ==========================================================
    std::cout << "\n--- Scenario 1: Flush Registration ---" << std::endl;
    
    WriteBatch batch;
    batch.Put(Key(CUID_A, 1), "val1");
    batch.Put(Key(CUID_B, 1), "val1");
    s = db->Write(WriteOptions(), &batch);
    Check(s.ok(), "Write Memtable");

    // 此时在 Memtable，引用计数应该是 0 (或者您实现了 Memtable 追踪，那就是 1)
    // 假设只追踪 SST：
    Check(GetRef(db, CUID_A) <= 0, "Before flush, Ref should be 0 (if only tracking SST)");

    s = db->Flush(FlushOptions());
    Check(s.ok(), "Flush to L0");

    // 关键验证点：Flush 后，CUID_A 必须被注册，且引用计数为 1
    int ref_a = GetRef(db, CUID_A);
    Check(ref_a == 1, "After flush, CUID_A Ref should be 1. Actual: " + std::to_string(ref_a));
    Check(NumL0(db) == 1, "L0 file count should be 1");

    // ==========================================================
    // 场景 2: 验证逻辑删除 (No Tombstone)
    // ==========================================================
    std::cout << "\n--- Scenario 2: Logical Delete ---" << std::endl;

    // 调用 RocksDB 标准 Delete 接口
    // 您的代码应该拦截它，不写 WAL/Memtable，直接改 GDCT
    s = db->Delete(WriteOptions(), Key(CUID_A, 1));
    Check(s.ok(), "Delete CUID_A");

    // 关键验证点 1: GDCT 标记为 Deleted
    Check(GetDeleted(db, CUID_A) == true, "CUID_A should be marked Deleted in GDCT");

    // 关键验证点 2: 引用计数 不变 (物理文件还在)
    Check(GetRef(db, CUID_A) == 1, "CUID_A Ref should still be 1 (File exists)");

    // 关键验证点 3: 读不到数据 (模拟 Tombstone 效果)
    std::string val;
    s = db->Get(ReadOptions(), Key(CUID_A, 1), &val);
    Check(s.IsNotFound(), "Get() should return NotFound for CUID_A");

    // CUID_B 没删，应该还能读到
    s = db->Get(ReadOptions(), Key(CUID_B, 1), &val);
    Check(s.ok(), "Get() should find CUID_B");

    // ==========================================================
    // 场景 3: 验证 Trivial Move (Smart Picker)
    // ==========================================================
    std::cout << "\n--- Scenario 3: Trivial Move (L0->L1) ---" << std::endl;
    
    // 目前 L0 有 1 个文件。L1 为空。
    // 根据 Smart Picker 逻辑，L0 和 L1 无重叠，应该触发 Trivial Move (直接移动文件)
    // 我们再写一个文件，凑够触发条件，或者直接手动 Compact
    
    // 触发 Compaction
    // 注意：CompactRange 内部可能会根据重叠情况决定是否 Trivial Move
    std::cout << "Triggering Compaction..." << std::endl;
    s = db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
    Check(s.ok(), "Compaction finished");

    // 验证：
    // 1. L0 空了，L1 有文件了
    Check(NumL0(db) == 0, "L0 should be empty");
    Check(NumL1(db) > 0, "L1 should have files");

    // 2. 检查 CUID_B (幸存数据) 的状态
    // 如果是 Trivial Move，文件 ID 没变，引用计数应该保持不变，或者先减后加
    // 最终状态必须是 Ref=1 (指向 L1 的那个文件)
    int ref_b = GetRef(db, CUID_B);
    Check(ref_b == 1, "CUID_B Ref should be 1 (Moved to L1). Actual: " + std::to_string(ref_b));
    Check(!GetDeleted(db, CUID_B), "CUID_B should NOT be deleted");

    // ==========================================================
    // 场景 4: 验证物理 GC (Physical Drop)
    // ==========================================================
    std::cout << "\n--- Scenario 4: Physical Garbage Collection ---" << std::endl;

    // 回顾：CUID_A 在场景 2 被逻辑删除了。
    // 在场景 3 的 Compaction 中，迭代器应该遇到了 CUID_A。
    // 因为 IsDeleted=true，迭代器应该 skip 掉它，不写入 L1 文件。
    // Compaction 结束后，CUID_A 对旧 L0 文件的引用被移除。
    // 因为没有写入新 L1 文件，它没有新引用。
    // Ref 归零 + IsDeleted -> GDCT 条目应该被 erase。

    // 验证：CUID_A 彻底消失 (Ref 返回 -1 表示 key 不存在)
    int final_ref_a = GetRef(db, CUID_A);
    
    if (final_ref_a == -1) {
        Check(true, "CUID_A successfully GC'ed (Entry removed from GDCT)");
    } else {
        // 调试信息
        std::cerr << "FAIL: CUID_A still tracked. Ref: " << final_ref_a 
                  << " Deleted: " << GetDeleted(db, CUID_A) << std::endl;
        exit(1);
    }

    std::cout << "\n===================================" << std::endl;
    std::cout << "Design Verification PASSED" << std::endl;
    std::cout << "===================================" << std::endl;

    delete db;
    return 0;
}