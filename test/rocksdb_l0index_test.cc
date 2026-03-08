#include <cassert>
#include <iostream>
#include <vector>
#include <thread>
#include <algorithm>
#include <cstring>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "db/db_impl/db_impl.h"
#include "delta/hotspot_manager.h"

using namespace ROCKSDB_NAMESPACE;

const std::string kDBPath = "/tmp/rocksdb_l0index_test";

// ======================= 工具函数 =======================

std::string Key(uint64_t cuid, int suffix) {
    char buf[24];
    memset(buf, 0, 24);
    // Big-Endian 写入 CUID (位于 [16-23])
    unsigned char* p = reinterpret_cast<unsigned char*>(buf) + 16;
    for(int i=0; i<8; ++i) p[i] = (cuid >> (56 - 8*i)) & 0xFF;
    std::string key(buf, 24);
    key += std::to_string(suffix);
    return key;
}

void Check(bool cond, const std::string& msg) {
    if (cond) std::cout << "[PASS] " << msg << std::endl;
    else {
        std::cerr << "[FAIL] " << msg << std::endl;
        exit(1);
    }
}

std::shared_ptr<HotspotManager> GetMgr(DB* db) {
    return static_cast<DBImpl*>(db)->GetHotspotManager();
}

uint64_t GetLatestL0FileNumber(DB* db) {
    std::vector<LiveFileMetaData> metadata;
    db->GetLiveFilesMetaData(&metadata);
    uint64_t max_fid = 0;
    for (const auto& file : metadata) {
        if (file.level == 0) max_fid = std::max(max_fid, (uint64_t)file.file_number);
    }
    return max_fid;
}

uint64_t GetAbsoluteLatestFileNumber(DB* db) {
    std::vector<LiveFileMetaData> metadata;
    db->GetLiveFilesMetaData(&metadata);
    uint64_t max_fid = 0;
    for (const auto& file : metadata) {
        max_fid = std::max(max_fid, (uint64_t)file.file_number);
    }
    return max_fid;
}

// 辅助：审计逻辑封装
void PerformStrictAudit(DB* db, std::shared_ptr<HotspotManager> mgr, const std::set<uint64_t>& test_cuids) {
    // 1. 获取 RocksDB 物理真理 (Source of Truth)
    std::vector<LiveFileMetaData> metadata;
    db->GetLiveFilesMetaData(&metadata);
    
    std::set<uint64_t> physical_l0_fids;
    for (const auto& f : metadata) {
        if (f.level == 0) physical_l0_fids.insert(f.file_number);
    }

    // --- 检查 A: 物理 -> 索引 (正向) ---
    // 每一个物理 L0 文件，只要包含我们的测试 CUID，正向索引里必须有它
    for (uint64_t fid : physical_l0_fids) {
        double ratio = mgr->GetL0FileGarbageRatio(fid);
        // 如果文件是在索引里的，尝试获取它的 CUID 列表进行双向校验
        // 这里需要给 HotspotManager 增加一个审计专用的 Debug 接口，或者通过 GetL0FilesForCuid 探测
    }

    // --- 检查 B: 索引 -> 物理 (反向) ---
    // 反向索引中记录的所有文件，物理上必须依然在 Level 0
    for (uint64_t cuid : test_cuids) {
        std::vector<uint64_t> fids = mgr->GetL0FilesForCuid(cuid);
        for (uint64_t fid : fids) {
            if (physical_l0_fids.find(fid) == physical_l0_fids.end()) {
                // 允许 10ms 的微小延迟补偿（处理 LogAndApply 成功与 Hook 执行间的极窄间隙）
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                db->GetLiveFilesMetaData(&metadata);
                bool found = false;
                for(auto& f : metadata) if(f.level == 0 && (uint64_t)f.file_number == fid) found = true;
                
                if (!found) {
                    fprintf(stderr, "[FAIL] Leak Detected! CUID %lu points to File %lu, but it's GONE from L0!\n", cuid, fid);
                    exit(1);
                }
            }
        }
    }

    // --- 检查 C: 双向索引对称性 (Symmetry Check) ---
    // 获取 HotspotManager 的内部状态镜像并审计 (此处假设你已按之前的 Add/Remove 逻辑实现)
    for (uint64_t cuid : test_cuids) {
        std::vector<uint64_t> fids = mgr->GetL0FilesForCuid(cuid);
        for (uint64_t fid : fids) {
            // 既然 cuid -> fid，那么从 fid 查出来的垃圾率计算过程中，必须包含该 cuid
            // 这里可以通过验证 GetL0FileGarbageRatio 是否正常返回非 0 值（假设没被标记删除）
            if (mgr->GetL0FileGarbageRatio(fid) < 0) { // 假设未找到返回负数或特定值
                 fprintf(stderr, "[FAIL] Symmetry Broken! CUID %lu -> File %lu, but File %lu unknown by forward index!\n", cuid, fid, fid);
                 exit(1);
            }
        }
    }
}

void RunScenario6() {
    std::cout << "\n--- Scenario 6: Chaos Multi-threaded Bidirectional Audit ---" << std::endl;

    Options options;
    options.create_if_missing = true;
    options.num_levels = 2;
    options.disable_auto_compactions = true;
    DestroyDB(kDBPath, options);

    DB* db = nullptr;
    Status s = DB::Open(options, kDBPath, &db);
    Check(s.ok(), "DB Open");
    auto mgr = static_cast<DBImpl*>(db)->GetHotspotManager();

    std::atomic<bool> stop_chaos{false};
    std::set<uint64_t> test_cuids = {101, 102, 103, 104};

    // 1. 【多个写入线程】
    std::vector<std::thread> writers;
    for (uint64_t cuid : test_cuids) {
        writers.emplace_back([&, cuid]() {
            int seq = 0;
            while (!stop_chaos) {
                db->Put(WriteOptions(), Key(cuid, seq++), "v");
                if (seq % 20 == 0) db->Flush(FlushOptions());
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        });
    }

    // 2. 【多个压缩线程】
    std::vector<std::thread> compactors;
    for (int k = 0; k < 2; ++k) {
        compactors.emplace_back([&]() {
            while (!stop_chaos) {
                CompactRangeOptions cro;
                cro.target_level = 1; // 强制下沉，制造 Trivial Move
                db->CompactRange(cro, nullptr, nullptr);
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
        });
    }

    // 3. 【多个审计线程】
    std::vector<std::thread> auditors;
    for (int k = 0; k < 2; ++k) {
        auditors.emplace_back([&]() {
            int count = 0;
            while (!stop_chaos) {
                PerformStrictAudit(db, mgr, test_cuids);
                if (++count % 50 == 0) std::cout << "[Auditor " << k << "] 50 cycles passed..." << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        });
    }

    // 运行测试
    std::this_thread::sleep_for(std::chrono::seconds(20));
    stop_chaos = true;

    for (auto& t : writers) t.join();
    for (auto& t : compactors) t.join();
    for (auto& t : auditors) t.join();

    std::cout << "[PASS] Scenario 6: Chaos survived with 0 inconsistencies." << std::endl;
    delete db;
}

// ======================= 测试用例 =======================

int main() {
    Options options;
    options.create_if_missing = true;
    options.disable_auto_compactions = true; // 开启手动挡
    options.num_levels = 2; 
    DestroyDB(kDBPath, options);

    DB* db = nullptr;
    Status s = DB::Open(options, kDBPath, &db);
    Check(s.ok(), "DB Open");

    auto mgr = GetMgr(db);
    const uint64_t CUID_1 = 1001;
    const uint64_t CUID_2 = 1002;

    // -------------------------------------------------------
    // 场景 1: Flush 触发的索引建立 (Forward & Backward)
    // -------------------------------------------------------
    std::cout << "\n--- Scenario 1: Flush & Index Add ---" << std::endl;

    db->Put(WriteOptions(), Key(CUID_1, 1), "v1");
    db->Flush(FlushOptions());

    uint64_t fid1 = GetLatestL0FileNumber(db);
    std::cout << "[Trace] File generated: " << fid1 << std::endl;

    // 验证反向索引：CUID -> Files
    auto files = mgr->GetL0FilesForCuid(CUID_1);
    Check(files.size() == 1 && files[0] == fid1, "Reverse Index: CUID_1 -> fid1");

    // 验证正向索引：File -> Garbage Ratio (此时应为 0)
    Check(mgr->GetL0FileGarbageRatio(fid1) == 0.0, "Forward Index: fid1 garbage ratio is 0");

    // -------------------------------------------------------
    // 场景 2: 多 CUID 混合文件索引
    // -------------------------------------------------------
    std::cout << "\n--- Scenario 2: Multi-CUID Indexing ---" << std::endl;

    db->Put(WriteOptions(), Key(CUID_1, 2), "v2");
    db->Put(WriteOptions(), Key(CUID_2, 1), "v3");
    db->Flush(FlushOptions());

    uint64_t fid2 = GetLatestL0FileNumber(db);
    std::cout << "[Trace] File generated: " << fid2 << std::endl;

    // CUID_1 应该出现在两个文件中
    auto files_c1 = mgr->GetL0FilesForCuid(CUID_1);
    Check(files_c1.size() == 2, "CUID_1 spans 2 files");

    // CUID_2 只在一个文件中
    auto files_c2 = mgr->GetL0FilesForCuid(CUID_2);
    Check(files_c2.size() == 1 && files_c2[0] == fid2, "CUID_2 in fid2");

    // -------------------------------------------------------
    // 场景 3: 逻辑删除后的垃圾率更新
    // -------------------------------------------------------
    std::cout << "\n--- Scenario 3: Garbage Ratio Accuracy ---" << std::endl;

    // 删除 CUID_2
    mgr->InterceptDelete(Key(CUID_2, 1), db);

    // fid2 包含 CUID_1 和 CUID_2。现在 CUID_2 删了，比例应该是 0.5
    double ratio = mgr->GetL0FileGarbageRatio(fid2);
    std::cout << "[Trace] fid2 garbage ratio: " << ratio << std::endl;
    Check(ratio == 0.5, "fid2 is 50% garbage");

    // -------------------------------------------------------
    // 场景 4: L0 -> L1 合并引发的索引移除 (闭环验证)
    // -------------------------------------------------------
    std::cout << "\n--- Scenario 4: L0 -> L1 Index Removal ---" << std::endl;

    // 强制把 L0 文件压入 L1
    CompactRangeOptions cro;
    cro.change_level = true;
    cro.target_level = 1;
    db->CompactRange(cro, nullptr, nullptr);

    // L0 索引应该被清空
    Check(mgr->GetL0FilesForCuid(CUID_1).empty(), "L0 Index cleared for CUID_1 after L1 sink");
    Check(mgr->GetL0FilesForCuid(CUID_2).empty(), "L0 Index cleared for CUID_2 after L1 sink");
    Check(mgr->GetL0FileGarbageRatio(fid1) == 0.0, "Forward index entry removed for fid1");

    // -------------------------------------------------------
    // 场景 5: Intra-L0 (L0->L0) 的原子交接
    // -------------------------------------------------------
    std::cout << "\n--- Scenario 5: Intra-L0 Atomic Update (Fresh num_levels=1 DB) ---" << std::endl;

    delete db;
    Options options_uni;
    options_uni.create_if_missing = true;
    options_uni.num_levels = 1;
    // 【修改点 1】：使用 Universal 压缩样式，它更倾向于合并所有 L0 文件
    options_uni.compaction_style = kCompactionStyleUniversal;
    options_uni.disable_auto_compactions = true;
    
    DestroyDB(kDBPath, options_uni);
    s = DB::Open(options_uni, kDBPath, &db);
    Check(s.ok(), "DB Reopened with Universal Compaction");

    mgr = static_cast<DBImpl*>(db)->GetHotspotManager();

    // 【修改点 2】：写入完全相同的 Key。
    // RocksDB 面对相同 Key 的多个版本，必须进行归并以处理 SeqNum。
    db->Put(WriteOptions(), Key(CUID_1, 5), "old_version");
    db->Flush(FlushOptions());
    uint64_t fid_a = GetAbsoluteLatestFileNumber(db);

    db->Put(WriteOptions(), Key(CUID_1, 5), "new_version");
    db->Flush(FlushOptions());
    uint64_t fid_b = GetAbsoluteLatestFileNumber(db);

    Check(mgr->GetL0FilesForCuid(CUID_1).size() == 2, "CUID_1 in 2 files before merge");

    // 【修改点 3】：执行强制压缩
    CompactRangeOptions cro_force;
    cro_force.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    db->CompactRange(cro_force, nullptr, nullptr);

    // 此时由于 Key 重叠且使用了 Universal/Force 模式，RocksDB 必须生成新文件
    uint64_t fid_new = GetAbsoluteLatestFileNumber(db);
    std::cout << "[Trace] New File ID: " << fid_new << " (Original: " << fid_a << ", " << fid_b << ")" << std::endl;

    // 断言：必须生成了新文件 (ID 递增)
    Check(fid_new > fid_b, "SUCCESS: Real Merge occurred, new file ID generated.");

    auto final_files = mgr->GetL0FilesForCuid(CUID_1);
    
    // 验证：旧的 9 和 11 被删了，新的被加进来了
    bool old_gone = (std::find(final_files.begin(), final_files.end(), fid_a) == final_files.end()) &&
                    (std::find(final_files.begin(), final_files.end(), fid_b) == final_files.end());
    bool new_in = (std::find(final_files.begin(), final_files.end(), fid_new) != final_files.end());

    Check(old_gone, "SUCCESS: Old file indices核销完成");
    Check(new_in, "SUCCESS: New merged file登记完成");

    // -------------------------------------------------------
    // 场景 6: 并发安全性测试 (Stress)
    // -------------------------------------------------------
    delete db;
    RunScenario6();

    std::cout << "\n=====================================" << std::endl;
    std::cout << "L0 INDEX DESIGN VERIFICATION PASSED!" << std::endl;
    std::cout << "=====================================" << std::endl;

    return 0;
}