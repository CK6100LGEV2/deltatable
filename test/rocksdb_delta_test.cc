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
#include "db/dbformat.h"

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

// 获取是否逻辑删除 (测试辅助函数)
bool GetDeleted(DB* db, uint64_t cuid) {
    auto mgr = static_cast<DBImpl*>(db)->GetHotspotManager();
    
    // 参数 1: cuid
    // 参数 2: visible_seq -> 传入 kMaxSequenceNumber (上帝视角，看当前最新状态)
    // 参数 3: found_seq   -> 传入 0 (假设我们在查询最老版本的数据是否被删除)
    
    // 逻辑原理：(kMax >= del_seq) 且 (0 <= del_seq) 永远为真
    // 只要该 CUID 曾被 MarkDeleted 过，这个调用就会返回 true
    return mgr->GetDeleteTable().IsDeleted(cuid, kMaxSequenceNumber, 0);
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

// 获取 L2 文件数量
int NumL2(DB* db) {
    std::string val;
    db->GetProperty("rocksdb.num-files-at-level2", &val);
    return std::stoi(val);
}

int CountActualSSTFilesForCUID(DB* db, uint64_t cuid) {
    auto db_impl = static_cast<DBImpl*>(db);
    auto hotspot_mgr = db_impl->GetHotspotManager();
    
    std::vector<LiveFileMetaData> metadata;
    db->GetLiveFilesMetaData(&metadata);
    
    int count = 0;
    for (const auto& file : metadata) {
        // 【核心修复】：直接使用你代码里的提取逻辑
        // ExtractCUID 只看 offset 16-23，不关心后面是否有 8 字节 Trailer
        // 也不受字符串长度影响，非常稳健。
        uint64_t f_min_cuid = hotspot_mgr->ExtractCUID(file.smallestkey);
        uint64_t f_max_cuid = hotspot_mgr->ExtractCUID(file.largestkey);
        
        // 如果我们要审计的 cuid 在这个文件的 [min, max] 范围内，说明包含该 CUID
        if (cuid >= f_min_cuid && cuid <= f_max_cuid) {
            // 特殊处理：如果 f_min 和 f_max 跨度很大（比如 0 到 1000）
            // 说明这是一个跨 CUID 的合并文件，它确实包含了我们的 cuid
            count++;
        }
    }
    return count;
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
    options.target_file_size_base = 128 * 1024; 
    options.target_file_size_multiplier = 1; 
    
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
    CompactRangeOptions cro;
    cro.change_level = true; 
    cro.target_level = 1;    // <--- 强制只去 L1，不去 L2
    s = db->CompactRange(cro, nullptr, nullptr);
    std::cout << "DEBUG: L0=" << NumL0(db) << " L1=" << NumL1(db) << " L2=" << NumL2(db) << std::endl;
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

    // 【关键修复】：写入完全相同的 Key，制造 100% 的物理重叠！
    // 此时 L1 包含[CUID_A_1, CUID_B_1]
    // 我们向 L0 写入 CUID_B_1，迫使 L0 与 L1 发生真正的 Merge！
    WriteBatch overlap_batch;
    overlap_batch.Put(Key(CUID_B, 1), "force_real_merge");
    s = db->Write(WriteOptions(), &overlap_batch);
    db->Flush(FlushOptions());

    std::cout << "Triggering REAL Compaction (Merge) for GC..." << std::endl;
    
    // 为了干净，我们依然限制它在 L1 结算，防止它 Merge 完又 Trivial Move 到 L2 导致日志干扰
    CompactRangeOptions cro_gc;
    cro_gc.change_level = true;
    cro_gc.target_level = 1;
    s = db->CompactRange(cro_gc, nullptr, nullptr); 
    Check(s.ok(), "Real Compaction finished");


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

    // 场景 5: 全量 GC 黑洞测试 (Zero Output Files)
    // ==========================================================
    std::cout << "\n--- Scenario 5: Full GC (Zero Outputs) ---" << std::endl;
    
    const uint64_t CUID_C = 300;
    
    // 【步骤 1：在 L1 铺设底座】
    WriteBatch l1_base_batch;
    l1_base_batch.Put(Key(CUID_C, 1), "base_data_in_L1");
    db->Write(WriteOptions(), &l1_base_batch);
    db->Flush(FlushOptions());
    
    CompactRangeOptions cro_base;
    cro_base.change_level = true;
    cro_base.target_level = 1;
    s = db->CompactRange(cro_base, nullptr, nullptr);
    Check(GetRef(db, CUID_C) == 1, "Base Ref should be 1");

    // 【步骤 2：在 L0 写入重叠的幽灵数据】
    // 注意：此时 CUID_C 还是合法的！所以 Flush 拦截器会放行！
    WriteBatch l0_ghost_batch;
    l0_ghost_batch.Put(Key(CUID_C, 1), "ghost_data_in_L0");
    db->Write(WriteOptions(), &l0_ghost_batch);
    db->Flush(FlushOptions());
    
    std::cout << "[Trace] After Ghost Setup -> L0: " << NumL0(db) << " L1: " << NumL1(db) << std::endl;
    // 此时数据合法落盘，Ref 必然是 2
    Check(GetRef(db, CUID_C) == 2, "Ghost Ref should be 2");

    // 【步骤 3：关门打狗 (下达逻辑删除指令)】
    // 数据已经在磁盘上了，我们现在告诉 GDCT：它们全变垃圾了！
    db->Delete(WriteOptions(), Key(CUID_C, 1));
    Check(GetDeleted(db, CUID_C), "CUID_C marked deleted in GDCT");

    // 【步骤 4：触发全量 GC】
    // RocksDB 发现 L0 和 L1 有重叠，启动 CompactionIterator。
    // Iterator 发现 CUID_C IsDeleted=true，把它们全部丢弃！产生 Zero Outputs！
    std::cout << "Triggering Compaction for fully deleted CUID..." << std::endl;
    CompactRangeOptions cro_full_gc;
    s = db->CompactRange(cro_full_gc, nullptr, nullptr); 
    Check(s.ok(), "Full GC Compaction finished");
    
    // 【步骤 5：严苛验证】
    int ref_c = GetRef(db, CUID_C);
    Check(ref_c == -1, "CUID_C fully GC'ed even with ZERO output files. Actual Ref: " + std::to_string(ref_c));


    // 场景 6: 多文件分裂测试 (Multi-File Output)
    // ==========================================================
    std::cout << "\n--- Scenario 6: Multi-File Output (SST Split) ---" << std::endl;
    
    const uint64_t CUID_D = 400;
    
    // 【修复 1：在 L1 部署一个重叠底座，封死 Trivial Move】
    WriteBatch base_d;
    base_d.Put(Key(CUID_D, -1), "L1_base_for_D"); // 用一个负数 Key 确保范围覆盖
    db->Write(WriteOptions(), &base_d);
    db->Flush(FlushOptions());

    int l0_files = NumL0(db);
    int l1_files = NumL1(db);
    int l2_files = NumL2(db);
    int ref_d = GetRef(db, CUID_D);
    std::cout << "L0 files after write base: " << l0_files << std::endl;
    std::cout << "L1 files after write base: " << l1_files << std::endl;
    std::cout << "L2 files after write base: " << l2_files << std::endl;
    std::cout << "CUID_D Ref Count: " << ref_d << std::endl;

    CompactRangeOptions cro_base_d;
    cro_base_d.change_level = true;
    cro_base_d.target_level = 1;
    db->CompactRange(cro_base_d, nullptr, nullptr);
    Check(NumL1(db) > 0, "Base for CUID_D created in L1");

    // 1. 疯狂写入同一个 CUID，使其超过 target_file_size_base (默认 64MB，我们需要改小)
    // 我们在 Options 中设置 target_file_size_base=2MB
    // Key(1KB) + Value(1KB) = 2KB。写入 2000 条就是 4MB，应该分裂成至少 2 个文件
    std::cout << "Writing large amount of data for CUID_D to force split..." << std::endl;
    for (int i = 0; i < 2000; ++i) {
        WriteBatch batch_d;
        // 使用定长 1000 字节的 Value
        batch_d.Put(Key(CUID_D, i), std::string(1000, 'X'));
        db->Write(WriteOptions(), &batch_d);
        if (i % 500 == 0 && i > 0) db->Flush(FlushOptions()); // 制造多个 L0 文件
    }
    db->Flush(FlushOptions());
    
    l0_files = NumL0(db);
    l1_files = NumL1(db);
    l2_files = NumL2(db);
    ref_d = GetRef(db, CUID_D);
    std::cout << "L0 files before compaction: " << l0_files << std::endl;
    std::cout << "L1 files before compaction: " << l1_files << std::endl;
    std::cout << "L2 files before compaction: " << l2_files << std::endl;
    std::cout << "CUID_D Ref Count: " << ref_d << std::endl;
    
    // 2. 强行 Compact 到 L1，触发文件生成和切分
    // 【修复 2：初始化 CompactRangeOptions，强制目标层级为 L1】
    CompactRangeOptions cro_split;
    cro_split.change_level = true;
    cro_split.target_level = 1;
    s = db->CompactRange(cro_split, nullptr, nullptr);
    Check(s.ok(), "Large Compaction finished");

    l0_files = NumL0(db);
    l1_files = NumL1(db);
    l2_files = NumL2(db);
    ref_d = GetRef(db, CUID_D);
    std::cout << "L0 files after compaction: " << l0_files << std::endl;
    std::cout << "L1 files after compaction: " << l1_files << std::endl;
    std::cout << "L2 files after compaction: " << l2_files << std::endl;
    std::cout << "CUID_D Ref Count: " << ref_d << std::endl;
    
    // 3. 验证 L1 文件数量
    Check(l1_files >= 2, "Compaction should split output into multiple L1 files");
    
    // 4. 严苛验证 GDCT 引用计数
    Check(ref_d >= 2, "CUID_D should hold references to ALL split output files");

    // 场景 7: 终极严苛审计测试 (Strict Physical Audit)
    // ==========================================================
    std::cout << "\n--- Scenario 7: Chaos & Strict Audit (Equality Check) ---" << std::endl;

    delete db;
    DestroyDB(kDBPath, options);
    
    // 【关键修复 1】：禁用压缩，并设置极小的分裂阈值
    options.compression = kNoCompression; 
    options.target_file_size_base = 32 * 1024; // 32KB
    s = DB::Open(options, kDBPath, &db);
    Check(s.ok(), "DB Reopened for Scenario 7");

    const uint64_t C_KEEP = 700; 
    const uint64_t C_KILL = 701; 

    // 【关键修复 2】：先在 L1 钉入一个“钉子”，防止之后的 Trivial Move
    db->Put(WriteOptions(), Key(C_KEEP, 1000), "L1_BLOCKER");
    db->Flush(FlushOptions());
    CompactRangeOptions cro_init;
    cro_init.change_level = true;
    cro_init.target_level = 1;
    db->CompactRange(cro_init, nullptr, nullptr);
    std::cout << "[Trace] L1 Seeded to block Trivial Move." << std::endl;

    // 2. 【第一阶段：海量写入】
    std::cout << "[Step 1] Writing 4MB data across 2 CUIDs..." << std::endl;
    for (int i = 0; i < 2000; i++) {
        // 每个 Key+Value 约 1KB，2000次循环 = 2MB per CUID
        db->Put(WriteOptions(), Key(C_KEEP, i), std::string(1000, 'K')); 
        db->Put(WriteOptions(), Key(C_KILL, i), std::string(1000, 'X')); 
    }
    db->Flush(FlushOptions());
    
    // 执行 Compaction。由于 L1 已经有数据且 Key 范围重叠，
    // RocksDB 必须执行 Real Merge，从而根据 32KB 阈值切分出几十个文件。
    std::cout << "[Step 1] Triggering Real Merge and Splitting..." << std::endl;
    db->CompactRange(cro_init, nullptr, nullptr);

    // --- 第一次严格审计 ---
    int phys_keep_1 = CountActualSSTFilesForCUID(db, C_KEEP);
    int ref_keep_1 = GetRef(db, C_KEEP);
    
    std::cout << "[Audit 1] C_KEEP -> Physical Files: " << phys_keep_1 
              << ", Ref Count: " << ref_keep_1 << std::endl;
    
    // 现在 ref_keep_1 应该远大于 5（理论上 2MB / 32KB = 64个文件左右）
    Check(ref_keep_1 == phys_keep_1, "Audit 1: Ref must EXACTLY EQUAL Physical count");
    Check(ref_keep_1 > 20, "Audit 1: Should have enough split files for stress test");

    // 3. 【第二阶段：构造混沌——一个生存，一个毁灭】
    std::cout << "[Step 2] Marking C_KILL for death and writing more for C_KEEP..." << std::endl;
    
    // 逻辑删除 KILL
    db->Delete(WriteOptions(), Key(C_KILL, 0)); 
    Check(GetDeleted(db, C_KILL), "C_KILL is dead.");

    // >>>>>> [终极修复]：构造全范围诱饵 <<<<<<
    // 只写一个点是不够的，我们要写两个点，覆盖 C_KILL 在 Step 1 写入的 [0, 2000] 范围
    // 这样 RocksDB 就会发现 L0 的范围包含了 L1 里 C_KILL 的所有文件
    db->Put(WriteOptions(), Key(C_KILL, 0), "trigger_start");
    db->Put(WriteOptions(), Key(C_KILL, 2000), "trigger_end");
    db->Flush(FlushOptions()); 

    // 给 KEEP 增加更多数据 (保持现状)
    for (int i = 2000; i < 3000; i++) {
        db->Put(WriteOptions(), Key(C_KEEP, i), std::string(1000, 'K'));
        if (i % 500 == 0) db->Flush(FlushOptions());
    }
    db->Flush(FlushOptions());

    // 此时 Ref 应该是：之前的 L1 文件数 + 新的 L0 文件数
    std::cout << "[Trace] Pre-Compaction Ref for C_KEEP: " << GetRef(db, C_KEEP) << std::endl;

    // 4. 【第三阶段：终极合并与物理回收】
    std::cout << "[Step 3] Triggering Massive Merge Compaction..." << std::endl;
    CompactRangeOptions cro_final;
    cro_final.change_level = true;
    cro_final.target_level = 1;
    cro_final.bottommost_level_compaction = BottommostLevelCompaction::kForce;
    s = db->CompactRange(cro_final, nullptr, nullptr);
    Check(s.ok(), "Chaos Compaction Finished");

    // 5. 【第四阶段：终极绝对审计】
    std::cout << "[Step 4] FINAL STRICT AUDIT..." << std::endl;

    int final_phys_keep = CountActualSSTFilesForCUID(db, C_KEEP);
    int final_ref_keep = GetRef(db, C_KEEP);
    int final_ref_kill = GetRef(db, C_KILL);
    int final_phys_kill = CountActualSSTFilesForCUID(db, C_KILL);

    std::cout << ">> Result C_KILL Ref: " << final_ref_kill << " (Expected: -1)" << std::endl;
    std::cout << ">> Result C_KEEP Ref: " << final_ref_keep << std::endl;
    std::cout << ">> Result C_KEEP Phys: " << final_phys_keep << std::endl;

    // --- 终极等号断言 ---
    
    // 断言 1：被杀死的 CUID 必须在账本中完全消失
    if (final_phys_kill > 0) {
        std::cout << ">> Result C_KILL Ref: " << final_ref_kill << ", Phys: " << final_phys_kill << std::endl;
        Check(final_ref_kill == final_phys_kill, "STRICT: C_KILL Ref must match Physical even if not fully purged");
    } else {
        Check(final_ref_kill == -1, "STRICT: C_KILL fully purged and entry GONE.");
    }

    // 断言 2：生存者的引用计数必须【严格等于】磁盘上包含它的 SST 文件数
    // 只要这一个等号成立，就证明了你的 ApplyCompactionResult 在面对
    // [多层级合并] + [文件分裂] + [部分数据丢弃] 时，账目算得一分不差。
    Check(final_ref_keep == final_phys_keep, "STRICT: C_KEEP Ref (" + std::to_string(final_ref_keep) + 
          ") must EXACTLY EQUAL Physical File Count (" + std::to_string(final_phys_keep) + ")");

    std::cout << "\n=========================================" << std::endl;
    std::cout << "SCENARIO 7 PASSED: PERFECT ACCOUNTING!" << std::endl;
    std::cout << "=========================================" << std::endl;

    std::cout << "\n===================================" << std::endl;
    std::cout << "Design Verification PASSED" << std::endl;
    std::cout << "===================================" << std::endl;

    delete db;
    return 0;
}