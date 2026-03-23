#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <iomanip>
#include <random>
#include <cmath>
#include <cstring>
#include <algorithm>
#include <memory>
#include <mutex>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/write_batch.h"

#include "delta/hotspot_manager.h"
#include "delta/global_delete_count_table.h"
#include "delta/cuid_partitioner.h"
#include "delta/cuid_prefix_extractor.h"
#include "test/delta_test_options.h"
#include "db/db_impl/db_impl.h"

using namespace ROCKSDB_NAMESPACE;

// ======================= 负载参数 =======================
const std::string kDBPath = "/tmp/rocksdb_delta_cf16_bench";
const int kNumThreads = 8;
const int kNumPartitions = 16; // 16 分区 (Column Families)

const uint64_t kTotalCUIDs = 100000;
const uint64_t kHotPoolSize = 1500;
const uint64_t kScanThresholdMin = 4000; 
const uint64_t kScanThresholdMax = 6000;
const int kValueSize = 1024;
const int kBatchSize = 128;
const int kTestDurationSecs = 300;

std::atomic<uint64_t> g_hot_pool[kHotPoolSize];
std::atomic<uint64_t> g_cold_survival_idx{kHotPoolSize};

// 状态跟踪
std::vector<uint64_t> g_cuid_thresholds(kTotalCUIDs);
std::vector<std::atomic<uint64_t>> g_cuid_seqs(kTotalCUIDs);
std::vector<std::atomic<bool>> g_cuid_dead(kTotalCUIDs);
std::atomic<bool> g_stop_flag{false};
std::chrono::steady_clock::time_point g_start_time;

// 指标统计
class LatencyHistogram {
public:
    LatencyHistogram() : buckets_(3000, 0), total_count_(0) {}
    void Record(uint64_t latency_us) {
        if (latency_us == 0) latency_us = 1;
        size_t bucket = 0;
        if (latency_us > 0) bucket = static_cast<size_t>(std::log10((double)latency_us) * 100);
        if (bucket >= buckets_.size()) bucket = buckets_.size() - 1;
        buckets_[bucket]++;
        total_count_++;
    }
    void Merge(const LatencyHistogram& other) {
        for (size_t i = 0; i < buckets_.size(); ++i) buckets_[i] += other.buckets_[i];
        total_count_ += other.total_count_;
    }
    uint64_t Percentile(double p) const {
        if (total_count_ == 0) return 0;
        uint64_t target = static_cast<uint64_t>(total_count_ * p);
        uint64_t sum = 0;
        for (size_t i = 0; i < buckets_.size(); ++i) {
            sum += buckets_[i];
            if (sum >= target) return static_cast<uint64_t>(std::pow(10, i / 100.0));
        }
        return 0;
    }
private:
    std::vector<uint64_t> buckets_;
    uint64_t total_count_;
};

struct ThreadMetrics {
    uint64_t write_ops = 0;
    uint64_t merge_ops = 0;
    uint64_t scanned_records = 0;
    uint64_t whitelist_files_total = 0;
    uint64_t partial_scan_ops = 0;
    uint64_t partial_scanned_records = 0;
    LatencyHistogram write_latencies;
    LatencyHistogram merge_latencies;
    LatencyHistogram partial_scan_latencies;

    void Merge(const ThreadMetrics& other) {
        write_ops += other.write_ops;
        merge_ops += other.merge_ops;
        scanned_records += other.scanned_records;
        whitelist_files_total += other.whitelist_files_total;
        partial_scan_ops += other.partial_scan_ops;
        partial_scanned_records += other.partial_scanned_records;
        write_latencies.Merge(other.write_latencies);
        merge_latencies.Merge(other.merge_latencies);
        partial_scan_latencies.Merge(other.partial_scan_latencies);
    }
};
std::vector<ThreadMetrics> g_thread_metrics(kNumThreads);

inline void EncodeDeltaKeyInPlace(char* buf, uint64_t cuid, uint64_t seq) {
    uint64_t cuid_be = __builtin_bswap64(cuid);
    uint64_t seq_be = __builtin_bswap64(seq);
    std::memcpy(buf + 16, &cuid_be, 8);
    std::memcpy(buf + 24, &seq_be, 8);
}

// 【新增】路由函数：根据 CUID 定位所属的 CF
inline uint32_t GetCFIndex(uint64_t cuid) {
    return std::hash<uint64_t>{}(cuid) % kNumPartitions;
}

// ======================= 工作线程 (16区路由版) =======================
// 【修改】传入 vector<ColumnFamilyHandle*>
void WorkerThread(DB* db, const std::vector<ColumnFamilyHandle*>& handles, int thread_id) {
    uint64_t seed = (uint64_t)std::chrono::high_resolution_clock::now().time_since_epoch().count() ^ thread_id;
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int> dist_100(0, 99);

    ThreadMetrics& metrics = g_thread_metrics[thread_id];
    Slice value_slice(std::string(kValueSize, 'D')); 

    DBImpl* db_impl = static_cast<DBImpl*>(db);
    auto hotspot_mgr = db_impl->GetHotspotManager();

    uint64_t my_hot_start = thread_id * (kHotPoolSize / kNumThreads);
    uint64_t my_hot_end = (thread_id + 1) * (kHotPoolSize / kNumThreads) - 1;
    std::uniform_int_distribution<uint64_t> hot_dist(my_hot_start, my_hot_end);

    uint64_t sticky_hot_slots[2];
    sticky_hot_slots[0] = hot_dist(rng);
    sticky_hot_slots[1] = hot_dist(rng);

    uint64_t my_local_cold_cuid = g_cold_survival_idx.fetch_add(1, std::memory_order_relaxed);

    WriteBatch batch;
    WriteBatch del_batch;
    std::vector<std::pair<uint64_t, int>> death_candidates;
    std::vector<uint64_t> partial_scan_candidates;

    char key_buf[32];
    std::memcpy(key_buf, "dws_p_0000000000", 16);

    while (!g_stop_flag.load(std::memory_order_relaxed)) {
        batch.Clear();
        death_candidates.clear();
        partial_scan_candidates.clear();

        for (int i = 0; i < kBatchSize; ++i) {
            bool is_hot = (dist_100(rng) < 77);
            uint64_t cuid = 0;
            int hot_slot = -1;

            if (is_hot) {
                int s_idx = rng() % 2; 
                hot_slot = sticky_hot_slots[s_idx];
                cuid = g_hot_pool[hot_slot].load(std::memory_order_relaxed);
            } else {
                cuid = my_local_cold_cuid;
            }

            if (cuid >= kTotalCUIDs) break; 

            uint64_t threshold = g_cuid_thresholds[cuid];
            uint64_t seq = g_cuid_seqs[cuid].fetch_add(1, std::memory_order_relaxed);

            if (seq >= threshold) {
                if (is_hot) {
                    uint64_t replacement = g_cold_survival_idx.fetch_add(1, std::memory_order_relaxed);
                    if (replacement < kTotalCUIDs) {
                        g_hot_pool[hot_slot].store(replacement, std::memory_order_relaxed);
                    }
                } else {
                    my_local_cold_cuid = g_cold_survival_idx.fetch_add(1, std::memory_order_relaxed);
                }
                --i; continue;
            }

            // 【核心修改】：模拟覆盖写 (Update/Overwrite)
            // 不再使用单调递增的 seq 作为 Key 的末尾，而是使用随机 RowID。
            // 这样新生成的 L0 文件 Key 范围会与 L1 里的旧数据产生物理重叠，
            // 从而禁用 Trivial Move，逼迫 Smart Picker 执行真正的归并计算。
            // 建议范围设在 0 ~ 5000 左右，确保 Key 空间既紧凑又有足够的碰撞概率。
            uint64_t random_row_id = rng() % 5000; 
            EncodeDeltaKeyInPlace(key_buf, cuid, random_row_id);
            
            // 【核心路由】：定向写入特定的 CF
            uint32_t cf_idx = GetCFIndex(cuid);
            batch.Put(handles[cf_idx], Slice(key_buf, 32), value_slice);

            uint64_t p_scan_interval = threshold / 5;
            if (p_scan_interval > 0 && seq > 0 && seq % p_scan_interval == 0 && seq < threshold - 1) {
                partial_scan_candidates.push_back(cuid);
            }

            if (seq == threshold - 1) {
                death_candidates.push_back({cuid, hot_slot});
            }
        }

        // 批量无锁写入
        auto t_w_start = std::chrono::steady_clock::now();
        WriteOptions w_opts;
        w_opts.disableWAL = true;  
        db->Write(w_opts, &batch);
        metrics.write_latencies.Record(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t_w_start).count());
        metrics.write_ops += batch.Count();

        // ================== Partial Scan ==================
        for (uint64_t cuid : partial_scan_candidates) {
            auto t_ps_start = std::chrono::steady_clock::now();
            ReadOptions scan_ropts;
            scan_ropts.readahead_size = 512 * 1024;
            
            std::vector<uint64_t> l0_whitelist;
            if (hotspot_mgr) {
                l0_whitelist = hotspot_mgr->GetL0FilesForCuid(cuid);
                if (!l0_whitelist.empty()) scan_ropts.l0_file_whitelist = &l0_whitelist;
            }
            
            // 【核心路由】：只在对应的 CF 中查找
            uint32_t cf_idx = GetCFIndex(cuid);
            std::unique_ptr<Iterator> it(db->NewIterator(scan_ropts, handles[cf_idx]));
            
            uint64_t current_seq = g_cuid_seqs[cuid].load(std::memory_order_relaxed);
            uint64_t start_seq = current_seq > 100 ? (rng() % (current_seq - 50)) : 0;
            EncodeDeltaKeyInPlace(key_buf, cuid, start_seq);
            it->Seek(Slice(key_buf, 32));
            
            uint64_t count = 0;
            while (it->Valid() && count < 100) { 
                if (it->key().size() < 24 || std::memcmp(it->key().data(), key_buf, 24) != 0) break;
                count++; it->Next();
            }
            metrics.partial_scan_latencies.Record(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t_ps_start).count());
            metrics.partial_scan_ops++;
            metrics.partial_scanned_records += count;
        }

        // ================== Full Scan + Delete ==================
        for (auto& death : death_candidates) {
            uint64_t cuid = death.first;
            auto t_m_start = std::chrono::steady_clock::now();
            ReadOptions scan_ropts;
            scan_ropts.readahead_size = 1024 * 1024;
            
            std::vector<uint64_t> l0_whitelist;
            if (hotspot_mgr) {
                l0_whitelist = hotspot_mgr->GetL0FilesForCuid(cuid);
                if (!l0_whitelist.empty()) {
                    scan_ropts.l0_file_whitelist = &l0_whitelist;
                    metrics.whitelist_files_total += l0_whitelist.size();
                }
            }
            
            // 【核心路由】：在对应的 CF 中执行全量查询与删除
            uint32_t cf_idx = GetCFIndex(cuid);
            std::unique_ptr<Iterator> it(db->NewIterator(scan_ropts, handles[cf_idx]));
            
            EncodeDeltaKeyInPlace(key_buf, cuid, 0);
            it->Seek(Slice(key_buf, 32));
            
            WriteOptions del_opts;
            del_opts.disableWAL = true;
            uint64_t count = 0;
            while (it->Valid()) {
                if (it->key().size() < 24 || std::memcmp(it->key().data(), key_buf, 24) != 0) break;
                
                // 【核心路由】：针对指定的 CF 执行删除，触发 GDCT 拦截
                db->Delete(del_opts, handles[cf_idx], it->key());
                count++; 
                it->Next();
            }
            // db->Write(WriteOptions(), &del_batch);
            
            metrics.merge_latencies.Record(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - t_m_start).count());
            metrics.merge_ops++;
            metrics.scanned_records += count;
            g_cuid_dead[cuid].store(true, std::memory_order_relaxed);
        }
    }
}

int main() {
    std::mt19937_64 init_rng(98765);
    std::uniform_int_distribution<uint64_t> thresh_dist(kScanThresholdMin, kScanThresholdMax);
    std::uniform_int_distribution<uint64_t> seq_init_dist(0, 1500); 

    for (uint64_t i = 0; i < kTotalCUIDs; ++i) {
        g_cuid_thresholds[i] = thresh_dist(init_rng);
        g_cuid_seqs[i] = (i < kHotPoolSize) ? seq_init_dist(init_rng) : 0;
        g_cuid_dead[i] = false;
    }
    for (uint64_t i = 0; i < kHotPoolSize; ++i) g_hot_pool[i].store(i);

    // ================== CF 初始化流程 ==================
    Options db_options;
    db_options.create_if_missing = true;
    db_options.statistics = CreateDBStatistics();
    
    ColumnFamilyOptions cf_options;
    // 【针对 16 分区的核心内存配置】
    // 将总预算均摊到 16 个区，保持每个区的 Flush 快大小适中
    // cf_options.write_buffer_size = 16 * 1024 * 1024; // 16MB per CF
    // cf_options.max_write_buffer_number = 2;
    // cf_options.level0_file_num_compaction_trigger = 8;
    // cf_options.level0_slowdown_writes_trigger = 40;
    // cf_options.level0_stop_writes_trigger = 60;
    
    OptimizeOptionsForDeltaNVME(cf_options);

    // 1. 创建基础库并预建 CF
    DestroyDB(kDBPath, db_options);
    DB* db_temp;
    Status s = DB::Open(db_options, kDBPath, &db_temp);
    if (!s.ok()) return 1;
    
    std::vector<ColumnFamilyHandle*> temp_handles;
    for (int i = 1; i < kNumPartitions; ++i) {
        ColumnFamilyHandle* cf;
        db_temp->CreateColumnFamily(cf_options, "cf_" + std::to_string(i), &cf);
        temp_handles.push_back(cf);
    }
    for (auto h : temp_handles) db_temp->DestroyColumnFamilyHandle(h);
    delete db_temp; 

    // 2. 正式挂载全量 CF 启动
    std::vector<ColumnFamilyDescriptor> cf_descriptors;
    cf_descriptors.push_back(ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
    for (int i = 1; i < kNumPartitions; ++i) {
        cf_descriptors.push_back(ColumnFamilyDescriptor("cf_" + std::to_string(i), cf_options));
    }

    std::vector<ColumnFamilyHandle*> handles;
    DB* db = nullptr;
    s = DB::Open(db_options, kDBPath, cf_descriptors, &handles, &db);
    if (!s.ok()) {
        std::cerr << "Multi-CF DB Open Failed: " << s.ToString() << std::endl;
        return 1;
    }

    std::cout << ">>> 16-PARTITIONS HTAP BENCH: Sticky Locality & Hash Routing <<<\n";

    g_start_time = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    for (int i = 0; i < kNumThreads; ++i) threads.emplace_back(WorkerThread, db, std::ref(handles), i);

    for (int i = 0; i < kTestDurationSecs; ++i) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (i > 0 && i % 10 == 0) {
            // 汇总 16 个分区的全局 LSM 状态
            int total_l0 = 0, total_l1 = 0;
            for (auto h : handles) {
                std::string l0, l1;
                db->GetProperty(h, "rocksdb.num-files-at-level0", &l0);
                db->GetProperty(h, "rocksdb.num-files-at-level1", &l1);
                total_l0 += std::stoi(l0);
                total_l1 += std::stoi(l1);
            }
            std::cout << "[Time: " << i << "s] Global L0: " << total_l0 << " | Global L1: " << total_l1 << std::endl;
        }
    }

    g_stop_flag.store(true);
    for (auto& t : threads) t.join();

    ThreadMetrics total;
    for (const auto& m : g_thread_metrics) total.Merge(m);

    double duration = kTestDurationSecs;
    std::cout << "\n==================== TEST RESULT SUMMARY ====================\n";
    std::cout << "[吞吐量维度]\n";
    std::cout << "  写入 TPS           : " << std::fixed << std::setprecision(1) << total.write_ops / duration << " ops/s\n";
    std::cout << "\n[读负载维度 (HTAP)]\n";
    std::cout << "  Partial Scan 频率  : " << total.partial_scan_ops / duration << " scans/s\n";
    std::cout << "  Delta Merge 频率   : " << total.merge_ops / duration << " merges/s\n";
    std::cout << "\n[读路径加速维度]\n";
    double avg_whitelist = total.merge_ops > 0 ? (double)total.whitelist_files_total / total.merge_ops : 0;
    std::cout << "  白名单命中(L0精准率): " << std::fixed << std::setprecision(2) << avg_whitelist << " files/merge\n";
    std::cout << "\n[延迟分布 (微秒)]\n";
    std::cout << "  Part. Scan  | P50: " << total.partial_scan_latencies.Percentile(0.5) << " | P99: " << total.partial_scan_latencies.Percentile(0.99) << "\n";
    std::cout << "  Full Merge  | P50: " << total.merge_latencies.Percentile(0.5) << " | P99: " << total.merge_latencies.Percentile(0.99) << "\n";
    std::cout << "\n[写放大]\n";
    uint64_t bytes_written = db_options.statistics->getTickerCount(BYTES_WRITTEN);
    uint64_t disk_write = db_options.statistics->getTickerCount(FLUSH_WRITE_BYTES) + db_options.statistics->getTickerCount(COMPACT_WRITE_BYTES);
    std::cout << "  系统写放大 (WA)    : " << (bytes_written > 0 ? (double)disk_write / bytes_written : 0) << " x\n";

    // 释放并清理所有句柄
    int final_l0 = 0, final_l1 = 0;
    for (auto h : handles) {
        std::string l0, l1;
        db->GetProperty(h, "rocksdb.num-files-at-level0", &l0);
        db->GetProperty(h, "rocksdb.num-files-at-level1", &l1);
        final_l0 += std::stoi(l0);
        final_l1 += std::stoi(l1);
        db->DestroyColumnFamilyHandle(h);
    }
    std::cout << "  最终 LSM 状态 (16区): Global L0=" << final_l0 << ", Global L1=" << final_l1 << "\n";
    std::cout << "=============================================================\n";

    delete db;
    return 0;
}