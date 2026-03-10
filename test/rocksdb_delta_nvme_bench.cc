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

// RocksDB 核心库
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/write_batch.h"

// Delta 优化组件
#include "delta/hotspot_manager.h"
#include "delta/global_delete_count_table.h"
#include "delta/cuid_partitioner.h"
#include "delta/cuid_prefix_extractor.h"
#include "test/delta_test_options.h"
#include "db/db_impl/db_impl.h"

using namespace ROCKSDB_NAMESPACE;

// ======================= 负载参数 =======================
const std::string kDBPath = "/tmp/rocksdb_delta_optimized_bench";
const int kNumThreads = 32;
const uint64_t kTotalCUIDs = 100000;
const uint64_t kHotPoolSize = 1500; 
const uint64_t kScanThresholdMin = 4000; 
const uint64_t kScanThresholdMax = 6000; 
const int kValueSize = 1024;              
const int kBatchSize = 100; 
const int kTestDurationSecs = 180; 

// ======================= 动态活跃池 (完全无锁设计) =======================
std::atomic<uint64_t> g_hot_pool[kHotPoolSize];
// 使用原子下标替代全局 Vector 锁
std::atomic<uint64_t> g_cold_survival_idx{kHotPoolSize};

// 状态跟踪
std::vector<uint64_t> g_cuid_thresholds(kTotalCUIDs);
std::vector<std::atomic<uint64_t>> g_cuid_seqs(kTotalCUIDs);
std::vector<std::atomic<bool>> g_cuid_dead(kTotalCUIDs); 
std::atomic<bool> g_stop_flag{false};
std::chrono::steady_clock::time_point g_start_time;

// ======================= 指标统计 =======================
class LatencyHistogram {
public:
    LatencyHistogram() : buckets_(3000, 0), total_count_(0) {}
    void Record(uint64_t latency_us) {
        if (latency_us == 0) latency_us = 1;
        size_t bucket = static_cast<size_t>(std::log10(latency_us) * 100);
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
    LatencyHistogram write_latencies;
    LatencyHistogram merge_latencies;

    void Merge(const ThreadMetrics& other) {
        write_ops += other.write_ops;
        merge_ops += other.merge_ops;
        scanned_records += other.scanned_records;
        whitelist_files_total += other.whitelist_files_total;
        write_latencies.Merge(other.write_latencies);
        merge_latencies.Merge(other.merge_latencies);
    }
};
std::vector<ThreadMetrics> g_thread_metrics(kNumThreads);

// ======================= 工具函数 =======================
std::string EncodeDeltaKey(uint64_t cuid, uint64_t seq) {
    char buf[32];
    std::memcpy(buf, "dws_p_0000000000", 16);
    unsigned char* p = reinterpret_cast<unsigned char*>(buf) + 16;
    for(int i = 0; i < 8; ++i) p[i] = static_cast<unsigned char>((cuid >> (56 - 8 * i)) & 0xFF);
    unsigned char* s = reinterpret_cast<unsigned char*>(buf) + 24;
    for(int i = 0; i < 8; ++i) s[i] = static_cast<unsigned char>((seq >> (56 - 8 * i)) & 0xFF);
    return std::string(buf, 32);
}

// ======================= 工作线程 =======================
void WorkerThread(DB* db, int thread_id) {
    std::mt19937_64 rng(thread_id + std::chrono::steady_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<int> dist_100(0, 99);
    std::uniform_int_distribution<int> hot_idx_dist(0, kHotPoolSize - 1);
    
    ThreadMetrics& metrics = g_thread_metrics[thread_id];
    std::string value_buf(kValueSize, 'D');

    DBImpl* db_impl = static_cast<DBImpl*>(db);
    auto hotspot_mgr = db_impl->GetHotspotManager();
    uint64_t my_hot_start = thread_id * 47;
    uint64_t my_hot_end = (thread_id + 1) * 47;

    // 每个线程分配一个初始的本地冷点
    uint64_t my_local_cold_cuid = g_cold_survival_idx.fetch_add(1);

    while (!g_stop_flag.load(std::memory_order_relaxed)) {
        WriteBatch batch;
        std::vector<std::pair<uint64_t, int>> death_candidates;

        for (int i = 0; i < kBatchSize; ++i) {
            bool is_hot = (dist_100(rng) < 77);
            uint64_t cuid = 0;
            int hot_slot = -1;

            if (is_hot) {
                std::uniform_int_distribution<uint64_t> hot_dist(my_hot_start, my_hot_end);
                cuid = g_hot_pool[hot_dist(rng)].load();
            } else {
                cuid = my_local_cold_cuid;
            }

            if (cuid >= kTotalCUIDs || g_cuid_dead[cuid].load(std::memory_order_relaxed)) continue;

            uint64_t threshold = g_cuid_thresholds[cuid];
            uint64_t seq = g_cuid_seqs[cuid].fetch_add(1, std::memory_order_relaxed);

            if (seq >= threshold) continue;

            batch.Put(EncodeDeltaKey(cuid, seq), value_buf);

            if (seq == threshold - 1) {
                death_candidates.push_back({cuid, is_hot ? hot_slot : -1});
            }
        }

        auto t_w_start = std::chrono::steady_clock::now();
        WriteOptions w_opts;
        w_opts.sync = false;       // 确保不启用硬件同步
        w_opts.disableWAL = true;  // 压测时尝试关闭 WAL 看看极限
        db->Write(w_opts, &batch);
        auto t_w_end = std::chrono::steady_clock::now();
        metrics.write_latencies.Record(std::chrono::duration_cast<std::chrono::microseconds>(t_w_end - t_w_start).count());
        metrics.write_ops += kBatchSize;

        // 【死亡处理与修复后的白名单使用】
        for (auto& death : death_candidates) {
            uint64_t cuid = death.first;
            int hot_slot = death.second;

            auto t_m_start = std::chrono::steady_clock::now();
            
            // 重要：Iterator 的 ReadOptions 必须在每次循环内独立配置
            ReadOptions scan_ropts;
            scan_ropts.readahead_size = 4 * 1024 * 1024;

            // 获取白名单索引
            std::vector<uint64_t> l0_whitelist = hotspot_mgr->GetL0FilesForCuid(cuid);
            
            // 修复逻辑：只有白名单里有文件，才挂载指针并计入统计
            if (!l0_whitelist.empty()) {
                scan_ropts.l0_file_whitelist = &l0_whitelist;
                metrics.whitelist_files_total += l0_whitelist.size();
            }

            std::unique_ptr<Iterator> it(db->NewIterator(scan_ropts));
            it->Seek(EncodeDeltaKey(cuid, 0));
            
            WriteBatch del_batch;
            uint64_t count = 0;
            while (it->Valid()) {
                if (hotspot_mgr->ExtractCUID(it->key()) != cuid) break;
                del_batch.Delete(it->key());
                count++;
                it->Next();
            }
            db->Write(WriteOptions(), &del_batch);

            auto t_m_end = std::chrono::steady_clock::now();
            metrics.merge_latencies.Record(std::chrono::duration_cast<std::chrono::microseconds>(t_m_end - t_m_start).count());
            metrics.merge_ops++;
            metrics.scanned_records += count;
            
            g_cuid_dead[cuid].store(true, std::memory_order_relaxed);

            // 无锁递补
            uint64_t replacement = g_cold_survival_idx.fetch_add(1);
            if (hot_slot != -1) {
                g_hot_pool[hot_slot].store(replacement, std::memory_order_relaxed);
            } else {
                my_local_cold_cuid = replacement;
            }
        }
    }
}

int main() {
    std::mt19937_64 init_rng(98765);
    std::uniform_int_distribution<uint64_t> thresh_dist(kScanThresholdMin, kScanThresholdMax);
    // 随机化初始 sequence，确保测试开始后不久就有连续的死亡/扫描发生
    std::uniform_int_distribution<uint64_t> seq_init_dist(0, 3000); 

    for (uint64_t i = 0; i < kTotalCUIDs; ++i) {
        g_cuid_thresholds[i] = thresh_dist(init_rng);
        g_cuid_seqs[i] = (i < kHotPoolSize + 500) ? seq_init_dist(init_rng) : 0;
        g_cuid_dead[i] = false;
    }
    for (uint64_t i = 0; i < kHotPoolSize; ++i) g_hot_pool[i].store(i);

    Options options;
    options.create_if_missing = true;
    options.statistics = CreateDBStatistics();
    
    // 应用 NVME 优化
    OptimizeOptionsForDeltaNVME(options);
    
    // 【关键修复】：缩小 Memtable，强制高频 Flush，使 L0 索引能被快速建立
    // options.write_buffer_size = 8 * 1024 * 1024; // 8MB
    // options.level0_file_num_compaction_trigger = 8;
    // options.level0_stop_writes_trigger = 100;
    // options.max_subcompactions = 8;

    DestroyDB(kDBPath, options);
    std::unique_ptr<DB> db;
    Status s = DB::Open(options, kDBPath, &db);
    if (!s.ok()) return 1;

    std::cout << ">>> OPTIMIZED BENCH: Forced Flush & Corrected Whitelist Scoping <<<\n";

    g_start_time = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    for (int i = 0; i < kNumThreads; ++i) threads.emplace_back(WorkerThread, db.get(), i);

    for (int i = 0; i < kTestDurationSecs; ++i) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (i > 0 && i % 5 == 0) {
            std::string l0, l1;
            db->GetProperty("rocksdb.num-files-at-level0", &l0);
            db->GetProperty("rocksdb.num-files-at-level1", &l1);
            std::cout << "[Time: " << i << "s] L0:" << l0 << " L1:" << l1 << std::endl;
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
    std::cout << "  Delta Merge 频率   : " << total.merge_ops / duration << " scans/s\n";
    std::cout << "  平均扫描长度       : " << (total.merge_ops > 0 ? total.scanned_records / total.merge_ops : 0) << " rows\n";
    
    std::cout << "\n[读路径加速维度]\n";
    double avg_whitelist = total.merge_ops > 0 ? (double)total.whitelist_files_total / total.merge_ops : 0;
    std::cout << "  白名单命中(L0精准率): " << std::fixed << std::setprecision(2) << avg_whitelist << " files/scan\n";

    std::cout << "\n[延迟分布 (微秒)]\n";
    std::cout << "  Row Put     | P50: " << total.write_latencies.Percentile(0.5) 
              << " | P90: " << total.write_latencies.Percentile(0.9) 
              << " | P99: " << total.write_latencies.Percentile(0.99) << "\n";
    std::cout << "  Delta Scan  | P50: " << total.merge_latencies.Percentile(0.5) 
              << " | P90: " << total.merge_latencies.Percentile(0.9) 
              << " | P99: " << total.merge_latencies.Percentile(0.99) << "\n";

    std::cout << "\n[写放大与资源回收]\n";
    uint64_t bytes_written = options.statistics->getTickerCount(BYTES_WRITTEN);
    uint64_t disk_write = options.statistics->getTickerCount(FLUSH_WRITE_BYTES) + options.statistics->getTickerCount(COMPACT_WRITE_BYTES);
    std::cout << "  系统写放大 (WA)    : " << (bytes_written > 0 ? (double)disk_write / bytes_written : 0) << " x\n";
    
    std::string l0, l1;
    db->GetProperty("rocksdb.num-files-at-level0", &l0);
    db->GetProperty("rocksdb.num-files-at-level1", &l1);
    std::cout << "  最终 LSM 状态      : L0=" << l0 << ", L1=" << l1 << "\n";
    std::cout << "=============================================================\n";

    return 0;
}