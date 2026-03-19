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

// 负载参数
const std::string kDBPath = "/tmp/rocksdb_delta_optimized_bench";
const int kNumThreads = 8;
const uint64_t kTotalCUIDs = 100000;
const uint64_t kHotPoolSize = 1500;
const uint64_t kScanThresholdMin = 1000;
const uint64_t kScanThresholdMax = 2000;
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
        // protect domain of log10 for small values
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

// 极致优化：零拷贝的大端序 Key 生成
inline void EncodeDeltaKeyInPlace(char* buf, uint64_t cuid, uint64_t seq) {
    // 假设前 16 字节 "dws_p_0000000000" 已在外部初始化好
    // 使用编译器内建函数进行极速大端序转换
    uint64_t cuid_be = __builtin_bswap64(cuid);
    uint64_t seq_be = __builtin_bswap64(seq);
    std::memcpy(buf + 16, &cuid_be, 8);
    std::memcpy(buf + 24, &seq_be, 8);
}

// 工作线程
void WorkerThread(DB* db, int thread_id) {
    // 线程内 RNG：基于时间 + thread_id + random_device 混合
    uint64_t seed = (uint64_t)std::chrono::high_resolution_clock::now().time_since_epoch().count();
    seed ^= (uint64_t(thread_id) << 32);
    // random_device used only once per thread to perturb seed (cheap)
    std::random_device rd;
    seed ^= (uint64_t)rd();
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int> dist_100(0, 99);

    ThreadMetrics& metrics = g_thread_metrics[thread_id];
    Slice value_slice(std::string(kValueSize, 'D')); // 常量 Value

    // 尝试获取 hotspot_mgr（db_impl）
    DBImpl* db_impl = static_cast<DBImpl*>(db);
    auto hotspot_mgr = db_impl->GetHotspotManager();

    uint64_t my_hot_start = thread_id * (kHotPoolSize / kNumThreads);
    uint64_t my_hot_end = (thread_id + 1) * (kHotPoolSize / kNumThreads) - 1;
    std::uniform_int_distribution<uint64_t> hot_dist(my_hot_start, my_hot_end);

    uint64_t my_local_cold_cuid = g_cold_survival_idx.fetch_add(1, std::memory_order_relaxed);

    // 循环外预分配，避免高频 malloc/free
    WriteBatch batch;
    WriteBatch del_batch;
    std::vector<std::pair<uint64_t, int>> death_candidates;
    death_candidates.reserve(10);

    // 栈上分配 Key Buffer 并初始化前 16 字节
    char key_buf[32];
    std::memcpy(key_buf, "dws_p_0000000000", 16);

    while (!g_stop_flag.load(std::memory_order_relaxed)) {
        batch.Clear();
        death_candidates.clear();

        for (int i = 0; i < kBatchSize; ++i) {
            bool is_hot = (dist_100(rng) < 77);
            uint64_t cuid = 0;
            int hot_slot = -1;

            if (is_hot) {
                hot_slot = static_cast<int>(hot_dist(rng));
                cuid = g_hot_pool[hot_slot].load(std::memory_order_relaxed);
            } else {
                cuid = my_local_cold_cuid;
            }

            if (cuid >= kTotalCUIDs) break; // 防止越界保护

            uint64_t threshold = g_cuid_thresholds[cuid];
            uint64_t seq = g_cuid_seqs[cuid].fetch_add(1, std::memory_order_relaxed);

            // 如果拿到了超额 seq，说明其他线程正在接管，直接跳过但不消耗批次名额
            if (seq >= threshold) {
                --i;
                continue;
            }

            // 零拷贝组装 Key
            EncodeDeltaKeyInPlace(key_buf, cuid, seq);
            batch.Put(Slice(key_buf, 32), value_slice);

            // 触发合并阈值
            if (seq == threshold - 1) {
                death_candidates.push_back({cuid, hot_slot});

                // 瞬间替换：避免其他线程陷入 continue 自旋风暴
                uint64_t replacement = g_cold_survival_idx.fetch_add(1, std::memory_order_relaxed);
                if (replacement < kTotalCUIDs) {
                    if (is_hot) {
                        g_hot_pool[hot_slot].store(replacement, std::memory_order_relaxed);
                    } else {
                        my_local_cold_cuid = replacement;
                    }
                }
            }
        }

        // 批量写入
        auto t_w_start = std::chrono::steady_clock::now();
        WriteOptions w_opts;
        w_opts.sync = false;
        w_opts.disableWAL = true;  // 压测核心路径性能，跳过 WAL
        Status ws = db->Write(w_opts, &batch);
        auto t_w_end = std::chrono::steady_clock::now();
        metrics.write_latencies.Record(std::chrono::duration_cast<std::chrono::microseconds>(t_w_end - t_w_start).count());
        if (!ws.ok()) {
            std::cout << "[Worker " << thread_id << "] Write batch failed: " << ws.ToString() << std::endl;
        }
        metrics.write_ops += batch.Count();

        // 执行 Delta Merge (Scan + Delete)
        for (auto& death : death_candidates) {
            uint64_t cuid = death.first;
            auto t_m_start = std::chrono::steady_clock::now();

            ReadOptions scan_ropts;
            scan_ropts.readahead_size = 512 * 1024; // 512KB readahead
            scan_ropts.prefix_same_as_start = true;
            scan_ropts.fill_cache = false;

            // 构造 upper bound: cuid + 1
            std::string upper(32, '\0');
            std::memcpy(&upper[0], "dws_p_0000000000", 16);
            EncodeDeltaKeyInPlace(&upper[0], cuid + 1, 0);
            Slice upper_slice(upper);
            scan_ropts.iterate_upper_bound = &upper_slice;

            // L0 白名单（如果 hotspot mgr 可用）
            std::vector<uint64_t> l0_whitelist;
            if (hotspot_mgr) {
                l0_whitelist = hotspot_mgr->GetL0FilesForCuid(cuid);
                if (!l0_whitelist.empty()) {
                    scan_ropts.l0_file_whitelist = &l0_whitelist;
                    metrics.whitelist_files_total += l0_whitelist.size();
                }
            }

            std::unique_ptr<Iterator> it(db->NewIterator(scan_ropts));
            // Seek 到 CUID 开头
            EncodeDeltaKeyInPlace(key_buf, cuid, 0);
            it->Seek(Slice(key_buf, 32));

            del_batch.Clear();
            uint64_t count = 0;
            while (it->Valid()) {
                // 安全检查：保证 key 长度足够再做 memcmp
                Slice itkey = it->key();
                if (itkey.size() < 24) {
                    break;
                }
                if (std::memcmp(itkey.data(), key_buf, 24) != 0) break;
                del_batch.Delete(itkey);
                count++;
                it->Next();
            }

            // 检查 iterator 是否出错
            Status ist = it->status();
            if (!ist.ok()) {
                std::cout << "[Worker " << thread_id << "] Iterator error during merge for cuid=" << cuid
                          << ": " << ist.ToString() << std::endl;
            }

            // 写删除批次
            Status ds = db->Write(WriteOptions(), &del_batch);
            if (!ds.ok()) {
                std::cout << "[Worker " << thread_id << "] Delete batch failed for cuid=" << cuid
                          << ": " << ds.ToString() << std::endl;
            }

            auto t_m_end = std::chrono::steady_clock::now();
            metrics.merge_latencies.Record(std::chrono::duration_cast<std::chrono::microseconds>(t_m_end - t_m_start).count());
            metrics.merge_ops++;
            metrics.scanned_records += count;

            g_cuid_dead[cuid].store(true, std::memory_order_relaxed);
        }
    }
}

int main() {
    std::mt19937_64 init_rng(98765);
    std::uniform_int_distribution<uint64_t> thresh_dist(kScanThresholdMin, kScanThresholdMax);
    std::uniform_int_distribution<uint64_t> seq_init_dist(0, kScanThresholdMin/3);

    // 初始化参数
    for (uint64_t i = 0; i < kTotalCUIDs; ++i) {
        g_cuid_thresholds[i] = thresh_dist(init_rng);
        g_cuid_seqs[i] = (i < kHotPoolSize + 500) ? seq_init_dist(init_rng) : 0;
        g_cuid_dead[i] = false;
    }
    for (uint64_t i = 0; i < kHotPoolSize; ++i) g_hot_pool[i].store(i);

    Options options;
    options.create_if_missing = true;
    options.statistics = CreateDBStatistics();

    OptimizeOptionsForDeltaNVME(options);

    options.max_background_jobs = 8;
    options.max_subcompactions = 4;
    options.allow_concurrent_memtable_write = true;
    options.enable_write_thread_adaptive_yield = true;
    options.unordered_write = true;

    DestroyDB(kDBPath, options);
    std::unique_ptr<DB> db;
    Status s = DB::Open(options, kDBPath, &db);
    if (!s.ok()) {
        std::cerr << "DB Open Failed: " << s.ToString() << std::endl;
        return 1;
    }

    std::cout << ">>> OPTIMIZED BENCH: Zero-Copy & Spin-Lock Free Edition <<<\n";

    g_start_time = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    for (int i = 0; i < kNumThreads; ++i) threads.emplace_back(WorkerThread, db.get(), i);

    for (int i = 0; i < kTestDurationSecs; ++i) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (i > 0 && i % 5 == 0) {
            std::string l0, l1;
            db->GetProperty("rocksdb.num-files-at-level0", &l0);
            db->GetProperty("rocksdb.num-files-at-level1", &l1);
            std::cout << "[Time: " << i << "s] L0: " << l0 << " | L1: " << l1 << std::endl;
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