/**
 * rocksdb_smart_bench_v2.cc
 * 
 * 多维度性能基准测试
 * 覆盖：写放大、吞吐量、Stall时间、Trivial Move比例、L0堆积情况
 */

#include <iostream>
#include <string>
#include <vector>
#include <numeric>
#include <thread>
#include <chrono>
#include <iomanip>
#include <sstream>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "db/db_impl/db_impl.h"

using namespace ROCKSDB_NAMESPACE;

const std::string kDBPath = "/tmp/rocksdb_smart_bench_v2";
const int kValueSize = 1024; // 1KB

// ======================= 辅助类：指标报告器 =======================
class BenchmarkReporter {
public:
    BenchmarkReporter(std::shared_ptr<Statistics> stats) : stats_(stats) {
        Reset();
    }

    void Reset() {
        start_time_ = std::chrono::steady_clock::now();
        start_user_bytes_ = stats_->getTickerCount(BYTES_WRITTEN);
        start_flush_bytes_ = stats_->getTickerCount(FLUSH_WRITE_BYTES);
        start_compact_bytes_ = stats_->getTickerCount(COMPACT_WRITE_BYTES);
        start_stall_micros_ = stats_->getTickerCount(STALL_MICROS);
    }

    void Report(const std::string& title, uint64_t total_ops) {
        auto end_time = std::chrono::steady_clock::now();
        double duration_seconds = std::chrono::duration<double>(end_time - start_time_).count();

        uint64_t end_user = stats_->getTickerCount(BYTES_WRITTEN);
        uint64_t end_flush = stats_->getTickerCount(FLUSH_WRITE_BYTES);
        uint64_t end_compact = stats_->getTickerCount(COMPACT_WRITE_BYTES);
        uint64_t end_stall = stats_->getTickerCount(STALL_MICROS);

        double user_mb = (end_user - start_user_bytes_) / 1048576.0;
        double disk_mb = (end_flush - start_flush_bytes_ + end_compact - start_compact_bytes_) / 1048576.0;
        
        // 核心指标计算
        double ops_per_sec = total_ops / duration_seconds;
        double write_amp = (user_mb > 0) ? (disk_mb / user_mb) : 0.0;
        double stall_ms = (end_stall - start_stall_micros_) / 1000.0;
        
        std::cout << "\n[" << title << "] Metrics:" << std::endl;
        std::cout << std::left << std::setw(25) << "1. Throughput (OPS):" << std::fixed << std::setprecision(0) << ops_per_sec << " ops/sec" << std::endl;
        std::cout << std::left << std::setw(25) << "2. Write Amp (WA):"   << std::setprecision(2) << write_amp << " x" << std::endl;
        std::cout << std::left << std::setw(25) << "3. Stall Time:"       << std::setprecision(1) << stall_ms << " ms" << std::endl;
        std::cout << std::left << std::setw(25) << "4. User Write:"       << std::setprecision(1) << user_mb << " MB" << std::endl;
        std::cout << std::left << std::setw(25) << "5. Disk Write:"       << std::setprecision(1) << disk_mb << " MB" << std::endl;
    }

private:
    std::shared_ptr<Statistics> stats_;
    std::chrono::time_point<std::chrono::steady_clock> start_time_;
    uint64_t start_user_bytes_;
    uint64_t start_flush_bytes_;
    uint64_t start_compact_bytes_;
    uint64_t start_stall_micros_;
};

// ======================= 工具函数 =======================
std::string GenerateKey(uint64_t cuid, int id) {
    char buf[24];
    memset(buf, 0, 24);
    // CUID at [16-23]
    unsigned char* p = reinterpret_cast<unsigned char*>(buf) + 16;
    for(int i=0; i<8; ++i) p[i] = (cuid >> (56 - 8*i)) & 0xFF;
    
    std::string key(buf, 24);
    std::string suffix = std::to_string(id);
    if(suffix.length() > 16) suffix = suffix.substr(0,16);
    memcpy(&key[0], suffix.data(), suffix.length());
    return key;
}

void PrintDbStats(DB* db) {
    std::string num_l0, num_l1;
    db->GetProperty("rocksdb.num-files-at-level0", &num_l0);
    db->GetProperty("rocksdb.num-files-at-level1", &num_l1);
    std::cout << "   Current Files -> L0: " << num_l0 << ", L1: " << num_l1 << std::endl;
}

// ======================= 测试主逻辑 =======================
int main() {
    // 0. 初始化
    DestroyDB(kDBPath, Options());
    
    Options options;
    options.create_if_missing = true;
    options.statistics = CreateDBStatistics();
    
    // 关键配置：让 Compaction 频繁发生以便观察
    options.level0_file_num_compaction_trigger = 4;
    options.level0_slowdown_writes_trigger = 20;
    options.level0_stop_writes_trigger = 30;
    options.target_file_size_base = 2 * 1024 * 1024; // 2MB
    options.max_bytes_for_level_base = 10 * 1024 * 1024; // L1 10MB
    options.disable_auto_compactions = false;

    DB* db = nullptr;
    Status s = DB::Open(options, kDBPath, &db);
    assert(s.ok());
    
    BenchmarkReporter reporter(options.statistics);

    std::cout << ">>> BENCHMARK START <<<" << std::endl;

    // ---------------------------------------------------------------
    // 场景 A: 顺序写入 (测试 Trivial Move)
    // 每次写入全新的 CUID，数据互不重叠
    // ---------------------------------------------------------------
    {
        std::cout << "\n--- Scenario A: Sequential Writes (Unique CUIDs) ---" << std::endl;
        reporter.Reset();
        
        int total_ops = 0;
        for (int i = 0; i < 20; ++i) { // 20个批次
            WriteBatch batch;
            uint64_t cuid = 1000 + i; 
            for (int j = 0; j < 1000; ++j) { // 1MB per batch
                batch.Put(GenerateKey(cuid, j), std::string(kValueSize, 'A'));
            }
            db->Write(WriteOptions(), &batch);
            total_ops += 1000;
            // 模拟业务间歇，给后台 Compaction 时间
            std::this_thread::sleep_for(std::chrono::milliseconds(50)); 
        }
        // 等待后台收敛
        int wait_cycles = 0;
        while(wait_cycles++ < 10) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            std::string pending;
            db->GetProperty("rocksdb.compaction-pending", &pending);
            if (pending == "0") break;
        }

        reporter.Report("Scenario A", total_ops);
        PrintDbStats(db);
    }

    // ---------------------------------------------------------------
    // 场景 B: 随机覆盖写 (测试 Efficiency & Smart Picker)
    // 针对同一个 CUID 反复写入，制造高重叠
    // ---------------------------------------------------------------
    {
        std::cout << "\n--- Scenario B: Random Overwrites (Hotspot Update) ---" << std::endl;
        
        // 1. 先铺底数据 (Base)
        uint64_t hot_cuid = 9999;
        {
            WriteBatch batch;
            for(int j=0; j<5000; ++j) {
                batch.Put(GenerateKey(hot_cuid, j), std::string(kValueSize, 'B'));
            }
            db->Write(WriteOptions(), &batch);
            db->Flush(FlushOptions());
            // 等待它压下去
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }

        // 2. 开始高频覆盖 (Overwrites)
        reporter.Reset();
        int total_ops = 0;
        for (int i = 0; i < 20; ++i) {
            WriteBatch batch;
            // 每次只更新 500 个 key (10% 的数据量)，但重叠度是 100%
            for (int j = 0; j < 500; ++j) {
                batch.Put(GenerateKey(hot_cuid, j), std::string(kValueSize, 'C'));
            }
            db->Write(WriteOptions(), &batch);
            total_ops += 500;
            // 快速写入，制造 L0 压力
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        
        // 等待收敛
        std::this_thread::sleep_for(std::chrono::seconds(3));
        
        reporter.Report("Scenario B", total_ops);
        PrintDbStats(db);
    }

    delete db;
    return 0;
}