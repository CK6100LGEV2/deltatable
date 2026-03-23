//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_picker_level.h"

#include <string>
#include <utility>
#include <vector>
#include <iostream>

#include "db/version_edit.h"
#include "logging/log_buffer.h"
#include "logging/logging.h"
#include "test_util/sync_point.h"
#include "delta/hotspot_manager.h"

namespace ROCKSDB_NAMESPACE {

bool LevelCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  if (!vstorage->ExpiredTtlFiles().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
    return true;
  }
  if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForForcedBlobGC().empty()) {
    return true;
  }
  for (int i = 0; i <= vstorage->MaxInputLevel(); i++) {
    if (vstorage->CompactionScore(i) >= 1) {
      return true;
    }
  }
  return false;
}

namespace {

enum class CompactToNextLevel {
  kNo,   // compact to the same level as the input file
  kYes,  // compact to the next level except the last level to the same level
  kSkipLastLevel,  // compact to the next level but skip the last level
};

// A class to build a leveled compaction step-by-step.
class LevelCompactionBuilder {
 public:
  LevelCompactionBuilder(const std::string& cf_name,
                         VersionStorageInfo* vstorage,
                         CompactionPicker* compaction_picker,
                         LogBuffer* log_buffer,
                         const MutableCFOptions& mutable_cf_options,
                         const ImmutableOptions& ioptions,
                         const MutableDBOptions& mutable_db_options)
      : cf_name_(cf_name),
        vstorage_(vstorage),
        compaction_picker_(compaction_picker),
        log_buffer_(log_buffer),
        mutable_cf_options_(mutable_cf_options),
        ioptions_(ioptions),
        mutable_db_options_(mutable_db_options) {}


  bool PickSmartL0ToL1Compaction();
  bool PickSmartL1Purge();
  // Pick and return a compaction.
  Compaction* PickCompaction();

  // Pick the initial files to compact to the next level. (or together
  // in Intra-L0 compactions)
  void SetupInitialFiles();

  // If the initial files are from L0 level, pick other L0
  // files if needed.
  bool SetupOtherL0FilesIfNeeded();

  // Compaction with round-robin compaction priority allows more files to be
  // picked to form a large compaction
  void SetupOtherFilesWithRoundRobinExpansion();
  // Based on initial files, setup other files need to be compacted
  // in this compaction, accordingly.
  bool SetupOtherInputsIfNeeded();

  Compaction* GetCompaction();

  // From `start_level_`, pick files to compact to `output_level_`.
  // Returns false if there is no file to compact.
  // If it returns true, inputs->files.size() will be exactly one for
  // all compaction priorities except round-robin. For round-robin,
  // multiple consecutive files may be put into inputs->files.
  // If level is 0 and there is already a compaction on that level, this
  // function will return false.
  bool PickFileToCompact();

  // Return true if a L0 trivial move is picked up.
  bool TryPickL0TrivialMove();

  // For L0->L0, picks the longest span of files that aren't currently
  // undergoing compaction for which work-per-deleted-file decreases. The span
  // always starts from the newest L0 file.
  //
  // Intra-L0 compaction is independent of all other files, so it can be
  // performed even when L0->base_level compactions are blocked.
  //
  // Returns true if `inputs` is populated with a span of files to be compacted;
  // otherwise, returns false.
  bool PickIntraL0Compaction();

  // When total L0 size is small compared to Lbase, try to pick intra-L0
  // compaction starting from the newest L0 file. This helps to prevent
  // L0->Lbase compaction with large write-amp.
  //
  // Returns true iff an intra-L0 compaction is picked.
  // `start_level_inputs_` and `output_level_` will be updated accordingly if
  // a compaction is picked.
  bool PickSizeBasedIntraL0Compaction();

  // Return true if TrivialMove is extended. `start_index` is the index of
  // the initial file picked, which should already be in `start_level_inputs_`.
  bool TryExtendNonL0TrivialMove(int start_index,
                                 bool only_expand_right = false);

  // Picks a file from level_files to compact.
  // level_files is a vector of (level, file metadata) in ascending order of
  // level. If compact_to_next_level is true, compact the file to the next
  // level, otherwise, compact to the same level as the input file.
  // If skip_last_level is true, skip the last level.
  void PickFileToCompact(
      const autovector<std::pair<int, FileMetaData*>>& level_files,
      CompactToNextLevel compact_to_next_level);

  const std::string& cf_name_;
  VersionStorageInfo* vstorage_;
  CompactionPicker* compaction_picker_;
  LogBuffer* log_buffer_;
  int start_level_ = -1;
  int output_level_ = -1;
  int parent_index_ = -1;
  int base_index_ = -1;
  double start_level_score_ = 0;
  bool is_l0_trivial_move_ = false;
  CompactionInputFiles start_level_inputs_;
  std::vector<CompactionInputFiles> compaction_inputs_;
  CompactionInputFiles output_level_inputs_;
  std::vector<FileMetaData*> grandparents_;
  CompactionReason compaction_reason_ = CompactionReason::kUnknown;

  const MutableCFOptions& mutable_cf_options_;
  const ImmutableOptions& ioptions_;
  const MutableDBOptions& mutable_db_options_;
  // Pick a path ID to place a newly generated file, with its level
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            int level);

  static const int kMinFilesForIntraL0Compaction = 4;
};

void LevelCompactionBuilder::PickFileToCompact(
    const autovector<std::pair<int, FileMetaData*>>& level_files,
    CompactToNextLevel compact_to_next_level) {
  for (auto& level_file : level_files) {
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);
    start_level_ = level_file.first;
    if ((compact_to_next_level == CompactToNextLevel::kSkipLastLevel &&
         start_level_ == vstorage_->num_non_empty_levels() - 1) ||
        (start_level_ == 0 &&
         !compaction_picker_->level0_compactions_in_progress()->empty())) {
      continue;
    }

    // Compact to the next level only if the file is not in the last level and
    // compact_to_next_level is kYes or kSkipLastLevel.
    if (compact_to_next_level != CompactToNextLevel::kNo &&
        (start_level_ < vstorage_->num_non_empty_levels() - 1)) {
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
    } else {
      output_level_ = start_level_;
    }
    start_level_inputs_.files = {level_file.second};
    start_level_inputs_.level = start_level_;
    if (compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                   &start_level_inputs_)) {
      return;
    }
  }
  start_level_inputs_.files.clear();
}

void LevelCompactionBuilder::SetupInitialFiles() {
  // Find the compactions by size on all levels.
  bool skipped_l0_to_base = false;
  for (int i = 0; i < compaction_picker_->NumberLevels() - 1; i++) {
    start_level_score_ = vstorage_->CompactionScore(i);
    start_level_ = vstorage_->CompactionScoreLevel(i);
    assert(i == 0 || start_level_score_ <= vstorage_->CompactionScore(i - 1));
    if (start_level_score_ >= 1) {
      if (skipped_l0_to_base && start_level_ == vstorage_->base_level()) {
        // If L0->base_level compaction is pending, don't schedule further
        // compaction from base level. Otherwise L0->base_level compaction
        // may starve.
        continue;
      }
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
      bool picked_file_to_compact = PickFileToCompact();
      TEST_SYNC_POINT_CALLBACK("PostPickFileToCompact",
                               &picked_file_to_compact);
      if (picked_file_to_compact) {
        // found the compaction!
        if (start_level_ == 0) {
          // L0 score = `num L0 files` / `level0_file_num_compaction_trigger`
          compaction_reason_ = CompactionReason::kLevelL0FilesNum;
        } else {
          // L1+ score = `Level files size` / `MaxBytesForLevel`
          compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
        }
        break;
      } else {
        // didn't find the compaction, clear the inputs
        start_level_inputs_.clear();
        if (start_level_ == 0) {
          skipped_l0_to_base = true;
          // L0->base_level may be blocked due to ongoing L0->base_level
          // compactions. It may also be blocked by an ongoing compaction from
          // base_level downwards.
          //
          // In these cases, to reduce L0 file count and thus reduce likelihood
          // of write stalls, we can attempt compacting a span of files within
          // L0.
          if (PickIntraL0Compaction()) {
            output_level_ = 0;
            compaction_reason_ = CompactionReason::kLevelL0FilesNum;
            break;
          }
        }
      }
    } else {
      // Compaction scores are sorted in descending order, no further scores
      // will be >= 1.
      break;
    }
  }
  if (!start_level_inputs_.empty()) {
    return;
  }

  // if we didn't find a compaction, check if there are any files marked for
  // compaction
  parent_index_ = base_index_ = -1;

  compaction_picker_->PickFilesMarkedForCompaction(
      cf_name_, vstorage_, &start_level_, &output_level_, &start_level_inputs_,
      /*skip_marked_file*/ [](const FileMetaData* /* file */) {
        return false;
      });
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kFilesMarkedForCompaction;
    return;
  }

  // Bottommost Files Compaction on deleting tombstones
  PickFileToCompact(vstorage_->BottommostFilesMarkedForCompaction(),
                    CompactToNextLevel::kNo);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kBottommostFiles;
    return;
  }

  // TTL Compaction
  if (ioptions_.compaction_pri == kRoundRobin &&
      !vstorage_->ExpiredTtlFiles().empty()) {
    auto expired_files = vstorage_->ExpiredTtlFiles();
    // the expired files list should already be sorted by level
    start_level_ = expired_files.front().first;
#ifndef NDEBUG
    for (const auto& file : expired_files) {
      assert(start_level_ <= file.first);
    }
#endif
    if (start_level_ > 0) {
      output_level_ = start_level_ + 1;
      if (PickFileToCompact()) {
        compaction_reason_ = CompactionReason::kRoundRobinTtl;
        return;
      }
    }
  }

  PickFileToCompact(vstorage_->ExpiredTtlFiles(),
                    CompactToNextLevel::kSkipLastLevel);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kTtl;
    return;
  }

  // Periodic Compaction
  PickFileToCompact(vstorage_->FilesMarkedForPeriodicCompaction(),
                    ioptions_.level_compaction_dynamic_level_bytes
                        ? CompactToNextLevel::kYes
                        : CompactToNextLevel::kNo);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kPeriodicCompaction;
    return;
  }

  // Forced blob garbage collection
  PickFileToCompact(vstorage_->FilesMarkedForForcedBlobGC(),
                    CompactToNextLevel::kNo);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kForcedBlobGC;
    return;
  }
}

bool LevelCompactionBuilder::SetupOtherL0FilesIfNeeded() {
  if (start_level_ == 0 && output_level_ != 0 && !is_l0_trivial_move_) {
    return compaction_picker_->GetOverlappingL0Files(
        vstorage_, &start_level_inputs_, output_level_, &parent_index_);
  }
  return true;
}

void LevelCompactionBuilder::SetupOtherFilesWithRoundRobinExpansion() {
  // We only expand when the start level is not L0 under round robin
  assert(start_level_ >= 1);

  // For round-robin compaction priority, we have 3 constraints when picking
  // multiple files.
  // Constraint 1: We can only pick consecutive files
  //  -> Constraint 1a: When a file is being compacted (or some input files
  //                    are being compacted after expanding, we cannot
  //                    choose it and have to stop choosing more files
  //  -> Constraint 1b: When we reach the last file (with largest keys), we
  //                    cannot choose more files (the next file will be the
  //                    first one)
  // Constraint 2: We should ensure the total compaction bytes (including the
  //               overlapped files from the next level) is no more than
  //               mutable_cf_options_.max_compaction_bytes
  // Constraint 3: We try our best to pick as many files as possible so that
  //               the post-compaction level size is less than
  //               MaxBytesForLevel(start_level_)
  // Constraint 4: We do not expand if it is possible to apply a trivial move
  // Constraint 5 (TODO): Try to pick minimal files to split into the target
  //               number of subcompactions
  TEST_SYNC_POINT("LevelCompactionPicker::RoundRobin");

  // Only expand the inputs when we have selected a file in start_level_inputs_
  if (start_level_inputs_.size() == 0) {
    return;
  }

  uint64_t start_lvl_bytes_no_compacting = 0;
  uint64_t curr_bytes_to_compact = 0;
  uint64_t start_lvl_max_bytes_to_compact = 0;
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);
  // Constraint 3 (pre-calculate the ideal max bytes to compact)
  for (auto f : level_files) {
    if (!f->being_compacted) {
      start_lvl_bytes_no_compacting += f->fd.GetFileSize();
    }
  }
  if (start_lvl_bytes_no_compacting >
      vstorage_->MaxBytesForLevel(start_level_)) {
    start_lvl_max_bytes_to_compact = start_lvl_bytes_no_compacting -
                                     vstorage_->MaxBytesForLevel(start_level_);
  }

  size_t start_index = vstorage_->FilesByCompactionPri(start_level_)[0];
  InternalKey smallest, largest;
  // Constraint 4 (No need to check again later)
  compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
  CompactionInputFiles output_level_inputs;
  output_level_inputs.level = output_level_;
  vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                  &output_level_inputs.files);
  if (output_level_inputs.empty()) {
    if (TryExtendNonL0TrivialMove((int)start_index,
                                  true /* only_expand_right */)) {
      return;
    }
  }
  // Constraint 3
  if (start_level_inputs_[0]->fd.GetFileSize() >=
      start_lvl_max_bytes_to_compact) {
    return;
  }
  CompactionInputFiles tmp_start_level_inputs;
  tmp_start_level_inputs = start_level_inputs_;
  // TODO (zichen): Future parallel round-robin may also need to update this
  // Constraint 1b (only expand till the end)
  for (size_t i = start_index + 1; i < level_files.size(); i++) {
    auto* f = level_files[i];
    if (f->being_compacted) {
      // Constraint 1a
      return;
    }

    tmp_start_level_inputs.files.push_back(f);
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &tmp_start_level_inputs) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {tmp_start_level_inputs}, output_level_,
            Compaction::EvaluateProximalLevel(vstorage_, mutable_cf_options_,
                                              ioptions_, start_level_,
                                              output_level_))) {
      // Constraint 1a
      tmp_start_level_inputs.clear();
      return;
    }

    curr_bytes_to_compact = 0;
    for (auto start_lvl_f : tmp_start_level_inputs.files) {
      curr_bytes_to_compact += start_lvl_f->fd.GetFileSize();
    }

    // Check whether any output level files are locked
    compaction_picker_->GetRange(tmp_start_level_inputs, &smallest, &largest);
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);
    if (!output_level_inputs.empty() &&
        !compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &output_level_inputs)) {
      // Constraint 1a
      tmp_start_level_inputs.clear();
      return;
    }

    uint64_t start_lvl_curr_bytes_to_compact = curr_bytes_to_compact;
    for (auto output_lvl_f : output_level_inputs.files) {
      curr_bytes_to_compact += output_lvl_f->fd.GetFileSize();
    }
    if (curr_bytes_to_compact > mutable_cf_options_.max_compaction_bytes) {
      // Constraint 2
      tmp_start_level_inputs.clear();
      return;
    }

    start_level_inputs_.files = tmp_start_level_inputs.files;
    // Constraint 3
    if (start_lvl_curr_bytes_to_compact > start_lvl_max_bytes_to_compact) {
      return;
    }
  }
}

bool LevelCompactionBuilder::SetupOtherInputsIfNeeded() {
  // Setup input files from output level. For output to L0, we only compact
  // spans of files that do not interact with any pending compactions, so don't
  // need to consider other levels.
  if (output_level_ != 0) {
    output_level_inputs_.level = output_level_;
    bool round_robin_expanding =
        ioptions_.compaction_pri == kRoundRobin &&
        compaction_reason_ == CompactionReason::kLevelMaxLevelSize;
    if (round_robin_expanding) {
      SetupOtherFilesWithRoundRobinExpansion();
    }
    if (!is_l0_trivial_move_ &&
        !compaction_picker_->SetupOtherInputs(
            cf_name_, mutable_cf_options_, vstorage_, &start_level_inputs_,
            &output_level_inputs_, &parent_index_, base_index_,
            round_robin_expanding)) {
      return false;
    }

    compaction_inputs_.push_back(start_level_inputs_);
    if (!output_level_inputs_.empty()) {
      compaction_inputs_.push_back(output_level_inputs_);
    }

    // In some edge cases we could pick a compaction that will be compacting
    // a key range that overlap with another running compaction, and both
    // of them have the same output level. This could happen if
    // (1) we are running a non-exclusive manual compaction
    // (2) AddFile ingest a new file into the LSM tree
    // We need to disallow this from happening.
    if (compaction_picker_->FilesRangeOverlapWithCompaction(
            compaction_inputs_, output_level_,
            Compaction::EvaluateProximalLevel(vstorage_, mutable_cf_options_,
                                              ioptions_, start_level_,
                                              output_level_))) {
      // This compaction output could potentially conflict with the output
      // of a currently running compaction, we cannot run it.
      return false;
    }
    if (!is_l0_trivial_move_) {
      compaction_picker_->GetGrandparents(vstorage_, start_level_inputs_,
                                          output_level_inputs_, &grandparents_);
    }
  } else {
    compaction_inputs_.push_back(start_level_inputs_);
  }
  return true;
}

struct L0CandidateScore {
  FileMetaData* file;
  int index;
  uint64_t l1_overlap_bytes;
  double score; // 分数越高优先级越高
};

// 辅助结构：记录窗口状态
struct WindowState {
  int start_index = -1;
  int end_index = -1;
  double score = -1.0;
  uint64_t l0_bytes = 0;
  uint64_t l1_bytes = 0;
  bool is_trivial_move = false;
  void Update(int s, int e, uint64_t l0, uint64_t l1, double sc, bool trivial) {
    start_index = s;
    end_index = e;
    l0_bytes = l0;
    l1_bytes = l1;
    score = sc;
    is_trivial_move = trivial;
  }
};

bool LevelCompactionBuilder::PickSmartL0ToL1Compaction() {
  const auto* icmp = vstorage_->InternalComparator();
  const auto& orig_l0_files = vstorage_->LevelFiles(0);
  if (orig_l0_files.empty()) return false;

  // ---------- global call counter (count every call) ----------
  static std::atomic<uint64_t> global_picker_calls{0};
  static std::atomic<uint64_t> last_primary_call{0};

  const size_t total_files = orig_l0_files.size();
  const int target_level = 1;
  auto hotspot_manager = ioptions_.hotspot_manager;

  const int compaction_trigger = mutable_cf_options_.level0_file_num_compaction_trigger;
  const size_t l0_pressure_threshold = static_cast<size_t>(compaction_trigger * 1.5);
  const size_t l0_critical_threshold = static_cast<size_t>(mutable_cf_options_.level0_slowdown_writes_trigger);
  const uint64_t kMaxCompactionBytes = mutable_cf_options_.max_compaction_bytes;

  // Emergency threshold (0.8 * slowdown)
  double emergency_frac = 0.8;
  size_t intra_emergency_threshold = static_cast<size_t>(std::ceil(static_cast<double>(l0_critical_threshold) * emergency_frac));
  if (intra_emergency_threshold < 1) intra_emergency_threshold = 1;

  // sorted L0 file list (key order)
  std::vector<FileMetaData*> l0_files(orig_l0_files.begin(), orig_l0_files.end());
  std::sort(l0_files.begin(), l0_files.end(), [&](FileMetaData* a, FileMetaData* b) {
      return icmp->Compare(a->smallest, b->smallest) < 0;
  });

  // 1) Ghost drop
  // if (hotspot_manager) {
  //   for (auto* f : l0_files) {
  //     if (f->being_compacted) continue;
  //     double garbage_ratio = hotspot_manager->GetL0FileGarbageRatio(f->fd.GetNumber());
  //     if (garbage_ratio > 0.98) {
  //       start_level_ = 0;
  //       start_level_inputs_.level = 0;
  //       start_level_inputs_.files.clear();
  //       start_level_inputs_.files.push_back(f);
  //       output_level_ = 0;
  //       compaction_reason_ = CompactionReason::kFilesMarkedForCompaction;
  //       std::cout << "[Delta-Smart] Ghost Drop for L0 File " << f->fd.GetNumber()
  //                 << " (garbage=" << garbage_ratio << ")" << std::endl;
  //       return true;
  //     }
  //   }
  // }

  // If not yet over trigger, skip smart heavy logic
  if (total_files < static_cast<size_t>(compaction_trigger)) {
    return false;
  }

  // Precompute L1 pressure (taint-aware)
  uint64_t global_effective_l1 = 0;
  uint64_t global_raw_l1 = 0;
  const auto& l1_files_all = vstorage_->LevelFiles(1);
  for (const auto* lf : l1_files_all) {
    uint64_t sz = lf->fd.GetFileSize();
    global_raw_l1 += sz;
    double taint_ratio = 0.0;
    if (hotspot_manager) {
      taint_ratio = hotspot_manager->GetL1FileTaintRatio(lf->fd.GetNumber(), sz);
      if (taint_ratio < 0.0) taint_ratio = 0.0;
      if (taint_ratio > 1.0) taint_ratio = 1.0;
    }
    uint64_t eff = static_cast<uint64_t>(sz * (1.0 - taint_ratio));
    global_effective_l1 += std::max<uint64_t>(1, eff);
  }
  uint64_t l1_capacity = std::max<uint64_t>(1, mutable_cf_options_.max_bytes_for_level_base);
  double l1_pressure_ratio = static_cast<double>(global_effective_l1) / static_cast<double>(l1_capacity);

  // Search best sink (L0->L1)
  double best_sink_score = -1.0;
  std::vector<FileMetaData*> best_sink_group;
  uint64_t best_sink_raw_l0 = 0;
  uint64_t best_sink_effective_l1 = 0;
  bool best_is_trivial = false;

  const size_t kMaxGroupFiles = std::max(static_cast<size_t>(total_files / 1.5), static_cast<size_t>(12));
  const double trivial_bonus = 2.0; // modest boost for trivial moves

  // base scoring controls (moderate)
  const double primary_base_score = 2e-9;      // baseline for "good" candidate
  const double fallback_min_score = 5e-10;    // permissive fallback bar

  auto compute_backlog_multiplier = [&](size_t files)->double {
    double m = 1.0;
    if (files > static_cast<size_t>(compaction_trigger)) {
      m += (static_cast<double>(files) / static_cast<double>(compaction_trigger) - 1.0) * 0.35;
      if (m > 2.5) m = 2.5;
    }
    return m;
  };

  for (size_t i = 0; i < total_files; ++i) {
    FileMetaData* seed = l0_files[i];
    if (seed->being_compacted) continue;

    // build overlap-connected component (cap group size)
    std::vector<FileMetaData*> group_files;
    group_files.push_back(seed);
    InternalKey group_smallest = seed->smallest;
    InternalKey group_largest = seed->largest;
    bool expanded = true;
    while (expanded && group_files.size() < kMaxGroupFiles) {
      expanded = false;
      for (size_t j = 0; j < total_files && group_files.size() < kMaxGroupFiles; ++j) {
        FileMetaData* f = l0_files[j];
        if (f->being_compacted) continue;
        if (std::find(group_files.begin(), group_files.end(), f) != group_files.end()) continue;
        if (icmp->Compare(f->largest, group_smallest) >= 0 &&
            icmp->Compare(f->smallest, group_largest) <= 0) {
          group_files.push_back(f);
          if (icmp->Compare(f->smallest, group_smallest) < 0) group_smallest = f->smallest;
          if (icmp->Compare(f->largest, group_largest) > 0) group_largest = f->largest;
          expanded = true;
        }
      }
    }

    // compute sizes and garbage hints
    uint64_t group_raw_l0 = 0;
    uint64_t group_effective_l0 = 0;
    double avg_l0_garbage = 0.0;
    for (auto* f : group_files) {
      uint64_t sz = f->fd.GetFileSize();
      group_raw_l0 += sz;
      double l0_garbage = 0.0;
      if (hotspot_manager) l0_garbage = hotspot_manager->GetL0FileGarbageRatio(f->fd.GetNumber());
      avg_l0_garbage += l0_garbage;
      uint64_t effective_sz = static_cast<uint64_t>(sz * (1.0 - l0_garbage));
      group_effective_l0 += std::max<uint64_t>(1, effective_sz);
    }
    if (!group_files.empty()) avg_l0_garbage /= static_cast<double>(group_files.size());

    // overlapping L1 inputs
    std::vector<FileMetaData*> l1_overlaps;
    vstorage_->GetOverlappingInputs(target_level, &group_smallest, &group_largest, &l1_overlaps);

    uint64_t group_effective_l1 = 0;
    uint64_t group_raw_l1 = 0;
    double avg_l1_taint = 0.0;
    for (const auto* l1_f : l1_overlaps) {
      uint64_t l1_sz = l1_f->fd.GetFileSize();
      group_raw_l1 += l1_sz;
      double taint_ratio = 0.0;
      if (hotspot_manager) {
        taint_ratio = hotspot_manager->GetL1FileTaintRatio(l1_f->fd.GetNumber(), l1_sz);
        if (taint_ratio < 0.0) taint_ratio = 0.0;
        if (taint_ratio > 1.0) taint_ratio = 1.0;
      }
      avg_l1_taint += taint_ratio;
      double taint_boost = 1.0;
      if (taint_ratio > 0.2) taint_boost = 0.9; // mild favor to dirty L1
      uint64_t effective_l1_piece = static_cast<uint64_t>(l1_sz * (1.0 - taint_ratio) * taint_boost);
      group_effective_l1 += std::max<uint64_t>(1, effective_l1_piece);
    }
    if (!l1_overlaps.empty()) avg_l1_taint /= static_cast<double>(l1_overlaps.size());

    bool is_trivial = l1_overlaps.empty();

    // compute wa (effective)
    double wa = static_cast<double>(group_effective_l0 + group_effective_l1) / static_cast<double>(group_effective_l0);

    // compute benefit & write_cost
    double garbage_cleanup = 0.0;
    if (group_raw_l0 > 0) {
      garbage_cleanup = static_cast<double>(group_raw_l0 - group_effective_l0) / static_cast<double>(group_raw_l0);
    }
    double backlog_multiplier = compute_backlog_multiplier(total_files);
    double hotspot_bonus = 1.0 + avg_l0_garbage * 1.2; // modest boost for cleanup potential
    double compactness_bonus = 1.0;
    if ((group_raw_l1 + group_raw_l0) > 0) {
      double overlap_ratio = static_cast<double>(group_raw_l1) / static_cast<double>(group_raw_l1 + group_raw_l0);
      compactness_bonus = 1.0 + (1.0 - overlap_ratio) * 0.8;
    }

    double benefit = static_cast<double>(group_files.size()) * (1.0 + 1.2 * garbage_cleanup) * backlog_multiplier * hotspot_bonus * compactness_bonus;
    double write_cost = static_cast<double>(group_effective_l0 + group_effective_l1);

    // mild overlap penalty (sqrt scale)
    double overlap_ratio = 0.0;
    if ((group_raw_l1 + group_raw_l0) > 0) {
      overlap_ratio = static_cast<double>(group_raw_l1) / static_cast<double>(group_raw_l1 + group_raw_l0);
    }
    double overlap_penalty = std::sqrt(1.0 + overlap_ratio);

    // score: penalize wa with power 1.5 (sensitive to larger WA but not square)
    double score = 0.0;
    if (write_cost > 0.0) {
      score = benefit / (write_cost * std::pow(std::max(1.0, wa), 1.5) * overlap_penalty);
    }

    if (is_trivial) score *= trivial_bonus;

    // size penalty for overly large groups
    if (group_raw_l0 > kMaxCompactionBytes) {
      double size_penalty = sqrt(static_cast<double>(kMaxCompactionBytes) / static_cast<double>(group_raw_l0));
      score *= size_penalty;
    }

    // keep best candidate
    if (score > best_sink_score) {
      best_sink_score = score;
      best_sink_group = group_files;
      best_sink_raw_l0 = group_raw_l0;
      best_sink_effective_l1 = group_effective_l1;
      best_is_trivial = is_trivial;
    }
  } // end for seeds

  // compute best intra-L0 candidate (for emergency/housekeeping)
  double best_align_score = -1.0;
  std::vector<FileMetaData*> best_align_files;
  double intra_priority_multiplier = 1.0;
  if (total_files >= static_cast<size_t>(compaction_trigger) * 2) {
    intra_priority_multiplier = std::min(2.0, 1.0 + (static_cast<double>(total_files) / static_cast<double>(compaction_trigger) - 2.0) * 0.25);
  }
  for (size_t win = 3; win <= 8; ++win) {
    if (total_files < win) break;
    for (size_t i = 0; i + win <= total_files; ++i) {
      bool valid = true;
      uint64_t window_raw_size = 0;
      std::vector<FileMetaData*> window_files;
      window_files.reserve(win);
      for (size_t j = 0; j < win; ++j) {
        FileMetaData* f = l0_files[i + j];
        if (f->being_compacted) { valid = false; break; }
        window_files.push_back(f);
        window_raw_size += f->fd.GetFileSize();
      }
      if (!valid) continue;
      uint64_t threshold = mutable_cf_options_.target_file_size_base * 3;
      if (window_raw_size > threshold * static_cast<uint64_t>(intra_priority_multiplier)) continue;
      double window_score = static_cast<double>(window_files.size()) * intra_priority_multiplier / (std::sqrt(static_cast<double>(window_raw_size)) + 1.0);
      if (window_score > best_align_score) {
        best_align_score = window_score;
        best_align_files = window_files;
      }
    }
  }

  // Decision logic: adaptive but conservative
  // compute scaled primary threshold (start moderate)
  double scaled_primary_threshold = primary_base_score * (1.0 + l1_pressure_ratio * 3.5);
  // relax when L0 grows, but gently (avoid accepting garbage)
  if (total_files >= static_cast<size_t>(compaction_trigger) * 2) scaled_primary_threshold *= 0.7;
  if (total_files >= static_cast<size_t>(compaction_trigger) * 3) scaled_primary_threshold *= 0.5;

  // compute final WA estimate for best candidate
  double final_wa = 0.0;
  if (best_sink_raw_l0 > 0) final_wa = static_cast<double>(best_sink_raw_l0 + best_sink_effective_l1) / static_cast<double>(best_sink_raw_l0);

  // Primary acceptance rules (strict WA, moderate score, small rate limit)
  double primary_allowed_wa = best_is_trivial ? 3.0 : 2.5; // trivial move allowed a little more
  const uint64_t min_primary_interval_calls = 2; // don't primary every picker call
  uint64_t last_primary = last_primary_call.load(std::memory_order_relaxed);

  if (best_sink_group.size() >= 2 && best_sink_score > scaled_primary_threshold) {
    if (final_wa <= primary_allowed_wa) {
      // rate limit to avoid continuous primary churn
        std::sort(best_sink_group.begin(), best_sink_group.end(),
                  [](FileMetaData* a, FileMetaData* b) { return a->fd.GetNumber() > b->fd.GetNumber(); });
        start_level_ = 0;
        start_level_inputs_.level = 0;
        start_level_inputs_.files = best_sink_group;
        output_level_ = target_level;
        compaction_reason_ = CompactionReason::kLevelL0FilesNum;
        std::cout << "[Delta-Smart] PRIMARY Sink chosen: files=" << best_sink_group.size()
                  << " score=" << best_sink_score << " WA=" << final_wa
                  << " trivial=" << (int)best_is_trivial << " L0=" << total_files << std::endl;
        return true;
    } else {
      std::cout << "[Delta-Smart] PRIMARY candidate rejected for WA " << final_wa << " > allowed " << primary_allowed_wa << " (L0=" << total_files << ")\n";
    }
  }

  // Emergency Intra-L0: when L0 reaches emergency threshold (fast brake)
  if (total_files >= intra_emergency_threshold) {
    if (!best_align_files.empty()) {
      start_level_ = 0;
      start_level_inputs_.level = 0;
      start_level_inputs_.files = best_align_files;
      output_level_ = 0;
      compaction_reason_ = CompactionReason::kLevelL0FilesNum;
      std::cout << "[Delta-Smart] Emergency Intra-L0 triggered (L0=" << total_files
                << " >= emergency=" << intra_emergency_threshold << "), window_files=" << best_align_files.size()
                << " score=" << best_align_score << std::endl;
      return true;
    } else {
      // simple pair fallback
      for (size_t i = 0; i + 1 < total_files; ++i) {
        FileMetaData* a = l0_files[i];
        FileMetaData* b = l0_files[i+1];
        if (!a->being_compacted && !b->being_compacted) {
          start_level_ = 0;
          start_level_inputs_.level = 0;
          start_level_inputs_.files.clear();
          start_level_inputs_.files.push_back(a);
          start_level_inputs_.files.push_back(b);
          output_level_ = 0;
          compaction_reason_ = CompactionReason::kLevelL0FilesNum;
          std::cout << "[Delta-Smart] Emergency Intra-L0 fallback (pair) triggered." << std::endl;
          return true;
        }
      }
    }
  }

  uint64_t call_idx = global_picker_calls.fetch_add(1, std::memory_order_relaxed);

  // periodic housekeeping intra (low freq)
  const uint64_t housekeeping_interval = 8;
  if ((call_idx % housekeeping_interval) == 0) {
    if (total_files >= static_cast<size_t>(compaction_trigger) * 2 && !best_align_files.empty()) {
      start_level_ = 0;
      start_level_inputs_.level = 0;
      start_level_inputs_.files = best_align_files;
      output_level_ = 0;
      compaction_reason_ = CompactionReason::kLevelL0FilesNum;
      std::cout << "[Delta-Smart] Housekeeping Intra-L0 triggered (periodic). window_files="
                << best_align_files.size() << " score=" << best_align_score << std::endl;
      return true;
    }
  }

  // Controlled Fallback sink (only when L0 is overloaded)
  if (total_files >= static_cast<size_t>(intra_emergency_threshold* 0.75)  && best_sink_group.size() >= 2) {
    double fallback_allowed_wa = 4.0; // permissive but bounded
    if (total_files >= static_cast<size_t>(compaction_trigger) * 3) {
      fallback_allowed_wa = 6.0; // last resort if extremely overloaded
    }
    if (best_sink_score > fallback_min_score && final_wa <= fallback_allowed_wa) {
      std::sort(best_sink_group.begin(), best_sink_group.end(),
                [](FileMetaData* a, FileMetaData* b) { return a->fd.GetNumber() > b->fd.GetNumber(); });
      start_level_ = 0;
      start_level_inputs_.level = 0;
      start_level_inputs_.files = best_sink_group;
      output_level_ = target_level;
      compaction_reason_ = CompactionReason::kLevelL0FilesNum;
      std::cout << "[Delta-Smart] FALLBACK Sink chosen: files=" << best_sink_group.size()
                << " score=" << best_sink_score << " WA=" << final_wa << " (L0=" << total_files << ")" << std::endl;
      return true;
    } else {
      std::cout << "[Delta-Smart] No acceptable fallback sink (best_score=" << best_sink_score
                << " WA=" << final_wa << " L0=" << total_files << ")" << std::endl;
    }
  }

  // Nothing selected
  return false;
}

bool LevelCompactionBuilder::PickSmartL1Purge() {
  auto hotspot_manager = ioptions_.hotspot_manager;
  if (!hotspot_manager) return false;

  const auto& l1_files = vstorage_->LevelFiles(1);
  if (l1_files.empty()) return false;

  for (size_t i = 0; i < l1_files.size(); ++i) {
      FileMetaData* f = l1_files[i];
      if (f->being_compacted) continue;

      double taint_ratio = hotspot_manager->GetL1FileTaintRatio(f->fd.GetNumber(), f->fd.GetFileSize());

      // 陨石坑爆破
      if (taint_ratio > 0.3) {
          start_level_ = 1; // 【关键修复】主动宣告接管 L1
          start_level_inputs_.level = 1;
          start_level_inputs_.files.push_back(f);
          
          output_level_ = 1; 
          compaction_reason_ = CompactionReason::kFilesMarkedForCompaction;

          std::cout <<
              "[Delta-Smart] L1 Meteor Crater! File: " << f->fd.GetNumber() << ", Taint: " << taint_ratio * 100.0 << "%. Triggering L1->L1 GC." << std::endl;
          return true;
      }
  }
  return false;
}

Compaction* LevelCompactionBuilder::PickCompaction() {
  start_level_inputs_.clear();
  output_level_inputs_.clear();
  compaction_inputs_.clear();

  // 1. 优先尝试我们的 Smart L0->L1 Picker (主动触发 L0)
  if (PickSmartL0ToL1Compaction()) {
      bool setup_ok = true;
      if (output_level_ != start_level_) {
          setup_ok = SetupOtherInputsIfNeeded();
      } else {
          // 同层合并 (Intra-L0)
          // 手动同步状态，否则 GetCompaction 会断言失败
          if (!start_level_inputs_.empty()) {
              compaction_inputs_.push_back(start_level_inputs_);
              setup_ok = true; 
          } else {
              setup_ok = false;
          }
      }

      if (setup_ok) {
          if (compaction_inputs_.empty()) {
              std::cout<<"[Delta-Smart] Logic Error: compaction_inputs_ is still empty.\n";
          } else {
              Compaction* c = GetCompaction();
              if (c != nullptr) {
                  TEST_SYNC_POINT_CALLBACK("LevelCompactionPicker::PickCompaction:Return", c);
                  return c;
              }
              std::cout<<"[Delta-Smart] Warning: GetCompaction() returned nullptr. Fallback invoked."<<std::endl;
          } 
      }
      
      // 失败清理，复位 start_level_
      start_level_inputs_.clear();
      output_level_inputs_.clear();
      compaction_inputs_.clear();
      start_level_ = -1; 
      output_level_ = -1;
  }

  // 2. 尝试 L1 陨石坑定点 GC (主动触发 L1)
  if (PickSmartL1Purge()) {
      Compaction* c = GetCompaction(); // L1->L1 原地合并，不需要 SetupOtherInputs
      if (c != nullptr) {
          TEST_SYNC_POINT_CALLBACK("LevelCompactionPicker::PickCompaction:Return", c);
          std::cout<<"[Delta-Smart] L1 GC Compaction picked successfully."<<std::endl;
          return c;
      }
      
      // 失败清理，复位
      start_level_inputs_.clear();
      output_level_inputs_.clear();
      compaction_inputs_.clear();
      start_level_ = -1;
      output_level_ = -1;
  }

  // 3. 原生兜底逻辑
  // 注意：只有走到这里，RocksDB 原生逻辑才会计算分数，并把 start_level_ 设置为 0 或 1
  SetupInitialFiles();
  if (start_level_inputs_.empty()) return nullptr;
  assert(start_level_ >= 0 && output_level_ >= 0);

  if (!SetupOtherL0FilesIfNeeded()) return nullptr;
  if (!SetupOtherInputsIfNeeded()) return nullptr;

  Compaction* c = GetCompaction();
  TEST_SYNC_POINT_CALLBACK("LevelCompactionPicker::PickCompaction:Return", c);
  return c;
}

/*Compaction* LevelCompactionBuilder::PickCompaction() {
  // Pick up the first file to start compaction. It may have been extended
  // to a clean cut.
  SetupInitialFiles();
  if (start_level_inputs_.empty()) {
    return nullptr;
  }
  assert(start_level_ >= 0 && output_level_ >= 0);

  // 原来的逻辑
  SetupInitialFiles();

  // If it is a L0 -> base level compaction, we need to set up other L0
  // files if needed.
  if (!SetupOtherL0FilesIfNeeded()) {
    return nullptr;
  }

  // Pick files in the output level and expand more files in the start level
  // if needed.
  if (!SetupOtherInputsIfNeeded()) {
    return nullptr;
  }

  // Form a compaction object containing the files we picked.
  Compaction* c = GetCompaction();

  TEST_SYNC_POINT_CALLBACK("LevelCompactionPicker::PickCompaction:Return", c);

  return c;
}*/

Compaction* LevelCompactionBuilder::GetCompaction() {
  // TryPickL0TrivialMove() does not apply to the case when compacting L0 to an
  // empty output level. So L0 files is picked in PickFileToCompact() by
  // compaction score. We may still be able to do trivial move when this file
  // does not overlap with other L0s. This happens when
  // compaction_inputs_[0].size() == 1 since SetupOtherL0FilesIfNeeded() did not
  // pull in more L0s.
  assert(!compaction_inputs_.empty());
  bool l0_files_might_overlap =
      start_level_ == 0 && !is_l0_trivial_move_ &&
      (compaction_inputs_.size() > 1 || compaction_inputs_[0].size() > 1);
  auto c = new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, mutable_db_options_,
      std::move(compaction_inputs_), output_level_,
      MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                          ioptions_.compaction_style, vstorage_->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options_.max_compaction_bytes,
      GetPathId(ioptions_, mutable_cf_options_, output_level_),
      GetCompressionType(vstorage_, mutable_cf_options_, output_level_,
                         vstorage_->base_level()),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level_),
      Temperature::kUnknown,
      /* max_subcompactions */ 0, std::move(grandparents_),
      /* earliest_snapshot */ std::nullopt, /* snapshot_checker */ nullptr,
      compaction_reason_,
      /* trim_ts */ "", start_level_score_, l0_files_might_overlap);

  // If it's level 0 compaction, make sure we don't execute any other level 0
  // compactions in parallel
  compaction_picker_->RegisterCompaction(c);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
  return c;
}

/*
 * Find the optimal path to place a file
 * Given a level, finds the path where levels up to it will fit in levels
 * up to and including this path
 */
uint32_t LevelCompactionBuilder::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, int level) {
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());

  // size remaining in the most recent path
  uint64_t current_path_size = ioptions.cf_paths[0].target_size;

  uint64_t level_size;
  int cur_level = 0;

  // max_bytes_for_level_base denotes L1 size.
  // We estimate L0 size to be the same as L1.
  level_size = mutable_cf_options.max_bytes_for_level_base;

  // Last path is the fallback
  while (p < ioptions.cf_paths.size() - 1) {
    if (level_size <= current_path_size) {
      if (cur_level == level) {
        // Does desired level fit in this path?
        return p;
      } else {
        current_path_size -= level_size;
        if (cur_level > 0) {
          if (ioptions.level_compaction_dynamic_level_bytes) {
            // Currently, level_compaction_dynamic_level_bytes is ignored when
            // multiple db paths are specified. https://github.com/facebook/
            // rocksdb/blob/main/db/column_family.cc.
            // Still, adding this check to avoid accidentally using
            // max_bytes_for_level_multiplier_additional
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier);
          } else {
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier *
                mutable_cf_options.MaxBytesMultiplerAdditional(cur_level));
          }
        }
        cur_level++;
        continue;
      }
    }
    p++;
    current_path_size = ioptions.cf_paths[p].target_size;
  }
  return p;
}

bool LevelCompactionBuilder::TryPickL0TrivialMove() {
  if (vstorage_->base_level() <= 0) {
    return false;
  }
  if (start_level_ == 0 && mutable_cf_options_.compression_per_level.empty() &&
      !vstorage_->LevelFiles(output_level_).empty() &&
      ioptions_.db_paths.size() <= 1) {
    // Try to pick trivial move from L0 to L1. We start from the oldest
    // file. We keep expanding to newer files if it would form a
    // trivial move.
    // For now we don't support it with
    // mutable_cf_options_.compression_per_level to prevent the logic
    // of determining whether L0 can be trivial moved to the next level.
    // We skip the case where output level is empty, since in this case, at
    // least the oldest file would qualify for trivial move, and this would
    // be a surprising behavior with few benefits.

    // We search from the oldest file from the newest. In theory, there are
    // files in the middle can form trivial move too, but it is probably
    // uncommon and we ignore these cases for simplicity.
    const std::vector<FileMetaData*>& level_files =
        vstorage_->LevelFiles(start_level_);

    InternalKey my_smallest, my_largest;
    for (auto it = level_files.rbegin(); it != level_files.rend(); ++it) {
      CompactionInputFiles output_level_inputs;
      output_level_inputs.level = output_level_;
      FileMetaData* file = *it;
      if (it == level_files.rbegin()) {
        my_smallest = file->smallest;
        my_largest = file->largest;
      } else {
        if (compaction_picker_->icmp()->Compare(file->largest, my_smallest) <
            0) {
          my_smallest = file->smallest;
        } else if (compaction_picker_->icmp()->Compare(file->smallest,
                                                       my_largest) > 0) {
          my_largest = file->largest;
        } else {
          break;
        }
      }
      vstorage_->GetOverlappingInputs(output_level_, &my_smallest, &my_largest,
                                      &output_level_inputs.files);
      if (output_level_inputs.empty()) {
        assert(!file->being_compacted);
        start_level_inputs_.files.push_back(file);
      } else {
        break;
      }
    }
  }

  if (!start_level_inputs_.empty()) {
    // Sort files by key range. Not sure it's 100% necessary but it's cleaner
    // to always keep files sorted by key the key ranges don't overlap.
    std::sort(start_level_inputs_.files.begin(),
              start_level_inputs_.files.end(),
              [icmp = compaction_picker_->icmp()](FileMetaData* f1,
                                                  FileMetaData* f2) -> bool {
                return (icmp->Compare(f1->smallest, f2->smallest) < 0);
              });

    is_l0_trivial_move_ = true;
    return true;
  }
  return false;
}

bool LevelCompactionBuilder::TryExtendNonL0TrivialMove(int start_index,
                                                       bool only_expand_right) {
  if (start_level_inputs_.size() == 1 &&
      (ioptions_.db_paths.empty() || ioptions_.db_paths.size() == 1) &&
      (mutable_cf_options_.compression_per_level.empty())) {
    // Only file of `index`, and it is likely a trivial move. Try to
    // expand if it is still a trivial move, but not beyond
    // max_compaction_bytes or 4 files, so that we don't create too
    // much compaction pressure for the next level.
    // Ignore if there are more than one DB path, as it would be hard
    // to predict whether it is a trivial move.
    const std::vector<FileMetaData*>& level_files =
        vstorage_->LevelFiles(start_level_);
    const size_t kMaxMultiTrivialMove = 4;
    FileMetaData* initial_file = start_level_inputs_.files[0];
    size_t total_size = initial_file->fd.GetFileSize();
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    // Expand towards right
    for (int i = start_index + 1;
         i < static_cast<int>(level_files.size()) &&
         start_level_inputs_.size() < kMaxMultiTrivialMove;
         i++) {
      FileMetaData* next_file = level_files[i];
      if (next_file->being_compacted) {
        break;
      }
      vstorage_->GetOverlappingInputs(output_level_, &(initial_file->smallest),
                                      &(next_file->largest),
                                      &output_level_inputs.files);
      if (!output_level_inputs.empty()) {
        break;
      }
      if (i < static_cast<int>(level_files.size()) - 1 &&
          compaction_picker_->icmp()
                  ->user_comparator()
                  ->CompareWithoutTimestamp(
                      next_file->largest.user_key(),
                      level_files[i + 1]->smallest.user_key()) == 0) {
        TEST_SYNC_POINT_CALLBACK(
            "LevelCompactionBuilder::TryExtendNonL0TrivialMove:NoCleanCut",
            nullptr);
        // Not a clean up after adding the next file. Skip.
        break;
      }
      total_size += next_file->fd.GetFileSize();
      if (total_size > mutable_cf_options_.max_compaction_bytes) {
        break;
      }
      start_level_inputs_.files.push_back(next_file);
    }
    // Expand towards left
    if (!only_expand_right) {
      for (int i = start_index - 1;
           i >= 0 && start_level_inputs_.size() < kMaxMultiTrivialMove; i--) {
        FileMetaData* next_file = level_files[i];
        if (next_file->being_compacted) {
          break;
        }
        vstorage_->GetOverlappingInputs(output_level_, &(next_file->smallest),
                                        &(initial_file->largest),
                                        &output_level_inputs.files);
        if (!output_level_inputs.empty()) {
          break;
        }
        if (i > 0 && compaction_picker_->icmp()
                             ->user_comparator()
                             ->CompareWithoutTimestamp(
                                 next_file->smallest.user_key(),
                                 level_files[i - 1]->largest.user_key()) == 0) {
          // Not a clean up after adding the next file. Skip.
          break;
        }
        total_size += next_file->fd.GetFileSize();
        if (total_size > mutable_cf_options_.max_compaction_bytes) {
          break;
        }
        // keep `files` sorted in increasing order by key range
        start_level_inputs_.files.insert(start_level_inputs_.files.begin(),
                                         next_file);
      }
    }
    return start_level_inputs_.size() > 1;
  }
  return false;
}

bool LevelCompactionBuilder::PickFileToCompact() {
  // level 0 files are overlapping. So we cannot pick more
  // than one concurrent compactions at this level. This
  // could be made better by looking at key-ranges that are
  // being compacted at level 0.
  if (start_level_ == 0 &&
      !compaction_picker_->level0_compactions_in_progress()->empty()) {
    if (PickSizeBasedIntraL0Compaction()) {
      return true;
    }
    TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
    return false;
  }

  start_level_inputs_.clear();
  start_level_inputs_.level = start_level_;

  assert(start_level_ >= 0);

  if (TryPickL0TrivialMove()) {
    return true;
  }
  if (start_level_ == 0 && PickSizeBasedIntraL0Compaction()) {
    return true;
  }

  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);

  // Pick the file with the highest score in this level that is not already
  // being compacted.
  const std::vector<int>& file_scores =
      vstorage_->FilesByCompactionPri(start_level_);

  unsigned int cmp_idx;
  for (cmp_idx = vstorage_->NextCompactionIndex(start_level_);
       cmp_idx < file_scores.size(); cmp_idx++) {
    int index = file_scores[cmp_idx];
    auto* f = level_files[index];

    // do not pick a file to compact if it is being compacted
    // from n-1 level.
    if (f->being_compacted) {
      if (ioptions_.compaction_pri == kRoundRobin) {
        // TODO(zichen): this file may be involved in one compaction from
        // an upper level, cannot advance the cursor for round-robin policy.
        // Currently, we do not pick any file to compact in this case. We
        // should fix this later to ensure a compaction is picked but the
        // cursor shall not be advanced.
        return false;
      }
      continue;
    }

    start_level_inputs_.files.push_back(f);
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &start_level_inputs_) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {start_level_inputs_}, output_level_,
            Compaction::EvaluateProximalLevel(vstorage_, mutable_cf_options_,
                                              ioptions_, start_level_,
                                              output_level_))) {
      // A locked (pending compaction) input-level file was pulled in due to
      // user-key overlap.
      start_level_inputs_.clear();

      if (ioptions_.compaction_pri == kRoundRobin) {
        return false;
      }
      continue;
    }

    // Now that input level is fully expanded, we check whether any output
    // files are locked due to pending compaction.
    //
    // Note we rely on ExpandInputsToCleanCut() to tell us whether any output-
    // level files are locked, not just the extra ones pulled in for user-key
    // overlap.
    InternalKey smallest, largest;
    compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);
    if (output_level_inputs.empty()) {
      if (start_level_ > 0 &&
          TryExtendNonL0TrivialMove(index,
                                    ioptions_.compaction_pri ==
                                        kRoundRobin /* only_expand_right */)) {
        break;
      }
    } else {
      if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                      &output_level_inputs)) {
        start_level_inputs_.clear();
        if (ioptions_.compaction_pri == kRoundRobin) {
          return false;
        }
        continue;
      }
    }

    base_index_ = index;
    break;
  }

  // store where to start the iteration in the next call to PickCompaction
  if (ioptions_.compaction_pri != kRoundRobin) {
    vstorage_->SetNextCompactionIndex(start_level_, cmp_idx);
  }
  return start_level_inputs_.size() > 0;
}

bool LevelCompactionBuilder::PickIntraL0Compaction() {
  start_level_inputs_.clear();
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(0 /* level */);
  if (level_files.size() <
          static_cast<size_t>(
              mutable_cf_options_.level0_file_num_compaction_trigger + 2) ||
      level_files[0]->being_compacted) {
    // If L0 isn't accumulating much files beyond the regular trigger, don't
    // resort to L0->L0 compaction yet.
    return false;
  }
  return FindIntraL0Compaction(level_files, kMinFilesForIntraL0Compaction,
                               std::numeric_limits<uint64_t>::max(),
                               mutable_cf_options_.max_compaction_bytes,
                               &start_level_inputs_);
}

bool LevelCompactionBuilder::PickSizeBasedIntraL0Compaction() {
  assert(start_level_ == 0);
  int base_level = vstorage_->base_level();
  if (base_level <= 0) {
    return false;
  }
  const std::vector<FileMetaData*>& l0_files =
      vstorage_->LevelFiles(/*level=*/0);
  size_t min_num_file =
      std::max(2, mutable_cf_options_.level0_file_num_compaction_trigger);
  if (l0_files.size() < min_num_file) {
    return false;
  }
  uint64_t l0_size = 0;
  for (const auto& file : l0_files) {
    assert(file->compensated_file_size >= file->fd.GetFileSize());
    // Compact down L0s with more deletions.
    l0_size += file->compensated_file_size;
  }

  // Avoid L0->Lbase compactions that are inefficient for write-amp.
  const double kMultiplier =
      std::max(10.0, mutable_cf_options_.max_bytes_for_level_multiplier) * 2;
  const uint64_t min_lbase_size = MultiplyCheckOverflow(l0_size, kMultiplier);
  assert(min_lbase_size >= l0_size);
  const std::vector<FileMetaData*>& lbase_files =
      vstorage_->LevelFiles(/*level=*/base_level);
  uint64_t lbase_size = 0;
  for (const auto& file : lbase_files) {
    lbase_size += file->fd.GetFileSize();
    if (lbase_size > min_lbase_size) {
      break;
    }
  }
  if (lbase_size <= min_lbase_size) {
    return false;
  }

  start_level_inputs_.clear();
  start_level_inputs_.level = 0;
  for (const auto& file : l0_files) {
    if (file->being_compacted) {
      break;
    }
    start_level_inputs_.files.push_back(file);
  }
  if (start_level_inputs_.files.size() < min_num_file) {
    start_level_inputs_.clear();
    return false;
  }
  output_level_ = 0;
  return true /* picked an intra-L0 compaction */;
}
}  // namespace

Compaction* LevelCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& /*existing_snapshots */,
    const SnapshotChecker* /*snapshot_checker*/, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, bool /* require_max_output_level*/) {
  LevelCompactionBuilder builder(cf_name, vstorage, this, log_buffer,
                                 mutable_cf_options, ioptions_,
                                 mutable_db_options);
  return builder.PickCompaction();
}
}  // namespace ROCKSDB_NAMESPACE
