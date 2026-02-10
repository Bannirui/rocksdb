//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <cstdio>

#include "file/sequence_file_reader.h"
#include "port/lang.h"
#include "rocksdb/env.h"
#include "test_util/sync_point.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE::log {

Reader::Reporter::~Reporter() = default;

Reader::Reader(std::shared_ptr<Logger> info_log,
               std::unique_ptr<SequentialFileReader>&& _file,
               Reporter* reporter, bool checksum, uint64_t log_num,
               bool track_and_verify_wals, bool stop_replay_for_corruption,
               uint64_t min_wal_number_to_keep,
               const PredecessorWALInfo& observed_predecessor_wal_info)
    : info_log_(info_log),
      file_(std::move(_file)),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      read_error_(false),
      eof_offset_(0),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      log_number_(log_num),
      track_and_verify_wals_(track_and_verify_wals),
      stop_replay_for_corruption_(stop_replay_for_corruption),
      min_wal_number_to_keep_(min_wal_number_to_keep),
      observed_predecessor_wal_info_(observed_predecessor_wal_info),
      recycled_(false),
      first_record_read_(false),
      compression_type_(kNoCompression),
      compression_type_record_read_(false),
      uncompress_(nullptr),
      hash_state_(nullptr),
      uncompress_hash_state_(nullptr) {}

Reader::~Reader() {
  delete[] backing_store_;
  if (uncompress_) {
    delete uncompress_;
  }
  if (hash_state_) {
    XXH3_freeState(hash_state_);
  }
  if (uncompress_hash_state_) {
    XXH3_freeState(uncompress_hash_state_);
  }
}

// For kAbsoluteConsistency, on clean shutdown we don't expect any error
// in the log files.  For other modes, we can ignore only incomplete records
// in the last log file, which are presumably due to a write in progress
// during restart (or from log recycling).
//
// TODO krad: Evaluate if we need to move to a more strict mode where we
// restrict the inconsistency to only the last log
// TODO (hx235): move `wal_recovery_mode` to be a member data like other
// information (e.g, `stop_replay_for_corruption`) to decide whether to
// check for and surface corruption in `ReadRecord()`
/**
 * 从文件里面读一个record出来 record就是逻辑层协议
 * 1 record是RocksDB抽象的概念 给wal和manifest用
 * 2 不关注怎么跟操作系统的文件系统交互的
 *   2.1 实际上每次跟操作系统读写单位是block
 *   2.2 RocksDB还抽象了fragment概念 1个block分割成多个fragment
 *   2.3 1个record可能是由1个或多个fragment组成
 * 3 最终出参record收到的是一个完整的record
 * @param record RocksDB抽象的概念 它由一个或多个fragment组成 已经被剥掉了物理层协议头7字节 现在就是物理层协议体的原始字节 变成了逻辑层协议
 *               对于manifest 此时的逻辑协议没有协议头 就直接是VersionEdit信息
 *               对于wal 此时的逻辑协议又是WriteBatch协议头+body的设计 逻辑协议头12字节
 * @param scratch 当record是由多个fragment组成的时候 它是用来当缓冲区不断拼接fragment 等整个record收集全了
 */
bool Reader::ReadRecord(Slice* record, std::string* scratch,
                        WALRecoveryMode wal_recovery_mode,
                        uint64_t* record_checksum) {
  scratch->clear();
  record->clear();
  if (record_checksum != nullptr) {
    if (hash_state_ == nullptr) {
      hash_state_ = XXH3_createState();
    }
    XXH3_64bits_reset(hash_state_);
  }
  if (uncompress_) {
    uncompress_->Reset();
  }
  /**
   * 这个标识的作用是拼接多个fragment的依赖
   * 1 record就是一个fragment 这个标识用不到
   * 2 record由多个fragment组成 那就意味着读到fragment的时候要拼成record
   *   fragment标识是record头 就要打上这个标识为true
   *   fragment标识是record中间 就继续拼接
   *   fragment标识是record尾 整个record就收集全了
   */
  bool in_fragmented_record = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;

  // LogReader对应的逻辑概念是RocksDB的record
  // 文件系统的读写单位是Block 因此RocksDB又建立了fragment概念
  // 1 文件系统的chunk是由多个block组成
  // 2 多个RocksDB的fragment组成block
  // 3 record由一个或多个fragment组成
  // LogReader对外交互的是record 跟系统对内交互的是block 因此它需要把block分割成多个fragment 然后再尝试组装成record
  Slice fragment;
  for (;;) {
    uint64_t physical_record_offset = end_of_buffer_offset_ - buffer_.size();
    size_t drop_size = 0;
    // 读一个fragment出来 物理层协议体
    const uint8_t record_type =
        ReadPhysicalRecord(&fragment, &drop_size, record_checksum);
    switch (record_type) {
      case kFullType:
      case kRecyclableFullType:
        if (in_fragmented_record && !scratch->empty()) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          ReportCorruption(scratch->size(), "partial record without end(1)");
        }
        // No need to compute record_checksum since the record
        // consists of a single fragment and the checksum is computed
        // in ReadPhysicalRecord() if WAL compression is enabled
        if (record_checksum != nullptr && uncompress_ == nullptr) {
          // No need to stream since the record is a single fragment
          *record_checksum = XXH3_64bits(fragment.data(), fragment.size());
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
      // 当前fragment就是一个record 这种情况最简单
        *record = fragment;
        last_record_offset_ = prospective_record_offset;
        first_record_read_ = true;
        return true;

      case kFirstType:
      case kRecyclableFirstType:
        if (in_fragmented_record && !scratch->empty()) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          ReportCorruption(scratch->size(), "partial record without end(2)");
          XXH3_64bits_reset(hash_state_);
        }
        if (record_checksum != nullptr) {
          XXH3_64bits_update(hash_state_, fragment.data(), fragment.size());
        }
        prospective_record_offset = physical_record_offset;
      // fragment是record头 fragment丢到缓冲区 等着后续的fragment拼接进来 打上标识让后面的fragment知道record正在收集fragment
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;  // switch

      case kMiddleType:
      case kRecyclableMiddleType:
        if (!in_fragmented_record) {
          // 防御性校验
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          if (record_checksum != nullptr) {
            XXH3_64bits_update(hash_state_, fragment.data(), fragment.size());
          }
          // 当前fragment是record中间的某个fragment 拼接到record里面
          scratch->append(fragment.data(), fragment.size());
        }
        break;  // switch

      case kLastType:
      case kRecyclableLastType:
        if (!in_fragmented_record) {
          // 防御性校验 当前fragment是record的最后一个 那么必须保证当前record明确标识由多个fragment组成
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          if (record_checksum != nullptr) {
            XXH3_64bits_update(hash_state_, fragment.data(), fragment.size());
            *record_checksum = XXH3_64bits_digest(hash_state_);
          }
          // 当前fragment是record的尾 拼接上去就收集全了record
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          first_record_read_ = true;
          return true;
        }
        break;  // switch

      case kSetCompressionType: {
        if (compression_type_record_read_) {
          ReportCorruption(fragment.size(),
                           "read multiple SetCompressionType records");
        }
        if (first_record_read_) {
          ReportCorruption(fragment.size(),
                           "SetCompressionType not the first record");
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        last_record_offset_ = prospective_record_offset;
        CompressionTypeRecord compression_record(kNoCompression);
        Status s = compression_record.DecodeFrom(&fragment);
        if (!s.ok()) {
          ReportCorruption(fragment.size(),
                           "could not decode SetCompressionType record");
        } else {
          InitCompression(compression_record);
        }
        break;  // switch
      }
      case kPredecessorWALInfoType:
      case kRecyclePredecessorWALInfoType: {
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        last_record_offset_ = prospective_record_offset;

        PredecessorWALInfo recorded_predecessor_wal_info;
        Status s = recorded_predecessor_wal_info.DecodeFrom(&fragment);
        if (!s.ok()) {
          ReportCorruption(fragment.size(),
                           "could not decode PredecessorWALInfoType record");
        } else {
          MaybeVerifyPredecessorWALInfo(wal_recovery_mode, fragment,
                                        recorded_predecessor_wal_info);
        }
        break;  // switch
      }
      case kUserDefinedTimestampSizeType:
      case kRecyclableUserDefinedTimestampSizeType: {
        if (in_fragmented_record && !scratch->empty()) {
          ReportCorruption(
              scratch->size(),
              "user-defined timestamp size record interspersed partial record");
        }
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        last_record_offset_ = prospective_record_offset;
        UserDefinedTimestampSizeRecord ts_record;
        Status s = ts_record.DecodeFrom(&fragment);
        if (!s.ok()) {
          ReportCorruption(
              fragment.size(),
              "could not decode user-defined timestamp size record");
        } else {
          s = UpdateRecordedTimestampSize(
              ts_record.GetUserDefinedTimestampSize());
          if (!s.ok()) {
            ReportCorruption(fragment.size(), s.getState());
          }
        }
        break;  // switch
      }

      case kBadHeader:
        if (wal_recovery_mode == WALRecoveryMode::kAbsoluteConsistency ||
            wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery) {
          // In clean shutdown we don't expect any error in the log files.
          // In point-in-time recovery an incomplete record at the end could
          // produce a hole in the recovered data. Report an error here, which
          // higher layers can choose to ignore when it's provable there is no
          // hole.
          ReportCorruption(drop_size, "truncated header");
        }
        FALLTHROUGH_INTENDED;

      case kEof:
        if (in_fragmented_record) {
          if (wal_recovery_mode == WALRecoveryMode::kAbsoluteConsistency ||
              wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery) {
            // In clean shutdown we don't expect any error in the log files.
            // In point-in-time recovery an incomplete record at the end could
            // produce a hole in the recovered data. Report an error here, which
            // higher layers can choose to ignore when it's provable there is no
            // hole.
            ReportCorruption(
                scratch->size(),
                "error reading trailing data due to encountering EOF");
          }
          // This can be caused by the writer dying immediately after
          //  writing a physical record but before completing the next; don't
          //  treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kOldRecord:
        if (wal_recovery_mode != WALRecoveryMode::kSkipAnyCorruptedRecords) {
          // Treat a record from a previous instance of the log as EOF.
          if (in_fragmented_record) {
            if (wal_recovery_mode == WALRecoveryMode::kAbsoluteConsistency ||
                wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery) {
              // In clean shutdown we don't expect any error in the log files.
              // In point-in-time recovery an incomplete record at the end could
              // produce a hole in the recovered data. Report an error here,
              // which higher layers can choose to ignore when it's provable
              // there is no hole.
              ReportCorruption(
                  scratch->size(),
                  "error reading trailing data due to encountering old record");
            }
            // This can be caused by the writer dying immediately after
            //  writing a physical record but before completing the next; don't
            //  treat it as a corruption, just ignore the entire logical record.
            scratch->clear();
          } else {
            if (wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery) {
              ReportOldLogRecord(scratch->size());
            }
          }
          return false;
        }
        FALLTHROUGH_INTENDED;

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;  // switch

      case kBadRecordLen:
        if (eof_) {
          if (wal_recovery_mode == WALRecoveryMode::kAbsoluteConsistency ||
              wal_recovery_mode == WALRecoveryMode::kPointInTimeRecovery) {
            // In clean shutdown we don't expect any error in the log files.
            // In point-in-time recovery an incomplete record at the end could
            // produce a hole in the recovered data. Report an error here, which
            // higher layers can choose to ignore when it's provable there is no
            // hole.
            ReportCorruption(drop_size, "truncated record body");
          }
          return false;
        }
        FALLTHROUGH_INTENDED;

      case kBadRecordChecksum:
        if (recycled_ && wal_recovery_mode ==
                             WALRecoveryMode::kTolerateCorruptedTailRecords) {
          scratch->clear();
          return false;
        }
        if (record_type == kBadRecordLen) {
          ReportCorruption(drop_size, "bad record length");
        } else {
          ReportCorruption(drop_size, "checksum mismatch");
        }
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;  // switch

      default: {
        if ((record_type & kRecordTypeSafeIgnoreMask) == 0) {
          std::string reason =
              "unknown record type " + std::to_string(record_type);
          ReportCorruption(
              (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
              reason.c_str());
        }
        in_fragmented_record = false;
        scratch->clear();
        break;  // switch
      }
    }
  }
  // unreachable
}

void Reader::MaybeVerifyPredecessorWALInfo(
    WALRecoveryMode wal_recovery_mode, Slice fragment,
    const PredecessorWALInfo& recorded_predecessor_wal_info) {
  if (!track_and_verify_wals_ ||
      wal_recovery_mode == WALRecoveryMode::kSkipAnyCorruptedRecords ||
      stop_replay_for_corruption_) {
    return;
  }
  assert(recorded_predecessor_wal_info.IsInitialized());
  uint64_t recorded_predecessor_log_number =
      recorded_predecessor_wal_info.GetLogNumber();

  // This is the first WAL recovered thus with no predecessor WAL info has been
  // initialized
  if (!observed_predecessor_wal_info_.IsInitialized()) {
    if (recorded_predecessor_log_number >= min_wal_number_to_keep_) {
      std::string reason = "Missing WAL of log number " +
                           std::to_string(recorded_predecessor_log_number);
      ReportCorruption(fragment.size(), reason.c_str(),
                       recorded_predecessor_log_number);
    }
  } else {
    if (observed_predecessor_wal_info_.GetLogNumber() !=
        recorded_predecessor_log_number) {
      std::string reason =
          "Mismatched predecessor log number of WAL file " +
          file_->file_name() + " Recorded " +
          std::to_string(recorded_predecessor_log_number) + ". Observed " +
          std::to_string(observed_predecessor_wal_info_.GetLogNumber());
      ReportCorruption(fragment.size(), reason.c_str(),
                       recorded_predecessor_log_number);
    } else if (observed_predecessor_wal_info_.GetLastSeqnoRecorded() !=
               recorded_predecessor_wal_info.GetLastSeqnoRecorded()) {
      std::string reason =
          "Mismatched last sequence number recorded in the WAL of log number " +
          std::to_string(recorded_predecessor_log_number) + ". Recorded " +
          std::to_string(recorded_predecessor_wal_info.GetLastSeqnoRecorded()) +
          ". Observed " +
          std::to_string(
              observed_predecessor_wal_info_.GetLastSeqnoRecorded()) +
          ". (Last sequence number equal to 0 indicates no WAL records)";
      ReportCorruption(fragment.size(), reason.c_str(),
                       recorded_predecessor_log_number);
    } else if (observed_predecessor_wal_info_.GetSizeBytes() !=
               recorded_predecessor_wal_info.GetSizeBytes()) {
      std::string reason =
          "Mismatched size of the WAL of log number " +
          std::to_string(recorded_predecessor_log_number) + ". Recorded " +
          std::to_string(recorded_predecessor_wal_info.GetSizeBytes()) +
          " bytes. Observed " +
          std::to_string(observed_predecessor_wal_info_.GetSizeBytes()) +
          " bytes.";
      ReportCorruption(fragment.size(), reason.c_str(),
                       recorded_predecessor_log_number);
    }
  }
}

uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

uint64_t Reader::LastRecordEnd() {
  return end_of_buffer_offset_ - buffer_.size();
}

void Reader::UnmarkEOF() {
  if (read_error_) {
    return;
  }
  eof_ = false;
  if (eof_offset_ == 0) {
    return;
  }
  UnmarkEOFInternal();
}

void Reader::UnmarkEOFInternal() {
  // If the EOF was in the middle of a block (a partial block was read) we have
  // to read the rest of the block as ReadPhysicalRecord can only read full
  // blocks and expects the file position indicator to be aligned to the start
  // of a block.
  //
  //      consumed_bytes + buffer_size() + remaining == kBlockSize

  size_t consumed_bytes = eof_offset_ - buffer_.size();
  size_t remaining = kBlockSize - eof_offset_;

  // backing_store_ is used to concatenate what is left in buffer_ and
  // the remainder of the block. If buffer_ already uses backing_store_,
  // we just append the new data.
  if (buffer_.data() != backing_store_ + consumed_bytes) {
    // Buffer_ does not use backing_store_ for storage.
    // Copy what is left in buffer_ to backing_store.
    memmove(backing_store_ + consumed_bytes, buffer_.data(), buffer_.size());
  }

  Slice read_buffer;
  // TODO: rate limit log reader with approriate priority.
  // TODO: avoid overcharging rate limiter:
  // Note that the Read here might overcharge SequentialFileReader's internal
  // rate limiter if priority is not IO_TOTAL, e.g., when there is not enough
  // content left until EOF to read.
  Status status =
      file_->Read(remaining, &read_buffer, backing_store_ + eof_offset_,
                  Env::IO_TOTAL /* rate_limiter_priority */);

  size_t added = read_buffer.size();
  end_of_buffer_offset_ += added;

  if (!status.ok()) {
    if (added > 0) {
      ReportDrop(added, status);
    }

    read_error_ = true;
    return;
  }

  if (read_buffer.data() != backing_store_ + eof_offset_) {
    // Read did not write to backing_store_
    memmove(backing_store_ + eof_offset_, read_buffer.data(),
            read_buffer.size());
  }

  buffer_ = Slice(backing_store_ + consumed_bytes,
                  eof_offset_ + added - consumed_bytes);

  if (added < remaining) {
    eof_ = true;
    eof_offset_ += added;
  } else {
    eof_offset_ = 0;
  }
}

void Reader::ReportCorruption(size_t bytes, const char* reason,
                              uint64_t log_number) {
  ReportDrop(bytes, Status::Corruption(reason), log_number);
}

void Reader::ReportDrop(size_t bytes, const Status& reason,
                        uint64_t log_number) {
  if (reporter_ != nullptr) {
    reporter_->Corruption(bytes, reason, log_number);
  }
}

void Reader::ReportOldLogRecord(size_t bytes) {
  if (reporter_ != nullptr) {
    reporter_->OldLogRecord(bytes);
  }
}

/**
 * 尝试从文件上读1个block 32kb大小 实际读到多少看文件系统的文件实际情况
 * @param drop_size 比如文件明明已经被读完了 理论上已经没有东西了
 * 但是当前buffer里面可能还残留了数据 会被丢掉
 * @param error 没读到的原因 比如文件已经被读完了
 * @return 没读成功
 */
bool Reader::ReadMore(size_t* drop_size, uint8_t* error) {
  if (!eof_ && !read_error_) {
    // Last read was a full read, so this is a trailer to skip
    buffer_.clear();
    // TODO: rate limit log reader with approriate priority.
    // TODO: avoid overcharging rate limiter:
    // Note that the Read here might overcharge SequentialFileReader's internal
    // rate limiter if priority is not IO_TOTAL, e.g., when there is not enough
    // content left until EOF to read.
    // 从文件中最读32KB 实际读到了多少数据看buffer里面被填了多少
    Status status = file_->Read(kBlockSize, &buffer_, backing_store_,
                                Env::IO_TOTAL /* rate_limiter_priority */);
    TEST_SYNC_POINT_CALLBACK("LogReader::ReadMore:AfterReadFile", &status);
    end_of_buffer_offset_ += buffer_.size();
    if (!status.ok()) {
      buffer_.clear();
      ReportDrop(kBlockSize, status);
      read_error_ = true;
      *error = kEof;
      return false;
    } else if (buffer_.size() < static_cast<size_t>(kBlockSize)) {
      // 想读32KB 实际读到的不到32KB 说明物理层的文件已经被读完了
      eof_ = true;
      eof_offset_ = buffer_.size();
    }
    return true;
  } else {
    // Note that if buffer_ is non-empty, we have a truncated header at the
    //  end of the file, which can be caused by the writer crashing in the
    //  middle of writing the header. Unless explicitly requested we don't
    //  considering this an error, just report EOF.
    if (buffer_.size()) {
      *drop_size = buffer_.size();
      buffer_.clear();
      *error = kBadHeader;
      return false;
    }
    buffer_.clear();
    *error = kEof;
    return false;
  }
}

/**
 * 这个函数是的RocksDB到操作系统中间的一层 并不是每次都直接读文件 它是流的概念 从文件里面一次读到一个Block 32KB
 * 里面可能会包含很多个fragment 每次拿到一个fragment
 * 拿到的fragment可能刚好就是一个record 也可能是一个record的其中一个fragment
 * @param result 读文件读到的fragment里面的body的原始字节 已经剥掉了物理层协议头7字节
 * @return fragment的type 这个标识在fragment的header里面
 */
uint8_t Reader::ReadPhysicalRecord(Slice* result, size_t* drop_size,
                                   uint64_t* fragment_checksum) {
  while (true) {
    // We need at least the minimum header size
    if (buffer_.size() < static_cast<size_t>(kHeaderSize)) {
      // the default value of r is meaningless because ReadMore will overwrite
      // it if it returns false; in case it returns true, the return value will
      // not be used anyway
      uint8_t r = kEof;
      // 尝试读一个block 32kb
      if (!ReadMore(drop_size, &r)) {
        // 没读到的原因
        return r;
      }
      continue;
    }

    // Parse the header
    const char* header = buffer_.data();
    // header一共7字节 4字节checksum+2字节length+1字节type+对应长度的body
    // 小端序 length是2个字节 先拿到低地址上的1个字节做低位 再拿高地址上的1字节做高位
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const uint8_t type = static_cast<uint8_t>(header[6]);
    // 跳过协议头 协议体的长度
    const uint32_t length = a | (b << 8);
    int header_size = kHeaderSize;
    const bool is_recyclable_type =
        ((type >= kRecyclableFullType && type <= kRecyclableLastType) ||
         type == kRecyclableUserDefinedTimestampSizeType ||
         type == kRecyclePredecessorWALInfoType);
    if (is_recyclable_type) {
      header_size = kRecyclableHeaderSize;
      if (first_record_read_ && !recycled_) {
        // A recycled log should have started with a recycled record
        return kBadRecord;
      }
      recycled_ = true;
      // We need enough for the larger header
      if (buffer_.size() < static_cast<size_t>(kRecyclableHeaderSize)) {
        uint8_t r = kEof;
        if (!ReadMore(drop_size, &r)) {
          return r;
        }
        continue;
      }
    }

    if (header_size + length > buffer_.size()) {
      assert(buffer_.size() >= static_cast<size_t>(header_size));
      *drop_size = buffer_.size();
      buffer_.clear();
      // If the end of the read has been reached without seeing
      // `header_size + length` bytes of payload, report a corruption. The
      // higher layers can decide how to handle it based on the recovery mode,
      // whether this occurred at EOF, whether this is the final WAL, etc.
      return kBadRecordLen;
    }

    if (is_recyclable_type) {
      const uint32_t log_num = DecodeFixed32(header + 7);
      if (log_num != log_number_) {
        buffer_.remove_prefix(header_size + length);
        return kOldRecord;
      }
    }

    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      // NOTE: this should never happen in DB written by new RocksDB versions,
      // since we turn off mmap writes to manifest and log files
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, length + header_size - 6);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        *drop_size = buffer_.size();
        buffer_.clear();
        return kBadRecordChecksum;
      }
    }

    // 推进指针 表示这个fragment已经被LogReader处理完了 我不要再继续看到他们了 那么下一次再看到的就是下一个fragment了
    // 实际上这个fragment的起始地址已经在上面用header指针记录了
    buffer_.remove_prefix(header_size + length);

    if (!uncompress_ || type == kSetCompressionType ||
        type == kPredecessorWALInfoType ||
        type == kRecyclePredecessorWALInfoType ||
        type == kUserDefinedTimestampSizeType ||
        type == kRecyclableUserDefinedTimestampSizeType) {
      // 上面用header记录了这个fragment的起始地址 跳过这个fragment的header 我只要它的body 把body拿出来放到result里面就是调用方拿到数据
      *result = Slice(header + header_size, length);
      return type;
    } else {
      // Uncompress compressed records
      uncompressed_record_.clear();
      if (fragment_checksum != nullptr) {
        if (uncompress_hash_state_ == nullptr) {
          uncompress_hash_state_ = XXH3_createState();
        }
        XXH3_64bits_reset(uncompress_hash_state_);
      }

      size_t uncompressed_size = 0;
      int remaining = 0;
      const char* input = header + header_size;
      do {
        remaining = uncompress_->Uncompress(
            input, length, uncompressed_buffer_.get(), &uncompressed_size);
        input = nullptr;
        if (remaining < 0) {
          buffer_.clear();
          return kBadRecord;
        }
        if (uncompressed_size > 0) {
          if (fragment_checksum != nullptr) {
            XXH3_64bits_update(uncompress_hash_state_,
                               uncompressed_buffer_.get(), uncompressed_size);
          }
          uncompressed_record_.append(uncompressed_buffer_.get(),
                                      uncompressed_size);
        }
      } while (remaining > 0 || uncompressed_size == kBlockSize);

      if (fragment_checksum != nullptr) {
        // We can remove this check by updating hash_state_ directly,
        // but that requires resetting hash_state_ for full and first types
        // for edge cases like consecutive fist type records.
        // Leaving the check as is since it is cleaner and can revert to the
        // above approach if it causes performance impact.
        *fragment_checksum = XXH3_64bits_digest(uncompress_hash_state_);
        uint64_t actual_checksum = XXH3_64bits(uncompressed_record_.data(),
                                               uncompressed_record_.size());
        if (*fragment_checksum != actual_checksum) {
          // uncompressed_record_ contains bad content that does not match
          // actual decompressed content
          return kBadRecord;
        }
      }
      *result = Slice(uncompressed_record_);
      return type;
    }
  }
}

// Initialize uncompress related fields
void Reader::InitCompression(const CompressionTypeRecord& compression_record) {
  compression_type_ = compression_record.GetCompressionType();
  compression_type_record_read_ = true;
  constexpr uint32_t compression_format_version = 2;
  uncompress_ = StreamingUncompress::Create(
      compression_type_, compression_format_version, kBlockSize);
  assert(uncompress_ != nullptr);
  uncompressed_buffer_ = std::unique_ptr<char[]>(new char[kBlockSize]);
  assert(uncompressed_buffer_);
}

Status Reader::UpdateRecordedTimestampSize(
    const std::vector<std::pair<uint32_t, size_t>>& cf_to_ts_sz) {
  for (const auto& [cf, ts_sz] : cf_to_ts_sz) {
    // Zero user-defined timestamp size are not recorded.
    if (ts_sz == 0) {
      return Status::Corruption(
          "User-defined timestamp size record contains zero timestamp size.");
    }
    // The user-defined timestamp size record for a column family should not be
    // updated in the same log file.
    if (recorded_cf_to_ts_sz_.count(cf) != 0) {
      return Status::Corruption(
          "User-defined timestamp size record contains update to "
          "recorded column family.");
    }
    recorded_cf_to_ts_sz_.insert(std::make_pair(cf, ts_sz));
  }
  return Status::OK();
}

bool FragmentBufferedReader::ReadRecord(Slice* record, std::string* scratch,
                                        WALRecoveryMode wal_recovery_mode

                                        ,
                                        uint64_t* /* checksum */) {
  assert(record != nullptr);
  assert(scratch != nullptr);
  record->clear();
  scratch->clear();
  if (uncompress_) {
    uncompress_->Reset();
  }

  uint64_t prospective_record_offset = 0;
  uint64_t physical_record_offset = end_of_buffer_offset_ - buffer_.size();
  size_t drop_size = 0;
  uint8_t fragment_type_or_err = 0;  // Initialize to make compiler happy
  Slice fragment;
  while (TryReadFragment(&fragment, &drop_size, &fragment_type_or_err)) {
    switch (fragment_type_or_err) {
      case kFullType:
      case kRecyclableFullType:
        if (in_fragmented_record_ && !fragments_.empty()) {
          ReportCorruption(fragments_.size(), "partial record without end(1)");
        }
        fragments_.clear();
        *record = fragment;
        prospective_record_offset = physical_record_offset;
        last_record_offset_ = prospective_record_offset;
        first_record_read_ = true;
        in_fragmented_record_ = false;
        return true;

      case kFirstType:
      case kRecyclableFirstType:
        if (in_fragmented_record_ || !fragments_.empty()) {
          ReportCorruption(fragments_.size(), "partial record without end(2)");
        }
        prospective_record_offset = physical_record_offset;
        fragments_.assign(fragment.data(), fragment.size());
        in_fragmented_record_ = true;
        break;

      case kMiddleType:
      case kRecyclableMiddleType:
        if (!in_fragmented_record_) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          fragments_.append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
      case kRecyclableLastType:
        if (!in_fragmented_record_) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          fragments_.append(fragment.data(), fragment.size());
          scratch->assign(fragments_.data(), fragments_.size());
          fragments_.clear();
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          first_record_read_ = true;
          in_fragmented_record_ = false;
          return true;
        }
        break;

      case kSetCompressionType: {
        if (compression_type_record_read_) {
          ReportCorruption(fragment.size(),
                           "read multiple SetCompressionType records");
        }
        if (first_record_read_) {
          ReportCorruption(fragment.size(),
                           "SetCompressionType not the first record");
        }
        fragments_.clear();
        prospective_record_offset = physical_record_offset;
        last_record_offset_ = prospective_record_offset;
        in_fragmented_record_ = false;
        CompressionTypeRecord compression_record(kNoCompression);
        Status s = compression_record.DecodeFrom(&fragment);
        if (!s.ok()) {
          ReportCorruption(fragment.size(),
                           "could not decode SetCompressionType record");
        } else {
          InitCompression(compression_record);
        }
        break;
      }
      case kPredecessorWALInfoType:
      case kRecyclePredecessorWALInfoType: {
        fragments_.clear();
        prospective_record_offset = physical_record_offset;
        last_record_offset_ = prospective_record_offset;
        in_fragmented_record_ = false;

        PredecessorWALInfo recorded_predecessor_wal_info;
        Status s = recorded_predecessor_wal_info.DecodeFrom(&fragment);
        if (!s.ok()) {
          ReportCorruption(fragment.size(),
                           "could not decode PredecessorWALInfoType record");
        } else {
          MaybeVerifyPredecessorWALInfo(wal_recovery_mode, fragment,
                                        recorded_predecessor_wal_info);
        }
        break;
      }
      case kUserDefinedTimestampSizeType:
      case kRecyclableUserDefinedTimestampSizeType: {
        if (in_fragmented_record_ && !scratch->empty()) {
          ReportCorruption(
              scratch->size(),
              "user-defined timestamp size record interspersed partial record");
        }
        fragments_.clear();
        prospective_record_offset = physical_record_offset;
        last_record_offset_ = prospective_record_offset;
        in_fragmented_record_ = false;
        UserDefinedTimestampSizeRecord ts_record;
        Status s = ts_record.DecodeFrom(&fragment);
        if (!s.ok()) {
          ReportCorruption(
              fragment.size(),
              "could not decode user-defined timestamp size record");
        } else {
          s = UpdateRecordedTimestampSize(
              ts_record.GetUserDefinedTimestampSize());
          if (!s.ok()) {
            ReportCorruption(fragment.size(), s.getState());
          }
        }
        break;
      }

      case kBadHeader:
      case kBadRecord:
      case kEof:
      case kOldRecord:
        if (in_fragmented_record_) {
          ReportCorruption(fragments_.size(), "error in middle of record");
          in_fragmented_record_ = false;
          fragments_.clear();
        }
        break;

      case kBadRecordChecksum:
        if (recycled_) {
          fragments_.clear();
          return false;
        }
        ReportCorruption(drop_size, "checksum mismatch");
        if (in_fragmented_record_) {
          ReportCorruption(fragments_.size(), "error in middle of record");
          in_fragmented_record_ = false;
          fragments_.clear();
        }
        break;

      default: {
        if ((fragment_type_or_err & kRecordTypeSafeIgnoreMask) == 0) {
          std::string reason =
              "unknown record type " + std::to_string(fragment_type_or_err);
          ReportCorruption(
              fragment.size() + (in_fragmented_record_ ? fragments_.size() : 0),
              reason.c_str());
        }
        in_fragmented_record_ = false;
        fragments_.clear();
        break;
      }
    }
  }
  return false;
}

void FragmentBufferedReader::UnmarkEOF() {
  if (read_error_) {
    return;
  }
  eof_ = false;
  UnmarkEOFInternal();
}

bool FragmentBufferedReader::TryReadMore(size_t* drop_size, uint8_t* error) {
  if (!eof_ && !read_error_) {
    // Last read was a full read, so this is a trailer to skip
    buffer_.clear();
    // TODO: rate limit log reader with approriate priority.
    // TODO: avoid overcharging rate limiter:
    // Note that the Read here might overcharge SequentialFileReader's internal
    // rate limiter if priority is not IO_TOTAL, e.g., when there is not enough
    // content left until EOF to read.
    Status status = file_->Read(kBlockSize, &buffer_, backing_store_,
                                Env::IO_TOTAL /* rate_limiter_priority */);
    end_of_buffer_offset_ += buffer_.size();
    if (!status.ok()) {
      buffer_.clear();
      ReportDrop(kBlockSize, status);
      read_error_ = true;
      *error = kEof;
      return false;
    } else if (buffer_.size() < static_cast<size_t>(kBlockSize)) {
      eof_ = true;
      eof_offset_ = buffer_.size();
      TEST_SYNC_POINT_CALLBACK(
          "FragmentBufferedLogReader::TryReadMore:FirstEOF", nullptr);
    }
    return true;
  } else if (!read_error_) {
    UnmarkEOF();
  }
  if (!read_error_) {
    return true;
  }
  *error = kEof;
  *drop_size = buffer_.size();
  if (buffer_.size() > 0) {
    *error = kBadHeader;
  }
  buffer_.clear();
  return false;
}

// return true if the caller should process the fragment_type_or_err.
bool FragmentBufferedReader::TryReadFragment(Slice* fragment, size_t* drop_size,
                                             uint8_t* fragment_type_or_err) {
  assert(fragment != nullptr);
  assert(drop_size != nullptr);
  assert(fragment_type_or_err != nullptr);

  while (buffer_.size() < static_cast<size_t>(kHeaderSize)) {
    size_t old_size = buffer_.size();
    uint8_t error = kEof;
    if (!TryReadMore(drop_size, &error)) {
      *fragment_type_or_err = error;
      return false;
    } else if (old_size == buffer_.size()) {
      return false;
    }
  }
  const char* header = buffer_.data();
  const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
  const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
  const uint8_t type = static_cast<uint8_t>(header[6]);
  const uint32_t length = a | (b << 8);
  int header_size = kHeaderSize;
  if ((type >= kRecyclableFullType && type <= kRecyclableLastType) ||
      type == kRecyclableUserDefinedTimestampSizeType ||
      type == kRecyclePredecessorWALInfoType) {
    if (first_record_read_ && !recycled_) {
      // A recycled log should have started with a recycled record
      *fragment_type_or_err = kBadRecord;
      return true;
    }
    recycled_ = true;
    header_size = kRecyclableHeaderSize;
    while (buffer_.size() < static_cast<size_t>(kRecyclableHeaderSize)) {
      size_t old_size = buffer_.size();
      uint8_t error = kEof;
      if (!TryReadMore(drop_size, &error)) {
        *fragment_type_or_err = error;
        return false;
      } else if (old_size == buffer_.size()) {
        return false;
      }
    }
    const uint32_t log_num = DecodeFixed32(header + 7);
    if (log_num != log_number_) {
      *fragment_type_or_err = kOldRecord;
      return true;
    }
  }

  while (header_size + length > buffer_.size()) {
    size_t old_size = buffer_.size();
    uint8_t error = kEof;
    if (!TryReadMore(drop_size, &error)) {
      *fragment_type_or_err = error;
      return false;
    } else if (old_size == buffer_.size()) {
      return false;
    }
  }

  if (type == kZeroType && length == 0) {
    buffer_.clear();
    *fragment_type_or_err = kBadRecord;
    return true;
  }

  if (checksum_) {
    uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
    uint32_t actual_crc = crc32c::Value(header + 6, length + header_size - 6);
    if (actual_crc != expected_crc) {
      *drop_size = buffer_.size();
      buffer_.clear();
      *fragment_type_or_err = kBadRecordChecksum;
      return true;
    }
  }

  buffer_.remove_prefix(header_size + length);

  if (!uncompress_ || type == kSetCompressionType ||
      type == kPredecessorWALInfoType ||
      type == kRecyclePredecessorWALInfoType ||
      type == kUserDefinedTimestampSizeType ||
      type == kRecyclableUserDefinedTimestampSizeType) {
    *fragment = Slice(header + header_size, length);
    *fragment_type_or_err = type;
    return true;
  } else {
    // Uncompress compressed records
    uncompressed_record_.clear();
    size_t uncompressed_size = 0;
    int remaining = 0;
    const char* input = header + header_size;
    do {
      remaining = uncompress_->Uncompress(
          input, length, uncompressed_buffer_.get(), &uncompressed_size);
      input = nullptr;
      if (remaining < 0) {
        buffer_.clear();
        *fragment_type_or_err = kBadRecord;
        return true;
      }
      if (uncompressed_size > 0) {
        uncompressed_record_.append(uncompressed_buffer_.get(),
                                    uncompressed_size);
      }
    } while (remaining > 0 || uncompressed_size == kBlockSize);
    *fragment = Slice(std::move(uncompressed_record_));
    *fragment_type_or_err = type;
    return true;
  }
}

}  // namespace ROCKSDB_NAMESPACE::log
