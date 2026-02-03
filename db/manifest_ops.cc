//  Copyright (c) Meta Platforms, Inc. and affiliates.
//
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/manifest_ops.h"

#include "file/filename.h"

namespace ROCKSDB_NAMESPACE {

/**
 * 拿到当前manifest文件的路径和编号
 * @param manifest_path 当前manifest文件路径
 * @param manifest_file_number manifest文件编号
 */
Status GetCurrentManifestPath(const std::string& dbname, FileSystem* fs,
                              bool is_retry, std::string* manifest_path,
                              uint64_t* manifest_file_number) {
  assert(fs != nullptr);
  assert(manifest_path != nullptr);
  assert(manifest_file_number != nullptr);

  IOOptions opts;
  // CURRENT文件里面的内容 就是一行内容 是manifest文件名
  std::string fname;
  if (is_retry) {
    opts.verify_and_reconstruct_read = true;
  }
  // 读CURRENT文件里面的内容 看看现在使用的是哪个manifest文件
  Status s = ReadFileToString(fs, CurrentFileName(dbname), opts, &fname);
  if (!s.ok()) {
    return s;
  }
  // 在CURRENT里面没读到manifest文件名
  if (fname.empty() || fname.back() != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  // remove the trailing '\n'
  fname.resize(fname.size() - 1);
  FileType type;
  // 从manifest文件名解析文件类型和文件编号
  bool parse_ok = ParseFileName(fname, manifest_file_number, &type);
  if (!parse_ok || type != kDescriptorFile) {
    return Status::Corruption("CURRENT file corrupted");
  }
  *manifest_path = dbname;
  if (dbname.back() != '/') {
    manifest_path->push_back('/');
  }
  manifest_path->append(fname);
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
