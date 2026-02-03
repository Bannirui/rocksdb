//
// Created by rui ding on 2026/1/26.
//

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <cassert>
#include <iostream>

int main() {
  rocksdb::Options options;
  options.create_if_missing = true;

  // 不在options中显式制定wal的目录就会用db_path
  std::string dbName = "/tmp/rocksdb_ctest_manifest";
  std::string walDir = dbName + "/wal";
  std::string sstDir = dbName + "/sst";
  options.wal_dir = walDir;
  std::vector<rocksdb::DbPath> sstPaths = {{sstDir + "/flash_path", 512},
                                           {sstDir + "/hard_drive", 1024}};
  options.db_paths = sstPaths;

  // 限制wal日志大小 不让复用 让出现多个wal
  options.max_total_wal_size = 1024;
  options.recycle_log_file_num = false;

  // 限制一下manifest单个文件大小 让roll第2个manifest出来
  options.max_manifest_file_size = 512;

  // 让数据堆积在内存 wal持续增加 不进sst
  options.disable_auto_compactions = true;
  options.max_write_buffer_number = 10;
  options.min_write_buffer_number_to_merge = 10;


  // sst目录属于资源目录 RocksDB不会帮我创建 要自己创建好
  auto* env = rocksdb::Env::Default();
  env->CreateDirIfMissing(dbName);
  env->CreateDirIfMissing(walDir);
  env->CreateDirIfMissing(sstDir);
  env->CreateDirIfMissing(sstDir + "/flash_path");
  env->CreateDirIfMissing(sstDir + "/hard_drive");

  std::unique_ptr<rocksdb::DB> db;
  auto s = rocksdb::DB::Open(options, dbName, &db);
  assert(s.ok());

  // 进sst
  for (int i = 0; i < 10; ++i) {
    db->Put(rocksdb::WriteOptions(), "hello-" + std::to_string(i),
            "world-" + std::to_string(i));
    db->Flush(rocksdb::FlushOptions());
  }
  // 进wal 不进sst
  for (int i = 10; i < 10000; ++i) {
    db->Put(rocksdb::WriteOptions(), "hello-" + std::to_string(i),
            "world-" + std::to_string(i));
  }
  // 触发close
  db->Close();
  return 0;
}
