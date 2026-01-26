//
// Created by rui ding on 2026/1/26.
//

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <cassert>
#include <iostream>

int main() {
  rocksdb::DB* db = nullptr;

  rocksdb::Options options;
  options.create_if_missing = true;

  std::string db_path = "/tmp/rocksdb_ctest_put";

  auto s = rocksdb::DB::Open(options, db_path, &db);
  assert(s.ok());

  s = db->Put(rocksdb::WriteOptions(), "hello", "rocksdb");
  assert(s.ok());
  std::string value;
  // get value
  s = db->Get(rocksdb::ReadOptions(), "hello", &value);
  assert(s.ok());
  assert(value == "rocksdb");

  delete db;
  return 0;
}
