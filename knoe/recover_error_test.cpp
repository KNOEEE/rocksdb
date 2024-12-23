#include <unistd.h>

#include <cstdio>
#include <iostream>
#include <memory>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/options.h"

using namespace ROCKSDB_NAMESPACE;

std::string kDBPath = "/tmp/rocksdbtest-1001/rocksdb_simple_example";

void Put(DB* db, uint64_t key = 0) {
  WriteBatch batch;
  auto kv = std::to_string(key);
  batch.Put(kv, kv);

  static WriteOptions write_options = WriteOptions();
  write_options.disableWAL = true;
  Status s = db->Write(write_options, &batch);
  if (!s.ok()) {
    std::cerr << "Put error " << std::string(s.getState()) << std::endl;
  }
}

void Get(DB* db, int k) {
  std::string value;
  Status s = db->Get(ReadOptions(), std::to_string(k), &value);
  if (s.ok()) {
    std::cout << "GetValue " << value << std::endl;
  } else {
    std::cerr << "Get error" << std::endl;
  }
}

int main() {
  DB* db;
  Options options;
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction(3 << 20);
  // create the DB if it's not already present
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  std::shared_ptr<SstFileManager> sst_file_manager(NewSstFileManager(options.env));
  options.sst_file_manager = sst_file_manager;
  // 通过设置多个db path禁止自动恢复
  ColumnFamilyOptions default_cfoptions, cf_options;
  default_cfoptions.cf_paths = std::vector<DbPath>{DbPath(kDBPath, 
                                                          0)};
  cf_options.cf_paths = std::vector<DbPath>{DbPath(kDBPath + "/1", 0)};
  std::vector<ColumnFamilyDescriptor> cf_desc;
  cf_desc.emplace_back(kDefaultColumnFamilyName, default_cfoptions);
  cf_desc.emplace_back("1", cf_options);
  std::vector<ColumnFamilyHandle*> cf_handles;

  // open db
  // Status s = DB::Open(options, kDBPath, &db);
  Status s = DB::Open(options, kDBPath, cf_desc, &cf_handles, &db); // db nullptr

  if (!s.ok()) {
    std::cerr << "Open error " << std::string(s.getState()) << std::endl;
    return 0;
  } else {
    std::cout << "Open success\n";
  }

  sst_file_manager->SetMaxAllowedSpaceUsage(2 << 10);
  for (int i = 0; i < 100; i++) 
    Put(db, i);
  s = db->Flush(FlushOptions());
  if (!s.ok()) {
    std::cerr << "Flush error " << std::string(s.getState()) << std::endl;
  } else {
    std::cout << "Flush success\n";
  }

  sst_file_manager->SetMaxAllowedSpaceUsage(20 << 20);
  s = db->Resume();
  if (!s.ok()) {
    std::cerr << "Resume error " << std::string(s.getState()) << std::endl;
    return 0;
  } else {
    std::cout << "Resume success\n";
  }
  std::cout << sst_file_manager->GetTotalSize() << std::endl;
  std::cout << sst_file_manager->IsMaxAllowedSpaceReached() << std::endl;
  std::cout << sst_file_manager->IsMaxAllowedSpaceReachedIncludingCompactions() << std::endl;
  for (int i = 100; i < 200; i++) 
    Put(db, i);
  s = db->Flush(FlushOptions());
  if (!s.ok()) {
    std::cerr << "Flush error " << std::string(s.getState()) << std::endl;
  } else {
    std::cout << "Flush success\n";
  }
  std::cout << sst_file_manager->GetTotalSize() << std::endl;
  std::cout << sst_file_manager->IsMaxAllowedSpaceReached() << std::endl;
  std::cout << sst_file_manager->IsMaxAllowedSpaceReachedIncludingCompactions() << std::endl;

  // 这里的表现似乎和本地不一致
  Get(db, 50);
  Get(db, 150);
  for (auto h : cf_handles) {
    db->DestroyColumnFamilyHandle(h);
  }
  delete db;

  return 0;
}