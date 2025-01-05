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

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
// std::string kDBPath = "/home/byteide/db/rocks/tmp/rocksdb_simple_example";
std::string kDBPath = "/tmp/rocksdbtest-1001/rocksdb_simple_example";
#endif

int main() {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction(3 << 20);
  // create the DB if it's not already present
  options.create_if_missing = true;
  options.create_missing_column_families = true;

  std::shared_ptr<Logger> logger;
  // declare in rocksdb_test/rocksdb/include/rocksdb/options.h
  Status logger_s = CreateLoggerFromOptions(kDBPath, options, &logger);
  std::shared_ptr<SstFileManager> sfm(NewSstFileManager(options.env, logger));
  options.sst_file_manager = sfm;

  // 模拟一个不能自动恢复的情况
  ColumnFamilyOptions default_cfoptions, cfoptions;
  // 默认就是只有一个default
  default_cfoptions.cf_paths = std::vector<DbPath>{DbPath(kDBPath, 
                                                          0)};
  cfoptions.cf_paths = std::vector<DbPath>{DbPath(kDBPath + "/1", 0)};
  std::vector<ColumnFamilyDescriptor> cf_desc;
  cf_desc.emplace_back(kDefaultColumnFamilyName, default_cfoptions);
  cf_desc.emplace_back("1", cfoptions);
  std::vector<ColumnFamilyHandle*> cfhs;

  // open DB
  // Status s = DB::Open(options, kDBPath, &db);
  Status s = DB::Open(options, kDBPath, cf_desc, &cfhs, &db); // db nullptr
  if (!s.ok()) {
    std::cerr << "Open error " << std::string(s.getState()) << std::endl;
    return 0;
  } else {
    std::cout << "Open success\n";
  }

  for (auto h : cfhs) {
    db->SetOptions(h, {{"level0_file_num_compaction_trigger", "10"}});
  }

  for (auto h : cfhs) {
    db->DestroyColumnFamilyHandle(h);
  }
  delete db;
}