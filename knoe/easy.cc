// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

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
/**
 * @brief 本demo测试sfm的相关功能 日志、停写与自动恢复
 * ./build.sh
 * blade build :easy --bundle=debug --toolchain=x86_64-clang1101 --enable-bpt --update-deps
 *
 * build ut: make import_column_family_test -j64
 */
void Put(DB* db, uint64_t key = 0) {
  int64_t time;
  db->GetEnv()->GetCurrentTime(&time);
  // std::cout << "start put time " << time << " " << key << std::endl;
  WriteBatch batch;
  for (uint64_t i = key << 10; i < (key << 10) + 1024; i++) {
    // 约8B
    auto kv = std::to_string(i);
    batch.Put(kv, kv);
  }
  // 8kB 最终是38KB
  static WriteOptions write_options = WriteOptions();
  write_options.disableWAL = true;
  Status s = db->Write(write_options, &batch);
  if (!s.ok()) {
    std::cerr << "put error " << std::string(s.getState()) << std::endl;
  }
  db->GetEnv()->GetCurrentTime(&time);
  // std::cout << "end put time " << time << std::endl;
}

void PutPerByte(DB* db) {
  int64_t time;
  db->GetEnv()->GetCurrentTime(&time);
  std::cout << "start put time " << time << std::endl;
  Status s;
  for (int i = 0; i < 1024; i++) {
    s = db->Put(WriteOptions(), std::to_string(i), "value");
    if (!s.ok()) {
      std::cerr << "put error " << std::string(s.getState()) << std::endl;
      return;
    }
  }
  db->GetEnv()->GetCurrentTime(&time);
  std::cout << "end put time " << time << std::endl;
}

void GetValue(DB* db, int k) {
  std::string value;
  Status s = db->Get(ReadOptions(), std::to_string(k), &value);
  if (s.ok()) {
    std::cout << "GetValue " << value << std::endl;
  } else {
    std::cerr << "Get error " << std::string(s.getState()) << std::endl;
  }
}

// 45G 相当于目录挂载的文件系统的空闲空间
void PrintDirSpace(std::string dir) {
  std::shared_ptr<FileSystem> fs = FileSystem::Default();
  uint64_t free_space = 0;
  Status s = fs->GetFreeSpace(dir, IOOptions(), &free_space, nullptr);
  std::cout << "free space is " << free_space << std::endl; 
}

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
  // GetValue(db, 512);
  // GetValue(db, 200 << 10);
  // delete db;
  // return 0;
  // 会在SstFileManagerImpl::ClearError中无限循环♻️
  sfm->SetMaxAllowedSpaceUsage(2 << 20);

  for (int i = 0; i < 150; i++) 
    Put(db, i);

  s = db->Flush(FlushOptions());
  if (!s.ok()) {
    std::cerr << "Flush error " << std::string(s.getState()) << std::endl;
  } else {
    std::cout << "Flush success\n";
  }

  sfm->SetMaxAllowedSpaceUsage(20 << 20);
  s = db->Resume();
  if (!s.ok()) {
    std::cerr << "Resume error " << std::string(s.getState()) << std::endl;
    return 0;
  } else {
    std::cout << "Resume success\n";
  }
  sleep(5);

  // 把第一次的SetMaxAllowedSpaceUsage去掉后 这里的计算size就正常了
  // 合理怀疑这里也是bug 说明如触发了space error 后面的计算就是错误的了
  // 但问题是 盘上的文件大小似乎是吻合的
  /**
   * @brief 这说明flush后 磁盘上的文件消失了
   * 理论上再次读这部分数据应该会丢失 观察日志发现 数据库进行了这个flush 但实际盘上并没有
   * 应该是flush失败了 导致文件不存在 
   * 果然 测试印证了我的猜想 这个逻辑是有问题的 直接打开之前的 数据丢了
   * 一个假设 是最后的这一次flush根本 没有成功 其实是失败的 只是看上去成功了
   */
  std::cout << sfm->GetTotalSize() << std::endl;
  std::cout << sfm->IsMaxAllowedSpaceReached() << std::endl;
  std::cout << sfm->IsMaxAllowedSpaceReachedIncludingCompactions() << std::endl;
  for (int i = 150; i < 300; i++) 
    Put(db, i);

  s = db->Flush(FlushOptions());
  if (!s.ok()) {
    std::cerr << "Flush error " << std::string(s.getState()) << std::endl;
  } else {
    std::cout << "Flush success\n";
  }
  std::cout << sfm->GetTotalSize() << std::endl;
  std::cout << sfm->IsMaxAllowedSpaceReached() << std::endl;
  std::cout << sfm->IsMaxAllowedSpaceReachedIncludingCompactions() << std::endl;

  // 这里的表现似乎和本地不一致
  GetValue(db, 512);
  GetValue(db, 200 << 10);
  for (auto h : cfhs) {
    db->DestroyColumnFamilyHandle(h);
  }
  delete db;

  return 0;
}
