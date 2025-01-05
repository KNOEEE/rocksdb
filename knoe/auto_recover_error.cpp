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

class StorageExtender : public rocksdb::EventListener
{
public:
    std::shared_ptr<SstFileManager> sst_file_manager;
    bool isRecovered = false;
    StorageExtender() = default;
    void OnErrorRecoveryBegin(
        rocksdb::BackgroundErrorReason reason,
        rocksdb::Status bg_error,
        bool* /*auto_recovery*/
    ) {
        std::cout << "Got error:" << bg_error.ToString() << std::endl;
        if (bg_error.subcode() == Status::SubCode::kSpaceLimit) {
          if (!sst_file_manager) return;
          sst_file_manager->SetMaxAllowedSpaceUsage(20 << 20);
        }
    }
    void OnErrorRecoveryCompleted(rocksdb::Status old_bg_error) {
        std::cout << "Recovered from error:" << old_bg_error.ToString() << std::endl;
        isRecovered = true;
    }
};

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

  std::shared_ptr<StorageExtender> listener = std::make_shared<StorageExtender>();
  listener->sst_file_manager = sst_file_manager;
  options.listeners.emplace_back(listener);

  // open db
  Status s = DB::Open(options, kDBPath, &db);

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
  int t = 0;
  while (!listener->isRecovered) {
      std::cout << t << "s" << std::endl;
      sleep(1);
      t++;
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

  // db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  // 这里的表现似乎和本地不一致
  Get(db, 50);
  Get(db, 150);
 
  delete db;

  return 0;
}