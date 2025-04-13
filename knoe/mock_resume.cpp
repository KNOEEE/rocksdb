#include <unistd.h>

#include <cstdio>
#include <iostream>
#include <memory>
#include <string>

#include "file/file_util.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_manager.h"
#include "rocksdb/options.h"
#include "test_util/sync_point.h"

using namespace ROCKSDB_NAMESPACE;

#if defined(OS_WIN)
std::string kDBPath = "C:\\Windows\\TEMP\\rocksdb_simple_example";
#else
std::string kDBPath = "/dev/shm/rocksdb_simple_example";
#endif

class KeyCompactionFilter : public CompactionFilter {
  public:
    KeyCompactionFilter() = default;
    bool Filter(int level, const Slice& key, const Slice& existing_value,
                std::string* new_value, bool* value_changed) const override {
      uint64_t num = std::stoul(key.ToString());
      if (num < (1501 << 10)) {
        return true;
      } 
      return false;
    }
    const char* Name() const override { return "Key-number-filter"; }
};

class StorageExtender : public rocksdb::EventListener {
 private:
  DB* db_;
 public:
  explicit StorageExtender(DB* db) : db_(db) {}
  void OnBackgroundError(BackgroundErrorReason reason, 
                         Status* bg_error) override {
    std::cout << "OnBackgroundError:" << bg_error->ToString() << std::endl;
  }
  void OnErrorRecoveryBegin(
      rocksdb::BackgroundErrorReason reason,
      rocksdb::Status bg_error,
      bool* /*auto_recovery*/) override {
    std::cout << "OnErrorRecoveryBegin:" << bg_error.ToString() << std::endl;
    db_->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  }

  void OnErrorRecoveryCompleted(rocksdb::Status old_bg_error) override {
    std::cout << "Recovered from error:" 
              << old_bg_error.ToString() << std::endl;
    TEST_SYNC_POINT("StorageExtender::OnErrorRecoveryCompleted recovered");
  }
};

void PutBatch(DB* db, ColumnFamilyHandle*& cf, uint64_t key = 0) {
  WriteBatch write_batch;
  // write 1024 keys every call
  for (uint64_t i = key << 10; i < (key << 10) + 1024; i++) {
    auto kv = std::to_string(i);
    write_batch.Put(cf, kv, kv);
  }
  static WriteOptions write_options = WriteOptions();
  write_options.disableWAL = true;
  Status s = db->Write(write_options, &write_batch);
}

void GetValue(DB* db, int k) {
  std::string value;
  Status s = db->Get(ReadOptions(), std::to_string(k), &value);
  if (s.ok()) {
    std::cout << "GetValue " << value << std::endl;
  } else {
    std::cerr << "GetValue error " << std::string(s.getState()) << std::endl;
  }
}

void PrintDirSpace(std::string dir) {
  std::shared_ptr<FileSystem> fs = FileSystem::Default();
  uint64_t free_space = 0;
  Status s = fs->GetFreeSpace(dir, IOOptions(), &free_space, nullptr);
  std::cout << "free space is " << free_space << std::endl; 
}

void PrintSSTFileStatus(std::shared_ptr<SstFileManager>& sfm) {
  std::cout << "GetTotalSize: ";
  std::cout << sfm->GetTotalSize() << std::endl;
  std::cout << "IsMaxAllowedSpaceReached: ";
  std::cout << sfm->IsMaxAllowedSpaceReached() << std::endl;
  std::cout << "IsMaxAllowedSpaceReachedIncludingCompactions: ";
  std::cout << sfm->IsMaxAllowedSpaceReachedIncludingCompactions() << std::endl;
}

Status BuildDB(DB*& db, std::vector<ColumnFamilyHandle*>& cfhs, 
               std::shared_ptr<SstFileManager>& sfm) {
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction(3 << 20);
  // create the DB if it's not already present
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.compaction_filter = new KeyCompactionFilter();
  std::shared_ptr<StorageExtender> listener = 
      std::make_shared<StorageExtender>(db);
  options.listeners.emplace_back(listener);

  std::shared_ptr<Logger> logger;
  // declare in rocksdb/include/rocksdb/options.h
  Status logger_s = CreateLoggerFromOptions(kDBPath, options, &logger);
  sfm.reset(NewSstFileManager(options.env, logger));
  options.sst_file_manager = sfm;

  ColumnFamilyOptions cf_options; 
  cf_options.cf_paths.emplace_back(
      DbPath(kDBPath + "/" + kDefaultColumnFamilyName, 0));

  cf_options.table_factory.reset(NewPlainTableFactory());
  cf_options.prefix_extractor.reset(NewNoopTransform());

  std::vector<ColumnFamilyDescriptor> cf_desc;
  cf_desc.emplace_back(kDefaultColumnFamilyName, cf_options);
  for (int i = 0; i <= 1; i++) {
    cf_options.cf_paths.clear();
    cf_options.cf_paths.emplace_back(
        DbPath(kDBPath + "/" + std::to_string(i), 0));
    cf_desc.emplace_back(std::to_string(i), cf_options);
  }
  return DB::Open(options, kDBPath, cf_desc, &cfhs, &db);
}

int main() {
  Status destroy_dir_status = DestroyDir(Env::Default(), kDBPath);
  if (!destroy_dir_status.ok() && !destroy_dir_status.IsNotFound()) {
    std::cout << "ERROR in file system\n";
    return 0;
  }

  DB* db{};
  std::vector<ColumnFamilyHandle *> handles;
  std::shared_ptr<SstFileManager> sfm;
  Status s = BuildDB(db, handles, sfm);
  if (!s.ok()) {
    std::cerr << "BuildDB error " << std::string(s.getState()) << std::endl;
    return 0;
  }

  SyncPoint::GetInstance()->LoadDependency(
      {{"StorageExtender::OnErrorRecoveryCompleted recovered",
        "MockResume::main retry"}});
  SyncPoint::GetInstance()->EnableProcessing();

  for (ColumnFamilyHandle* cf_handle : handles) {
    std::cout << "cf name " << cf_handle->GetName() << std::endl;
    std::cout << "cf id " << cf_handle->GetID() << std::endl;
    ColumnFamilyDescriptor cf_desc;
    s = cf_handle->GetDescriptor(&cf_desc);
    std::cout << "cf path " << cf_desc.options.cf_paths[0].path << std::endl;
  }

  for (int i = 0; i < 1500; i++) 
    PutBatch(db, handles[1], i);
  s = db->Flush(FlushOptions(), handles);
  if (!s.ok()) {
    std::cerr << "Flush 1st error " << std::string(s.getState()) << std::endl;
  } else {
    std::cout << "Flush 1st success\n";
  }
  for (int i = 1500; i < 3000; i++) 
    PutBatch(db, handles[1], i);
  // This will fail
  s = db->Flush(FlushOptions(), handles);
  if (!s.ok()) {
    std::cerr << "Flush 2nd error " << std::string(s.getState()) << std::endl;
  } else {
    std::cout << "Flush 2nd success\n";
  }
  PrintSSTFileStatus(sfm);
  s = db->Resume();
  if (!s.ok()) {
    std::cerr << "Resume error " << std::string(s.getState()) << std::endl;
  } else {
    std::cout << "Resume success\n";
  }

  s = db->CompactRange(CompactRangeOptions(), handles[1], nullptr, nullptr);
  if (!s.ok()) {
    std::cerr << "CompactRange error " << std::string(s.getState()) << std::endl;
  } else {
    std::cout << "CompactRange success\n";
  }

  // Waiting for recovery from out-of-space error.
  TEST_SYNC_POINT("MockResume::main retry");

  for (int i = 0; i < 150; i++) 
    PutBatch(db, handles[1], i);
  s = db->Flush(FlushOptions(), handles);
  if (!s.ok()) {
    std::cerr << "Flush error " << std::string(s.getState()) << std::endl;
  } else {
    std::cout << "Flush success\n";
  }
  PrintSSTFileStatus(sfm);

  for (auto h : handles) {
    db->DestroyColumnFamilyHandle(h);
  }
  delete db;
}