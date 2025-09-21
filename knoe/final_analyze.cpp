#include <unistd.h>

#include <cstdio>
#include <iostream>
#include <memory>
#include <queue>
#include <string>

#include <gflags/gflags.h>
#include <gtest/gtest.h> 

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

class StorageListener : public rocksdb::EventListener {
 private:
  DB* db_{};
  std::vector<ColumnFamilyHandle*> cfhs_;
  bool active_resume_ = false;
  Status PruneObsoleteSST() {
    if (db_ == nullptr || cfhs_.empty()) {
      return Status::Aborted("Bad StorageListener member");
    }
    struct FileCreateOrder {
      bool operator()(const SstFileMetaData* f1, const SstFileMetaData* f2) {
        return f1->file_creation_time > f2->file_creation_time;
      }
    };
    using SstHeap = 
        std::priority_queue<const SstFileMetaData*, 
                            std::vector<const SstFileMetaData*>, 
                            FileCreateOrder>;
    SstHeap heap;
    int max_level = 0;
    for (ColumnFamilyHandle* cf_handle : cfhs_) {
      ColumnFamilyMetaData single_cf_meta;
      db_->GetColumnFamilyMetaData(cf_handle, &single_cf_meta);
      heap = SstHeap();
      std::vector<std::string> files;
      for (LevelMetaData& level_meta : single_cf_meta.levels) {
        if (level_meta.level > max_level) { max_level = level_meta.level; }
        for (const SstFileMetaData& file : level_meta.files) {
          // std::cout << file.relative_filename << " " 
          //           << (file.being_compacted ? " compacting" : "static") 
          //           << std::endl;
          if (file.being_compacted) continue;
          heap.push(&file);
        }
      }
      if (heap.empty()) continue;
      // choose the earliest file
      std::string relative_filename = heap.top()->relative_filename;
      std::cout << "Ready to compact " << relative_filename << std::endl;
      files.emplace_back(relative_filename);
      Status s = db_->CompactFiles(
          CompactionOptions(), cf_handle, files, max_level);
      if (!s.ok()) return s;
    }
    return Status::OK();
  }
  std::string BGErrorToString(BackgroundErrorReason reason) {
    switch (reason) {
    case rocksdb::BackgroundErrorReason::kFlush:
      return "kFlush";
    case rocksdb::BackgroundErrorReason::kCompaction:
      return "kCompaction";
    case rocksdb::BackgroundErrorReason::kWriteCallback:
      return "kWriteCallback";
    case rocksdb::BackgroundErrorReason::kMemTable:
      return "kMemTable";
    case rocksdb::BackgroundErrorReason::kManifestWrite:
      return "kManifestWrite";
    case rocksdb::BackgroundErrorReason::kFlushNoWAL:
      return "kFlushNoWAL";
    case rocksdb::BackgroundErrorReason::kManifestWriteNoWAL:
      return "kManifestWriteNoWAL";
    default:
      return "OtherReason";
    }
  }
 public:
  StorageListener() = default;
  explicit StorageListener(DB*& db, std::vector<ColumnFamilyHandle*>& cf_handles) 
      : db_(db), cfhs_(cf_handles) {}
  void SetDB(DB* db) { db_ = db; }
  void SetColumnFamilyHandles(std::vector<ColumnFamilyHandle*>& cf_handles) {
    cfhs_ = cf_handles;
  }
  void EnableActiveResume() { active_resume_ = true; }
  void DisableActiveResume() { active_resume_ = false; }

  void OnBackgroundError(BackgroundErrorReason reason, 
                         Status* bg_error) override {
    std::cout << "OnBackgroundError: " << bg_error->ToString() << " | Reason: " 
              << BGErrorToString(reason) << std::endl;
    if (!active_resume_) return;
    Status s = PruneObsoleteSST();
    if (!s.ok()) {
      std::cerr << "CompactRange error " << s.ToString() << std::endl;
    } else {
      std::cout << "CompactRange success\n";
    }
  }
  void OnErrorRecoveryBegin(
      rocksdb::BackgroundErrorReason reason,
      rocksdb::Status bg_error,
      bool* /*auto_recovery*/) override {
    std::cout << "OnErrorRecoveryBegin:" << bg_error.ToString() << std::endl;
  }

  void OnErrorRecoveryCompleted(rocksdb::Status old_bg_error) override {
    std::cout << "Recovered from error:" 
              << old_bg_error.ToString() << std::endl;
    TEST_SYNC_POINT("StorageListener::OnErrorRecoveryCompleted recovered");
  }
  void OnFlushBegin(DB* db, const FlushJobInfo& flush_job_info) override {
    return;
    std::cout << "OnFlushBegin TableProperties: \n";
    // plaintable cannot get table properties
    // std::cout << flush_job_info.table_properties.ToString() << std::endl;
    ColumnFamilyHandle* cf_handle{};
    for (ColumnFamilyHandle* cf_ : cfhs_) {
      if (cf_->GetID() == flush_job_info.cf_id) {
        cf_handle = cf_;
        break;
      }
    }
    if (cf_handle == nullptr) return;
    // It means nothing to get mem size, due to imm has fixed size
    // so we only check size of imm
    uint64_t memtable_size;
    db_->GetIntProperty(
        cf_handle, "rocksdb.cur-size-active-mem-table", &memtable_size);
    std::cout << "rocksdb.cur-size-active-mem-table: " 
              << memtable_size << std::endl;
    db_->GetIntProperty(
        cf_handle, "rocksdb.cur-size-all-mem-tables", &memtable_size);
    std::cout << "rocksdb.cur-size-all-mem-tables: "
              << memtable_size << std::endl;
    db_->GetIntProperty(
        cf_handle, "rocksdb.size-all-mem-tables", &memtable_size);
    std::cout << "rocksdb.size-all-mem-tables: "
              << memtable_size << std::endl;
  }
};
struct InitOptions {
  InitOptions() = default;
  size_t write_buffer_size = 64 << 20;
  bool optimized = false;
};

class SpaceLimitTest : public ::testing::Test {
public:
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
  void SetUp() override {
    Status destroy_dir_status = DestroyDir(Env::Default(), kDBPath);
    if (!destroy_dir_status.ok() && !destroy_dir_status.IsNotFound()) {
      std::cout << "ERROR in file system\n";
      GTEST_FAIL();
    }
  }
  void TearDown() override {
    for (auto h : cf_handles) {
      db->DestroyColumnFamilyHandle(h);
    }
    if (db != nullptr)
      delete db;
  }
  DB* db{};
  std::vector<ColumnFamilyHandle *> cf_handles;
  std::shared_ptr<SstFileManager> sst_manager;
  std::shared_ptr<StorageListener> listener;

  Status BuildDB(InitOptions opts = InitOptions()) {
    Options options;
    if (opts.optimized) {
      options.IncreaseParallelism();
      options.OptimizeLevelStyleCompaction(3 << 20);
    }
    // create the DB if it's not already present
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.compaction_filter = new KeyCompactionFilter();
    listener = std::make_shared<StorageListener>(db, cf_handles);
    options.listeners.emplace_back(listener);

    std::shared_ptr<Logger> logger;
    // declare in rocksdb/include/rocksdb/options.h
    Status logger_s = CreateLoggerFromOptions(kDBPath, options, &logger);
    sst_manager.reset(NewSstFileManager(options.env, logger));
    options.sst_file_manager = sst_manager;

    ColumnFamilyOptions cf_options; 
    cf_options.cf_paths.emplace_back(
        DbPath(kDBPath + "/" + kDefaultColumnFamilyName, 0));

    cf_options.table_factory.reset(NewPlainTableFactory());
    cf_options.prefix_extractor.reset(NewNoopTransform());
    cf_options.write_buffer_size = opts.write_buffer_size;

    std::vector<ColumnFamilyDescriptor> cf_desc;
    cf_desc.emplace_back(kDefaultColumnFamilyName, cf_options);
    for (int i = 0; i <= 1; i++) {
      cf_options.cf_paths.clear();
      cf_options.cf_paths.emplace_back(
          DbPath(kDBPath + "/" + std::to_string(i), 0));
      cf_desc.emplace_back(std::to_string(i), cf_options);
    }
    Status s = DB::Open(options, kDBPath, cf_desc, &cf_handles, &db);
    if (s.ok()) {
      listener->SetDB(db);
      listener->SetColumnFamilyHandles(cf_handles);
    }
    return s;
  }
  void PutBatch(ColumnFamilyHandle*& cf, uint64_t key = 0) {
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

  void GetValue(int k) {
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

  void PrintSSTFileStatus() {
    std::cout << "GetTotalSize: ";
    std::cout << sst_manager->GetTotalSize() << std::endl;
    std::cout << "IsMaxAllowedSpaceReached: ";
    std::cout << sst_manager->IsMaxAllowedSpaceReached() << std::endl;
    std::cout << "IsMaxAllowedSpaceReachedIncludingCompactions: ";
    std::cout << sst_manager->IsMaxAllowedSpaceReachedIncludingCompactions() << std::endl;
  }
  void PrintCFInfo() {
    for (ColumnFamilyHandle* cf_handle : cf_handles) {
      std::cout << "cf name " << cf_handle->GetName() << std::endl;
      std::cout << "cf id " << cf_handle->GetID() << std::endl;
      ColumnFamilyDescriptor cf_desc;
      Status s = cf_handle->GetDescriptor(&cf_desc);
      std::cout << "cf path " << cf_desc.options.cf_paths[0].path << std::endl;
    }
  }
};

TEST_F(SpaceLimitTest, Reiteration) {
  Status s = BuildDB();
  ASSERT_TRUE(s.ok());

  for (int i = 0; i < 1500; i++) 
    PutBatch(cf_handles[1], i);
  s = db->Flush(FlushOptions(), cf_handles);
  ASSERT_TRUE(s.ok());
  uint64_t sst_size = sst_manager->GetTotalSize();
  ASSERT_LE(sst_size, 35 << 20);
  ASSERT_FALSE(sst_manager->IsMaxAllowedSpaceReached());
  ASSERT_FALSE(sst_manager->IsMaxAllowedSpaceReachedIncludingCompactions());

  for (int i = 1500; i < 3000; i++) 
    PutBatch(cf_handles[1], i);
  s = db->Flush(FlushOptions(), cf_handles);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(s.severity(), Status::Severity::kFatalError);
  ASSERT_EQ(s.subcode(), 0);
  ASSERT_EQ(s.ToString(), "IO error: Writer has previous error.");
  ASSERT_EQ(sst_size, sst_manager->GetTotalSize());
  ASSERT_FALSE(sst_manager->IsMaxAllowedSpaceReached());
  ASSERT_FALSE(sst_manager->IsMaxAllowedSpaceReachedIncludingCompactions());

  s = db->Resume();
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(s.severity(), Status::Severity::kFatalError);
  ASSERT_EQ(s.subcode(), 0);
  ASSERT_EQ(s.ToString(), "IO error: Writer has previous error.");
}

TEST_F(SpaceLimitTest, SetMaxSpace) {
  Status s = BuildDB();
  ASSERT_TRUE(s.ok());
  sst_manager->SetMaxAllowedSpaceUsage(40 << 20);

  for (int i = 0; i < 1500; i++) 
    PutBatch(cf_handles[1], i);
  s = db->Flush(FlushOptions(), cf_handles);
  ASSERT_TRUE(s.ok());
  uint64_t sst_size = sst_manager->GetTotalSize();
  ASSERT_LE(sst_size, 35 << 20);
  ASSERT_FALSE(sst_manager->IsMaxAllowedSpaceReached());
  ASSERT_FALSE(sst_manager->IsMaxAllowedSpaceReachedIncludingCompactions());

  for (int i = 1500; i < 2000; i++) 
    PutBatch(cf_handles[1], i);
  s = db->Flush(FlushOptions(), cf_handles);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(s.severity(), Status::Severity::kHardError);
  ASSERT_EQ(s.subcode(), Status::SubCode::kSpaceLimit);
  ASSERT_GE(sst_manager->GetTotalSize(), 40 << 20);
  ASSERT_TRUE(sst_manager->IsMaxAllowedSpaceReached());
  ASSERT_TRUE(sst_manager->IsMaxAllowedSpaceReachedIncludingCompactions());

  s = db->Resume();
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(s.severity(), Status::Severity::kNoError);
  ASSERT_EQ(s.subcode(), Status::SubCode::kSpaceLimit);

  s = db->CompactRange(
      CompactRangeOptions(), cf_handles[1], nullptr, nullptr);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(s.severity(), Status::Severity::kHardError);
  ASSERT_EQ(s.subcode(), Status::SubCode::kSpaceLimit);
}

TEST_F(SpaceLimitTest, ShrinkCompactionSizeOriginal) {
  InitOptions options;
  options.write_buffer_size = 4 << 20;
  Status s = BuildDB(options);
  ASSERT_TRUE(s.ok());
  sst_manager->SetMaxAllowedSpaceUsage(40 << 20);
  listener->EnableActiveResume();

  for (int i = 0; i < 1500; i++) 
    PutBatch(cf_handles[1], i);
  s = db->Flush(FlushOptions(), cf_handles);
  ASSERT_TRUE(s.ok());
  ASSERT_LE(sst_manager->GetTotalSize(), 35 << 20);
  ASSERT_FALSE(sst_manager->IsMaxAllowedSpaceReached());
  ASSERT_FALSE(sst_manager->IsMaxAllowedSpaceReachedIncludingCompactions());

  for (int i = 1500; i < 2000; i++) 
    PutBatch(cf_handles[1], i);
  s = db->Flush(FlushOptions(), cf_handles);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(s.severity(), Status::Severity::kHardError);
  ASSERT_EQ(s.subcode(), Status::SubCode::kSpaceLimit);
  ASSERT_GE(sst_manager->GetTotalSize(), 40 << 20);
  ASSERT_TRUE(sst_manager->IsMaxAllowedSpaceReached());
  ASSERT_TRUE(sst_manager->IsMaxAllowedSpaceReachedIncludingCompactions());

  s = db->Resume();
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(s.severity(), Status::Severity::kNoError);
  ASSERT_EQ(s.subcode(), Status::SubCode::kSpaceLimit);
}

TEST_F(SpaceLimitTest, ShrinkCompactionSize) {
  InitOptions options;
  options.write_buffer_size = 4 << 20;
  options.optimized = true;
  Status s = BuildDB(options);
  ASSERT_TRUE(s.ok());
  sst_manager->SetMaxAllowedSpaceUsage(40 << 20);

  SyncPoint::GetInstance()->LoadDependency(
      {{"StorageListener::OnErrorRecoveryCompleted recovered",
        "SpaceLimitTest::ShrinkCompactionSize retry"}});
  SyncPoint::GetInstance()->EnableProcessing();

  for (int i = 0; i < 1500; i++) 
    PutBatch(cf_handles[1], i);
  s = db->Flush(FlushOptions(), cf_handles);
  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(s.severity(), Status::Severity::kHardError);
  ASSERT_EQ(s.subcode(), Status::SubCode::kSpaceLimit);
  ASSERT_GE(sst_manager->GetTotalSize(), 40 << 20);
  ASSERT_TRUE(sst_manager->IsMaxAllowedSpaceReached());
  ASSERT_TRUE(sst_manager->IsMaxAllowedSpaceReachedIncludingCompactions());

  for (int i = 1500; i < 2000; i++) 
    PutBatch(cf_handles[1], i);
  s = db->Flush(FlushOptions(), cf_handles);

  ASSERT_TRUE(s.IsIOError());
  ASSERT_EQ(s.severity(), Status::Severity::kHardError);
  ASSERT_EQ(s.subcode(), Status::SubCode::kSpaceLimit);
  ASSERT_GE(sst_manager->GetTotalSize(), 40 << 20);
  ASSERT_TRUE(sst_manager->IsMaxAllowedSpaceReached());
  ASSERT_TRUE(sst_manager->IsMaxAllowedSpaceReachedIncludingCompactions());

  s = db->Resume();

  ASSERT_TRUE(s.ok());

  // Waiting for recovery from error.
  TEST_SYNC_POINT("SpaceLimitTest::ShrinkCompactionSize retry");
  PutBatch(cf_handles[1], 2000);
  s = db->Flush(FlushOptions(), cf_handles);
  ASSERT_TRUE(s.ok());
  ASSERT_LE(sst_manager->GetTotalSize(), 35 << 20);
  ASSERT_FALSE(sst_manager->IsMaxAllowedSpaceReached());
  ASSERT_FALSE(sst_manager->IsMaxAllowedSpaceReachedIncludingCompactions());

  std::string value;
  s = db->Get(ReadOptions(), cf_handles[1], std::to_string(2000 << 10), &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(value, std::to_string(2000 << 10));
}

// Test: it must fail due to not enough room
TEST_F(SpaceLimitTest, CompactionRoom) {
  Status s = BuildDB();
  sst_manager->SetMaxAllowedSpaceUsage(64 << 20);
  for (int i = 0; i < 1500; i++) 
    PutBatch(cf_handles[1], i);
  s = db->Flush(FlushOptions(), cf_handles);
  ASSERT_LE(sst_manager->GetTotalSize(), 35 << 20);
  ASSERT_FALSE(sst_manager->IsMaxAllowedSpaceReached());
  ASSERT_FALSE(sst_manager->IsMaxAllowedSpaceReachedIncludingCompactions());

  CompactRangeOptions options = CompactRangeOptions();
  s = db->CompactRange(options, cf_handles[1], nullptr, nullptr);
  ASSERT_TRUE(s.IsCompactionTooLarge());
  ASSERT_EQ(s.severity(), Status::Severity::kNoError);

  options.max_subcompactions = 12;
  s = db->CompactRange(options, cf_handles[1], nullptr, nullptr);
  ASSERT_TRUE(s.IsCompactionTooLarge());
  ASSERT_EQ(s.severity(), Status::Severity::kNoError);
}

TEST_F(SpaceLimitTest, ScanPlain) {
  Status s = BuildDB();
  for (int i = 0; i < 1500; i++) 
    PutBatch(cf_handles[1], i);
  s = db->Flush(FlushOptions(), cf_handles);
  Iterator* iter = db->NewIterator(ReadOptions(), cf_handles[1]);
  int count = 0;
  for (iter->SeekToFirst(); iter->Valid() && count < 10; iter->Next()) {
    std::cout << iter->key().ToString() << std::endl;
    count++;
  }
  // Note: This must delete iter here, otherwise iter will still be a Reference
  // for ColumnFamilyData:1. ColumnFamilyData::UnrefAndTryDelete will return
  // false, and ~ColumnFamilySet() will fail.
  delete iter;
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}