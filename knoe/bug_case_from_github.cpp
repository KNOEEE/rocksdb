#include <iostream>
#include "rocksdb/db.h"
#include <unistd.h>
class StorageExtender : public rocksdb::EventListener
{
public:
    bool isRecovered = false;
    StorageExtender() = default;
    void OnErrorRecoveryBegin(
        rocksdb::BackgroundErrorReason reason,
        rocksdb::Status bg_error,
        bool* /*auto_recovery*/
    ) {
        std::cout << "Got error:" << bg_error.ToString() << std::endl;
        if (bg_error.IsNoSpace()) {
            system("mount -o remount,size=256M /mnt/mytmpfs");
        }
    }
    void OnErrorRecoveryCompleted(rocksdb::Status old_bg_error) {
        std::cout << "Recovered from error:" << old_bg_error.ToString() << std::endl;
        isRecovered = true;
    }
};
int main()
{
    auto storageExtender = std::make_shared<StorageExtender>();
    rocksdb::DB* db{};
    rocksdb::Options options;
    options.listeners.push_back(storageExtender);
    options.create_if_missing = true;
    system("umount /mnt/mytmpfs");
    system("mount -t tmpfs -o size=1024K tmpfs /mnt/mytmpfs");
    rocksdb::Status s = rocksdb::DB::Open(options, "/mnt/mytmpfs", &db);
    if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
        return 1;
    }
    rocksdb::WriteBatch wb;
    for (int i = 0; i < 1024 * 1024 * 5; ++i) {
        auto kv = std::to_string(i);
        s = wb.Put(kv, kv);
    }
    rocksdb::WriteOptions wo;
    s = db->Write(wo, &wb);
    std::cout << s.ToString() << std::endl;
    // system("mount -o remount,size=32768K /mnt/mytmpfs");
    std::cout << "Waiting for recovery to complete" << std::endl;
    while (!storageExtender->isRecovered) {
        std::cout << "."  << std::endl;
        sleep(1);
    }
    std::cout << "Done" << std::endl;
    s = db->Write(wo, &wb);
    std::cout << s.ToString() << std::endl;
    return 0;
}