#include "db/db_test_util.h"
#include "test_util/sync_point.h"

using namespace ROCKSDB_NAMESPACE;

void Fun1() {
   printf("1\n");
   TEST_SYNC_POINT("SyncPointDemo::Fun1:1");
   TEST_SYNC_POINT("SyncPointDemo::Fun1:4");
   printf("4\n");
}

void Fun2() {
    TEST_SYNC_POINT("SyncPointDemo::Fun2:2");
    printf("2\n");
    printf("3\n");
    TEST_SYNC_POINT("SyncPointDemo::Fun2:3");
}

// https://void-shana.moe/posts/sync-point
TEST(SyncPointDemoTest, SequentialPrintNumbers) {
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing(); 
    ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->LoadDependency(
        {{"SyncPointDemo::Fun1:1", "SyncPointDemo::Fun2:2"}, 
         {"SyncPointDemo::Fun2:3", "SyncPointDemo::Fun1:4"}});
    std::thread t1(Fun1);
    std::thread t2(Fun2);
    t1.join();
    t2.join();
    SyncPoint::GetInstance()->DisableProcessing();
}

int do_something() {
    int retval = 0;

    // complex logic here
    // ...

    // When executing to this point, the callback will be invoked with argument `retval`
    TEST_SYNC_POINT_CALLBACK("SyncPointDemo::retval_inject", &retval);

    return retval;
}

TEST(SyncPointDemoTest, CallbackSetErrorCode) {
    SyncPoint::GetInstance()->EnableProcessing();
    std::function<void(void*)> cb = [] (void *arg) -> void {
      int *ptr = static_cast<int *>(arg);
      *ptr = -1;
    };
    SyncPoint::GetInstance()->SetCallBack("SyncPointDemo::retval_inject", cb);

    auto p = do_something();
    ASSERT_EQ(p, -1);
}


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}