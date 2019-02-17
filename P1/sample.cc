#include "Schema.cc"
#include "DBFile.cc"
#include "Comparison.cc"
#include "File.cc"
#include "Record.cc"
#include "TwoWayList.cc"
#include <gtest/gtest.h>
TEST(CreateTest, TestCreate) {
   DBFile dbfile;
   char myfname[] = "lee.txt";
   fType heap = heap;
   void* ptr;
   int val = dbfile.Create(myfname, heap, &ptr);
   ASSERT_EQ(1, val);
}
int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

