#include "DBFile.cc"
#include "File.cc"
#include "TwoWayList.cc"
#include "Record.cc"
#include "Comparison.cc"
#include "ComparisonEngine.cc"
#include "Schema.cc"
#include <gtest/gtest.h>
TEST(CreateTest, TestCreate) {
   DBFile dbfile;
   char myfname[] = "lee.txt";
   fType heap = heap;
   void* ptr;
   int val = dbfile.Create(myfname, heap, &ptr);
   ASSERT_EQ(1, val);
}

/*TEST(OpenTest, TestCreate) {
   DBFile dbfile;
   char myfname[] = "lee.txt";
   fType heap = heap;
   void* ptr;
   int val = dbfile.Create(myfname, heap, &ptr);
   ASSERT_EQ(1, val);
}
*/
 
/*TEST(CreateTest, PositiveNos) { 
    ASSERT_EQ(6, squareRoot(36.0));
    ASSERT_EQ(18.0, squareRoot(324.0));
    ASSERT_EQ(25.4, squareRoot(645.16));
    ASSERT_EQ(0, squareRoot(0.0));
}
 
TEST(SquareRootTest, NegativeNos) {
    ASSERT_EQ(-1.0, squareRoot(-15.0));
    ASSERT_EQ(-1.0, squareRoot(-0.2));
}
*/ 
int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
