#include "DBFile.cc"
#include "ComparisonEngine.cc"
#include "File.cc"
#include "Record.cc"
#include "Schema.cc"
#include <cstdlib>

#include <gtest/gtest.h>


TEST(CreateTest, CreateSuccess) { 
    DBFile dbfile;
    char myfname[] = "lee.txt";
    fType heap = heap;
    void* ptr;
    int val = dbfile.Create(myfname, heap, &ptr);
    ASSERT_EQ(1, val);
}

// TEST(CreateTest, CreateFailure) { 
//     DBFile dbfile;
//     char myfname[] = "lee.txt";
//     fType heap = heap;
//     void* ptr;
//     int val = dbfile.Create(myfname, heap, &ptr);
//     ASSERT_EQ(1, val);
// }

TEST(OpenTest, OpenSuccess) { 
    DBFile dbfile;
    char myfname[] = "lee.txt";
    fType heap = heap;
    void* ptr;
    int val = dbfile.Create(myfname, heap, &ptr);
    ASSERT_EQ(1, val);
    val = dbfile.Open(myfname);
    ASSERT_EQ(1, val);
}

// TEST(OpenTest, OpenFailure) { 
//     DBFile dbfile;
//     char myfname[] = "rightname.txt";
//     fType heap = heap;
//     void* ptr;
//     int val = dbfile.Create(myfname, heap, &ptr);
//     ASSERT_EQ(1, val);
//     val = dbfile.Open("Wrongname.txt");
//     ASSERT_EQ(1, val);
// }

TEST(OpenTest, OpenSuccess) { 
    DBFile dbfile;
    char myfname[] = "lee.txt";
    fType heap = heap;
    void* ptr;
    int val = dbfile.Create(myfname, heap, &ptr);
    ASSERT_EQ(1, val);
    val = dbfile.Open(myfname);
    ASSERT_EQ(1, val);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}