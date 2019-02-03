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

TEST(CreateTest, CreateMetaDataFilesTest) { 
    DBFile dbfile;
    char myfname[] = "test.txt";
    fType heap = heap;
    void* ptr;
    int val = dbfile.Create(myfname, heap, &ptr);
    FILE* f = fopen("test_lpage.txt", "r");
    ASSERT_TRUE(f != NULL);
    FILE* f2 = fopen("test_dpage.txt", "r");
    ASSERT_TRUE(f != NULL);
}

TEST(GetValueFromTxtTest, GetValueSuccess) { 
    off_t temp_val = 0;
    off_t zero = 0;
    off_t one = 0;

    DBFile dbfile;

    char l_page [] = "test_lpage.txt";
    temp_val = dbfile.GetValueFromTxt(l_page);
    ASSERT_EQ(zero, temp_val);
    char d_page [] = "test_lpage.txt";
    temp_val = dbfile.GetValueFromTxt(d_page);
    ASSERT_EQ(one, temp_val);
}
TEST(SetValueFromTxtTest, SetValueSuccess) { 
    off_t temp_val = 98;
    off_t get_val = 0;
    DBFile dbfile;
    char d_page [] = "test_lpage.txt";
    dbfile.SetValueFromTxt(d_page, get_val);
    ASSERT_EQ(get_val, dbfile.GetValueFromTxt(d_page));
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}