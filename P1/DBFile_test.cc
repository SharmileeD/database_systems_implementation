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
    dbfile.Close();
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
    dbfile.Close();
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

/*TEST(OpenTest, OpenWithoutCreate) {
     DBFile dbfile;
     char myfname[] = "lee.txt";
     fType heap = heap;
     void* ptr;
     //int val = dbfile.Create(myfname, heap, &ptr);
     //ASSERT_EQ(1, val);
     int val = dbfile.Open("lee.txt");
     ASSERT_EQ(0, val);
 }*/

TEST(Closetest, CloseSuccess) {
     DBFile dbfile;
     char myfname[] = "lee.txt";
     fType heap = heap;
     void* ptr;
     int val = dbfile.Create(myfname, heap, &ptr);
     ASSERT_EQ(1, val);
     val = dbfile.Open("lee.txt");
     ASSERT_EQ(1, val);
     val = dbfile.Close();
     ASSERT_EQ(1, val);
     dbfile.Close();
 }

TEST(Loadtest, PreLoadSuccess) {
     DBFile dbfile;
     char myfname[] = "lee.txt";
     fType heap = heap;
     void* ptr;
     int val = dbfile.Create(myfname, heap, &ptr);
     ASSERT_EQ(1, val);
     val = dbfile.Open("lee.txt");
     ASSERT_EQ(1, val);
     int initial_length = dbfile.file_instance.GetLength();
     ASSERT_EQ(1, val);
     dbfile.Close();
 }

TEST(Loadtest, PostLoadSuccess) {
     DBFile dbfile; 
     char myfname[] = "lee.txt";
     fType heap = heap;
     void* ptr;
     int val = dbfile.Create(myfname, heap, &ptr);
     ASSERT_EQ(1, val);
     val = dbfile.Open("lee.txt");
     ASSERT_EQ(1, val);
     int initial_length = dbfile.file_instance.GetLength();
     Schema mySchema ("catalog", "lineitem");
     dbfile.Load(mySchema, "tables/lineitem.tbl");
     int new_length = dbfile.file_instance.GetLength();
     ASSERT_LE(initial_length, new_length); 
     dbfile.Close();
 }


TEST(AddPagetest, AddPageSuccess) {
     DBFile dbfile;
     char myfname[] = "lee.txt";
     fType heap = heap;
     void* ptr;
     int val = dbfile.Create(myfname, heap, &ptr);
     ASSERT_EQ(1, val);
     val = dbfile.Open("lee.txt");
     ASSERT_EQ(1, val);
     Schema mySchema ("catalog", "lineitem");
     dbfile.Load(mySchema, "tables/lineitem.tbl");
     int initial_length = dbfile.file_instance.GetLength();
     Record temp;
     const char* loadpath = "tables/lineitem.tbl";
     FILE *tableFile = fopen (loadpath, "r");
     int count = 606;
     while (temp.SuckNextRecord (&mySchema, tableFile) == 1 && count > 0){
            dbfile.Add(temp);
     }
     int current_length = dbfile.file_instance.GetLength();
     ASSERT_LT(initial_length, current_length);
     dbfile.Close(); 
}

TEST(GetNextTest, GetLast) {
     DBFile dbfile;
     char myfname[] = "lee.txt";
     fType heap = heap;
     void* ptr;
     int val = dbfile.Create(myfname, heap, &ptr);
     ASSERT_EQ(1, val);
     val = dbfile.Open("lee.txt");
     ASSERT_EQ(1, val);
     int initial_length = dbfile.file_instance.GetLength();
     Schema mySchema ("catalog", "customer");
     dbfile.Load(mySchema, "tables/customer.tbl");
     int new_length = dbfile.file_instance.GetLength();
     dbfile.record_offset = 0;
     dbfile.current_page = 2;
     Record next_rec;
     int result = dbfile.GetNext(next_rec);
     ASSERT_EQ(0, result);
     dbfile.Close();
}


TEST(GetNextTest, GNSuccess) {
     DBFile dbfile;
     char myfname[] = "lee.txt";
     fType heap = heap;
     void* ptr;
     int val = dbfile.Create(myfname, heap, &ptr);
     ASSERT_EQ(1, val);
     val = dbfile.Open("lee.txt");
     ASSERT_EQ(1, val);
     int initial_length = dbfile.file_instance.GetLength();
     Schema mySchema ("catalog", "lineitem");
     dbfile.Load(mySchema, "tables/lineitem.tbl");
     int new_length = dbfile.file_instance.GetLength();
     dbfile.record_offset = 0;
     dbfile.current_page = 0;
     Record next_rec;
     int result = dbfile.GetNext(next_rec);
     ASSERT_EQ(1, result);
     dbfile.Close();
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
    dbfile.Close();
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
