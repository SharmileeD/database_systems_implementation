#include "DBFile.cc"
#include "ComparisonEngine.cc"
#include "Comparison.cc"
#include "Comparison.h"

#include "File.cc"
#include "Record.cc"
#include "Schema.cc"
#include <cstdlib>
#include "BigQ.cc"
#include "Pipe.cc"
#include "test.h"
#include <stdlib.h>
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
    off_t ans = 1;
    off_t result = 1;

    DBFile dbfile;

    char l_page [] = "test_lpage.txt";
    temp_val = dbfile.GetValueFromTxt(l_page);
    ASSERT_EQ(ans, temp_val);
    char d_page [] = "test_lpage.txt";
    temp_val = dbfile.GetValueFromTxt(d_page);
    ASSERT_EQ(result, temp_val);
}
TEST(SetValueFromTxtTest, SetValueSuccess) { 
    off_t temp_val = 98;
    off_t get_val = 0;
    DBFile dbfile;
    char d_page [] = "test_lpage.txt";
    dbfile.SetValueFromTxt(d_page, get_val);
    ASSERT_EQ(get_val, dbfile.GetValueFromTxt(d_page));
}

TEST(CreateRunTest, checkNumRecs) {
    DBFile dbfile;
    DBFile to_test;
    Record inprec;
   
    int count = 0;
    int runcount = 0;
	dbfile.Open("orders.bin");
    to_test.Open("runs.bin");
    while (dbfile.GetNext(inprec) == 1) {
        count++;
    }
   
    while (to_test.GetNext(inprec) == 1) {
        runcount++;
    }
    dbfile.Close();
    to_test.Close();
    
    ASSERT_EQ(count, runcount);

}

TEST(CreateRunTest, checkNumPages) {
    DBFile dbfile;
    DBFile to_test;
    
	dbfile.Open("orders.bin");
    to_test.Open("runs.bin");
    int count = dbfile.file_instance.GetLength();
    int runcount = to_test.file_instance.GetLength();
    dbfile.Close();
    to_test.Close();
    
    ASSERT_EQ(count, runcount);

}

TEST(sortRecorsTest, checkRunSort1) {
    DBFile dbfile;
    dbfile.Open("runs.bin");
    Page pg;
    CNF cnf;
    Record r1;
    Record r2;

    ComparisonEngine ceng;
    //extern struct AndList *final;
    
    Schema mySchema ("catalog", "orders");
    OrderMaker sortorder(&mySchema);
    
    dbfile.file_instance.GetPage(&pg,0);
    pg.GetFirst(&r1);
    pg.GetFirst(&r2);
    
    //r1.Print(&mySchema);
    //r2.Print(&mySchema);
	
    int val = ceng.Compare (&r2, &r1, &sortorder);
    ASSERT_EQ(val,1);
    dbfile.Close();
}

TEST(sortRecorsTest, checkRunSort2) {
    DBFile dbfile;
    dbfile.Open("runs.bin");
    Page pg;
    CNF cnf;
    Record r1;
    Record r2;

    ComparisonEngine ceng;
    //extern struct AndList *final;
    
    Schema mySchema ("catalog", "orders");
    OrderMaker sortorder(&mySchema);
    
    dbfile.file_instance.GetPage(&pg,1);
    pg.GetFirst(&r1);
    pg.GetFirst(&r2);
    
    //r1.Print(&mySchema);
    //r2.Print(&mySchema);
	
    int val = ceng.Compare (&r1, &r2, &sortorder);
    ASSERT_EQ(val,-1);
    dbfile.Close();
}

TEST(sortRecorsTest, checkRunSort3) {
    DBFile dbfile;
    dbfile.Open("runs.bin");
    Page pg;
    CNF cnf;
    Record r1;
    Record r2;

    ComparisonEngine ceng;
    //extern struct AndList *final;
    
    Schema mySchema ("catalog", "orders");
    OrderMaker sortorder(&mySchema);
    
    dbfile.file_instance.GetPage(&pg,0);
    pg.GetFirst(&r1);
    r2.Copy(&r1);
    
    //r1.Print(&mySchema);
    //r2.Print(&mySchema);
	
    int val = ceng.Compare (&r1, &r2, &sortorder);
    ASSERT_EQ(val,0);
    dbfile.Close();
}

TEST(mergeSortTest,testRec) {
    DBFile dbfile;
    dbfile.Open("runs.bin");
    Page pg;
    Record arr[10];
    ComparisonEngine ceng;
    
    dbfile.file_instance.GetPage(&pg,0);

    for(int i =9;i >= 0; i--) {
        pg.GetFirst(&arr[i]);
    }
    Schema mySchema ("catalog", "orders");
    OrderMaker sortorder(&mySchema);
    mergeSort(arr,0,9,sortorder);
    int val;
    for(int i =0;i < 9; i++) {
        val = ceng.Compare (&arr[i], &arr[i+1], &sortorder);
        ASSERT_EQ(val,-1);
    }

}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
