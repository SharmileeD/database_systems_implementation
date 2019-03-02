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


TEST(CreateTestHeap, CreateSuccessHeap) { 
    DBFile dbfile;
    char myfname[] = "lee.txt";
    fType heap = heap;
    void* ptr;
    int val = dbfile.Create(myfname, heap, &ptr);
    ASSERT_EQ(1, val);
    dbfile.Close();
}


TEST(OpenTestHeap, OpenSuccessHeap) { 
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

TEST(ClosetestHeap, CloseSuccessHeap) {
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

TEST(LoadtestHeap, PreLoadSuccessHeap) {
     Heap dbfile;
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

TEST(LoadtestHeap, PostLoadSuccessHeap) {
     Heap dbfile; 
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


TEST(AddPagetestHeap, AddPageSuccessHeap) {
     Heap dbfile;
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

TEST(GetNextTestHeap, GetLastHeap) {
     Heap dbfile;
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


TEST(GetNextTestHeap, GNSuccessHeap) {
     Heap dbfile;
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
    Heap dbfile;
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

    Heap dbfile;

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
    Heap dbfile;
    char d_page [] = "test_lpage.txt";
    dbfile.SetValueFromTxt(d_page, get_val);
    ASSERT_EQ(get_val, dbfile.GetValueFromTxt(d_page));
}

// TEST(CreateRunTest, checkNumRecs) {
//     Heap dbfile;
//     Heap to_test;
//     Record inprec;
   
//     int count = 0;
//     int runcount = 0;
// 	dbfile.Open("nation.bin");
//     to_test.Open("runs.bin");
//     dbfile.MoveFirst();
//     to_test.MoveFirst();
//     while (dbfile.GetNext(inprec) == 1) {
//         count++;
//     }
   
//     while (to_test.GetNext(inprec) == 1) {
//         runcount++;
//     }
//     dbfile.Close();
//     to_test.Close();
    
//     ASSERT_EQ(count, runcount);

// }

// TEST(CreateRunTest, checkNumPages) {
//     Heap dbfile;
//     Heap to_test;
    
// 	dbfile.Open("nation.bin");
//     to_test.Open("runs.bin");
//     int count = dbfile.file_instance.GetLength();
//     int runcount = to_test.file_instance.GetLength();
//     dbfile.Close();
//     to_test.Close();
    
//     ASSERT_EQ(count, runcount);

// }

// TEST(sortRecorsTest, checkRunSort1) {
//     Heap dbfile;
//     dbfile.Open("runs.bin");
//     Page pg;
//     CNF cnf;
//     Record r1;
//     Record r2;

//     ComparisonEngine ceng;
//     //extern struct AndList *final;
    
//     Schema mySchema ("catalog", "orders");
//     OrderMaker sortorder(&mySchema);
    
//     dbfile.file_instance.GetPage(&pg,0);
//     pg.GetFirst(&r1);
//     pg.GetFirst(&r2);
    
//     //r1.Print(&mySchema);
//     //r2.Print(&mySchema);
	
//     int val = ceng.Compare (&r2, &r1, &sortorder);
//     ASSERT_EQ(val,1);
//     dbfile.Close();
// }

// TEST(sortRecorsTest, checkRunSort2) {
//     Heap dbfile;
//     dbfile.Open("runs.bin");
//     Page pg;
//     CNF cnf;
//     Record r1;
//     Record r2;

//     ComparisonEngine ceng;
//     //extern struct AndList *final;
    
//     Schema mySchema ("catalog", "orders");
//     OrderMaker sortorder(&mySchema);
    
//     dbfile.file_instance.GetPage(&pg,1);
//     pg.GetFirst(&r1);
//     pg.GetFirst(&r2);
    
//     //r1.Print(&mySchema);
//     //r2.Print(&mySchema);
	
//     int val = ceng.Compare (&r1, &r2, &sortorder);
//     ASSERT_EQ(val,-1);
//     dbfile.Close();
// }

// TEST(sortRecorsTest, checkRunSort3) {
//     Heap dbfile;
//     dbfile.Open("runs.bin");
//     Page pg;
//     CNF cnf;
//     Record r1;
//     Record r2;

//     ComparisonEngine ceng;
//     //extern struct AndList *final;
    
//     Schema mySchema ("catalog", "orders");
//     OrderMaker sortorder(&mySchema);
    
//     dbfile.file_instance.GetPage(&pg,0);
//     pg.GetFirst(&r1);
//     r2.Copy(&r1);
    
//     //r1.Print(&mySchema);
//     //r2.Print(&mySchema);
	
//     int val = ceng.Compare (&r1, &r2, &sortorder);
//     ASSERT_EQ(val,0);
//     dbfile.Close();
// }

// TEST(mergeSortTest,testRec) {
//     Heap dbfile;
//     dbfile.Open("runs.bin");
//     Page pg;
//     Record arr[10];
//     ComparisonEngine ceng;
    
//     dbfile.file_instance.GetPage(&pg,0);

//     for(int i =9;i >= 0; i--) {
//         pg.GetFirst(&arr[i]);
//     }
//     Schema mySchema ("catalog", "orders");
//     OrderMaker sortorder(&mySchema);
//     mergeSort(arr,0,9,sortorder);
//     int val;
//     for(int i =0;i < 9; i++) {
//         val = ceng.Compare (&arr[i], &arr[i+1], &sortorder);
//         ASSERT_EQ(val,-1);
//     }

// }

TEST(CreateTestSorted, CreateSuccessSorted) { 
    DBFile dbfile;
    char myfname[] = "sortedTestFile";
    // fType sorted = sorted;

    int runLength = 1;
    Schema mySchema ("catalog", "nation");
    OrderMaker sortorder(&mySchema);
    
    struct {OrderMaker *o; int l;} startup = {&sortorder, runLength};
    int val = dbfile.Create(myfname, sorted, &startup);
    ASSERT_EQ(1, val);
    // dbfile.Close();
}


TEST(OpenTestSorted, OpenSuccessSorted) { 
    DBFile dbfile;
    char myfname[] = "sortedTestFile";
    // fType sorted = sorted;
   
     int runLength = 1;
    Schema mySchema ("catalog", "nation");
    OrderMaker sortorder(&mySchema);

    struct {OrderMaker *o; int l;} startup = {&sortorder, runLength};
    int val = dbfile.Create(myfname, sorted, &startup);
    ASSERT_EQ(1, val);
    val = dbfile.Open(myfname);
    ASSERT_EQ(1, val);
    // dbfile.Close();
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
