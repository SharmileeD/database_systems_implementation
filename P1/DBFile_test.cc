#include "DBFile.cc"
#include "ComparisonEngine.cc"
#include "Comparison.cc"
#include "Comparison.h"
#include "Function.cc"
#include "RelOp.h"
#include "File.cc"
#include "Record.cc"
#include "Schema.cc"
#include <cstdlib>
#include "BigQ.cc"
#include "Pipe.cc"
#include "RelOp.cc"
#include "Statistics.cc"
#include <stdlib.h>
#include "ParseTree.h"
#include <gtest/gtest.h>
// extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
// extern "C" int yyparse(void);
// extern struct AndList *final;

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

TEST(sortRecorsTest, checkRunSort1) {
    Heap dbfile;
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



TEST(mergeSortTest,testRec) {
    Heap dbfile;
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
    remove( "sortedTestFile" );
	remove( "sortedTestFile_dpage.txt" );
	remove( "sortedTestFile_lpage.txt" );
	remove( "sortedTestFile_type.txt" );
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
    remove( "sortedTestFile" );
	remove( "sortedTestFile_dpage.txt" );
	remove( "sortedTestFile_lpage.txt" );
	remove( "sortedTestFile_type.txt" );
    // dbfile.Close();
}

TEST(AddCustomerTestSorted, AddCustomerSuccessSorted) { 
    DBFile dbfile;
	Heap hp;
	Record tempRec;
	// dbfile.Create("test_phase2.bin",heap,NULL);
	Schema mySchema ("catalog", "customer");
	OrderMaker sortorder(&mySchema);
	int runlen = 2;
	struct {OrderMaker *o; int l;} startup = {&sortorder, runlen};
	dbfile.Create("test_phase2.bin",sorted,&startup);
	dbfile.Close();
	dbfile.Open("test_phase2.bin");
	int res;
	Record temp;
	FILE *tblfile = fopen ("tables/customer.tbl", "r");
    int count =0;
	while ((res = temp.SuckNextRecord (&mySchema, tblfile))) {
		dbfile.Add (temp);
        count++;

	}
	dbfile.instVar->mergePipeAndFile();
    dbfile.Close();
	
	Heap dbfile_test;
	dbfile_test.Open ("test_phase2.bin");

	Record inprec;
	int counter = 0;
	Page test_page;
	dbfile_test.MoveFirst ();

	while (dbfile_test.GetNext(inprec) == 1) {
		
		counter += 1;
		
		if(counter % 5000 ==0){
			cout<< "inside populate loop "<< counter<<endl;
		}
	}
    ASSERT_EQ(counter, count);
	dbfile_test.Close();
	remove( "test_phase2.bin" );
	remove( "test_phase2_dpage.txt" );
	remove( "test_phase2_lpage.txt" );
	remove( "test_phase2_type.txt" );
}
TEST(AddRegionTestSorted, AddRegionSuccessSorted) { 
    DBFile dbfile;
	Heap hp;
	Record tempRec;
	// dbfile.Create("test_phase2.bin",heap,NULL);
	Schema mySchema ("catalog", "region");
	OrderMaker sortorder(&mySchema);
	int runlen = 2;
	struct {OrderMaker *o; int l;} startup = {&sortorder, runlen};
	dbfile.Create("test_phase2.bin",sorted,&startup);
	dbfile.Close();
	dbfile.Open("test_phase2.bin");
	int res;
	Record temp;
	FILE *tblfile = fopen ("tables/region.tbl", "r");
    int count =0;
	while ((res = temp.SuckNextRecord (&mySchema, tblfile))) {
		dbfile.Add (temp);
        count++;

	}
	dbfile.instVar->mergePipeAndFile();
    dbfile.Close();
	
	Heap dbfile_test;
	dbfile_test.Open ("test_phase2.bin");

	Record inprec;
	int counter = 0;
	Page test_page;
	dbfile_test.MoveFirst ();

	while (dbfile_test.GetNext(inprec) == 1) {
		
		counter += 1;
		
		if(counter % 5000 ==0){
			cout<< "inside populate loop "<< counter<<endl;
		}
	}
    ASSERT_EQ(counter, count);
	dbfile_test.Close();
	remove( "test_phase2.bin" );
	remove( "test_phase2_dpage.txt" );
	remove( "test_phase2_lpage.txt" );
	remove( "test_phase2_type.txt" );
}

TEST(AddNationTestSorted, AddNationSuccessSorted) { 
    DBFile dbfile;
	Heap hp;
	Record tempRec;
	// dbfile.Create("test_phase2.bin",heap,NULL);
	Schema mySchema ("catalog", "nation");
	OrderMaker sortorder(&mySchema);
	int runlen = 2;
	struct {OrderMaker *o; int l;} startup = {&sortorder, runlen};
	dbfile.Create("test_phase2.bin",sorted,&startup);
	dbfile.Close();
	dbfile.Open("test_phase2.bin");
	int res;
	Record temp;
	FILE *tblfile = fopen ("tables/nation.tbl", "r");
    int count =0;
	while ((res = temp.SuckNextRecord (&mySchema, tblfile))) {
		dbfile.Add (temp);
        count++;

	}
	dbfile.instVar->mergePipeAndFile();
    dbfile.Close();
	
	Heap dbfile_test;
	dbfile_test.Open ("test_phase2.bin");

	Record inprec;
	int counter = 0;
	Page test_page;
	dbfile_test.MoveFirst ();

	while (dbfile_test.GetNext(inprec) == 1) {
		
		counter += 1;
		
		if(counter % 5000 ==0){
			cout<< "inside populate loop "<< counter<<endl;
		}
	}
    ASSERT_EQ(counter, count);
	dbfile_test.Close();
	remove( "test_phase2.bin" );
	remove( "test_phase2_dpage.txt" );
	remove( "test_phase2_lpage.txt" );
	remove( "test_phase2_type.txt" );
}
TEST(PipeGetFirstSlot, PipeGetFirstSlotSuccess){
    Schema mySchema ("catalog", "customer");
	int res;
	Record temp;
	FILE *tblfile = fopen ("tables/customer.tbl", "r");
    int count =0;
	Pipe in_pipe = Pipe(100);
	while ((res = temp.SuckNextRecord (&mySchema, tblfile))) {
		// temp.Print(&mySchema);
		in_pipe.Insert(&temp);
        count++;
		if (count == 50){
			break;
		}
	}
	cout<<"testing"<<endl;
    ASSERT_EQ(in_pipe.getFirstSlot(), 0);
	cout<< in_pipe.getLastSlot()<<endl;
}
TEST(PipeGetLastSlot, PipeGetLastSlotSuccess){
    Schema mySchema ("catalog", "customer");
	int res;
	Record temp;
	FILE *tblfile = fopen ("tables/customer.tbl", "r");
    int count =0;
	Pipe in_pipe = Pipe(100);
	while ((res = temp.SuckNextRecord (&mySchema, tblfile))) {
		// temp.Print(&mySchema);
		in_pipe.Insert(&temp);
        count++;
		if (count == 50){
			break;
		}
	}
	cout<<"testing"<<endl;
    ASSERT_EQ(in_pipe.getLastSlot(), count);
}
TEST(PipeReset, PipeResetSuccess){
    Schema mySchema ("catalog", "customer");
	int res;
	Record temp;
	FILE *tblfile = fopen ("tables/customer.tbl", "r");
    int count =0;
	Pipe in_pipe = Pipe(100);
	while ((res = temp.SuckNextRecord (&mySchema, tblfile))) {
		// temp.Print(&mySchema);
		in_pipe.Insert(&temp);
        count++;
		if (count == 50){
			break;
		}
	}
	cout<<"testing"<<endl;
    in_pipe.resetPipe();
    ASSERT_EQ(in_pipe.getLastSlot(), 0);
    ASSERT_EQ(in_pipe.getFirstSlot(), 0);
}
int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	// cout<< "Inside clear pipe!!"<<endl;
	// FILE *writefile = fopen ("opclear.txt", "w");
	string strrec;
	int cnt = 0;
	const char * c;
	while (in_pipe.Remove (&rec)) {
		// strrec = rec.returnRecord(schema);
		// c = strrec.c_str();
		// fprintf(writefile, c);
		if (print) {
			rec.Print (schema);
		}
		cnt++;
	}
	// rec.Print (schema);
	// cout << "clear pipe count = "<<cnt<<endl;
	return cnt;
}
/*TEST(RelopTest, SFTest){
    Schema mySchema ("catalog", "supplier");
    Record lit;
    CNF cnf;
    DBFile db;
    SelectFile sf;
    Pipe op(100);
    db.Open("supplier.bin");
    struct AndList *final;	
    char * pred_str = "(s_suppkey = s_suppkey)";

    cnf.GrowFromParseTree(final,&mySchema, lit);
    // cnf.Print();
    sf.Run (db, op, cnf, lit);
	int cnt = clear_pipe (op, &mySchema, false);
	sf.WaitUntilDone ();

	// int cnt = clear_pipe (_ps, ps->schema (), true);
	// cout << "\n\n query1 returned " << cnt << " records \n";
    ASSERT_EQ(cnt,100);
	db.Close ();

}
*/
TEST(RelopTest, SelectPipeTest){
    Pipe inpipe(100);
	Pipe outPipe(100);
	Pipe outPipeProject(100);
	Schema mySchema ("catalog", "nation");
	struct AndList *final;	
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &mySchema, literal);
	// myComparison.Print ();
    int res;
	Record temp;
	FILE *tblfile = fopen ("tables/nation.tbl", "r");
    int count =0;
	while ((res = temp.SuckNextRecord (&mySchema, tblfile))) {
        inpipe.Insert(&temp);
		count++;

	}
	// cout<<"Added "<<count<<" records to inpipe"<<endl;
	inpipe.ShutDown();
	// grow the CNF expression from the parse tree 
	
    int recs = 0;
	SelectPipe sp;
    Record * tempRec;
    Record outRec;
    tempRec = &outRec;
	sp.Run(inpipe, outPipe, myComparison,literal);
    while (outPipe.Remove(tempRec)==1) {
		recs++;
	}

	sp.WaitUntilDone();
    ASSERT_EQ(recs,25);
}

TEST(RelopTest, ProjectTest){
    Pipe inpipe(100);
	Pipe outPipe(100);
	Pipe outPipeProject(100);
	Schema mySchema ("catalog", "nation");
	struct AndList *final;	
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &mySchema, literal);
	// myComparison.Print ();
    int res;
	Record temp;
	FILE *tblfile = fopen ("tables/nation.tbl", "r");
    int count =0;
	while ((res = temp.SuckNextRecord (&mySchema, tblfile))) {
        inpipe.Insert(&temp);
		count++;

	}
	// cout<<"Added "<<count<<" records to inpipe"<<endl;
	inpipe.ShutDown();
	// grow the CNF expression from the parse tree 
	

	SelectPipe sp;
	sp.Run(inpipe, outPipe, myComparison,literal);
	sp.WaitUntilDone();
	
	int keepMe[] = {0,1};
	int numAttsIn = 4;
	int numAttsOut = 2;
	Project p;
	p.Run(outPipe, outPipeProject, keepMe, numAttsIn, numAttsOut);
	p.WaitUntilDone();
	Record * tempRec;
	Record outrec;
	tempRec = &outrec;
	int recs;
	while (outPipeProject.Remove(tempRec)==1) {
		recs++;
	}	
	// cout <<"Records = "<<recs<<endl;
    ASSERT_EQ(recs,count);

}

TEST(RelopTest, DuplicateRemovalTest){
    Pipe inpipe(100);
	Pipe outPipe(100);
	Pipe outPipeProject(100);
	Schema mySchema ("catalog", "nation");
	
	// suck up the schema from the file
	string rec;
	int res;
	Record temp;
	FILE *tblfile = fopen ("tables/nation.tbl", "r");
    int count =0;
	while ((res = temp.SuckNextRecord (&mySchema, tblfile))) {
        rec= temp.returnRecord(&mySchema);
		inpipe.Insert(&temp);
		// cout<< rec<<endl;
		count++;

	}
	Record temp2;
	FILE *tblfile2 = fopen ("tables/nation.tbl", "r");
	// cout<<"Added "<<count<<" records to inpipe after first while loop"<<endl;
	while ((res = temp2.SuckNextRecord (&mySchema, tblfile2))) {
        inpipe.Insert(&temp2);
		count++;

	}
	Record temp3;
	FILE *tblfile3 = fopen ("tables/nation.tbl", "r");
	// cout<<"Added "<<count<<" records to inpipe after first while loop"<<endl;
	while ((res = temp3.SuckNextRecord (&mySchema, tblfile3))) {
        inpipe.Insert(&temp3);
		count++;

	}
	inpipe.ShutDown();
    DuplicateRemoval dr;
	dr.Run(inpipe, outPipe, mySchema);
    Record *tempRec;
    Record outRec;
    tempRec = &outRec;
    int recs = 0;
    while (outPipe.Remove(tempRec)==1) {
		recs++;
	}
    ASSERT_EQ(recs,25);
	dr.WaitUntilDone();
}

TEST(RelopTest, WriteTest) {
    Pipe inpipe(100);
	Pipe outPipe(100);
	Pipe outPipeProject(100);
	Schema mySchema ("catalog", "customer");
	// fillInputPipe(&inpipe, &mySchema);
	
	WriteOut W;
		// _s (input pipe)
		
	FILE *writefile = fopen ("outputfile_new.txt", "w");
	Pipe _s_ps (100);
	W.Run (inpipe, writefile, mySchema);
	int res;
	Record temp;
	FILE *tblfile = fopen ("tables/customer.tbl", "r");
    int count =0;
	while ((res = temp.SuckNextRecord (&mySchema, tblfile))) {
		inpipe.Insert(&temp);
		count++;

	}
	// cout<<"Added "<<count<<" records to inpipe"<<endl;
	inpipe.ShutDown();
	W.WaitUntilDone ();
    ASSERT_EQ(1500,count);		
	
}

TEST(RelopTest, SFTest){
    Schema mySchema ("catalog", "supplier");
    Record lit;
    CNF cnf;
    DBFile db;
    SelectFile sf;
    Pipe op(100);
    db.Open("supplier.bin");
    struct AndList *final;	
    char * pred_str = "(s_suppkey = s_suppkey)";

    cnf.GrowFromParseTree(final,&mySchema, lit);
    // cnf.Print();
    sf.Run (db, op, cnf, lit);
	int cnt = clear_pipe (op, &mySchema, false);
	sf.WaitUntilDone ();

	// int cnt = clear_pipe (_ps, ps->schema (), true);
	// cout << "\n\n query1 returned " << cnt << " records \n";
    ASSERT_EQ(cnt,100);
	db.Close ();

}

TEST(StatisticsTest, Validation1Test1){
    Statistics s;
    char *relName[] = {"orders","customer","nation"};
	s.AddRel(relName[0],1500000);
	s.AddAtt(relName[0], "o_custkey",150000);
	s.AddRel(relName[1],150000);
	s.AddAtt(relName[1], "c_custkey",150000);
	s.AddAtt(relName[1], "c_nationkey",25);
	s.AddRel(relName[2],25);
	s.AddAtt(relName[2], "n_nationkey",25);
   	// Join the first two relations in relName
    // struct AndList *final;	
	// char *cnf = "(c_custkey = o_custkey)";
	// yy_scan_string(cnf);
	// yyparse();
    // s.Apply(final, relName, 2);
    //simulate join of orders and customer

    int o_pid = s.relToId.at(relName[0]); 
    int c_pid = s.relToId.at(relName[1]);

    s.relToId.at(relName[1]) = o_pid;
    s.idToRel.at(o_pid).insert(s.idToRel.at(o_pid).end(), s.idToRel.at(c_pid).begin(), s.idToRel.at(c_pid).end());
    s.idToRel.erase(c_pid);
    for(auto it = s.idToRel.begin(); it != s.idToRel.end(); it++) {
        for(auto it2 = it->second.begin(); it2 != it->second.end(); it2++)
            cout << it->first << "  "<<(*it2) <<endl;
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    // ::testing::GTEST_FLAG(filter) = "AddSuccessSorted*";
    return RUN_ALL_TESTS();
}
