#include "test.h"
#include "BigQ.h"
#include <pthread.h>
#include <unistd.h>
#include "Record.h"
#include <vector>
#include <algorithm>    
#include "Schema.h"
#include "Pipe.h"
#include "File.h"
#include "pthread.h"
#include "DBFile.h"
#include <array>
#include <iostream>
#include <queue>
// struct worker_data{
// 	Pipe *in_pipe;
// 	Pipe *out_pipe;
// 	OrderMaker *sort_order;
// 	int run_length;
// };

// struct worker_data input;

struct record_container {
	Record rec;
	int run;
};

// struct CompareRecords {
// 	bool operator()(record_container r1, record_container r2) {
// 		ComparisonEngine ceng;
// 		int val = ceng.Compare (&r1.rec, &r2.rec, input.sort_order);
// 		if(val != 1)
// 			return false;
// 		return true;
// 	} 

// };

// void setup () {
// 	cout << " \n** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **\n";
// 	cout << " catalog location: \t" << catalog_path << endl;
// 	cout << " tpch files dir: \t" << tpch_dir << endl;
// 	cout << " heap files dir: \t" << dbfile_dir << endl;
// 	cout << " \n\n";

// 	s = new relation (supplier, new Schema (catalog_path, supplier), dbfile_dir);
// 	ps = new relation (partsupp, new Schema (catalog_path, partsupp), dbfile_dir);
// 	p = new relation (part, new Schema (catalog_path, part), dbfile_dir);
// 	n = new relation (nation, new Schema (catalog_path, nation), dbfile_dir);
// 	li = new relation (lineitem, new Schema (catalog_path, lineitem), dbfile_dir);
// 	r = new relation (region, new Schema (catalog_path, region), dbfile_dir);
// 	o = new relation (orders, new Schema (catalog_path, orders), dbfile_dir);
// 	c = new relation (customer, new Schema (catalog_path, customer), dbfile_dir);
// }

void phase2tpmms_test(struct worker_data *input_args, int numRuns);

void test_phase2(){
	// This logic writes three page worth of records to a file named test_phase2.bin
	
	// DBFile dbfile_test;
	// Schema mySchema ("catalog", "lineitem");
	// // cout << " DBFile will be created at "<< endl;
	// // dbfile_test.Open ("test_phase2.bin");
	// dbfile_test.Create("test_phase2.bin",heap,NULL);
	// Record inprec;
	// int counter = 0;
	
	// DBFile dbfile;
	// dbfile.Open ("lineitem.bin");
	
	// dbfile.MoveFirst ();

	// while (dbfile.GetNext(inprec) == 1) {
	// 	counter += 1;
	// 	cout<< "inside populate loop "<< counter<<endl;
	// 	if(counter == 2000){
	// 		break;
	// 	}
	// 	dbfile_test.Add(inprec);
	// }
	// cout<< "outside loop "<<endl;
	// dbfile.Close ();
	// dbfile_test.Close();

	//This opens the file and checks the length of the file

	// DBFile dbfile2;
	// // Schema mySchema ("catalog", "lineitem");
	// dbfile2.Open ("test_phase2.bin");
	// cout<< "Length of the file is"<<endl;
	
	// // while (dbfile.GetNext(inprec) == 1) {
	// // 	counter += 1;
	// // 	cout<< "inside populate loop "<< counter<<endl;
	// // 	// if(counter == 2000){
	// // 	// 	break;
	// // 	// }
	// // 	inprec.Print(&mySchema);
	// // }
	// cout<< dbfile2.file_instance.GetLength()<<endl;
	// dbfile2.Close ();
	// input.run_length = 9;
	// Schema mySchema ("catalog", "lineitem");
	// OrderMaker sortorder(&mySchema);
	// input.sort_order = &sortorder;
	// phase2tpmms_test(&input, 9);

}	

void test_write(){
	DBFile dbfile_test;
	// cout << " DBFile will be created at "<< endl;
	dbfile_test.Open ("test_phase2.bin");
	// dbfile_test.Create("test_phase2.bin",heap,NULL);
	Record inprec;
	int counter = 0;
	Page test_page;
	dbfile_test.MoveFirst ();
	Schema mySchema ("catalog", "lineitem");

	while (dbfile_test.GetNext(inprec) == 1) {
		counter += 1;
		cout<< "inside populate loop "<< counter<<endl;
		if(counter == 2000){
			break;
		}
		// inprec.Print(&mySchema);
	}
	cout<< "outside loop "<< counter <<endl;
	dbfile_test.Close();
}

void check_num_records(char f_path[]){
	Heap dbfile_test;
	// cout << " DBFile will be created at "<< endl;
	dbfile_test.Open (f_path);
	// dbfile_test.Create("test_phase2.bin",heap,NULL);
	Record inprec;
	int counter = 0;
	Page test_page;
	dbfile_test.MoveFirst ();
	Schema mySchema ("catalog", "customer");

	while (dbfile_test.GetNext(inprec) == 1) {
		
		counter += 1;
		
		if(counter % 5000 ==0){
			cout<< "inside populate loop "<< counter<<endl;
		}
		inprec.Print(&mySchema);
	}
	cout<< f_path <<endl;
	cout<< "Number of records is "<< counter <<endl;
	dbfile_test.Close();
}

void check_recs(char f_path[]) {
	Heap dbfile_test;
	// cout << " DBFile will be created at "<< endl;
	dbfile_test.Open (f_path);
	// dbfile_test.Create("test_phase2.bin",heap,NULL);
	Record inprec;
	int counter = 0;
	int tot_counter = 0;
	Page test_page;
	
	for(int pages = 0; pages < 97; pages++) {
		dbfile_test.file_instance.GetPage(&test_page, pages);
		while(test_page.GetFirst(&inprec))
			counter++;
		tot_counter = tot_counter + counter;
		// cout << "Page num: " << pages+1 << "      Records: " << counter << endl;	
		counter = 0;
	}
	cout << "Total: " << tot_counter << endl;

}

void test_check_duplicates(){
	Page page90_li;
	Page page90;
	Page page99;
	Page page105;
	Record tempRec;
	File file_inst_li;
	File file_inst;
	Schema mySchema ("catalog", "lineitem");
	file_inst_li.Open(1, "lineitem.bin");
	file_inst.Open(1, "runs.bin");
	
	cout << "Trying to get page 99 of LI"<<endl;
	file_inst_li.GetPage(&page90_li, 97);
	cout << "Trying to get page 99 of runs"<<endl;
	file_inst.GetPage(&page90, 97);
	cout << "Printing page 10 onwards"<<endl;
	
	cout << "********"<<endl;
	int count =0;

}


void test_getLength(){
	// DBFile dbfile_test;
	Heap dbfile;
	Heap dbfile2;
	// dbfile_test.Create("runs.bin",heap,NULL);
	// cout<<"Length is "<< dbfile_test.file_instance.GetLength()<<endl;
	dbfile.Open("aux_file.bin");
	cout<<"Length is "<< dbfile.file_instance.GetLength()<<endl;
	dbfile2.Open("customer.bin");
	cout<<"Length is "<< dbfile2.file_instance.GetLength()<<endl;
}

void test_read_write_logic(){   
    // char metadata_file_name [] = "l_page.txt";
    // cout << "Hello world!" << endl;
    // FILE * fp;
    // const char * ip;
    // ip = metadata_file_name;
    // off_t big_var = 190791;
    // fp = fopen(ip, "w");
    // fwrite(&big_var, sizeof(off_t), 1, fp);
    // fclose(fp);

    FILE * fp2;
    off_t target = 0;
    fp2 = fopen("database_systems_implementation/P1/runs_lpage.txt", "r");
    fread(&target, sizeof(off_t),1, fp2);
    cout << "Off t variable incremented" << endl;
    cout<< target<<endl;
    fclose(fp2);
    string test = "hello";
    string test2 = "world";
    cout << test+test2<< endl;

}
void test_DBFile_create(){
	DBFile dbfile;
	Heap hp;
	Record tempRec;
	// dbfile.Create("test_phase2.bin",heap,NULL);
	Schema mySchema ("catalog", "customer");
	OrderMaker sortorder(&mySchema);
	int runlen = 2;
	struct {OrderMaker *o; int l;} startup = {&sortorder, runlen};
	dbfile.Create("test_phase2.bin",sorted,&startup);
	// dbfile.Close();
	hp.Open("test_phase2.bin");

	int va = hp.file_instance.GetLength();
	int v = dbfile.instVar->GetNext(tempRec);
	cout<< "length "<<v<<endl;
	dbfile.Close();
	// int ret = dbfile.Open("test_phase2.bin");
	//cout<<ret<<endl;
}
void test_GetPage(){
	DBFile file;
	file.Open("runs.bin");
	Page test;
	file.instVar->GetPage(&test, 0);
	cout<<"testing end"<<endl;
}

void test_sorted_add(){
	DBFile dbfile_test;
	Schema mySchema ("catalog", "customer");
	// cout << " DBFile will be created at "<< endl;
	// dbfile_test.Open ("test_phase2.bin");
	dbfile_test.Open("test_phase2.bin");
	Record inprec;
	int counter = 0;
	
	Heap hpfile;
	hpfile.Open ("customer.bin");
	
	hpfile.MoveFirst ();

	while (hpfile.GetNext(inprec) == 1) {
		counter += 1;
		cout<< "inside populate loop "<< counter<<endl;
		
		dbfile_test.Add(inprec);
		// inprec.Print(&mySchema);
	}
	counter = 0;
	// while (hpfile.GetNext(inprec) == 1) {
	// 	counter += 1;
	// 	cout<< "inside populate loop "<< counter<<endl;
	// 	if(counter == 1000){
	// 		break;
	// 	}
	// 	dbfile_test.Add(inprec);
	// 	// inprec.Print(&mySchema);
	// }
	cout<< "outside loop "<<endl;
	dbfile_test.instVar->mergePipeAndFile();
	// counter = 0;
	// while (hpfile.GetNext(inprec) == 1) {
	// 	counter += 1;
	// 	cout<< "inside populate loop "<< counter<<endl;
	// 	if(counter == 1000){
	// 		break;
	// 	}
	// 	dbfile_test.Add(inprec);
	// 	// inprec.Print(&mySchema);
	// }
	// counter = 0;
	// while (hpfile.GetNext(inprec) == 1) {
	// 	counter += 1;
	// 	cout<< "inside populate loop "<< counter<<endl;
	// 	if(counter == 1000){
	// 		break;
	// 	}
	// 	dbfile_test.Add(inprec);
	// 	// inprec.Print(&mySchema);
	// }
	// dbfile_test.instVar->mergePipeAndFile();
	cout<< "outside loop "<<endl;
	hpfile.Close ();
	// dbfile_test.Close();
	
}
void test_sorted_load(){
	DBFile dbfile; 
	Schema mySchema ("catalog", "lineitem");
	OrderMaker sortorder(&mySchema);
	int runlen = 2;
	struct {OrderMaker *o; int l;} startup = {&sortorder, runlen};
	dbfile.Create("aux_file.bin",sorted,&startup);
    dbfile.Load(mySchema, "tables/lineitem.tbl");
	dbfile.instVar->mergePipeAndFile();
	dbfile.Close();

}
void test_sorted_getnext(){
	DBFile dbfile_test;
	Schema mySchema ("catalog", "lineitem");
	// cout << " DBFile will be created at "<< endl;
	dbfile_test.Open ("test_phase2.bin");
	// dbfile_test.Open("aux_file.bin");
	Record inprec;
	dbfile_test.MoveFirst ();

    while (dbfile_test.GetNext (inprec) == 1) {
		inprec.Print(&mySchema);
	}
}

void testQuery() {
        Schema lineitem ("catalog", "nation");
        Heap binfile;
		// relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};
        // rel = rel_ptr [0];

		OrderMaker om(&lineitem);
        rel->get_sort_order (om);
        Record fileRec1;
        Record fileRec2;
        int runlen = 0;
        ComparisonEngine ceng;
		
		//while (runlen < 1) {
                cout << "\t\n specify runlength:\n\t ";
                cin >> runlen;
        //}
        CNF myComparison;
        Record literal;
        myComparison.GrowFromParseTree (final, &lineitem, literal);
        cout << "Priniting comparison to screen"<<endl;
        myComparison.Print ();
        // OrderMaker o1(&lineitem);
        // cout << "Printing sort_order ORdermaker" << endl;
        // o1.Print();
        // OrderMaker query = o1.makeQuery(myComparison);
		// return query;
        // int binval = binfile.Open("nation.bin.bigq");
        // binfile.MoveFirst();
        // binfile.GetNext(fileRec1);
        // binfile.GetNext(fileRec2);
        // int val = ceng.Compare(&fileRec2, &fileRec1, &query);
        // query.Print();

        // cout <<"Val = "<<val<<endl;
}


void test_query() {
	DBFile dbfile_test;
	Schema mySchema ("catalog", "orders");
	Record temp, literal;
	CNF cnf;

	dbfile_test.Open ("test_phase2.bin");
	dbfile_test.GetNext(temp,cnf,literal);
	dbfile_test.Close();
}

int pageBinSearch(int start, int end, OrderMaker query, Record literal) {
	Heap dbfile;
	dbfile.Open("orders.bin");
	
	Record last;
	Record first;
	Page pg;

	int firstval, lastval;
	int mid = (start + end)/2;
			
	ComparisonEngine ceng;
	Schema mySchema ("catalog", "orders");

		
	while (start <= end) {
		mid = (start +  end)/2;
		dbfile.file_instance.GetPage(&pg, mid);
		
		pg.GetFirst(&first);

		dbfile.file_instance.GetPage(&pg, mid);
		if(pg.GetLast(&last)) {
			// cout << "Last record"<<endl;
			// last.Print(&mySchema);
		}
		else
		{
				cout <<"No last record"<<endl;
		}
		// literal.Print(&mySchema);
		lastval = ceng.Compare(&last, &literal, &query);
		firstval = ceng.Compare(&first, &literal, &query);
		cout <<"------------------------------------------------------------------------"<<endl;
		cout << "First record:"<<endl;
		first.Print(&mySchema);
		cout <<endl<<endl<<"Second record" <<endl;
		last.Print(&mySchema);
		cout << endl <<endl;

		//either its first or last record or anything between them, send the page
		if(lastval == 0 || firstval ==0 || (lastval > 0 && firstval < 0)) {
			
			dbfile.Close();
			return mid;
		}

		//if last record on page is smaller then check second half
		if(lastval < 0)
			start = mid+1;

		//if first record on page is bigger then check first half
		else if (firstval > 0)
			end = mid-1;		

		cout << "Start= "<<start<<"   end = "<<end<<"  mid="<<mid<<endl;
	}
	if(start > end) {
		dbfile.Close();
		return -1;
	}
	
}


void testComp() {
	Heap dbfile;
	dbfile.Open("orders.bin");
	Record literal, temp;
	int startrec=0, counter=0;
	CNF cnf;
	Page buffPage, searchPg;
	ComparisonEngine ceng;
	Schema mySchema ("catalog", "orders");
	dbfile.file_instance.GetPage(&buffPage, 1);
	bool found = false;
	vector<Record> recArr; 
	// Record recArr[10];

	OrderMaker o(&mySchema);
    rel->get_sort_order (o);
	
	cnf.GrowFromParseTree (final, &mySchema, literal);
	cout <<"CNF print:"<<endl;
	cnf.Print();
	
	OrderMaker query;
	// OrderMaker query(&mySchema) ;
	int arr[MAX_ANDS];
	arr[0] = 2;
	// arr[1] = 1;
	// arr[2] = 7;
	// arr[3] = 3;
	// arr[4] = 2;
	// arr[5] = 4;
	// arr[6] = 5;
	Type whicharr[MAX_ANDS];
	whicharr[0] = String;
	// whicharr[1] = Int;
	// whicharr[2] = Int;
	// whicharr[3] = Double;
	// whicharr[4] = String;
	// whicharr[5] = String;
	// whicharr[6] = String;
	OrderMaker o1(&mySchema);
	query = o1.makeQuery(cnf);
	// query = query.getOrderMaker(1,arr,whicharr);
	cout <<"Query ordermaker"<<endl;
	query.Print();
	int startpg = dbfile.current_page;
	int endpg = dbfile.file_instance.GetLength();
	if(endpg>0)
		endpg = endpg - 2; 
	cout << "start = " << startpg <<"  end = "<<endpg <<endl <<endl;
	
	dbfile.Close();
	int val = pageBinSearch(startpg,endpg,query,literal);
	cout << "Page value="<<val<<endl;

	dbfile.Open("orders.bin");
	dbfile.current_page = val;
	
	// dbfile.file_instance.GetPage(&buffPage, val);

	//if the page is current_page then go forward by record_offset to access records after the current record
	// if(val!=startpg){
	// 	startrec = dbfile.record_offset;
	// 	while(startrec>0) {
	// 		buffPage.GetFirst(&temp);
	// 		startrec--;
	// 	}
		
	// }

	if(val != startpg)
		dbfile.record_offset = 0;
	//Sequentially search the given page
	
	while(dbfile.GetNext(temp)) {
		int val = ceng.Compare(&temp,&literal,&query);
		//if query evaluates to true then check cnf else return 0
		// temp.Print(&mySchema);
		if(val == 0) {
			found =  true;
			// temp.Print(&mySchema);
			int val1 = ceng.Compare(&temp,&literal, &cnf);
			//if cnf evaluates to true then send it to the caller else check next record
			if(val1==1) {
				temp.Print(&mySchema);
				break;
			}
			
		}
			
	}
	// //Now that we found a maching record we seuentially scan for records that satisfy query and cnf in that order
	//
	// if(found) {
	//  		while(buffPage.GetFirst(&temp)) {
	// 			int val = ceng.Compare(&temp,&literal,&query);
	// 			if(val==0)
	// 			{
	// 				int val1 = ceng.Compare(&temp,&literal, &cnf);
	// 				if (val1 == 1) {
	// 					//cnf evaluation is true so return the record tothe caller
	// 					cout << "Match!"<<endl;
	// 					temp.Print(&mySchema);
	// 					// recArr[counter].Copy(&temp);
	// 					// counter++;
	// 				}
	// 					// recArr[counter++] = temp;
	// 			}
	//  		}
	// }


	// cout <<endl<<endl<<"Matching records:"<<endl;
	// for(int i =0; i <counter;i++)
			// recArr[i].Print(&mySchema);

}

void test_GetLast(){
	
	Heap dbfile;
	dbfile.Open("orders.bin");
	Page buffPage;
	Record rec;
	
	Schema mySchema ("catalog", "orders");
	dbfile.file_instance.GetPage(&buffPage, 0);
	if(buffPage.GetLast(&rec)) {
		cout << "Last record"<<endl;
		// rec.Print(&mySchema);
	}
	
	dbfile.Close();
	
}


int main(){
	setup();
	relation *rel_ptr[] = {n, r, c, p, ps, s, o, li};
	rel = rel_ptr [6];
	// Schema mySchema ("catalog", "lineitem");
	// OrderMaker sortorder(&mySchema);
	// int option = 1;
	// int buffsz = 100; // pipe cache size
	// Pipe output (buffsz);
	// input.out_pipe = &output;
	// input.sort_order = &sortorder;
	// // thread to dump data into the input pipe (for BigQ's consumption)

	// // thread to read sorted data from output pipe (dumped by BigQ)
	// pthread_t thread2;
	// testutil tutil = {&output, &sortorder, false, false};
	// if (option == 2) {
	// 	tutil.print = true;
	// }
	// else if (option == 3) {
	// 	tutil.write = true;
	// }
	// cout<<"Creating thread"<<endl;
	// pthread_create (&thread2, NULL, consumer, (void *)&tutil);

	// BigQ bq (input, output, sortorder, runlen);

	
	// test_phase2();
	// test_check_duplicates();
	// test_write();
	// test_getLength();
	// check_recs("aux_file.bin");
	// check_recs("lineitem.bin");
	// test_DBFile_create();
	// test_sorted_add();
	// test_getLength();
	// check_num_records("aux_file.bin");
	// check_num_records("runs.bin");
	// pthread_join (thread2, NULL);
	// test_DBFile_create();
	// test_GetPage();
	// test_DBFile_create();
	// test_sorted_add();
	// test_sorted_load();
	// test_sorted_getnext();

	// test_query();
	// testQuery();
	// test_GetLast();
	
	testComp();
	return 0;
}