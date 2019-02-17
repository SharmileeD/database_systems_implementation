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
struct worker_data{
	Pipe *in_pipe;
	Pipe *out_pipe;
	OrderMaker *sort_order;
	int run_length;
};

struct worker_data input;

struct record_container {
	Record rec;
	int run;
};

struct CompareRecords {
	bool operator()(record_container r1, record_container r2) {
		ComparisonEngine ceng;
		int val = ceng.Compare (&r1.rec, &r2.rec, input.sort_order);
		if(val != 1)
			return false;
		return true;
	} 

};
void phase2tpmms_test(struct worker_data *input_args, int numRuns);
// void *producer (void *arg) {

// 	Pipe *myPipe = (Pipe *) arg;

// 	Record inprec;
// 	int counter = 0;

// 	DBFile dbfile;
// 	dbfile.Open (rel->path ());
// 	cout << " producer: opened DBFile " << rel->path () << endl;
// 	dbfile.MoveFirst ();

// 	while (dbfile.GetNext (inprec) == 1) {
// 		counter += 1;
// 		if (counter%100000 == 0) {
// 			 cerr << " producer: " << counter << endl;	
// 		}
// 		myPipe->Insert (&inprec);
// 	}

// 	dbfile.Close ();
// 	myPipe->ShutDown ();

// 	cout << " producer: inserted " << counter << " recs into the pipe\n";
// }

// void *consumer (void *arg) {
	
// 	testutil *t = (testutil *) arg;

// 	ComparisonEngine ceng;

// 	DBFile dbfile;
// 	char outfile[100];

// 	if (t->write) {
// 		sprintf (outfile, "%s.bigq", rel->path ());
// 		dbfile.Create (outfile, heap, NULL);
// 	}

// 	int err = 0;
// 	int i = 0;

// 	Record rec[2];
// 	Record *last = NULL, *prev = NULL;

// 	while (t->pipe->Remove (&rec[i%2])) {
// 		prev = last;
// 		last = &rec[i%2];

// 		if (prev && last) {
// 			if (ceng.Compare (prev, last, t->order) == 1) {
// 				err++;
// 			}
// 			if (t->write) {
// 				dbfile.Add (*prev);
// 			}
// 		}
// 		if (t->print) {
// 			last->Print (rel->schema ());
// 		}
// 		i++;
// 	}

// 	cout << " consumer: removed " << i << " recs from the pipe\n";

// 	if (t->write) {
// 		if (last) {
// 			dbfile.Add (*last);
// 		}
// 		cerr << " consumer: recs removed written out as heap DBFile at " << outfile << endl;
// 		dbfile.Close ();
// 	}
// 	cerr << " consumer: " << (i - err) << " recs out of " << i << " recs in sorted order \n";
// 	if (err) {
// 		cerr << " consumer: " <<  err << " recs failed sorted order test \n" << endl;
// 	}
// }





// int main () {

// 	// sort order for records
// 	Schema mySchema ("catalog", "nation");
// 	OrderMaker sortorder(&mySchema);
// 	cout<< "Started the call to main"<<endl;
// 	// Logic to get sort order input from the user
// 	// Confused about what happens here as of now
// 	// cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
// 	// if (yyparse() != 0) {
// 	// 	cout << "Can't parse your sort CNF.\n";
// 	// 	exit (1);
// 	// }
// 	// cout << " \n";
// 	// Record literal;
// 	// CNF sort_pred;
// 	// Schema mySchema ("catalog", "nation");
// 	// sort_pred.GrowFromParseTree (final, &mySchema, literal); // constructs CNF predicate
// 	// OrderMaker dummy;
// 	// sort_pred.GetSortOrders (sortorder, dummy);
// 	int runlen =10;
// 	// int option = 1;
// 	int buffsz = 100; // pipe cache size
// 	Pipe input (buffsz);
// 	Pipe output (buffsz);

// 	// thread to dump data into the input pipe (for BigQ's consumption)
// 	// pthread_t thread1;
// 	// pthread_create (&thread1, NULL, producer, (void *)&input);
// 	Record inprec;
// 	int counter = 0;

// 	DBFile dbfile;
// 	dbfile.Open ("nation.bin");
	
// 	dbfile.MoveFirst ();

// 	while (dbfile.GetNext(inprec) == 1) {
// 		counter += 1;
// 		cout<< "inside populate loop "<< counter<<endl;
// 		if(counter == 1000){
// 			break;
// 		}
// 		if (counter%100000 == 0) {
// 			 cerr << " producer: " << counter << endl;	
// 		}
// 		input.Insert (&inprec);
// 	}
// 	cout<< "outside loop "<<endl;
// 	dbfile.Close ();
// 	input.ShutDown ();

// 	cout << " producer: inserted " << counter << " recs into the pipe\n";
// 	// thread to read sorted data from output pipe (dumped by BigQ)
// 	pthread_t thread2;
// 	testutil tutil = {&output, &sortorder, false, false};
// 	// if (option == 2) {
// 	// 	tutil.print = true;
// 	// }
// 	// else if (option == 3) {
// 	// 	tutil.write = true;
// 	// }
// 	pthread_create (&thread2, NULL, consumer, (void *)&tutil);

// 	BigQ bq (input, output, sortorder, runlen);

// 	// pthread_join (thread1, NULL);
// 	pthread_join (thread2, NULL);
// 	return 0;
// }

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
	input.run_length = 10;
	Schema mySchema ("catalog", "lineitem");
	OrderMaker sortorder(&mySchema);
	input.sort_order = &sortorder;
	phase2tpmms_test(&input, 10);

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
	DBFile dbfile_test;
	// cout << " DBFile will be created at "<< endl;
	dbfile_test.Open (f_path);
	// dbfile_test.Create("test_phase2.bin",heap,NULL);
	Record inprec;
	int counter = 0;
	Page test_page;
	dbfile_test.MoveFirst ();
	Schema mySchema ("catalog", "lineitem");

	while (dbfile_test.GetNext(inprec) == 1) {
		
		counter += 1;
		
		if(counter % 5000 ==0){
			cout<< "inside populate loop "<< counter<<endl;
		}
		// inprec.Print(&mySchema);
	}
	cout<< f_path <<endl;
	cout<< "Number of records is "<< counter <<endl;
	dbfile_test.Close();
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
	// while(count <10){
		
	// 	page90.GetFirst(&tempRec);
	// 	tempRec.Print(&mySchema);
	// 	cout << "----------"<<endl;
	// 	count++;
	// }
	
	// file_inst.GetPage(&page90, 21);
	// cout << "Printing page 21 onwards"<<endl;
	// count =0;
	// while(count <10){
	// 	page90.GetFirst(&tempRec);
	// 	tempRec.Print(&mySchema);
	// 	cout << "----------"<<endl;
	// 	count++;
	// }
	
	// tempRec.Print(&mySchema);
	
	
	// cout << "Printing page 90"<<endl;
	// // tempRec.Print(&mySchema);
	// file_inst.GetPage(&page99, 97);
	// page99.GetFirst(&tempRec);
	// cout << "Printing page 99"<<endl;
	// tempRec.Print(&mySchema);
}

void phase2tpmms_test(struct worker_data *input_args, int numRuns) {
	
	Page runPage[numRuns];
	int currPage[numRuns] = {0};
	int run_index = 0;
	int get_first = 0;
	int load_more = 0;
	int runLength = input_args->run_length;
	struct record_container to_push;
	struct record_container que[numRuns];
	priority_queue<record_container, vector<record_container>, CompareRecords> recQ;
	struct record_container temp;
	struct record_container new_elemnt;
	Schema mySchema ("catalog", "lineitem"); 	
	int pgCountLastRun = 8;
	DBFile file;
	file.Open("runs.bin");
	//Get page from every run
	
	
	
	//First time load the runPage array with the first page of each run
	for(int i =0 ; i < numRuns;i++) {
		
		file.file_instance.GetPage(&runPage[i], i*runLength);
		runPage[i].GetFirst(&que[i].rec);

		que[i].run = i;
		// que[i].rec.Print(&mySchema);
		recQ.push(que[i]);

	}
	int count = 0;
	int count3=0;
	int count2=0;
	int count1=0;
	while(recQ.size()!=0){
		
		if (recQ.size()==3){
			count3++;
		}
		if (recQ.size()==2){
			count2++;
		}
		if (recQ.size()==1){
			count1++;
		}
		//Step 1: Getting the first record(smallest) of the priority queue
		temp = recQ.top();
		run_index = temp.run;
		// input_args->out_pipe->Insert(&temp.rec);
		count++;
		temp.rec.Print(&mySchema);
		//cout << "Smallest record : " << endl;
		recQ.pop();

		//Now that we have poped a struct out of the priority queue
		//we need to push one struct in from the same page 
		//Step 2: Getting the next record from the same page which recently served the record
		get_first = runPage[run_index].GetFirst(&to_push.rec);
		//Step 2.0: This is the case where we are checking if the PAGE has any RECORDS
		//Step 2.1: This is the case where the PAGE does not have any RECORDS
		if (get_first ==0){
			//Need to load next page from run_index
			//Step 2.1.1: This is the case where the RUN is OUT OF PAGES
			int limit;
			if(run_index < numRuns - 1)
				limit = runLength -1;
			else
				limit = pgCountLastRun - 1;
			
			if(currPage[run_index]==limit){
				currPage[run_index] = -1;
				continue;
			}
			//Step 2.1.2: This is the case where the RUN has a page so getting it and incrementing currPage[run_index] 
			else
			{
				currPage[run_index] = currPage[run_index] + 1;
				file.file_instance.GetPage(&runPage[run_index], (run_index*runLength)+currPage[run_index]);
				int temp_getf=0;
				temp_getf = runPage[run_index].GetFirst(&new_elemnt.rec);
				new_elemnt.run= run_index;
				// new_elemnt.rec.Print(&mySchema);
				recQ.push(new_elemnt);
			}
			

		}
		//Step 2.2: This is the case where the PAGE has a next RECORD for you
		else if (get_first ==1)
		{
			// The current page still has some records so just getting the first record from that page
			to_push.run= run_index;
			// new_elemnt.rec.Print(&mySchema);
			recQ.push(to_push);
		}
	}
	
	cout<<"Number of records considered "<< count<<endl;
	cout<<"Number of 3 records considered "<< count3<<endl;

	cout<<"Number of 2 records considered "<< count2<<endl;
	cout<<"Number of 1 records considered "<< count1<<endl;
	for(int i =0;i<numRuns;i++){
		cout<<currPage[i]<<endl;
	}
	//temp = recQ.top();
	
	file.Close();

}

int main(){
	test_phase2();
	// test_check_duplicates();
	// test_write();
	// check_num_records("runs.bin");
	// check_num_records("lineitem.bin");
	return 0;
}