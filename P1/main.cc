#include "test.h"
#include "BigQ.h"
#include <pthread.h>
#include <unistd.h>
#include "Record.h"
#include <vector>
#include <algorithm>    

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
	DBFile dbfile_test;
	Schema mySchema ("catalog", "lineitem");
	// cout << " DBFile will be created at "<< endl;
	dbfile_test.Open ("test_phase2.bin");
	// dbfile_test.Create("test_phase2.bin",heap,NULL);
	Record inprec;
	int counter = 0;
	
	DBFile dbfile;
	dbfile.Open ("lineitem.bin");
	
	dbfile.MoveFirst ();

	while (dbfile.GetNext(inprec) == 1) {
		counter += 1;
		cout<< "inside populate loop "<< counter<<endl;
		if(counter == 2000){
			break;
		}
		dbfile_test.Add(inprec);
	}
	cout<< "outside loop "<<endl;
	dbfile.Close ();
	dbfile_test.Close();


	DBFile dbfile2;
	// Schema mySchema ("catalog", "lineitem");
	dbfile2.Open ("test_phase2.bin");
	cout<< "Length of the file is"<<endl;
	
	// while (dbfile.GetNext(inprec) == 1) {
	// 	counter += 1;
	// 	cout<< "inside populate loop "<< counter<<endl;
	// 	// if(counter == 2000){
	// 	// 	break;
	// 	// }
	// 	inprec.Print(&mySchema);
	// }
	cout<< dbfile2.file_instance.GetLength()<<endl;
	dbfile2.Close ();


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
	dbfile_test.file_instance.GetPage(&test_page, 4);
	// while (dbfile_test.GetNext(inprec) == 1) {
	// 	counter += 1;
	// 	cout<< "inside populate loop "<< counter<<endl;
	// 	if(counter == 2000){
	// 		break;
	// 	}
	// 	inprec.Print(&mySchema)
	// }
	cout<< "outside loop "<<endl;
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
	
	
	file_inst_li.GetPage(&page90_li, 10);
	file_inst.GetPage(&page90, 0);
	cout << "Printing page 10 onwards"<<endl;
	
	cout << "********"<<endl;
	int count =0;
	while(count <10){
		
		page90.GetFirst(&tempRec);
		tempRec.Print(&mySchema);
		cout << "----------"<<endl;
		count++;
	}
	
	// file_inst.GetPage(&page90, 21);
	// cout << "Printing page 21 onwards"<<endl;
	// count =0;
	// while(count <10){
	// 	page90.GetFirst(&tempRec);
	// 	tempRec.Print(&mySchema);
	// 	cout << "----------"<<endl;
	// 	count++;
	// }
	
	tempRec.Print(&mySchema);
	
	
	cout << "Printing page 90"<<endl;
	// // tempRec.Print(&mySchema);
	// file_inst.GetPage(&page99, 97);
	// page99.GetFirst(&tempRec);
	// cout << "Printing page 99"<<endl;
	// tempRec.Print(&mySchema);
}
int main(){
	// test_phase2();
	test_check_duplicates();
	return 0;
}