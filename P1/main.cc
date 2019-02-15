#include "test.h"
#include "BigQ.h"
#include <pthread.h>
#include <unistd.h>

void *producer (void *arg) {

	Pipe *myPipe = (Pipe *) arg;

	Record inprec;
	int counter = 0;

	DBFile dbfile;
	dbfile.Open (rel->path ());
	cout << " producer: opened DBFile " << rel->path () << endl;
	dbfile.MoveFirst ();

	while (dbfile.GetNext (inprec) == 1) {
		counter += 1;
		if (counter%100000 == 0) {
			 cerr << " producer: " << counter << endl;	
		}
		myPipe->Insert (&inprec);
	}

	dbfile.Close ();
	myPipe->ShutDown ();

	cout << " producer: inserted " << counter << " recs into the pipe\n";
}

void *consumer (void *arg) {
	
	testutil *t = (testutil *) arg;

	ComparisonEngine ceng;

	DBFile dbfile;
	char outfile[100];

	if (t->write) {
		sprintf (outfile, "%s.bigq", rel->path ());
		dbfile.Create (outfile, heap, NULL);
	}

	int err = 0;
	int i = 0;

	Record rec[2];
	Record *last = NULL, *prev = NULL;

	while (t->pipe->Remove (&rec[i%2])) {
		prev = last;
		last = &rec[i%2];

		if (prev && last) {
			if (ceng.Compare (prev, last, t->order) == 1) {
				err++;
			}
			if (t->write) {
				dbfile.Add (*prev);
			}
		}
		if (t->print) {
			last->Print (rel->schema ());
		}
		i++;
	}

	cout << " consumer: removed " << i << " recs from the pipe\n";

	if (t->write) {
		if (last) {
			dbfile.Add (*last);
		}
		cerr << " consumer: recs removed written out as heap DBFile at " << outfile << endl;
		dbfile.Close ();
	}
	cerr << " consumer: " << (i - err) << " recs out of " << i << " recs in sorted order \n";
	if (err) {
		cerr << " consumer: " <<  err << " recs failed sorted order test \n" << endl;
	}
}

int main () {

	// sort order for records
	OrderMaker sortorder;

	cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
	if (yyparse() != 0) {
		cout << "Can't parse your sort CNF.\n";
		exit (1);
	}
	cout << " \n";
	Record literal;
	CNF sort_pred;
	Schema mySchema ("catalog", "nation");
	sort_pred.GrowFromParseTree (final, &mySchema, literal); // constructs CNF predicate
	OrderMaker dummy;
	sort_pred.GetSortOrders (sortorder, dummy);
	int runlen =10;
	int option = 1;
	int buffsz = 100; // pipe cache size
	Pipe input (buffsz);
	Pipe output (buffsz);

	// thread to dump data into the input pipe (for BigQ's consumption)
	// pthread_t thread1;
	// pthread_create (&thread1, NULL, producer, (void *)&input);
	Record inprec;
	int counter = 0;

	DBFile dbfile;
	dbfile.Open ("nation.bin");
	
	dbfile.MoveFirst ();

	while (dbfile.GetNext(inprec) == 1) {
		counter += 1;
		if (counter%100000 == 0) {
			 cerr << " producer: " << counter << endl;	
		}
		input.Insert (&inprec);
	}
	dbfile.Close ();
	input.ShutDown ();

	cout << " producer: inserted " << counter << " recs into the pipe\n";
	// thread to read sorted data from output pipe (dumped by BigQ)
	// pthread_t thread2;
	// testutil tutil = {&output, &sortorder, false, false};
	// if (option == 2) {
	// 	tutil.print = true;
	// }
	// else if (option == 3) {
	// 	tutil.write = true;
	// }
	// pthread_create (&thread2, NULL, consumer, (void *)&tutil);
	cout<<"Debug log main start"<<endl;
	cout << &input<< endl;
	cout << &output<< endl;
	cout << &sortorder<< endl;
	cout << runlen<< endl;
	cout<<"Debug log main end"<<endl;
	BigQ bq (input, output, sortorder, runlen);

	// pthread_join (thread1, NULL);
	// pthread_join (thread2, NULL);
	sleep(30);
	return 0;
}
