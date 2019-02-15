#include "test.h"
#include "BigQ.h"
#include <pthread.h>
#include <unistd.h>
#include "Record.h"
#include <vector>
#include <algorithm>    

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

void merge(Record arr [], int l, int m, int r, OrderMaker sort_order) 
{ 
    int i, j, k; 
    int n1 = m - l + 1; 
    int n2 =  r - m; 
	int val=0;
	ComparisonEngine ceng;
    /* create temp arrays */
    Record L[n1], R[n2]; 
	Schema mySchema ("catalog", "nation");
    /* Copy data to temp arrays L[] and R[] */
	// cout<<"L i initial state"<<endl;
    for (i = 0; i < n1; i++) 
        L[i] = arr[l + i]; 
		// arr[l + i].Print(&mySchema);
	// cout<<"R j initial state"<<endl;
    for (j = 0; j < n2; j++) 
        R[j] = arr[m + 1+ j]; 
		// arr[m + 1+ j].Print(&mySchema);
	
    /* Merge the temp arrays back into arr[l..r]*/
    i = 0; // Initial index of first subarray 
    j = 0; // Initial index of second subarray 
    k = l; // Initial index of merged subarray 
    while (i < n1 && j < n2) 
    { 
		val = ceng.Compare (&L[i], &R[j], &sort_order);
		
        if (val != 1)
        { 
            arr[k] = L[i]; 
            i++; 
        } 
        else
        { 
            arr[k] = R[j]; 
            j++; 
        } 
		k++; 

    }

	
	
  
    /* Copy the remaining elements of L[], if there 
       are any */
    while (i < n1) 
    { 
        arr[k] = L[i]; 
        i++; 
        k++; 
    } 
  
    /* Copy the remaining elements of R[], if there 
       are any */
    while (j < n2) 
    { 
        arr[k] = R[j]; 
        j++; 
        k++; 
    } 
	// for(int i =0; i < 25;i++){
	// 	arr[i].Print(&mySchema);
	// }
	// cout<< "**********************************" << endl;
} 
  
/* l is for left index and r is right index of the 
   sub-array of arr to be sorted */
void mergeSort(Record arr[], int l, int r, OrderMaker sort_order) 
{ 
    if (l < r) 
    { 
        // Same as (l+r)/2, but avoids overflow for 
        // large l and h 
        int m = l+(r-l)/2; 
  
        // Sort first and second halves 
        mergeSort(arr, l, m, sort_order); 
        mergeSort(arr, m+1, r, sort_order); 
  
        merge(arr, l, m, r, sort_order); 
    } 
}

  
/* Driver program to test above functions */
//Function to test the mergesort function
// int main() 
// { 
// 	Schema mySchema ("catalog", "nation");
// 	OrderMaker sortorder(&mySchema);
// 	cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";

// 	vector<Record> vec_arr; 

// 	DBFile dbfile;
// 	dbfile.Open ("nation.bin");
	
// 	dbfile.MoveFirst ();
// 	int counter = 0;
// 	Record inprec;
// 	while (dbfile.GetNext(inprec) == 1) {
// 		counter += 1;
// 		vec_arr.push_back(inprec);
// 	}
// 	dbfile.Close ();

//     int arr_size = vec_arr.size(); 
// 	reverse(vec_arr.begin(),vec_arr.end());
// 	Record arr[arr_size];
//     mergeSort(arr, 0, arr_size - 1, sortorder); 
//     return 0; 
// } 

int main () {

	// sort order for records
	OrderMaker sortorder(&mySchema);
	
	// Logic to get sort order input from the user
	// Confused about what happens here as of now
	// cout << "\n specify sort ordering (when done press ctrl-D):\n\t ";
	// if (yyparse() != 0) {
	// 	cout << "Can't parse your sort CNF.\n";
	// 	exit (1);
	// }
	// cout << " \n";
	// Record literal;
	// CNF sort_pred;
	// Schema mySchema ("catalog", "nation");
	// sort_pred.GrowFromParseTree (final, &mySchema, literal); // constructs CNF predicate
	// OrderMaker dummy;
	// sort_pred.GetSortOrders (sortorder, dummy);
	// int runlen =10;
	// int option = 1;
	// int buffsz = 100; // pipe cache size
	// Pipe input (buffsz);
	// Pipe output (buffsz);

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

	BigQ bq (input, output, sortorder, runlen);

	// pthread_join (thread1, NULL);
	// pthread_join (thread2, NULL);
	return 0;
}
