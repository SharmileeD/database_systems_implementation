#include "BigQ.h"
#include "Record.h"
#include "Schema.h"
#include "Pipe.h"
#include "File.h"
#include "pthread.h"
#include "DBFile.h"
#include <unistd.h>
#include <vector>
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

struct QueueNode {
	Record rec;
	int run;
};

struct CompareRecords {
	bool operator()(Record r1, Record r2) {
		ComparisonEngine ceng;
		int val = ceng.Compare (&r1, &r2, input.sort_order);
		if(val != 1)
			return false;
		return true;
	} 

};

void mergeSort(Record arr[], int l, int r, OrderMaker sort_order); 

void phase2tpmms(struct worker_data *input_args, int numRuns) {
	
	Page runPage[numRuns];
	int currPage[numRuns] = {0};

	priority_queue<Record, vector<Record>, CompareRecords> recQ;
	Record temp;
	Schema mySchema ("catalog", "lineitem"); 	

	DBFile file;
	file.Open("runFile.txt");
	cout << "File len: " << file.file_instance.GetLength() << endl;
	//Get page from every run
	int i =0;
	//for(int i =0 ; i < numRuns; i++) {
	//	file.GetPage(&runPage[i], currPage[i]);
	//	runPage->GetFirst(&temp);
	//	recQ.push(temp);
	//}

	//temp = recQ.top();
	//cout << "Smallest record : " << endl;
	temp.Print(&mySchema);
	file.Close();

}

void createRun(vector<Record> vec_arr, OrderMaker sort_order) {
	
	int ind =0;
	Schema mySchema ("catalog", "lineitem");
	off_t page_num;
	int arr_size = vec_arr.size();
	Record arr[arr_size];
	for(int i =0; i < arr_size; i++)
		arr[i] = vec_arr[i];
	DBFile dbfile;
	dbfile.Open("runs.bin"); 
	dbfile.buffer_page.EmptyItOut();
	//sort the run
	mergeSort(arr,0,arr_size-1,sort_order);
	

	for(int j=0; j<arr_size;j++){
		dbfile.Add(arr[j]);
	}
	cout << "Added records"<<endl;
	dbfile.Close();
	
}

void *sort_tpmms (void *arg) {
	struct worker_data * input_args;
	input_args = (struct worker_data *)arg;	
	
	int pageCount = 0;
	int numRuns = 0;
	bool writeRun = false;

	Schema mySchema ("catalog", "lineitem"); 
	
	Record * tempRec;
	Record outrec;
	tempRec = &outrec;
	fType fileType = heap;
	DBFile runFile;

	cout << "Creating file" << endl;
	
	runFile.Create("runs.bin", fileType, NULL);
	runFile.Close();
	
	
	Page dummy;
 	vector<Record> vec_arr; 

	while (input_args->in_pipe->Remove(tempRec)) {
		
		writeRun = false;
		vec_arr.push_back(*tempRec);
		if(dummy.Append(tempRec)!= 1) {
			//If page is full
			pageCount ++;

			if(pageCount == input_args->run_length) {
			
				//create a run

				numRuns++;
				createRun(vec_arr, *input_args->sort_order);
				writeRun = true;
				cout << "Created run: " << numRuns << endl;
				pageCount = 0;
				vec_arr.clear();
			}
			dummy.EmptyItOut();	
		}
 		
      //  input_args->out_pipe->Insert(tempRec);
 	}
	//write last run to file
	if(!writeRun) {
			numRuns++;
			createRun(vec_arr, *input_args->sort_order);
			pageCount = 0;
			vec_arr.clear();
			cout << "Created last run: " << numRuns << endl;
	}
	
//	phase2tpmms(input_args, numRuns);
	
	input_args->out_pipe->ShutDown();
 	cout<< "Exiting the worker thread"<<endl;
	pthread_exit(NULL);
	
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
	Schema mySchema ("catalog", "lineitem");
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
} 
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

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages
	cout << "BigQ: Start"<<endl;
	// storing address of the reference of in pipe coming in to the BigQ in the struct in_pipe variable
	input.in_pipe = &in; 
	input.out_pipe = &out;
	input.sort_order = &sortorder;
	input.run_length = runlen;

	
    // // construct priority queue over sorted runs and dump sorted data 
 	// // into the out pipe
	pthread_t worker;
	pthread_attr_t attr;
	pthread_attr_init(&attr);
    int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_DETACHED);
	if (det){
		cout<<"Some issue with setting detached"<<endl;
	}
	else{
		cout<<"Thread attr set to detached"<<endl;
	}
	pthread_create (&worker, &attr, sort_tpmms, (void*) &input);

    // // finally shut down the out pipe
}

BigQ::~BigQ () {
}
