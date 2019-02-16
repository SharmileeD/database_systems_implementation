#include "BigQ.h"
#include "Record.h"
#include "Schema.h"
#include "Pipe.h"
#include "File.h"
#include "pthread.h"
#include <unistd.h>
#include <vector>
#include <array>
#include <iostream>

struct worker_data{
	Pipe *in_pipe;
	Pipe *out_pipe;
	OrderMaker *sort_order;
	int run_length;
};
struct worker_data input;

void mergeSort(Record arr[], int l, int r, OrderMaker sort_order); 

void createRun(vector<Record> vec_arr, File file, struct worker_data *input_args) {
	
	Page *temp = new Page();
	int i =0;
	off_t page_num;
	int arr_size = vec_arr.size();
	Record arr[arr_size];
				
	for(int i =0; i < arr_size; i++)
		arr[i] = vec_arr[i];

	//sort the run
	mergeSort(arr,0,0,*input_args->sort_order);
	
	//write run to file
	while(i < vec_arr.size()) {
		if(temp->Append(&arr[i]) != 1)
		{
			//if page is full add page to file, flush and add new record
			page_num = file.GetLength();
			file.AddPage(temp,page_num);
			temp->EmptyItOut();
			temp->Append(&arr[i]);
		}	

		i++;
	}
	//Add the last page
	//************TO DO : Check if its empty
	file.AddPage(temp,file.GetLength());
	
}

void *sort_tpmms (void *arg) {
	struct worker_data * input_args;
	input_args = (struct worker_data *)arg;	
	int pageCount = 0;
	int numRuns = 0;
	Schema mySchema ("catalog", "lineitem"); 
	
	Record * tempRec;
	Record outrec;
	tempRec = &outrec;

	File runFile;
	runFile.Open(0, "runFile.txt");
	Page *dummy = new Page();
 	
	vector<Record> vec_arr; 

	while (input_args->in_pipe->Remove(tempRec)) {
		
		vec_arr.push_back(*tempRec);
		//Page is full
		if(dummy->Append(tempRec)!= 1) {
			
			pageCount ++;
			
			if(pageCount == input_args->run_length) {
			
				//create a run 		
				numRuns++;
				createRun(vec_arr, runFile, input_args);
				cout << "Created run: " << numRuns << endl;
				pageCount = 0;
			//	vec_arr.clear();
			}
				
			dummy->EmptyItOut();	
		}
 		//cout<<"writing to outpipe"<<endl;
		//tempRec->Print(&mySchema);

      //  input_args->out_pipe->Insert(tempRec);
 	}
	//write last run to file
	if(vec_arr.size() > 0) {
			numRuns++;
			createRun(vec_arr, runFile, input_args);
			cout << "Created last run: " << numRuns << endl;
	}
	
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
