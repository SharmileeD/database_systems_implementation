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

int pgCountLastRun = 0;

void mergeSort(Record arr[], int l, int r, OrderMaker sort_order); 

void phase2tpmms(struct worker_data *input_args, int numRuns) {
	
	Page runPage[numRuns];
	int currPage[numRuns] = {0};
	int run_index = 0;
	int get_first = 0;
	int load_more = 0;
	int runLength = input_args->run_length;
	record_container to_push;
	priority_queue<record_container, vector<record_container>, CompareRecords> recQ;
	record_container temp;
	Schema mySchema ("catalog", "lineitem"); 	

	DBFile file;
	file.Open("runs.bin");
	//Get page from every run
	
	
	struct record_container que;
	//First time load the runPage array with the first page of each run
	for(int i =0 ; i < numRuns;i++) {
		file.file_instance.GetPage(&runPage[i], i*runLength);
		runPage[i].GetFirst(&que.rec);

		que.run = i;
		
		recQ.push(que);

	}

	while(!recQ.empty()){
		//Step 1: Getting the first record(smallest) of the priority queue
		temp = recQ.top();
		run_index = temp.run;
		// input_args->out_pipe->Insert(&temp.rec);
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
			}
			

		}
		//Step 2.2: This is the case where the PAGE has a next RECORD for you
		else if (get_first ==1)
		{
			// The current page still has some records so just getting the first record from that page
			struct record_container new_elemnt;
			runPage[run_index].GetFirst(&new_elemnt.rec);
			new_elemnt.run= run_index;
			recQ.push(new_elemnt);
		}
		
		to_push.run = run_index;
		recQ.push(to_push);
	}
	
	
	//temp = recQ.top();
	file.Close();

}

void createRun(vector<Record> vec_arr, OrderMaker sort_order) {
	
	Schema mySchema ("catalog", "lineitem");
	Page temp;
	off_t page_num;
	int arr_size = vec_arr.size();
	Record arr[arr_size];
	for(int i =0; i < arr_size; i++)
		arr[i] = vec_arr[i];
	// File file;
	// file.Open(1,"runs.bin"); 
	//***********
	DBFile dbfile;
	dbfile.Open("runs.bin"); 
	dbfile.buffer_page.EmptyItOut();
	//***********
	//sort the run
	mergeSort(arr,0,arr_size-1,sort_order);
	
	
	// off_t last_page_added = 0;
	// bool dirty = false;
	// for(int j=0; j<arr_size;j++){
	// 	dirty = false;
	// 	if(temp.Append(&arr[j]) != 1)
	// 	{
	// 		dirty = true;
	// 		//if page is full add page to file, flush and add new record

	// 		if (file.GetLength() != 0){
	// 			last_page_added = file.GetLength()-1;
	// 		}

	// 		file.AddPage(&temp,last_page_added);
	// 		temp.EmptyItOut();
	// 		temp.Append(&arr[j]);

	// 	}	

	// }
	// if(!dirty){
		
	// 	if (file.GetLength() != 0){
	// 			last_page_added = file.GetLength()-1;
	// 	}
	// 	file.AddPage(&temp,last_page_added);
	// 	temp.EmptyItOut();
	// }

	//***********
	for(int j=0; j<arr_size;j++){
		dbfile.Add(arr[j]);
	}
	//***********

	cout << "Added records"<<endl;
	// file.Close();
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
	// File file;
	cout << "Creating file" << endl;
	
	runFile.Create("runs.bin", fileType, NULL);
	runFile.Close();
	// file.Open(0,"runs.bin");
	// file.Close();
	
	Page dummy;
 	vector<Record> vec_arr; 
	
	while (input_args->in_pipe->Remove(tempRec)) {
		
		writeRun = false;
		vec_arr.push_back(*tempRec);
		if(dummy.Append(tempRec)!= 1) {
			//If page is full
			pageCount ++;

			if(pageCount == input_args->run_length) {
			
				//create a run when you have reached the run length limit. 
				//Sort the records and write them out to disk
				numRuns++;
				createRun(vec_arr, *input_args->sort_order);
				writeRun = true;
				pageCount = 0;
				vec_arr.clear();

			}
			dummy.EmptyItOut();	
		}
 		
 	}
	//write last run to file if the page was never emptied into the dbifile
	if(!writeRun) {
			cout<<"Last run writing out"<<endl;
			numRuns++;
			createRun(vec_arr, *input_args->sort_order);
			pgCountLastRun = pageCount;
			pageCount = 0;
			vec_arr.clear();
			
	}
	
	//phase2tpmms(input_args, numRuns);
	
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
