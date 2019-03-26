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

// pthread_mutex_t count_mutex;
struct worker_data{
	Pipe *in_pipe;
	Pipe *out_pipe;
	OrderMaker *sort_order;
	int run_length;
	string filename;
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

void phase2tpmms(struct worker_data *input_args, int numRuns, int numPages) {
	
	// cout << "Reached phase 2 for file: "<< input_args->filename<<endl;
	//Stores a page from each run
	Page runPage[numRuns];
	//Stores which page from "run_index" is loaded in runPage
	int currPage[numRuns] = {0};
	int run_index = 0;
	int get_first = 0;
	int load_more = 0;
	int runLength = input_args->run_length; //Number of pages in each run
	struct record_container to_push; //Struct to store entity to push to the priority queue
	struct record_container que[numRuns]; //Array of record_container struct which acts as the priority queue
	priority_queue<record_container, vector<record_container>, CompareRecords> recQ;
	struct record_container temp; 
	struct record_container new_elemnt;
	int bqcnt = 0;
	Heap file;
	file.Open((input_args->filename+".bin").c_str());
	//Get page from every run
	
	//First time load the runPage array with the first page of each run
	for(int i =0 ; i < numRuns;i++) {

		file.file_instance.GetPage(&runPage[i], i*runLength);
		runPage[i].GetFirst(&que[i].rec);

		que[i].run = i;
	   	recQ.push(que[i]);

	}
	// cout << "recQ.size = "<<recQ.size()<<endl;
	while(recQ.size()!=0){
		//Step 1: Getting the first record(smallest) of the priority queue
		temp = recQ.top();
		run_index = temp.run;
		bqcnt++;
		input_args->out_pipe->Insert(&temp.rec);

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
			//Also the last page for the kast run might be different than that for other runs so taking care of that 
			int limit = 0;
			if(run_index == numRuns-1){
				limit = numPages-1;
			}
			else{
				limit = runLength-1;
			}
			//Just setting the currPage value for "run_index" to -1 in case all records from the run are exhausted
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
				recQ.push(new_elemnt);
			}
			

		}
		//Step 2.2: This is the case where the PAGE has a next RECORD for you
		else if (get_first ==1)
		{
			// The current page still has some records so just getting the first record from that page
			to_push.run= run_index;
			recQ.push(to_push);
		}
	}
	
	cout << "Done for file: "<< bqcnt<<endl;
	file.Close();
}

//Function that creates a sorted run and writes it to a file named runs.bin
void createRun(vector<Record> vec_arr, OrderMaker sort_order, int numRuns, string filename) {
	
	Page temp;
	off_t page_num;
	int mfSize = 100;
	char meta_file_name[mfSize];
	int arr_size = vec_arr.size();
	Record arr[arr_size];
	for(int i =0; i < arr_size; i++)
		arr[i] = vec_arr[i];
	// cout << "Creating runs for file: "<< filename<<endl;
	Heap dbfile;
	//If this is the first run then we need to create the dbfile else just open the existing runs.bin file
	if(numRuns == 1){
		fType fileType = heap;
		// sprintf (meta_file_name, "%s_runs.bin", );
		dbfile.Create((filename+".bin").c_str(), fileType, NULL);
	}
	else{
		dbfile.Open((filename+".bin").c_str()); 
	}
	//The first page is loaded when we call dbfile.Open so we need to empty it out
	dbfile.buffer_page.EmptyItOut();

	//sort the run 	
	mergeSort(arr,0,arr_size-1,sort_order);
 
	//Just add the run to the runs.bin dbfile
	for(int j=0; j<arr_size;j++){
		dbfile.Add(arr[j]);
	}

	dbfile.Close();
	
}

//Function to implement phase one and two of the tpmms external sortingalgorithm
void *sort_tpmms (void *arg) {
	struct worker_data * input_args;
	input_args = (struct worker_data *)arg;	
	// cout << "Start sort tppms for file: "<< input_args->filename<<endl;
	int pageCount = 0;
	int numRuns = 0;
	bool writeRun = false;
		
	Record * tempRec;
	Record outrec;
	tempRec = &outrec;
	Record pushThis;

	// Schema mySchema("catalog","partsupp");
	// int count = 0;
	Page dummy;
 	vector<Record> vec_arr; 
	while (input_args->in_pipe->Remove(tempRec)==1) {
		// tempRec->Print(&mySchema);
	// count++;
	// cout << "bigq Count= "<<count<<endl;
		writeRun = false;
		vec_arr.push_back(*tempRec);
			
		if(dummy.Append(tempRec)!= 1){
			//If page is full
			pageCount ++;
			if(pageCount == input_args->run_length) {
				
				//create a run when you have reached the run length limit. 
				//Sort the records and write them out to disk
				numRuns++;
				vec_arr.pop_back();
				createRun(vec_arr, *input_args->sort_order, numRuns, input_args->filename);
				writeRun = true;
				pageCount = 0;
				
				vec_arr.clear();
				vec_arr.push_back(*tempRec);

			}
			dummy.EmptyItOut();	
			int app = dummy.Append(tempRec);

		}
 	}
	//write last run to file if the page was never emptied into the dbifile
	if(!writeRun) {
			numRuns++;
			createRun(vec_arr, *input_args->sort_order, numRuns, input_args->filename);
			pgCountLastRun = pageCount;
			pageCount = 0;
			vec_arr.clear();
			pgCountLastRun++;
			
	}
	// cout << "sort tppms for file: "<< input_args->filename<<endl;
	phase2tpmms(input_args, numRuns, pgCountLastRun);

	//Done with external sort so shutting down the outpipe
	input_args->out_pipe->ShutDown();
 	cout<< "Exiting the worker thread"<<endl;
	
	remove((input_args->filename+".bin").c_str());
	remove((input_args->filename+"_lpage.txt").c_str());
	remove((input_args->filename+"_dpage.txt").c_str());
	pthread_exit(NULL);
	
}
void merge(Record arr [], int l, int m, int r, OrderMaker sort_order) { 
    int i, j, k; 
    int n1 = m - l + 1; 
    int n2 =  r - m; 
	int val=0;
	ComparisonEngine ceng;
    /* create temp arrays */
    Record L[n1], R[n2]; 
    /* Copy data to temp arrays L[] and R[] */

    for (i = 0; i < n1; i++) 
        L[i] = arr[l + i]; 

    for (j = 0; j < n2; j++) 
        R[j] = arr[m + 1+ j]; 
	
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
void mergeSort(Record arr[], int l, int r, OrderMaker sort_order) { 
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

string getRandomString(int n){ 
    char alphabet[36] = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 
                          'h', 'i', 'j', 'k', 'l', 'm', 'n',  
                          'o', 'p', 'q', 'r', 's', 't', 'u', 
                          'v', 'w', 'x', 'y', 'z','1', '2','3','4','5','6','7','8','9','0'}; 
  
    string res = ""; 
    for (int i = 0; i < n; i++)  
        res = res + alphabet[rand() % 36]; 
    
    return res; 
} 
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages
	// pthread_mutex_lock( &count_mutex );
	cout << "BigQ: Start"<<endl;
	// storing address of the reference of in pipe coming in to the BigQ in the struct in_pipe variable
	input.in_pipe = &in; 
	input.out_pipe = &out;
	input.sort_order = &sortorder;
	input.run_length = runlen;
	input.filename = getRandomString(10);
    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe
	pthread_t worker;
	pthread_attr_t attr;
	pthread_attr_init(&attr);
    int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_DETACHED);
	if (det){
		cout<<"Some issue with setting detached"<<endl;
	}
	else{
		cout<<"BigQ Thread attr set to detached"<<endl;
	}
	pthread_create (&worker, &attr, sort_tpmms, (void*) &input);
	// pthread_mutex_unlock( &count_mutex );
    // finally shut down the out pipe
}

BigQ::~BigQ () {
}
