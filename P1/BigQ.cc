#include "BigQ.h"
#include "Record.h"
#include "Schema.h"
#include "Pipe.h"
#include "pthread.h"
#include <unistd.h>
struct worker_data{
	Pipe *in_pipe;
	Pipe *out_pipe;
	OrderMaker *sort_order;
	int run_length;
};
struct worker_data input;
void *sort_tpmms (void *arg) {
	struct worker_data * input_args;
	input_args = (struct worker_data *)arg;	
	
	Schema mySchema ("catalog", "nation"); 

	Record * tempRec;

	Record outrec;
	tempRec = &outrec;

 	while (input_args->in_pipe->Remove(tempRec)) {
		
 		cout<<"writing to outpipe"<<endl;
		tempRec->Print(&mySchema);

        input_args->out_pipe->Insert(tempRec);
 	}
// 	cout << " Worker doing some work here"<<endl;
	input_args->out_pipe->ShutDown();
 	cout<< "Exiting the worker thread"<<endl;
	pthread_exit(NULL);
	
	
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
