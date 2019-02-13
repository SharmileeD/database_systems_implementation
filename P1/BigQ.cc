#include "BigQ.h"
#include "Record.h"
#include "Schema.h"
#include "Pipe.h"
#include "pthread.h"
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

	Record * recrec;
	Record test;
	int err = 0;
	int i = 0;
	Schema mySchema ("catalog", "nation"); 
	cout<<"Tryin to debug the issue"<<endl;
	
	cout<<input_args->in_pipe<<endl;
	cout<<input_args->out_pipe<<endl;
	input_args->sort_order->Print();
	cout<<input_args->run_length<<endl;

	cout<<"Tryin to debug the issue end"<<endl;
	while (input_args->in_pipe->Remove(recrec)) {
		cout<<"writing to outpipe"<<endl;
		test = *recrec;
        input_args->out_pipe->Insert(&test);
	
		i++;
	}
	cout << " Worker doing some work here"<<endl;
	// pthread_exit (NULL);
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages
	
	// storing address of the reference of in pipe coming in to the BigQ in the struct in_pipe variable
	input.in_pipe = &in; 
	input.out_pipe = &out;
	input.sort_order = &sortorder;
	input.run_length = runlen;

	cout<<"Dbug in the Bigq class"<<endl;
	cout<<input.in_pipe<<endl;
	cout<<input.out_pipe<<endl;
	cout<<input.sort_order<<endl;
	cout<<input.run_length<<endl;
	cout<<"Dbug in the Bigq class end"<<endl;
	// // THIS WORKS. READS RECORDS FROM IN PIPE AND PRINTS THEM OUT
	// Record rec[2];
	// Record *last = NULL, *prev = NULL;
	// int err = 0;
	// int i = 0;
	// Schema mySchema ("catalog", "nation"); 
 	// while (in.Remove(&rec[i%2])) {
	// 	prev = last;
	// 	last = &rec[i%2];
	// 	last->Print(&mySchema);
	// 	i++;
	// }
	cout << "Inside BIGQ"<<endl;
    // // construct priority queue over sorted runs and dump sorted data 
 	// // into the out pipe
	pthread_t worker;
	pthread_create (&worker, NULL, sort_tpmms, (void*) &input);
    // // finally shut down the out pipe
	out.ShutDown ();
	pthread_exit (NULL);
}

BigQ::~BigQ () {
}
