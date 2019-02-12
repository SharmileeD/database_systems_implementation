#include "BigQ.h"
#include "Record.h"
#include "Schema.h"
#include "Pipe.h"
struct worker_data{
	Pipe *in_pipe;
	Pipe *out_pipe;
	OrderMaker *sort_order;
	int run_length;
};
void *sort_tpmms (void *arg) {
	// struct worker_data * input_args = (worker_data *)arg;
	// Record newinrec; 

	// Record newrec[2];
	// Record *newlast = NULL, *newprev = NULL;
	// int err = 0;
	// int i = 0;
	// Schema mySchema ("catalog", "nation"); 

	// while (input_args->in_pipe->Remove (&newrec[i%2])) {
	// 	newprev = newlast;
	// 	newlast = &newrec[i%2];
	// 	input_args->out_pipe->Insert(newlast);
	// 	i++;
	// }
	cout << " Worker doing some work here"<<endl;
	//pthread_exit(NULL);
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages
	struct worker_data input;
	//storing address of the reference of in pipe coming in to the BigQ in the struct in_pipe variable
	input.in_pipe = &in; 
	input.out_pipe = &out;
	input.sort_order = &sortorder;
	input.run_length = runlen;
	// THIS WORKS. READS RECORDS FROM IN PIPE AND PRINTS THEM OUT
	Record rec[2];
	Record *last = NULL, *prev = NULL;
	int err = 0;
	int i = 0;
	Schema mySchema ("catalog", "nation"); 
 	while (in.Remove(&rec[i%2])) {
		prev = last;
		last = &rec[i%2];
		last->Print(&mySchema);
		i++;
	}
	cout << "Inside BIGQ"<<endl;
    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe
	pthread_t worker;
	pthread_create (&worker, NULL, sort_tpmms, (void *)&input);

    // finally shut down the out pipe
	out.ShutDown ();
	// pthread_join (worker, NULL);
}

BigQ::~BigQ () {
}
