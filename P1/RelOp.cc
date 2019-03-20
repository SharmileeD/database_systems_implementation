#include "iostream"
#include "RelOp.h"
#include "BigQ.h"
#include "Record.h"
#include "Schema.h"
#include "Pipe.h"
#include "File.h"
#include "pthread.h"
#include "DBFile.h"

struct selectStruct {
	DBFile *inFile;
	Pipe *outpipe;
	CNF *selop;
	Record * literal;
};


void* selectHelper(void *args) {
	
	Schema mySchema ("catalog","partsupp");
	struct selectStruct * input_args;
	input_args = (struct selectStruct *)args;
	Record temprec;
	
	cout << "File length:" << input_args->inFile->instVar->file_instance.GetLength()<<endl;
	input_args->inFile->instVar->MoveFirst();
	// input_args->inFile->instVar->GetNext(temprec);
	input_args->selop->Print();
	// input_args->literal->Print(&mySchema);
	while(input_args->inFile->instVar->GetNext(temprec, *input_args->selop, *input_args->literal) == 1) {
		temprec.Print(&mySchema);
		input_args->outpipe->Insert(&temprec);
	}
	

}

struct selectStruct selectInput;
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	
	// cout << "Here!!!!!!"<<endl;
	
	pthread_attr_t attr;
	pthread_attr_init(&attr);
		
	selectInput.inFile = &inFile;
	selectInput.outpipe = &outPipe;
	selectInput.selop = &selOp;
	selectInput.literal = &literal;

    int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
	if (det){
		cout<<"Some issue with setting detached"<<endl;
	}
	else{
		cout<<"Thread attr set to joinable"<<endl;
	}
	pthread_create (&worker, &attr, selectHelper, (void*) &selectInput);

	selectInput.outpipe->ShutDown();

	//get one record at a time till input pipe is empty


	//apply cnf to the record
	//if cnf accepts the record, add it to the output pipe
}

void SelectFile::WaitUntilDone () {
	pthread_join (worker, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}

struct select_pipe_data{
	Pipe *in_pipe;
	Pipe *out_pipe;
	CNF *select_op;
	Record *literal;
};

struct select_pipe_data sp_data;
void *select_pipe (void *arg) {
	cout<<"in: select_pipe"<<endl;
	struct select_pipe_data * input_args;
	input_args = (struct select_pipe_data *)arg;
	
	Record * tempRec;
	Record outrec;
	tempRec = &outrec;
	Record pushThis;
	Schema mySchema ("catalog", "orders");
	ComparisonEngine ceng;

	
	Page dummy;
	while (input_args->in_pipe->Remove(tempRec)==1) {
		// Do something with the records
		cout<<"while loop: inpipe recs"<<endl;
		if (ceng.Compare(tempRec, input_args->literal,input_args->select_op)){
			input_args->out_pipe->Insert(tempRec);
		}

	}
}
// With a couple of exceptions, operations always get their data from input pipes and put the result of the operation into an output pipe. 
// When someone wants to use one of the relational operators, they just create an instance of the operator that they want. Then they call the Run operation on the operator that they are using (Run is implemented by each derived class; see below). 
// The Run operation sets up the operator by causing the operator to create any internal data structures (THIS IS WHERE WE USE THE STRUCT)it needs, and then it spawns a thread that is internal to the relational operation and actually does the work.
// Once the thread has been created and is ready to go, Run returns and the operation does its work in a non-blocking fashion.
//  After the operation has been started up, the caller can call WaitUntilDone, which will block until the operation finishes and the thread inside of the operation has been destroyed. An operation knows that it finishes when it has finished processing all of the tuples that came through its input pipe (or pipes). Before an operation finishes, it should always shut down its output pipe.

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) { 
	sp_data.in_pipe = &inPipe;
	sp_data.out_pipe = &outPipe;
	sp_data.select_op = &selOp;
	sp_data.literal = &literal;
	pthread_attr_t attr;
	pthread_attr_init(&attr);
    int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
	if (det){
		cout<<"Some issue with setting joinable"<<endl;
	}
	else{
		cout<<"Thread attr set to joinable"<<endl;
	}
	pthread_create (&worker, &attr, select_pipe, (void*) &sp_data);
	outPipe.ShutDown();

}
void SelectPipe::WaitUntilDone () { 
	cout<<"in: SelectPipe::WaitUntilDone"<<endl;
	pthread_join (worker, NULL);
}
void SelectPipe::Use_n_Pages (int n) { 

}


void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) { 

}
void Project::WaitUntilDone () { 

}
void Project::Use_n_Pages (int n) { 
	
}
struct joinStruct {
	Pipe *opL;
	Pipe *opR;
	Pipe *op;
	OrderMaker *left;
	OrderMaker *right;
	CNF *selop;
	Record * literal;
};

struct joinStruct joinInput;

void* joinHelper (void * args) {

}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 
	// Use 2 BigQs to store all of the tuples comingfrom the left input pipe, and a second BigQ for the right input pipe
	// perform a merge in order to join the two input pipes.
	int pipesz = 100; // buffer sz allowed for each pipe
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	Pipe opL (pipesz);
	Pipe opR (pipesz);

	OrderMaker left;
	OrderMaker right;

	selOp.GetSortOrders(left, right);
	left.Print();
	right.Print();

	BigQ bqL(inPipeL, opL, left, 1);
	BigQ bqR(inPipeR, opR, right, 1);

	joinInput.left = &left;
	joinInput.right = &right;
	joinInput.literal = &literal;
	joinInput.opL = &opL;
	joinInput.opR = &opR;
	joinInput.op = &outPipe;
	joinInput.selop = &selOp;


	int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
	if (det){
		cout<<"Some issue with setting detached"<<endl;
	}
	else{
		cout<<"Thread attr set to joinable"<<endl;
	}
	pthread_create (&worker, &attr, joinHelper, (void*) &joinInput);

}
void Join::WaitUntilDone () {
	pthread_join (worker, NULL);
 }

void Join::Use_n_Pages (int n) { }

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { 

}
void DuplicateRemoval::WaitUntilDone () { 

}
void DuplicateRemoval::Use_n_Pages (int n) { 

}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { 

}
void Sum::WaitUntilDone () { 

}
void Sum::Use_n_Pages (int n) { 

}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { 

}
void GroupBy::WaitUntilDone () { 

}
void GroupBy::Use_n_Pages (int n) { 

}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { 
	
}
void WriteOut::WaitUntilDone () { 

}
void WriteOut::Use_n_Pages (int n) { 

}

