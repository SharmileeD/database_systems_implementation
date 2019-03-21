#include "iostream"
#include "RelOp.h"
#include "BigQ.h"
#include "Record.h"
#include "Schema.h"
#include "Pipe.h"
#include "File.h"
#include "pthread.h"
#include "DBFile.h"
#include <sstream>

using namespace std;

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

/**
    Struct for the SelectPipe worker thread
*/
struct select_pipe_data{
	Pipe *in_pipe;
	Pipe *out_pipe;
	CNF *select_op;
	Record *literal;
};

struct select_pipe_data sp_data;
/**
    Function that the worker thread of select_pipe calls when spawned
    @param arg Pointer to the struct that contains data that the worker needs to use to generate output
    @return void. 
*/
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
	input_args->out_pipe->ShutDown();
}
/**
    Run function for SelectPipe.
	Spawns a worker thread and returns back to the caller.

    @param inPipe Pipe from which we pick records from
	@param outPipe Pipe to which we write records to
	@param selOp CNF type parameter used to compare incoming records
	@param literal Record type parameter we need to compare incoming records against
    @return void. 
*/
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

}
/**
    WaitUntilDone function for SelectPipe.
	Gives the caller a way to block everything else till the selectPipe worker is done
    @return void. 
*/
void SelectPipe::WaitUntilDone () { 
	cout<<"in: SelectPipe::WaitUntilDone"<<endl;
	pthread_join (worker, NULL);
}
void SelectPipe::Use_n_Pages (int n) { 

}


/**
    Struct for the Project worker thread
*/
struct project_data{
	Pipe *in_pipe;
	Pipe *out_pipe;
	int *keepMe;
	int numAttsInput;
	int numAttsOutput;
};

struct project_data proj_data;
/**
    Function that the worker thread of select_pipe calls when spawned
    @param arg Pointer to the struct that contains data that the worker needs to use to generate output
    @return void. 
*/
void *project_data_worker (void *arg) {
	cout<<"in: project_data_worker"<<endl;
	struct project_data * input_args;
	input_args = (struct project_data *)arg;
	
	Record * tempRec;
	Record outrec;
	tempRec = &outrec;
	Record pushThis;
	ComparisonEngine ceng;

	
	Page dummy;
	while (input_args->in_pipe->Remove(tempRec)==1) {
		// Do something with the records
		tempRec->Project(input_args->keepMe, input_args->numAttsOutput, input_args->numAttsInput);
		input_args->out_pipe->Insert(tempRec);
		

	}
	input_args->out_pipe->ShutDown();
}
/**
    Run function for Project.
	Spawns a worker thread and returns back to the caller.

    @param inPipe Pipe from which we pick records from
	@param outPipe Pipe to which we write records to
	@param keepMe Array of integers indicating the atributes of the record to keep
	@param numAttsInput The number of attributes in input Schema
	@param numAttsOutput The number of attributes to output(essentially the size of keepMe int array)
    @return void. 
*/
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) { 
	proj_data.in_pipe = &inPipe;
	proj_data.out_pipe = &outPipe;
	proj_data.keepMe = keepMe;
	proj_data.numAttsInput = numAttsInput;
	proj_data.numAttsOutput = numAttsOutput;

	pthread_attr_t attr;
	pthread_attr_init(&attr);
    int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
	if (det){
		cout<<"Some issue with setting joinable"<<endl;
	}
	else{
		cout<<"Thread attr set to joinable"<<endl;
	}
	pthread_create (&worker, &attr, project_data_worker, (void*) &proj_data);

}
void Project::WaitUntilDone () { 
	cout<<"in: SelectPipe::WaitUntilDone"<<endl;
	pthread_join (worker, NULL);
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



/**
    Struct for the Project worker thread
*/
struct duplicate_removal_data{
	Pipe *in_pipe;
	Pipe *out_pipe;
	Schema *mySchema;
};

struct duplicate_removal_data dup_rem;
/**
    Function that the worker thread of select_pipe calls when spawned
    @param arg Pointer to the struct that contains data that the worker needs to use to generate output
    @return void. 
*/
void *duplicate_removal_worker (void *arg) {
	cout<<"in: duplicate_removal_worker"<<endl;
	struct duplicate_removal_data * input_args;
	input_args = (struct duplicate_removal_data *)arg;
	OrderMaker sortorder(input_args->mySchema);
	Record * tempRec;
	Record outrec;
	tempRec = &outrec;
	Record previous;
	ComparisonEngine ceng;
	Pipe biq_out(100);
	BigQ bq (*input_args->in_pipe, biq_out, sortorder, 1);
	// 
	Page dummy;
	ComparisonEngine cng;
	int count = 0;
	int un_recs = 1;
	int i = 0;

	Record rec[2];
	Record *last = NULL, *prev = NULL;
	int op=0;
	while (biq_out.Remove(tempRec)==1) {
		
		if(count ==0){
			// previous.Copy(tempRec);
			previous.Copy(tempRec);
			input_args->out_pipe->Insert(tempRec);
			count++;
			continue;
		}
		op = cng.Compare(&previous, tempRec, &sortorder);
		if(cng.Compare(&previous, tempRec, &sortorder)!=0){
			previous.Copy(tempRec);
			input_args->out_pipe->Insert(tempRec);
			un_recs++;

		}
		else{
			continue;
		}
		count++;

	}
	cout<<"Unique records "<<un_recs<<endl;
	input_args->out_pipe->ShutDown();
}
/**
    Run function for Duplicate Removal.
	Spawns a worker thread and returns back to the caller.

    @param inPipe Pipe from which we pick records from
	@param outPipe Pipe to which we write records to
	@param mySchema Schema object which needs to be used to compare records
    @return void. 
*/
void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { 
	dup_rem.in_pipe = &inPipe;
	dup_rem.out_pipe = &outPipe;
	dup_rem.mySchema = &mySchema;

	pthread_attr_t attr;
	pthread_attr_init(&attr);
    int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
	if (det){
		cout<<"Some issue with setting joinable"<<endl;
	}
	else{
		cout<<"Thread attr set to joinable"<<endl;
	}
	pthread_create (&worker, &attr, duplicate_removal_worker, (void*) &dup_rem);
}
void DuplicateRemoval::WaitUntilDone () { 
	cout<<"in: DuplicateRemoval::WaitUntilDone"<<endl;
	pthread_join (worker, NULL);
}
void DuplicateRemoval::Use_n_Pages (int n) { 

}

/**
    Struct for the Project worker thread
*/
struct sum_data{
	Pipe *in_pipe;
	Pipe *out_pipe;
	Function *computeMe;
};

struct sum_data sm_data;
/**
    Function that the worker thread of select_pipe calls when spawned
    @param arg Pointer to the struct that contains data that the worker needs to use to generate output
    @return void. 
*/
void *sum_worker (void *arg) {
	struct sum_data * input_args;
	input_args = (struct sum_data *)arg;
	Attribute DA = {"double", Double};
	Attribute IA = {"int", Int};
	Record * tempRec;
	Record outrec;
	tempRec = &outrec;
	ComparisonEngine ceng;
	int intres;
	int finIntres = 0;
	double dobres;
	double finDobres = 0.0;
	Page dummy;
	while (input_args->in_pipe->Remove(tempRec)==1) {
		// Do something with the records
		input_args->computeMe->Apply(*tempRec, intres, dobres);
		finIntres = finIntres + intres;
		finDobres = finDobres + dobres;
		

	}
	Record sum_rec;
	
	
	if (finIntres!=0 and finDobres == 0.0){
		Schema sum_sch ("sum_sch", 1, &IA);
		stringstream ss;
    	ss << finIntres;
    	const char* str = ss.str().c_str();
		sum_rec.ComposeRecord(&sum_sch,str);
	}
	if (finIntres==0 and finDobres != 0.0){
		Schema sum_sch ("sum_sch", 1, &DA);
		stringstream ss;
    	ss << finDobres;
    	const char* str = ss.str().c_str();
		sum_rec.ComposeRecord(&sum_sch,str);
	}
	// sum_rec.Print(&sum_sch);
	input_args->out_pipe->Insert(&sum_rec);
	input_args->out_pipe->ShutDown();
}
/**
    Run function for Sum.
	Spawns a worker thread and returns back to the caller.

    @param inPipe Pipe from which we pick records from
	@param outPipe Pipe to which we write records to
	@param computeMe Function which is used to compute the Sum
    @return void. 
*/
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) { 
	sm_data.in_pipe = &inPipe;
	sm_data.out_pipe = &outPipe;
	sm_data.computeMe = &computeMe;


	pthread_attr_t attr;
	pthread_attr_init(&attr);
    int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
	if (det){
		cout<<"Some issue with setting joinable"<<endl;
	}
	else{
		cout<<"Thread attr set to joinable"<<endl;
	}
	pthread_create (&worker, &attr, sum_worker, (void*) &sm_data);
}
void Sum::WaitUntilDone () { 
	pthread_join (worker, NULL);
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

