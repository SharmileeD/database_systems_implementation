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
	Record * temprec;
	
	cout << "File length:" << input_args->inFile->instVar->file_instance.GetLength()<<endl;

	// while(input_args->inFile->GetNext(*temprec, *input_args->selop, *input_args->literal) == 1) {
		// temprec->Print(&mySchema);
		// input_args->outpipe->Insert(temprec);
	// }
	

}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	
	// cout << "Here!!!!!!"<<endl;
	
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	
	selectStruct input;
	input.inFile = &inFile;
	input.outpipe = &outPipe;
	input.selop = &selOp;
	input.literal = &literal;

    int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
	if (det){
		cout<<"Some issue with setting detached"<<endl;
	}
	else{
		cout<<"Thread attr set to joinable"<<endl;
	}
	pthread_create (&thread, &attr, selectHelper, (void*) &input);

	input.outpipe->ShutDown();

	//get one record at a time till input pipe is empty


	//apply cnf to the record
	//if cnf accepts the record, add it to the output pipe
}

void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
}

void SelectFile::Use_n_Pages (int runlen) {

}
