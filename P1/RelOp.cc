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
#include <unistd.h>
#include <vector>
#include <array>
#include <string>
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
	int count = 0;
	// cout << "File length:" << input_args->inFile->instVar->file_instance.GetLength()<<endl;
	input_args->inFile->instVar->MoveFirst();
	// input_args->inFile->instVar->GetNext(temprec);
	input_args->selop->Print();
	// input_args->literal->Print(&mySchema);
	while(input_args->inFile->instVar->GetNext(temprec, *input_args->selop, *input_args->literal) == 1) {
		// temprec.Print(&mySchema);
		input_args->outpipe->Insert(&temprec);
		count++;
	}
	// cout<<"Select file count "<<count<<endl;
	input_args->outpipe->ShutDown();

}


void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	
	struct selectStruct *selectInput;
	selectInput = (selectStruct *) malloc(sizeof(selectStruct));
	pthread_attr_t attr;
	pthread_attr_init(&attr);
		
	selectInput->inFile = &inFile;
	selectInput->outpipe = &outPipe;
	selectInput->selop = &selOp;
	selectInput->literal = &literal;

    int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
	if (det){
		cout<<"Some issue with setting detached"<<endl;
	}
	else{
		cout<<"Thread attr set to joinable"<<endl;
	}
	pthread_create (&worker, &attr, selectHelper, (void*) selectInput);

	//get one record at a time till input pipe is empty


	//apply cnf to the record
	//if cnf accepts the record, add it to the output pipe
}

void SelectFile::WaitUntilDone () {
	cout << "In selectfile wait until done...."<<endl;
	pthread_join (worker, NULL);
	cout << "Done with SF wait"<<endl;
	
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
	
	
	struct select_pipe_data *sp_data;
	sp_data = (select_pipe_data *) malloc(sizeof(select_pipe_data));

	sp_data->in_pipe = &inPipe;
	sp_data->out_pipe = &outPipe;
	sp_data->select_op = &selOp;
	sp_data->literal = &literal;
	pthread_attr_t attr;
	pthread_attr_init(&attr);
    int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
	if (det){
		cout<<"Some issue with setting joinable"<<endl;
	}
	else{
		cout<<"Thread attr set to joinable"<<endl;
	}
	pthread_create (&worker, &attr, select_pipe, (void*) sp_data);

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
	struct project_data *proj_data;
	
	proj_data = (project_data *) malloc(sizeof(project_data));
	proj_data->in_pipe = &inPipe;
	proj_data->out_pipe = &outPipe;
	proj_data->keepMe = keepMe;
	proj_data->numAttsInput = numAttsInput;
	proj_data->numAttsOutput = numAttsOutput;

	pthread_attr_t attr;
	pthread_attr_init(&attr);
    int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
	if (det){
		cout<<"Some issue with setting joinable"<<endl;
	}
	else{
		cout<<"Thread attr set to joinable"<<endl;
	}
	pthread_create (&worker, &attr, project_data_worker, (void*) proj_data);

}
void Project::WaitUntilDone () { 
	cout<<"in: SelectPipe::WaitUntilDone"<<endl;
	pthread_join (worker, NULL);
}
void Project::Use_n_Pages (int n) { 
	
}

// struct nestedBlockStruct {
// 	Pipe *ipL;
// 	Pipe *ipR;
// 	Pipe *op;
// 	CNF *selop;
// 	Record * literal;
// };

// struct nestedBlockStruct nbStruct;
struct joinStruct {
	Pipe *ipL;
	Pipe *ipR;
	Pipe *op;
	OrderMaker left;
	OrderMaker right;
	CNF *selop;
	Record * literal;
};




void * nestedBlock(void * args) {
	cout << "Inside block join"<<endl;
	struct joinStruct *input_args;
	input_args = (struct joinStruct *)args;
	ComparisonEngine ceng;
	vector<Record> vec_right; //right
	vector<Record> vec_left; //left
	int val;
	int buff_size 	= 10;
	int count 		= 0;
	bool moreRec 	= true;
	Record lRec, rRec;
	Record * tempRec;
	Record outrec, outrecl;
	tempRec = &outrec;
	Record * mergeRec;
	mergeRec = &outrecl;
	
	int leftCount 	= 0;
	int rightCount 	= 0;
	leftCount		= input_args->selop->leftAttrCount;
	rightCount		= input_args->selop->rightAttrCount;

    cout<< "Inside clear pipe!!"<<endl;
	FILE *writefile = fopen ("nested.txt", "w");
	string strrec;
	const char * ch;

	Schema mySchemaL ("catalog","supplier");
	Schema mySchemaR ("catalog","partsupp");
	int c = 0;
	int mergedCount=0;
	int startOfRight = 0;
	mergedCount = leftCount + rightCount;
	int attsToKeep[mergedCount];
	int idx = 0;
	for(int i = 0; i < mergedCount; i++){
		if(i==leftCount){
			startOfRight = i;
			idx = 0;
		}
		attsToKeep[i] = idx;
		idx++;
	}
	//fill the left buffer with 100 left pipe records
	for(int i = 0; i < buff_size ; i++) {
		if((val = input_args->ipL->Remove(tempRec))==1) {
			vec_left.push_back(*tempRec);
			count ++;
		}
		else{
			moreRec = false;
			break;
		}
			
	}
	//get the records from right table
	while(input_args->ipR->Remove(tempRec)==1) {
		vec_right.push_back(*tempRec);
	}

	// sleep(1);
	//perform block nested join
	do {
		cout << "Here!!"<<endl;
		// cout<<"Inside do while"<<endl;
		for(int r = 0; r < vec_right.size(); r++) {
			for(int l =0; l< vec_left.size(); l++ ) {
				// cout<< "l ="<<l<<"  r="<<r<<endl;
				if(ceng.Compare(&vec_left[l], &vec_right[r], input_args->selop)==1) {
					// vec_left[l].Print(&mySchemaL);
					// vec_right[r].Print(&mySchemaR);
					lRec.Copy(&vec_left[l]);
					rRec.Copy(&vec_right[r]);
					strrec = vec_left[l].returnRecord(&mySchemaL);
					ch = strrec.c_str();
					fprintf(writefile, ch);
					//--------------------------------
					strrec = vec_right[r].returnRecord(&mySchemaR);
					ch = strrec.c_str();
					fprintf(writefile, ch);
					//------------------------
					c++;
					cout <<"--------------------------------------------------------------final = "<<c<<endl;
					// vec_left[l].Copy(&lRec);
					// vec_right[r].Copy(&rRec);
					mergeRec->MergeRecords(&lRec, &rRec, leftCount, rightCount, attsToKeep, mergedCount, startOfRight);
					
					input_args->op->Insert(mergeRec);

					// tempRec->Print(&mySchemaR);
				} //if
				
			}//left
		}//right
		//clear the left buffer
		cout << "------before clear!!"<<endl;
		vec_left.clear();
		//refill the left buffer
		// cout<<"------->Inside do while"<<endl;
		for(int i = 0; i < buff_size ; i++) {
			if((val = input_args->ipL->Remove(tempRec))==1) {
				vec_left.push_back(*tempRec);
				count ++;
			}
			else{
				moreRec = false;
				break;
			}
			
		}	
	} while(moreRec);

	cout <<"Count = "<<count<<endl;
	
	input_args->op->ShutDown();
	//fill page with records from ipR
	//pick one record of page and compare it with record of ipL, push the record of ipL in vector.


}



void* joinHelper (void * args) {
	
	struct joinStruct *input_args;
	input_args = (struct joinStruct *)args;
	Record  lRec, rRec;
	Record * tempRec;
	Record outrec, outrecl, outrecr;
	tempRec = &outrec;
	Record * mergeRec;
	mergeRec = &outrecl;
	// rRec = &outrecr;
	int leftCount = 0;
	int rightCount = 0;
	leftCount= input_args->selop->leftAttrCount;
	rightCount= input_args->selop->rightAttrCount;
	Pipe outpLeft(100);
	Pipe outpRight(100);
	Schema mySchemaL ("catalog","supplier");
	Schema mySchemaR ("catalog","partsupp");
	int leftrl = 1;
	BigQ bqL(*input_args->ipL, outpLeft, input_args->left, leftrl);
	// sleep(1);
	int rightrl = 10;
	BigQ bqR(*input_args->ipR, outpRight, input_args->right, rightrl);
	ComparisonEngine ceng;
	// sleep(1);
	int count =0;
	// cout <<"Inside join thread!"<<endl;

	int cr = 0, cl = 0;
	vector<Record> vec_right; //right
	vector<Record> vec_left; //left

	while(outpLeft.Remove(tempRec)==1) {
		// tempRec->Print(&mySchemaL);
		cl++;
		cout <<"left----------->"<<cl<<endl;
		vec_left.push_back(*tempRec);
	}
	
	int mergedCount=0;
	int startOfRight = 0;
	mergedCount = leftCount + rightCount;
	int attsToKeep[mergedCount];
	int idx = 0;
	for(int i = 0; i < mergedCount; i++){
		if(i==leftCount){
			startOfRight = i;
			idx = 0;
		}
		attsToKeep[i] = idx;
		idx++;
	}
	while(outpRight.Remove(tempRec)==1) {
		// tempRec->Print(&mySchemaR);
		cr++;
		cout <<"right----------->"<<cr<<endl;
		vec_right.push_back(*tempRec);
	}

	// int attsToKeep[7] = {0,0,2,3,4};
	

	// FILE *writefile = fopen ("sortmerge.txt", "w");
	// string strrec;
	// const char * c;

	int l = 0, r =0 ;
	int prev_l, prev_r;
	int fincnt = 0;
	
	while(l < vec_left.size() && r < vec_right.size()) {
		
		int cmp = ceng.Compare(&vec_left[l], &input_args->left, &vec_right[r], &input_args->right);
		
		if(cmp < 0)
			l++;
		else if(cmp > 0)
			r++;
		else {
			//if r and l recs match merge and push
			lRec.Copy(&vec_left[l]);
			rRec.Copy(&vec_right[r]);
			// lRec.Print(&mySchemaL);
			// rRec.Print(&mySchemaR);
			fincnt++;
			// if(fincnt == 7999){
			// 	cout <<"reached1"<<endl;
			// 	lRec.Print(&mySchemaL);
			// 	rRec.Print(&mySchemaR);
			// }
				
			mergeRec->MergeRecords(&lRec, &rRec, leftCount, rightCount, attsToKeep, mergedCount, startOfRight);
			input_args->op->Insert(mergeRec);

			//store current left record pointer
			prev_l = l;

			//check other l records that match rRec and merge 
			l++;
						
			while(l < vec_left.size() && r <vec_right.size()) {
				//if match found merge and push else break the loop

				if(ceng.Compare(&vec_left[l], &input_args->left, &vec_right[r], &input_args->right)==0) {
					lRec.Copy(&vec_left[l]);
					rRec.Copy(&vec_right[r]);
					// lRec.Print(&mySchemaL);
					// rRec.Print(&mySchemaR);
					fincnt++;
					// if(fincnt == 7999){
					// 	cout <<"reached1"<<endl;
					// 	lRec.Print(&mySchemaL);
					// 	rRec.Print(&mySchemaR);
					// }
					mergeRec->MergeRecords(&lRec, &rRec, leftCount, rightCount, attsToKeep, mergedCount, startOfRight);
					input_args->op->Insert(mergeRec);
					l++;
				}
				else
					break;				
			}

			//restore the left record pointer
			l = prev_l;

			//store current right record pointer
			prev_r = r;

			//check other r records that match the lRec and merge
			r++;

			while(r < vec_right.size() && l < vec_left.size() ) {
				//if match found merge and push else break the loop

				// vec_right[r].Print(&mySchemaR);
				if(ceng.Compare(&vec_left[l], &input_args->left, &vec_right[r], &input_args->right)==0) {
					lRec.Copy(&vec_left[l]);
					rRec.Copy(&vec_right[r]);
					// lRec.Print(&mySchemaL);
					// rRec.Print(&mySchemaR);
					fincnt++;

					
					mergeRec->MergeRecords(&lRec, &rRec, leftCount, rightCount, attsToKeep, mergedCount, startOfRight);
					input_args->op->Insert(mergeRec);
					r++;
				}
				else
					break;				
			}

			//restore right record pointer
			r = prev_r;
			//increment left and right record pointer
			l++;
			r++;	
		} //else
	}//while

	
	input_args->op->ShutDown();
	
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { 
	// Use 2 BigQs to store all of the tuples comingfrom the left input pipe, and a second BigQ for the right input pipe
	// perform a merge in order to join the two input pipes.
	struct joinStruct *joinInput;
	joinInput = (joinStruct *) malloc(sizeof(joinStruct));
	pthread_attr_t attr;
	pthread_attr_init(&attr);

	OrderMaker left;
	OrderMaker right;

	if(selOp.GetSortOrders(left, right)==0) {
			//block nested join
		cout <<"OrderMAker empty!!"<<endl;
		joinInput->literal = &literal;
		joinInput->ipL = &inPipeL;
		joinInput->ipR = &inPipeR;
		joinInput->op = &outPipe;
		joinInput->selop = &selOp;

		int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
		if (det){
			cout<<"Some issue with setting joinable"<<endl;
		}
		else{
			cout<<"Thread attr set to joinable"<<endl;
		}

		pthread_create (&worker, &attr, nestedBlock, (void*) joinInput);
		// outPipe.ShutDown();
	}
	else {
		joinInput->left = left;
		joinInput->right = right;
		joinInput->literal = &literal;
		joinInput->ipL = &inPipeL;
		joinInput->ipR = &inPipeR;
		joinInput->op = &outPipe;
		joinInput->selop = &selOp;


		int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
		if (det){
			cout<<"Some issue with setting joinable"<<endl;
		}
		else{
			cout<<"Thread attr set to joinable"<<endl;
		}

		pthread_create (&worker, &attr, joinHelper, (void*) joinInput);
	}
	// cout <<"----------------------------------------------------------------------------------------------"<<endl;
	// selOp.Print();
	// left.Print();
	// right.Print();
	
	// cout << "Called here!!"<<endl;
}

void Join::WaitUntilDone () {
	// cout << "WaitUntil Done for Join...."<<endl;
	pthread_join (worker, NULL);
 }

void Join::Use_n_Pages (int n) { }


/**
    Struct for the Duplicate Removal worker thread
*/
struct duplicate_removal_data{
	Pipe *in_pipe;
	Pipe *out_pipe;
	Schema *mySchema;
};
/**
    Function that the worker thread of duplicate_removal calls when spawned
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
	ComparisonEngine cng;
	int count = 0;
	Record rec[2];
	Record *last = NULL, *prev = NULL;

	while (biq_out.Remove(tempRec)==1) {
		
		if(count ==0){
			previous.Copy(tempRec);
			input_args->out_pipe->Insert(tempRec);
			count++;
			continue;
		}
		if(cng.Compare(&previous, tempRec, &sortorder)!=0){
			previous.Copy(tempRec);
			input_args->out_pipe->Insert(tempRec);
			count++;
		}
		else{
			continue;
		}
		

	}
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

	struct duplicate_removal_data *dup_rem;
	dup_rem = (duplicate_removal_data *) malloc(sizeof(duplicate_removal_data));
	dup_rem->in_pipe = &inPipe;
	dup_rem->out_pipe = &outPipe;
	dup_rem->mySchema = &mySchema;

	pthread_attr_t attr;
	pthread_attr_init(&attr);
    int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
	if (det){
		cout<<"Some issue with setting joinable"<<endl;
	}
	else{
		cout<<"Thread attr set to joinable"<<endl;
	}
	pthread_create (&worker, &attr, duplicate_removal_worker, (void*) dup_rem);
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
    	const char* str = (ss.str()+"|").c_str();
		sum_rec.ComposeRecord(&sum_sch,str);
		// sum_rec.Print(&sum_sch);
	}
	if (finIntres==0 and finDobres != 0.0){
		Schema sum_sch ("sum_sch", 1, &DA);
		stringstream ss;
    	ss << finDobres;
    	const char* str = (ss.str()+"|").c_str();
		sum_rec.ComposeRecord(&sum_sch,str);
		// sum_rec.Print(&sum_sch);
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

	struct sum_data *sm_data;
	sm_data = (sum_data *) malloc(sizeof(sum_data)); 

	sm_data->in_pipe = &inPipe;
	sm_data->out_pipe = &outPipe;
	sm_data->computeMe = &computeMe;

	pthread_attr_t attr;
	pthread_attr_init(&attr);
    int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
	if (det){
		cout<<"Some issue with setting joinable"<<endl;
	}
	else{
		cout<<"Thread attr set to joinable"<<endl;
	}
	pthread_create (&worker, &attr, sum_worker, (void*) sm_data);
}
void Sum::WaitUntilDone () { 
	pthread_join (worker, NULL);
}
void Sum::Use_n_Pages (int n) { 

}

/**
    Struct for the Group By worker thread
*/
struct group_by_data{
	Pipe *in_pipe;
	Pipe *out_pipe;
	OrderMaker *groupAtts;
	Function *computeMe;
};

/**
    Function that the worker thread of group_by calls when spawned
    @param arg Pointer to the struct that contains data that the worker needs to use to generate output
    @return void. 
*/
void *group_by (void *arg) {
	cout<<"in: group by"<<endl;
	struct group_by_data * input_args;
	input_args = (struct group_by_data *)arg;
	
	int count = 0;
	int intres = 0;
	int finIntres = 0;
	double dobres = 0.0;
	double finDobres = 0.0;

	Record *tempRec;
	Record outrec;
	tempRec = &outrec;

	Attribute IA = {"int", Int};
	Attribute SA = {"string", String};
	Attribute DA = {"double", Double};

	Record pushThis;
	Record prev;

	Schema mySchema ("catalog", "supplier");
	
	Pipe bq_out(100);
	ComparisonEngine ceng;
	int runLen = 1;
	Record sum;
	BigQ bq(*input_args->in_pipe, bq_out, *input_args->groupAtts, runLen);
	cout <<"------------------------------->"<<input_args->groupAtts->sch->GetNumAtts()<<endl;
	input_args->groupAtts->Print();
	// while(bq_out.Remove(tempRec)==1) {
	// 	count ++;
	// 	cout <<"Removing record "<<count<<" from big q" <<endl;
	// 	input_args->out_pipe->Insert(tempRec);
	// }

	
	Attribute *inp = input_args->computeMe->sch->GetAtts();
	int schemaAtts = input_args->computeMe->sch->GetNumAtts();
	int newAtts = schemaAtts + 1;

	Attribute* newSchemaAttsInt;
	newSchemaAttsInt = new Attribute[newAtts]; 
	newSchemaAttsInt[0] = IA;

	Attribute* newSchemaAttsDoub;
	newSchemaAttsDoub = new Attribute[newAtts]; 
	newSchemaAttsDoub[0] = DA;

	for (int it = 1; it < newAtts; it++) {
		newSchemaAttsDoub[it] = inp[it-1];
		newSchemaAttsInt[it] = inp[it-1];
	}

	int cnt = 0;
	while(bq_out.Remove(tempRec)==1) {
		cnt++;
		cout<<"inside bigq remove of groupby "<<cnt<<endl;
		// tempRec->Print(&mySchema);
		if(count ==0){
			prev.Copy(tempRec);
			input_args->computeMe->Apply(*tempRec, intres, dobres);
			finIntres = finIntres + intres;
			
			finDobres = finDobres + dobres;
			cout << "finDobres="<<finDobres<<"  finIntres="<<finIntres<<endl;
			count++;
			continue;
		}
		//if the records can be grouped add them
		if(ceng.Compare(&prev, tempRec, input_args->groupAtts)==0) {
			input_args->computeMe->Apply(*tempRec, intres, dobres);
			finIntres = finIntres + intres;
			
			finDobres = finDobres + dobres;
			cout << "finDobres="<<finDobres<<"  finIntres="<<finIntres<<endl;
			count++;
		} else {
		//if cannot be grouped, create record of the current sum and push to the outpipe.	
			count++;
			Record sum_rec;
			if (finIntres!=0 && finDobres == 0.0){
				
				Schema sum_sch("sum_sch",1, &IA);
				// Schema sum_sch ("sum_sch", newAtts, newSchemaAttsInt);
				stringstream ss;
    			ss << finIntres;
				// char * intermediateStr = ss.str();
				// ss << prev.bits;
				const char* str = (ss.str()+"|").c_str();
				sum_rec.ComposeRecord(&sum_sch,str);
				// sum_rec.Print(&sum_sch);

				finIntres = 0;
				// string temp = ss.str();
    			// string rec_bits(prev.bits);
				// const char* str = (temp + rec_bits).c_str();
				
				// string final = (temp + rec_bits).c_str();
				// cout << "record int= "<<str<<endl;
				// sum.ComposeRecord(&sum_sch,str);
				// sum.Print(&sum_sch);
			}
			if (finIntres==0 && finDobres != 0.0){
				// Schema sum_sch ("sum_sch", newAtts, newSchemaAttsDoub);
				
				Schema sum_sch("sum_sch",1, &DA);
				// cout << "------------------>";
				// OrderMaker dummy(&sum_sch);
				// dummy.Print();
				stringstream ss;
    			ss << finDobres;
				const char* str = (ss.str()+"|").c_str();
				sum_rec.ComposeRecord(&sum_sch,str);
				// sum_rec.Print(&sum_sch);
				cout << "-------------------->"<<endl;
				finDobres = 0.0;
				// string temp = ss.str();
    			// string rec_bits = prev.returnRecord(input_args->computeMe->sch);
				// // ss << prev.bits;
				// string fin = temp + "|" + rec_bits;
				// const char* str = fin.c_str();
    			// // const char* str = ss.str().c_str();
				// cout << "record double= "<<str<<endl;
				// // <<"  record bits"<<rec_bits <<endl;
				// sum.ComposeRecord(&sum_sch,str);
				// // sum.Print(&sum_sch);
			}
			
			prev.Copy(tempRec);	
			input_args->computeMe->Apply(*tempRec, intres, dobres);
			finIntres = finIntres + intres;
			
			finDobres = finDobres + dobres;
			cout << "finDobres="<<finDobres<<"  finIntres="<<finIntres<<endl;

			input_args->out_pipe->Insert(&sum_rec);
		}
		cout << "Final group by count ="<<count<<endl; 
	}
	Record lastrec;
	if(finDobres != 0.0 && finIntres == 0) {
		Schema sum_sch("sum_sch",1, &DA);
		stringstream ss;
    	ss << finDobres;
		const char* str = (ss.str()+"|").c_str();
		lastrec.ComposeRecord(&sum_sch,str);
		input_args->out_pipe->Insert(&lastrec);
	}
	else if(finDobres == 0.0 && finIntres != 0) {
		Schema sum_sch("sum_sch",1, &IA);
		stringstream ss;
    	ss << finIntres;
		const char* str = (ss.str()+"|").c_str();
		lastrec.ComposeRecord(&sum_sch,str);
		input_args->out_pipe->Insert(&lastrec);
	}
	
	
	input_args->out_pipe->ShutDown();
}

/***/

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) { 
	
	struct group_by_data *gb_data;
	gb_data = (group_by_data *) malloc(sizeof(group_by_data)); 
	gb_data->in_pipe = &inPipe;
	gb_data->out_pipe = &outPipe;
	gb_data->groupAtts = &groupAtts;
	gb_data->computeMe = &computeMe;
	
	pthread_attr_t attr;
	pthread_attr_init(&attr);
    int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
	if (det){
		cout<<"Some issue with setting joinable"<<endl;
	}
	else{
		cout<<"Thread attr set to joinable in groupby"<<endl;
	}
	

	// ipR, *input_args->opR, input_args->right, 1);
	pthread_create (&worker, &attr, group_by, (void*) gb_data);
}
void GroupBy::WaitUntilDone () { 
	cout<<"in: GroupBy::WaitUntilDone "<<endl;
	pthread_join (worker, NULL);
}
void GroupBy::Use_n_Pages (int n) { 

}


/**
    Struct for the Write Out worker thread
*/
struct writeout_data{
	Pipe *in_pipe;
	FILE *outFile;
	Schema *mySchema;
};


/**
    Function that the worker thread of write_out calls when spawned
    @param arg Pointer to the struct that contains data that the worker needs to use to generate output
    @return void. 
*/
void *write_out_worker (void *arg) {
	cout<<"in: project_data_worker"<<endl;
	struct writeout_data * input_args;
	input_args = (struct writeout_data *)arg;
	
	Record * tempRec;
	Record outrec;
	tempRec = &outrec;
	Record pushThis;
	ComparisonEngine ceng;

	string rec;
	Page dummy;
	int count = 0;
	const char * c;
	while (input_args->in_pipe->Remove(tempRec)==1) {
		rec = tempRec->returnRecord(input_args->mySchema);
		c= rec.c_str();
		fprintf(input_args->outFile, c);
		count++;

	}
}
/**
    Run function for WriteOut.
	Spawns a worker thread and returns back to the caller.

    @param inPipe Pipe from which we pick records from
	@param outPipe Pipe to which we write records to
	@param mySchema Schema object which needs to be used to compare records
    @return void. 
*/
void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { 
	struct writeout_data *wo_data;
	wo_data = (writeout_data *) malloc(sizeof(writeout_data)); 
	wo_data->in_pipe = &inPipe;
	wo_data->outFile = outFile;
	wo_data->mySchema = &mySchema;

	pthread_attr_t attr;
	pthread_attr_init(&attr);
    int det = pthread_attr_setdetachstate(&attr,PTHREAD_CREATE_JOINABLE);
	if (det){
		cout<<"Some issue with setting joinable"<<endl;
	}
	else{
		cout<<"Thread attr set to joinable"<<endl;
	}
	pthread_create (&worker, &attr, write_out_worker, (void*) wo_data);
}
void WriteOut::WaitUntilDone () { 
	pthread_join (worker, NULL);
}
void WriteOut::Use_n_Pages (int n) { 

}

