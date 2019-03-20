#include "test.h"
#include "BigQ.h"
#include <pthread.h>
#include <unistd.h>
#include "Record.h"
#include <vector>
#include <algorithm>    
#include "Schema.h"
#include "Pipe.h"
#include "File.h"
#include "pthread.h"
#include "DBFile.h"
#include <array>
#include <iostream>
#include <queue>
#include "RelOp.h"

void test_select_pipe_and_project(){
	cout<<"test_select_pipe_and_project start"<<endl;
	Pipe inpipe(100);
	Pipe outPipe(100);
	Pipe outPipeProject(100);
	Schema mySchema ("catalog", "nation");
	cout << "Enter in your CNF: ";
  	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit (1);
	}
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &mySchema, literal);
	myComparison.Print ();
	// suck up the schema from the file
	
	int res;
	Record temp;
	FILE *tblfile = fopen ("tables/nation.tbl", "r");
    int count =0;
	while ((res = temp.SuckNextRecord (&mySchema, tblfile))) {
        inpipe.Insert(&temp);
		count++;

	}
	cout<<"Added "<<count<<" records to inpipe"<<endl;
	inpipe.ShutDown();
	// grow the CNF expression from the parse tree 
	

	SelectPipe sp;
	sp.Run(inpipe, outPipe, myComparison,literal);
	sp.WaitUntilDone();
	
	int keepMe[] = {0,1};
	int numAttsIn = 4;
	int numAttsOut = 2;
	Project p;
	p.Run(outPipe, outPipeProject, keepMe, numAttsIn, numAttsOut);
	p.WaitUntilDone();
	Record * tempRec;
	Record outrec;
	tempRec = &outrec;
	int recs;
	while (outPipeProject.Remove(tempRec)==1) {
		recs++;
		// tempRec->Print(&mySchema);
		// cout<<"getting records from outpipe"<<endl;
	}	
	cout<<"Got "<< recs<<" records from outpipe"<<endl;
	cout<<"test_select_pipe end"<<endl;


}

void test_sum(){
	Pipe inpipe(100);
	Pipe outPipe(100);
	Pipe outPipeProject(100);
	Schema mySchema ("catalog", "partsupp");
	// fillInputPipe(&inpipe, &mySchema);
	
	Sum T;
		// _s (input pipe)
		Pipe _out (1);
		Function func;
			char *str_sum = "(ps_supplycost)";
			get_cnf (str_sum, &mySchema, func);
			func.Print ();
	T.Use_n_Pages (1);

	Pipe _s_ps (100);
	T.Run (inpipe, _out, func);
	int res;
	Record temp;
	FILE *tblfile = fopen ("tables/partsupp.tbl", "r");
    int count =0;
	while ((res = temp.SuckNextRecord (&mySchema, tblfile))) {
		inpipe.Insert(&temp);
		count++;

	}
	cout<<"Added "<<count<<" records to inpipe"<<endl;
	inpipe.ShutDown();
	T.WaitUntilDone ();
}

void test_duplicate_removal(){
	cout<<"test_select_pipe_and_project start"<<endl;
	Pipe inpipe(100);
	Pipe outPipe(100);
	Pipe outPipeProject(100);
	Schema mySchema ("catalog", "nation");
	
	// suck up the schema from the file
	
	int res;
	Record temp;
	FILE *tblfile = fopen ("tables/nation.tbl", "r");
    int count =0;
	while ((res = temp.SuckNextRecord (&mySchema, tblfile))) {
        inpipe.Insert(&temp);
		count++;

	}
	Record temp2;
	FILE *tblfile2 = fopen ("tables/nation.tbl", "r");
	cout<<"Added "<<count<<" records to inpipe after first while loop"<<endl;
	while ((res = temp2.SuckNextRecord (&mySchema, tblfile2))) {
        inpipe.Insert(&temp2);
		count++;

	}
	inpipe.ShutDown();
	cout<<"Added "<<count<<" records to inpipe"<<endl;
	DuplicateRemoval dr;
	dr.Run(inpipe, outPipe, mySchema);
	dr.WaitUntilDone();
}
int main(){
	cout<<"Main start"<<endl;
	// test_select_pipe_and_project();
	test_duplicate_removal();
	cout<<"Main end"<<endl;
	return 0;
}