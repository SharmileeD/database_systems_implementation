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

void test_select_pipe(){
	cout<<"test_select_pipe start"<<endl;
	Pipe inpipe(100);
	Pipe outPipe(100);
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
	Record * tempRec;
	Record outrec;
	tempRec = &outrec;
	int recs;
	while (outPipe.Remove(tempRec)==1) {
		recs++;
		cout<<"getting records from outpipe"<<endl;
	}	
	cout<<"Got "<< recs<<" records from outpipe"<<endl;
	cout<<"test_select_pipe end"<<endl;


}

int main(){
	cout<<"Main start"<<endl;
	test_select_pipe();
	cout<<"Main end"<<endl;
	return 0;
}