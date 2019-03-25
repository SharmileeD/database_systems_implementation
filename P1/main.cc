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
#include "test.h"
#include <array>
#include "Function.h"
#include <queue>
#include "RelOp.h"
using namespace std;

Attribute IA = {"int", Int};
Attribute SA = {"string", String};
Attribute DA = {"double", Double};
int pAtts = 9;
int psAtts = 5;
int liAtts = 16;
int oAtts = 9;
int sAtts = 7;
int cAtts = 8;
int nAtts = 4;
int rAtts = 3;


int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
	Record rec;
	cout<< "Inside clear pipe!!"<<endl;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		cnt++;
	}
	// rec.Print (schema);
	return cnt;
}

int clear_pipe1 (Pipe &in_pipe,  bool print) {
	Record rec;
	cout<< "============================================Inside clear pipe!!"<<endl;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		// if (print) {
			// rec.Print (schema);
		cout << "Removing record = "<<cnt<<endl;	
		// }
		cnt++;
	}
	// rec.Print (schema);
	return cnt;
}


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
	Schema mySchema ("catalog", "nation");
	// fillInputPipe(&inpipe, &mySchema);
	
	Sum T;
		// _s (input pipe)
		Pipe _out (1);
		Function func;
			char *str_sum = "(n_nationkey)";
			get_cnf (str_sum, &mySchema, func);
			func.Print ();
	T.Use_n_Pages (1);

	Pipe _s_ps (100);
	T.Run (inpipe, _out, func);
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
	T.WaitUntilDone ();
	Record * tempRec;
	Record outrec;
	tempRec = &outrec;
	int recs;
	while (_out.Remove(tempRec)==1) {
		recs++;
		// tempRec->Print(&mySchema);
		// cout<<"getting records from outpipe"<<endl;
	}	
	cout<<"Got "<< recs<<" records from outpipe"<<endl;

}

void test_duplicate_removal(){
	cout<<"test_select_pipe_and_project start"<<endl;
	Pipe inpipe(100);
	Pipe outPipe(100);
	Pipe outPipeProject(100);
	Schema mySchema ("catalog", "nation");
	
	// suck up the schema from the file
	string rec;
	int res;
	Record temp;
	FILE *tblfile = fopen ("tables/nation.tbl", "r");
    int count =0;
	while ((res = temp.SuckNextRecord (&mySchema, tblfile))) {
        rec= temp.returnRecord(&mySchema);
		inpipe.Insert(&temp);
		cout<< rec<<endl;
		count++;

	}
	Record temp2;
	FILE *tblfile2 = fopen ("tables/nation.tbl", "r");
	cout<<"Added "<<count<<" records to inpipe after first while loop"<<endl;
	while ((res = temp2.SuckNextRecord (&mySchema, tblfile2))) {
        inpipe.Insert(&temp2);
		count++;

	}
	Record temp3;
	FILE *tblfile3 = fopen ("tables/nation.tbl", "r");
	cout<<"Added "<<count<<" records to inpipe after first while loop"<<endl;
	while ((res = temp3.SuckNextRecord (&mySchema, tblfile3))) {
        inpipe.Insert(&temp3);
		count++;

	}
	inpipe.ShutDown();
	cout<<"Added "<<count<<" records to inpipe"<<endl;
	DuplicateRemoval dr;
	dr.Run(inpipe, outPipe, mySchema);
	dr.WaitUntilDone();
}

void test_groupby() {
	Pipe inpipe(100);
	Pipe outPipe(100);
	Pipe outPipeProject(100);
	Schema mySchema ("catalog", "partsupp");
	char *pred_parts = "(ps_suppkey = ps_suppkey)";
	Record lit;
	Record temp;
	int res;
	DBFile db;
	CNF cnf_parts;
	SelectFile sf;

	db.Open("partsupp.bin");
    get_cnf (pred_parts, &mySchema, cnf_parts, lit);
	int count =0;
	
	GroupBy G;
	Pipe _out (1);
	Function func;
	char *str_sum = "(ps_supplycost)";
	get_cnf (str_sum, &mySchema, func);

	OrderMaker grp_order(&mySchema);
	grp_order.Print();
	sf.Run(db, inpipe, cnf_parts, lit);
	// int cnt_parts = clear_pipe1 (parts, &mySchemaP, false);
	
	G.Run(inpipe,outPipe,grp_order,func);
	int cnt_parts = clear_pipe(outPipe, &mySchema, false);
	sf.WaitUntilDone();
	G.WaitUntilDone();

	// cout<<"Added "<<count<<" records to inpipe"<<endl;
}

void test_write_out(){
	Pipe inpipe(100);
	Pipe outPipe(100);
	Pipe outPipeProject(100);
	Schema mySchema ("catalog", "customer");
	// fillInputPipe(&inpipe, &mySchema);
	
	WriteOut W;
		// _s (input pipe)
		
	FILE *writefile = fopen ("outputfile_new.txt", "w");
	Pipe _s_ps (100);
	W.Run (inpipe, writefile, mySchema);
	int res;
	Record temp;
	FILE *tblfile = fopen ("tables/customer.tbl", "r");
    int count =0;
	while ((res = temp.SuckNextRecord (&mySchema, tblfile))) {
		inpipe.Insert(&temp);
		count++;

	}
	cout<<"Added "<<count<<" records to inpipe"<<endl;
	inpipe.ShutDown();
	W.WaitUntilDone ();
}



void test_join() {
	cout << " query4 \n";
	char *pred_s = "(s_suppkey = s_suppkey)";
	char *pred_parts = "(ps_suppkey = ps_suppkey)";
	Schema mySchemaS("catalog","supplier");//left
	Schema mySchemaP("catalog","partsupp");//right
	
	DBFile db1, db2;
	CNF cnf_sup, cnf_parts;
	Record lit_parts, lit_sup, lit_join;
	SelectFile SF_sup, SF_parts;
	Pipe sup1(100);
	Pipe parts(100);
	Pipe op(100);
	Join J;
	OrderMaker left;
	OrderMaker right;
	Record lRec, rRec;
	CNF cnf_join;

	db1.Open("supplier.bin");
	get_cnf (pred_s, &mySchemaS, cnf_sup, lit_sup);
	
	SF_sup.Run (db1, sup1, cnf_sup, lit_sup);
	// int cnt_sup = clear_pipe1 (sup1, &mySchemaS, false);
	sleep(2);
	db2.Open("partsupp.bin");
	get_cnf (pred_parts, &mySchemaP, cnf_parts, lit_parts);
	sleep(2);
	
	SF_parts.Run (db2, parts, cnf_parts, lit_parts);
	// int cnt_parts = clear_pipe (parts, &mySchemaP, false);
	get_cnf ("(s_suppkey = ps_suppkey)", &mySchemaS, &mySchemaP, cnf_join, lit_join);	
	sleep(1);
	J.Run (sup1, parts, op, cnf_join, lit_join);
	int cnt_parts = clear_pipe1 (op, false);

	int outAtts = sAtts + psAtts;
	Attribute ps_supplycost = {"ps_supplycost", Double};
	Attribute joinatt[] = {IA,SA,SA,IA,SA,DA,SA, IA,IA,IA,ps_supplycost,SA};
	Schema join_sch ("join_sch", outAtts, joinatt);

	Sum T;
		// _s (input pipe)
	Pipe _out (1);
	Function func;
	char *str_sum = "(ps_supplycost)";
	get_cnf (str_sum, &join_sch, func);
	// func.Print ();

	// T.Run (op, _out, func);
	
	
	SF_parts.WaitUntilDone();
	SF_sup.WaitUntilDone();

	J.WaitUntilDone ();
	// T.WaitUntilDone();
	
    db1.Close();
	db2.Close();
	 cout << "Records :" << cnt_parts << endl;
	//  cout << "Records read sup:"<<cnt_sup<<endl;
}

int main(){
	cout<<"Main start"<<endl;
	// test_select_pipe_and_project();
	// test_duplicate_removal();
	// test_write_out();
	// test_duplicate_removal();
	// test_sum();
	test_join();
	// test_groupby();
	
	cout<<"Main end"<<endl;
	return 0;
}