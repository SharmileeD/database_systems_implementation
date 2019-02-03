#include <iostream>
#include "Record.h"
#include "DBFile.h"
#include <stdlib.h>

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;



void test_add(){
	// try to parse the CNF
	cout << "Enter in your CNF: ";
  	if (yyparse() != 0) {
		cout << "Can't parse your CNF.\n";
		exit (1);
	}

	// suck up the schema from the file
	Schema lineitem ("catalog", "lineitem");

	// grow the CNF expression from the parse tree 
	CNF myComparison;
	Record literal;
	myComparison.GrowFromParseTree (final, &lineitem, literal);
	
	// print out the comparison to the screen
	myComparison.Print ();

	// now open up the text file and start procesing it
        FILE *tableFile = fopen ("tables/lineitem.tbl", "r");

        Record temp;
        Schema mySchema ("catalog", "lineitem");

	//char *bits = literal.GetBits ();
	//cout << " numbytes in rec " << ((int *) bits)[0] << endl;
	//literal.Print (&supplier);

        // read in all of the records from the text file and see if they match
	// the CNF expression that was typed in

	ComparisonEngine comp;
	DBFile newFile;
        char myfname[] = "lee.txt";
        fType heap = heap;
        void* ptr;
        newFile.Create(myfname, heap, &ptr);
        newFile.Open(myfname);
       	
	int counter = 0;
	temp.SuckNextRecord (&mySchema, tableFile);
        while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
		counter++;
		if (counter % 10000 == 0) {
			cerr << counter << "\n";
		}

		if (comp.Compare (&temp, &literal, &myComparison)) {
			newFile.Add(temp);
			
                	//temp.Print (&mySchema);
		        // newPage.Append(&temp);	
			// //newPage.GetFirst(&temp);
		}

        }
	newFile.Close();

}
int test_open(){
	Page newPage;
	char* myfname = "lee.bin";
	Record temp;
	Schema mySchema ("catalog", "lineitem");
	DBFile dbfile;
	Record new_rec;
	dbfile.Open(myfname);
	//cout << dbfile.file_instance.GetLength() << endl;
	int val = dbfile.buffer_page.GetFirst(&new_rec);
	new_rec.Print(&mySchema);
	cout << "Opened 4" << endl;
	dbfile.Close();
	return 0;
	
}

int test_meta_data(){
	FILE * fp2;
	off_t target = 0;
	fp2 = fopen("lee_lpage.txt", "r");
	fread(&target, sizeof(off_t),1, fp2);
	cout << "L Page" << endl;
	cout<< target<<endl;
	fclose(fp2);

	fp2 = fopen("lee_dpage.txt", "r");
	fread(&target, sizeof(off_t),1, fp2);
	cout << "D page " << endl;
	cout<< target<<endl;
	fclose(fp2);

	return(0);


}

int test_move_first(){
	cout << "Inside test_move_first" << endl;
	Page newPage;
	char * myfname = "lee.bin";
	Schema mySchema ("catalog", "lineitem");
	DBFile dbfile;
	Record new_rec;
	dbfile.Open(myfname);
	dbfile.current_page = 40;
	dbfile.record_offset = 0;
	cout<< "Old Current page: " << dbfile.current_page << endl;
	cout << "Old Record Offset: " << dbfile.record_offset << endl;
	dbfile.MoveFirst();
	cout<< "Done with move first" << endl;
	dbfile.Close();
	return 1;
	
}

int test_get_next(){
	cout << "Inside test_move_first" << endl;
	char myfname[] = "lee.bin";
	Schema mySchema ("catalog", "lineitem");
	DBFile dbfile;
	const char * fname;
	fname = myfname;
	dbfile.Open(fname);
	dbfile.MoveFirst ();

	dbfile.current_page = 0;
	dbfile.record_offset = 604;
	Record next_rec;
	dbfile.GetNext(next_rec);
	cout << "Get next is done" << endl;
	cout << "Current page after getnext (should be 1)" << dbfile.current_page << endl;
	cout << "record offset after getnext (should be 0)" << dbfile.record_offset << endl;
	next_rec.Print(&mySchema);
	cout << "Print done" << endl;
	dbfile.Close();
	return 0;
}
void test_load(){
	DBFile dbfile;
	char myfname[] = "lee.bin";
    fType heap = heap;
    void* ptr;
    dbfile.Create(myfname, heap, &ptr);
    dbfile.Open(myfname);

	Schema mySchema ("catalog", "lineitem");
	dbfile.Load(mySchema, "tables/lineitem.tbl");
	dbfile.Close();
}
int main () {
	//test_add();
	//test_open();
	//test_move_first();
	test_get_next();
	//test_load();
	//test_meta_data();

	cout << "Done Testing" << endl;
	// Schema mySchema ("catalog", "lineitem");
        // File newFile;
	// File a;
        // char myfname[] = "lee.txt";	
        // // newFile.Open(0,myfname);
	// // newFile.AddPage(&newPage, 0);	
        // // newFile.AddPage(&newPage, 1);
        // Page dums;
        // newFile.Close();
        // a.Open(100,myfname);
        // a.GetPage(&dums, 7);
        // while(dums.GetFirst(&temp) != 0)
	// 	temp.Print(&mySchema);	

	
	// newFile.Close();
}
