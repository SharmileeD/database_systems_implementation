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
	cout<< "Number of pages added "<< counter<< endl;
}
int test_open(){
	Page newPage;
	char myfname[] = "lee.txt";
	Record temp;
	Schema mySchema ("catalog", "lineitem");
	DBFile dbfile;
	Record new_rec;
	const char * fname;
	fname = myfname;
	dbfile.Open(fname);
	cout<<"Ho jaa bhai" << endl;
	cout << dbfile.file_instance.GetLength() << endl;
	int val = dbfile.buffer_page.GetFirst(&new_rec);

	cout<<"Ho jaa " << val << endl;
	new_rec.Print(&mySchema);
	// cout << "Opened 1" << endl;
	// newPage = dbfile.buffer_page;
	// cout << "Opened 2" << endl;
	// newPage.GetFirst(&temp);
	// cout << "Opened 3" << endl;
        // temp.Print(&mySchema);

	cout << "Opened 4" << endl;
	return 0;
	
}
int test_meta_data(){
    FILE * fp2;
    off_t target = 0;
    fp2 = fopen("l_page.txt", "r");
    fread(&target, sizeof(off_t),1, fp2);
    cout << "Off t variable incremented" << endl;
    cout<< target<<endl;
    fclose(fp2);
    
    return(0);
}

int main () {
	//test_add();
	//test_meta_data();
	test_open();
	cout << "Print" << endl;
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
