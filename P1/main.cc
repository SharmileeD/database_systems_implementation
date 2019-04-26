
#include <iostream>
#include "ParseTree.h"
#include "Tree.h"
#include <vector>

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}
extern struct TableList *tables;
extern struct NameList *groupingAtts; //groupby
extern struct AndList *boolean;
extern  struct NameList *attsToSelect; //projection
extern struct FuncOperator *finalFunction; // NULL if Sum not present
extern int distinctAtts; //only distinct
extern int distinctFunc; //Sum and distinct


vector <OrList*> joinVector;
vector <OrList*> selectionVector;

//Store selection and join in separate vectors
void separateJoinSelection() {
	struct AndList *currAnd = boolean;
	while(currAnd) {
		struct OrList *currOr = currAnd->left;
		int llcode = currOr->left->left->code;
        int lrcode = currOr->left->right->code;
		while(currOr) {
			// its a join
			if(llcode == NAME && lrcode == NAME) 
				joinVector.push_back(currOr);
			// its a selection	
			else 
				selectionVector.push_back(currOr);
		}//or while
	}//and while
}

// helper class to enumerate the permutations of the joins Dynamic programming


// class for node of the tree
// tree class with all operations

// helper to print query plan


int main () {
	cout<<"Enter the query"<<endl;
	yyparse();
	// separateJoinSelection();

	cout<<"Table name "<<tables->tableName<<endl;
	cout<<"Name list grouping atts "<<groupingAtts->name<<endl;
	cout<<"Name list attsToSelect "<<attsToSelect->name<<endl;
	cout<<"Distinct Atts "<<distinctAtts<<endl;
	cout<<"Distinct Funcs "<<distinctFunc<<endl;

}


