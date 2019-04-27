
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "TreeNode.h"
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



//Store selection and join in separate vectors
void separateJoinSelection(vector <OrList*> &joinVector, vector <OrList*> &selectionVector) {
	struct AndList *currAnd = boolean;
	while(currAnd) {
		struct OrList *currOr = currAnd->left;
		int llcode = currOr->left->left->code;
        int lrcode = currOr->left->right->code;
		while(currOr) {
			// its a join
			if(llcode == NAME && lrcode == NAME) {
				joinVector.push_back(currOr);
			}	
			// its a selection	
			else {
				selectionVector.push_back(currOr);
			}
			currOr = currOr->rightOr;
		}//or while
		currAnd = currAnd->rightAnd;
	}//and while
}

// helper class to enumerate the permutations of the joins Dynamic programming
void getOptimalJoinSequence() {
	//TODO
}
 
void calculateCost() {

}

// helper to print query plan
void printQueryPlan() {
	
}

// class for node of the tree
// tree class with all operations

int main () {
	cout<<"Enter the query"<<endl;
	yyparse();
	vector <OrList*> joinVector;
	vector <OrList*> selectionVector;
	separateJoinSelection(joinVector, selectionVector);
	cout<<"size of join "<< joinVector.size()<<endl;
	cout<<"size of selection "<< selectionVector.size()<<endl;

	cout<<"Table name "<<tables->tableName<<endl;
	// cout<<"Name list grouping atts "<<groupingAtts->name<<endl;
	cout<<"Name list attsToSelect 1 "<<attsToSelect->name<<endl;
	cout<<"Name list attsToSelect 2 "<<attsToSelect->next->name<<endl;
	if(attsToSelect->next->next == NULL){
		cout << "only two atts present"<<endl;
	}
	cout<<"Distinct Atts "<<distinctAtts<<endl;
	cout<<"Distinct Funcs "<<distinctFunc<<endl;
	cout<<"Distinct Funcs "<<boolean->left->left->code<<endl;
	
	if (finalFunction == NULL){
		cout<<"NO SUM PRESENT"<<endl;
	}
	else{
		cout<<"SUM PRESENT"<<endl;
	}

	Statistics s;
	char *relName[] = { "part",  "partsupp","supplier"};

	s.AddRel(relName[0],200000);
	s.AddAtt(relName[0], "p_partkey",200000);
	s.AddAtt(relName[0], "p_name", 199996);

	s.AddRel(relName[1],800000);
	s.AddAtt(relName[1], "ps_partkey",200000);
	s.AddAtt(relName[1], "ps_suppkey",10000);
	
	s.AddRel(relName[2],10000);
	s.AddAtt(relName[2], "s_suppkey",10000);

	s.CopyRel("part","p");
	s.CopyRel("partsupp","ps");
	s.CopyRel("supplier","s");
	
	struct AndList A;
	struct AndList *tempAnd = &A;

	cout<<"selection vector size"<<endl;
	cout<<selectionVector[0]->left->left->value<<endl;
	if (tempAnd->left==NULL){
		cout<<"selection vector size"<<endl;

	}
	tempAnd->left = selectionVector[0];
	tempAnd->rightAnd = NULL;
	double res = s.Estimate(tempAnd, relName, 3);
	cout<< res<<endl;
	TreeNode n;
	Join_node j;
	Project_node p;
	p.left_child = &j;
	
	// tempAnd->left = 
	// cout<< res<<endl;


	cout<<"Table name "<<tables->tableName<<endl;
	if (groupingAtts!=NULL){
		cout<<"Name list grouping atts "<<groupingAtts->name<<endl;
	}
	
	cout<<"Name list attsToSelect "<<attsToSelect->name<<endl;
	cout<<"Distinct Atts "<<distinctAtts<<endl;
	cout<<"Distinct Funcs "<<distinctFunc<<endl;
}


