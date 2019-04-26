
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}
extern struct TableList *tables;
extern struct NameList *groupingAtts;
extern struct AndList *boolean;
extern  struct NameList *attsToSelect;
extern struct FuncOperator *finalFunction;
extern int distinctAtts;
extern int distinctFunc;

int main () {
	cout<<"Enter the query"<<endl;
	yyparse();
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

	double res = s.Estimate(boolean, relName, 3);
	cout<< res<<endl;

}


