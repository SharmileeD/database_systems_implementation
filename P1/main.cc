
#include <iostream>
#include "ParseTree.h"

using namespace std;

extern "C" {
	int yyparse(void);   // defined in y.tab.c
}
extern struct TableList *tables;
extern struct NameList *groupingAtts;
extern struct AndList *boolean;
extern  struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

int main () {
	cout<<"Enter the query"<<endl;
	yyparse();
	cout<<"Table name "<<tables->tableName<<endl;
	cout<<"Name list grouping atts "<<groupingAtts->name<<endl;
	cout<<"Name list attsToSelect "<<attsToSelect->name<<endl;
	cout<<"Distinct Atts "<<distinctAtts<<endl;
	cout<<"Distinct Funcs "<<distinctFunc<<endl;

}


