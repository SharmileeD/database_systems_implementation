#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <unordered_map>
#include <string>

using namespace std;
// typedef unordered_map<char*, int> attributeMap;
// typedef unordered_map<char *, pair<int, attributeMap>>  relationMap;
struct  relation_struct{
	int num_tuples;
	unordered_map<char*, int> innerMap;
};

class Statistics
{
	public:
	// struct relation_struct * rel_struct;
	unordered_map<char *, struct relation_struct>  relationMap;	
	// unordered_map<int , int> relationMap;
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
	
	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

};

#endif
