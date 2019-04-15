#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <unordered_map>
#include <string>
#include <vector>
using namespace std;
// typedef unordered_map<char*, int> attributeMap;
// typedef unordered_map<char *, pair<int, attributeMap>>  relationMap;
struct  relation_struct{
	double num_tuples;
	unordered_map<string, double> innerMap;
};

class Statistics
{
	public:
	// struct relation_struct * rel_struct;
	int partition_id = 0;
	unordered_map<string, struct relation_struct>  relationMap;	
	unordered_map<string, int> relToId;
	unordered_map<int, vector<string>> idToRel;
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

// int Statistics::partition_id = 0;

#endif
