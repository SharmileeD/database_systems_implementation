#include "Statistics.h"
#include <unordered_map>
#include <iostream>
#include <stdlib.h>
using namespace std;

Statistics::Statistics()
{
    // relationMap 

}
Statistics::Statistics(Statistics &copyMe)
{
    for(auto it = copyMe.relationMap.begin(); it != copyMe.relationMap.end(); it++) {
        this->AddRel(it->first, it->second.num_tuples);
		for(auto it2 = 	it->second.innerMap.begin(); it2 != it->second.innerMap.end(); it2++) {
                this->AddAtt(it->first, it2->first, it2->second);
		}
    }

}
Statistics::~Statistics()
{
    // delete relationMap;
}


void Statistics::AddRel(char *relName, int numTuples)
{
    //if new realtion add to the structure
    if(relationMap.find(relName) == relationMap.end()) {
        relation_struct relstruct;
        relstruct.num_tuples = numTuples;
        relationMap.insert({relName, relstruct});
    }
    //if relation already exists update the value 
    else {
        relationMap.at(relName).num_tuples = numTuples;
    }

}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
     //if new relation add a new entry with numTuples = numDistincts
     if(relationMap.find(relName) == relationMap.end()){
        relation_struct relstruct;
        relstruct.num_tuples = numDistincts;
        relstruct.innerMap.insert({attName, numDistincts});
        relationMap.insert({relName, relstruct});
    } 
    //if relation aready exists
    else {
        // relation_struct restruct = this->relationMap.find(relName)->second; 
        //if its a new attribute
        if(this->relationMap.find(relName)->second.innerMap.find(attName) == this->relationMap.find(relName)->second.innerMap.end())
        {
            if(numDistincts != -1)
                this->relationMap.find(relName)->second.innerMap.insert({attName, numDistincts});
            else
                this->relationMap.find(relName)->second.innerMap.insert({attName, this->relationMap.find(relName)->second.num_tuples});
            
        }
        //if the attribute is  already present, update distinctValues
        else
        {
            if(numDistincts != -1)
                this->relationMap.find(relName)->second.innerMap.at(attName)= this->relationMap.find(relName)->second.num_tuples;
                // insert({attName, numDistincts});
            else
                this->relationMap.find(relName)->second.innerMap.at(attName)= numDistincts;
        }
        

    }
}
void Statistics::CopyRel(char *oldName, char *newName)
{
    relation_struct new_relation;
    std::unordered_map<char*,relation_struct>::const_iterator got = this->relationMap.find (oldName);

    if ( got != this->relationMap.end() ){
        new_relation = got->second;
        this->relationMap.insert({newName, new_relation});
    }
}
	
void Statistics::Read(char *fromWhere)
{
}
void Statistics::Write(char *fromWhere)
{
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
}

