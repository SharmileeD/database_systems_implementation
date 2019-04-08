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
}
Statistics::~Statistics()
{
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

