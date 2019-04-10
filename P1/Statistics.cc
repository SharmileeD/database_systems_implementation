#include "Statistics.h"
#include <unordered_map>
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <string>
#include <bits/stdc++.h> 
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
    if(this->relationMap.find(relName) == this->relationMap.end()) {
        relation_struct relstruct;
        relstruct.num_tuples = numTuples;
        this->relationMap.insert({relName, relstruct});
    }
    //if relation already exists update the value 
    else {
        this->relationMap.at(relName).num_tuples = numTuples;
    }

}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
     //if new relation add a new entry with numTuples = numDistincts
     if(this->relationMap.find(relName) == this->relationMap.end()){
        relation_struct relstruct;
        relstruct.num_tuples = numDistincts;
        relstruct.innerMap.insert({attName, numDistincts});
        this->relationMap.insert({relName, relstruct});
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
    FILE *readfile;
    if((readfile = fopen (fromWhere, "r"))== NULL)
    {
        cout << "File is empty"<<endl;
    }
    else 
    {
        string line;
        size_t pos;
        string token, temp, numTuples, numDistinctVals;
        int num_tuples;
        char *carr;
        // fclose(readfile);
        ifstream file(fromWhere);
        
        while(getline(file,line)) {

            string delim = "|";
            //read relation name
            pos = line.find(delim);
            temp = line.substr(0, pos);
            char * relname = new char[temp.length()+1] ;
            // cout << "temp="<<temp<<endl;
            strcpy(relname, temp.c_str());
            // cout << "temp="<<temp << "  relname="<<relname<<endl;
            line.erase(0,pos+1);

            // read num of tuples of relation
            pos = line.find(delim);
            numTuples = line.substr(0, pos);
            line.erase(0,pos+1);
                                
            this->AddRel(relname, stoi(numTuples));
           
           //read the sttributes and their distinct values.
            while((pos =line.find(delim) ) != string::npos) {
                temp = line.substr(0, line.find(delim));
                line.erase(0,pos+1);
                char *attr = new char[temp.length()+1];
                strcpy(attr, temp.c_str());
                
                pos = line.find(delim);
                numDistinctVals = line.substr(0, pos);
                line.erase(0,pos+1);
                
                // cout <<"attr:"<< attr<<" distinctval:"<<numDistinctVals <<endl;

                this->AddAtt(relname, attr, stoi(numDistinctVals));

            }//while
           

           
        }
        // for(auto it = this->relationMap.begin(); it != this->relationMap.end(); it++) {
        //     cout << (*it).first <<", " << (*it).second.num_tuples <<endl;
        //     cout << "size of iiner map:" << it->second.innerMap.size()<<endl;
		//     for(auto it2 = 	it->second.innerMap.begin(); it2 != it->second.innerMap.end(); it2++) 
		// 		cout <<" ----" <<(*it2).first <<", " << (*it2).second <<endl;
        // }
    }
    

}
void Statistics::Write(char *fromWhere)
{
    FILE *writefile = fopen (fromWhere, "w");
    const char *ch;
    for(auto relation_itr = this->relationMap.begin(); relation_itr != this->relationMap.end(); relation_itr++) {
        // ch = (*relation_itr).first;
        fprintf(writefile, (*relation_itr).first);
        fprintf(writefile, "|");
        ch = to_string((*relation_itr).second.num_tuples).c_str();
        fprintf(writefile, ch);
        fprintf(writefile, "|");
        for(auto attr_itr = relation_itr->second.innerMap.begin(); attr_itr != relation_itr->second.innerMap.end(); attr_itr++) {
            
            fprintf(writefile, (*attr_itr).first);
            fprintf(writefile, "|");
            ch = to_string((*attr_itr).second).c_str();
            fprintf(writefile, ch);
            fprintf(writefile, "|");
        }
        fprintf(writefile, "\n");

    }
    fclose(writefile);
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
}

