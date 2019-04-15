#include "Statistics.h"
#include <unordered_map>
#include <iostream>
#include <fstream>
#include <stdlib.h>
#include <string>
#include <algorithm>
#include <bits/stdc++.h>
#include <vector> 
using namespace std;


// int Statistics::partition_id = 0;
Statistics::Statistics()
{
    // this->idToRel; 
    // this->partition_id = 0;

}
Statistics::Statistics(Statistics &copyMe)
{
    for(auto it = copyMe.relationMap.begin(); it != copyMe.relationMap.end(); it++) {
        char relname [it->first.size()];
        strcpy(relname, it->first.c_str());
        this->AddRel(relname, it->second.num_tuples);
		for(auto it2 = 	it->second.innerMap.begin(); it2 != it->second.innerMap.end(); it2++) {
                char attname [it2->first.size()];
                strcpy(attname, it2->first.c_str());
                this->AddAtt(relname, attname, it2->second);
		}
    }
    // this->relToId =  new unordered_map<string, int>();
    this->relToId.clear();
    for(auto it1 = copyMe.relToId.begin(); it1 != copyMe.relToId.end(); it1++) {
        char relname [it1->first.size()];
        strcpy(relname, it1->first.c_str());
        this->relToId.insert({relname, it1->second});
    }
    this->idToRel.clear();
    
    for(auto it1 = copyMe.idToRel.begin(); it1 != copyMe.idToRel.end(); it1++) {
        vector <string> dummyVec;
        for(auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) 
            dummyVec.push_back((*it2));
        this->idToRel.insert({(*it1).first, dummyVec});
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
        //update maps
        this->relToId.insert({relName, this->partition_id});
        
        vector<string> relVector;
        relVector.push_back(relName);
        
        this->idToRel.insert({this->partition_id, relVector});
        this->partition_id ++;
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
        //update maps
        this->relToId.insert({relName, this->partition_id});

        vector<string> relVector;
        relVector.push_back(relName);

        this->idToRel.insert({this->partition_id, relVector});
        this->partition_id++;
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
            if(numDistincts == -1)
                this->relationMap.find(relName)->second.innerMap.at(attName)= this->relationMap.find(relName)->second.num_tuples;
            else
                this->relationMap.find(relName)->second.innerMap.at(attName)= numDistincts;
        }
        

    }
}
void Statistics::CopyRel(char *oldName, char *newName)
{
    relation_struct new_relation;
    std::string oldNamestr(oldName);
    std::string newNamestr(newName);
    std::unordered_map<string,relation_struct>::const_iterator got = this->relationMap.find (oldNamestr);

    if ( got != this->relationMap.end() ){
        new_relation = got->second;
        this->relationMap.insert({newName, new_relation});
        this->relToId.insert({newNamestr, this->partition_id});
        
        vector<string> relVector;
        relVector.push_back(newNamestr);
        
        this->idToRel.insert({this->partition_id, relVector});
        this->partition_id ++;
    }

}
	
void Statistics::Read(char *fromWhere)
{
    FILE *readfile;
    if((readfile = fopen (fromWhere, "r"))== NULL)
            cout << "File is empty"<<endl;
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
            strcpy(relname, temp.c_str());
            line.erase(0,pos+1);
            //read num of tuples of relation
            pos = line.find(delim);
            numTuples = line.substr(0, pos);
            line.erase(0,pos+1);

            //add relation name and its total no. of tuples in the statistics                  
            this->AddRel(relname, stoi(numTuples));
           
           //read the attributes and their distinct values.
            while((pos =line.find(delim) ) != string::npos) {
                temp = line.substr(0, line.find(delim));
                line.erase(0,pos+1);
                char *attr = new char[temp.length()+1];
                strcpy(attr, temp.c_str());
                
                pos = line.find(delim);
                numDistinctVals = line.substr(0, pos);
                line.erase(0,pos+1);
                
                //add attributes and the distinctVal to the cooresponding map in relation
                this->AddAtt(relname, attr, stoi(numDistinctVals));

            }//while
        }
        fclose(readfile);
        //read the relToId hashmap
        char relToIdFile[100];
        sprintf(relToIdFile, "%s_relToId", fromWhere);
        ifstream rifile(relToIdFile);
        string pid;
        this->relToId.clear();
        while(getline(rifile, line)) {
            string delim = "|";
            pos = line.find(delim);
            temp = line.substr(0,pos);
            char * relname = new char[temp.length()+1] ;
            strcpy(relname, temp.c_str());
            line.erase(0,pos+1);
            //read partition_id    
            pos = line.find(delim);
            pid = line.substr(0, pos);
            line.erase(0,pos+1);
            //insert entry to hashmap relToId
            // cout << "Inside read ->"<< relname << " pid=" <<pid<<endl;
            this->relToId.insert({relname,stoi(pid)});
        }

        //read the idToRel hashmap
        char idToRelFile[100];
        sprintf(idToRelFile, "%s_idToRel", fromWhere);
        ifstream irfile(idToRelFile);
        this->idToRel.clear();
        while(getline(irfile,line)) {
            string delim = "|";
            pos = line.find(delim);
            pid = line.substr(0,pos);
            line.erase(0,pos+1);
            //read all the relations correspnding to the pid
            vector<string> idToRelVector;
            while((pos =line.find(delim) ) != string::npos) {
                temp = line.substr(0,pos);
                char * relname = new char[temp.length()+1] ;
                strcpy(relname, temp.c_str());
                line.erase(0,pos+1);
                //add the relation name to the vector
                idToRelVector.push_back(relname);
            }
            this->idToRel.insert({stoi(pid), idToRelVector});
        }
    }
    
}
void Statistics::Write(char *fromWhere)
{
    FILE *writefile = fopen (fromWhere, "w");
    const char *ch;
    for(auto relation_itr = this->relationMap.begin(); relation_itr != this->relationMap.end(); relation_itr++) {
        // ch = (*relation_itr).first;
        fprintf(writefile, ((*relation_itr).first).c_str());
        fprintf(writefile, "|");
        ch = to_string((*relation_itr).second.num_tuples).c_str();
        fprintf(writefile, ch);
        fprintf(writefile, "|");
        for(auto attr_itr = relation_itr->second.innerMap.begin(); attr_itr != relation_itr->second.innerMap.end(); attr_itr++) {
            
            fprintf(writefile,((*attr_itr).first).c_str());
            fprintf(writefile, "|");
            ch = to_string((*attr_itr).second).c_str();
            fprintf(writefile, ch);
            fprintf(writefile, "|");
        }
        fprintf(writefile, "\n");

    }
    fclose(writefile);
    //write hashmap relToId
    char relToIdFile[100];
    sprintf(relToIdFile, "%s_relToId", fromWhere);
    writefile = fopen(relToIdFile, "w");   
    for(auto hmap1_it = this->relToId.begin(); hmap1_it != this->relToId.end(); hmap1_it++) {
        fprintf(writefile, (*hmap1_it).first.c_str());
        fprintf(writefile, "|");
        ch = to_string((*hmap1_it).second).c_str();
        fprintf(writefile, ch);
        fprintf(writefile, "|");
        fprintf(writefile, "\n");
    }
    fclose(writefile);
    //write hashmap idToRel
    char idToRelFile[100];
    sprintf(idToRelFile, "%s_idToRel", fromWhere);
    writefile = fopen(idToRelFile, "w");   
    for(auto hmap2_it = this->idToRel.begin(); hmap2_it != this->idToRel.end(); hmap2_it++) {
        ch = to_string((*hmap2_it).first).c_str();
        fprintf(writefile, ch);
        fprintf(writefile, "|");
        for(auto it2 = (*hmap2_it).second.begin(); it2 != (*hmap2_it).second.end(); it2++){
            fprintf(writefile,(*it2).c_str());
            fprintf(writefile, "|");
        }
        fprintf(writefile, "\n");
    }
    fclose(writefile);
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    
    double numTuples = 0.0;
    double orResult;
    double andResult = -1.0;
    double n;
    //TODO: Validation of JOIN or not will happen here
    if (this->validateRels(relNames, numToJoin)){

    }

   
   //processing all ANDS
   if(parseTree != NULL) {
              
        struct AndList *currAnd = parseTree;
        string relNameLeft;
        string relNameRight;
        string relName;
       while(currAnd)
       {
            struct OrList *currOr = currAnd->left;
            vector<double> orVector;
            int l, r;
            bool isJoin = false;
            int index = -1;
            while(currOr){
                string llval = currOr->left->left->value;
                string lrval = currOr->left->right->value;
                int llcode = currOr->left->left->code;
                int lrcode = currOr->left->right->code;
                //if both operands are name then JOIN
                
                if(llcode == NAME && lrcode == NAME ) {
                    isJoin = true;
                    int exceptionCount;
                    double totalLeft, totalRight,attLeft, attRight;
                    this->extractValues(llval, relNameLeft, totalLeft, attLeft,numToJoin, relNames);
                    this->extractValues(lrval, relNameRight, totalRight, attRight, numToJoin, relNames);
                    
                    // Initialising the number of tuples of the relation and the number of unique tuples of the attribute
                    
                    // andResult = totalLeft * totalRight;
                    //TODO: handle condition where attribute name is incorrect
                    //if both belong to the same partition then selection else join
                    // if(this->relToId.at(this->relationMap.find(relNameRight)->first) == this->relToId.at(this->relationMap.find(relNameLeft)->first))
                    //     orResult = (double)(totalLeft)/std::max(attLeft,attRight);    
                    // else
                    //     orResult = (double)(totalLeft*totalRight)/std::max(attLeft,attRight);
                    orResult = (double)(totalLeft*totalRight)/std::max(attLeft,attRight);
                    andResult = orResult;
                    //update the Statistics after join
                    int pid = this->relToId.at(relNameLeft); //left relation pid
                    int prev_pid = this->relToId.at(relNameRight); //right relation pid

                    //update partition_id of relations that are in the same partition as the right relation
                    if(pid !=prev_pid){
                        for(auto temp =this->idToRel.at(prev_pid).begin(); temp != this->idToRel.at(prev_pid).end(); temp++ ) {
                            this->relToId.at(*temp) = pid;
                        }
                        this->relToId.at(this->relationMap.find(relNameRight)->first) = pid; 
                        
                        //update relation names in idToRel
                        
                        vector<string> prev_vector = this->idToRel.at(prev_pid);

                        for(auto temp2 =prev_vector.begin(); temp2 != prev_vector.end(); temp2++ ) {
                            // cout<<"temp "<<*temp2<<endl;
                            this->idToRel.at(pid).push_back(*temp2);
                        }
                        //remove the old entry of right side relation name from idToRel

                        // this->printDicts();
                        this->idToRel.erase(prev_pid);
                    }

                    // this->printDicts();
                    this->relationMap.at(relNameLeft).num_tuples = orResult;
                    this->relationMap.at(relNameRight).num_tuples = orResult; //redundant since this happens in loop below

                    // this->printDicts();
                    vector<string> temp_vector = this->idToRel.at(pid);
                    
                    //if relations were already joined, update the relations in the partition for left
                    for(auto outerit = temp_vector.begin(); outerit != temp_vector.end(); outerit++) {
                        this->relationMap.at((*outerit)).num_tuples = orResult;
                        //now update their attributes too
                        for(auto innerit = this->relationMap.at((*outerit)).innerMap.begin(); innerit != this->relationMap.at((*outerit)).innerMap.end(); innerit++) 
                            if((*innerit).second > orResult) 
                                (*innerit).second = orResult;
                            
                    }
                }
                // else SELECTION
                else {
                    // cout<<"Debug prints"<<endl;
                    // this->printDicts();
                    isJoin = false;
                    double numAttrs, numTotal;
                    
                    //if both are literals 
                    if(llcode != NAME && lrcode != NAME){
                        // Error since both are literals 
                    }
                    else {
                        // SET LLVAL LRVAL RELNAME LEFT AND RELNAMERIGHT
                        // Check if left is a NAME
                        if(llcode == NAME){
                            this->extractValues(llval, relName, numTotal, numAttrs, numToJoin, relNames);
                            // for(l = 0; l < numToJoin; l++) {
                            //    if(this->relationMap.find(relNameLeft)->second.innerMap.find(llval) != this->relationMap.find(relNameLeft)->second.innerMap.end()) {
                            //        numAttrs = this->relationMap.at(relNameLeft).innerMap.at(llval);
                            //        numTotal = this->relationMap.find(relNameLeft)->second.num_tuples;
                            //        index = l;
                            //        break;
                            //    }
                            // }
                        }
                        // Check if right is a NAME
                        else if(lrcode == NAME) {
                            this->extractValues(lrval, relName, numTotal, numAttrs, numToJoin, relNames);
                            // for(r = 0; r < numToJoin; r++) {
                            //     if(this->relationMap.find(relNameRight)->second.innerMap.find(lrval) != this->relationMap.find(relNameRight)->second.innerMap.end()) {
                            //         numAttrs = this->relationMap.at(relNameRight).innerMap.at(lrval);
                            //         numTotal = this->relationMap.find(relNameRight)->second.num_tuples;
                            //         index = r;
                            //         break;
                            //     }
                            // }
                        }
                        switch(currOr->left->code) {
                            case LESS_THAN:
                                orResult = (double)numTotal/3.0;
                                break;
                            case GREATER_THAN:
                                orResult = (double)numTotal/3.0;
                                break;
                            case EQUALS:
                                orResult = (double)numTotal/numAttrs;
                                break;    
                        }
                        // cout << "Oresult = "<<orResult<< "numtotal = "<< numTotal <<endl;
                        if(andResult == -1)
                            n = numTotal;
                        else
                            n = andResult;                        
                        //update statistics for selection
                        // this->relationMap.at(relNames[index]).num_tuples = orResult;
                        //if sleceted table was joined, update the values in correspnding relations as well 
                        // int pid = this->relToId.at(relNames[index]);
                            
                        orVector.push_back(orResult);    
                    }//else validation check
                    
                }//else join or selection
                
                currOr = currOr->rightOr;
            }
            //calculate for combined Or
           
           //if not join calculate the combined or.
            if(!isJoin) {
                 double orCombined = 1.0;
                 for(auto it = orVector.begin(); it != orVector.end(); it++) {
                    // cout << "individual or results = " << (*it) <<endl;
                    orCombined = (double)orCombined*(1 - (*it)/n);
                 }
                //  cout << "\n\nCombined OR = " << orCombined<<endl;
                 andResult = (double)n * (1 - orCombined);
                //  cout <<"andResult = "<<andResult<<endl;       
                //update the values in statistics
                 this->relationMap.at(relName).num_tuples = andResult;
                 int pid = this->relToId.at(relName);
                 for(auto outerit = this->idToRel.at(pid).begin(); outerit != this->idToRel.at(pid).end(); outerit++) {
                    this->relationMap.at((*outerit)).num_tuples = andResult;
                            //also update the attribute values
                    for(auto innerit = this->relationMap.at((*outerit)).innerMap.begin(); innerit != this->relationMap.at((*outerit)).innerMap.end(); innerit++) 
                         if((*innerit).second > andResult) 
                             (*innerit).second = andResult;
                } 

            }
            //if join then update the result
           else 
             andResult = orResult;
            // cout <<"andResult = "<<andResult<<endl;

            currAnd = currAnd->rightAnd;
            numTuples = andResult;
       }
       
   }
   
}



double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
   
    double numTuples = 0.0;
    double orResult;
    double andResult = -1.0;
    double n;
    //TODO: Validation of JOIN or not will happen here
    if (this->validateRels(relNames, numToJoin)){

    }

   Statistics cpyStat(*this);
   //processing all ANDS
   if(parseTree != NULL) {
              
        struct AndList *currAnd = parseTree;
        string relNameLeft;
        string relNameRight;
        string relName;
       while(currAnd)
       {
            struct OrList *currOr = currAnd->left;
            vector<double> orVector;
            int l, r;
            bool isJoin = false;
            int index = -1;
            while(currOr){
                string llval = currOr->left->left->value;
                string lrval = currOr->left->right->value;
                int llcode = currOr->left->left->code;
                int lrcode = currOr->left->right->code;
                //if both operands are name then JOIN
                
                if(llcode == NAME && lrcode == NAME ) {
                    isJoin = true;
                    int exceptionCount;
                    double totalLeft, totalRight,attLeft, attRight;
                    cpyStat.extractValues(llval, relNameLeft, totalLeft, attLeft,numToJoin, relNames);
                    cpyStat.extractValues(lrval, relNameRight, totalRight, attRight, numToJoin, relNames);
                    
                    // Initialising the number of tuples of the relation and the number of unique tuples of the attribute
                    
                    // andResult = totalLeft * totalRight;
                    //TODO: handle condition where attribute name is incorrect
                    //if both belong to the same partition then selection else join
                    // if(cpyStat.relToId.at(cpyStat.relationMap.find(relNameRight)->first) == cpyStat.relToId.at(cpyStat.relationMap.find(relNameLeft)->first))
                    //     orResult = (double)(totalLeft)/std::max(attLeft,attRight);    
                    // else
                    //     orResult = (double)(totalLeft*totalRight)/std::max(attLeft,attRight);
                    orResult = (double)(totalLeft*totalRight)/std::max(attLeft,attRight);
                    andResult = orResult;
                    //update the Statistics after join
                    int pid = cpyStat.relToId.at(relNameLeft); //left relation pid
                    int prev_pid = cpyStat.relToId.at(relNameRight); //right relation pid

                    //update partition_id of relations that are in the same partition as the right relation
                    if(pid !=prev_pid){
                        for(auto temp =cpyStat.idToRel.at(prev_pid).begin(); temp != cpyStat.idToRel.at(prev_pid).end(); temp++ ) {
                            cpyStat.relToId.at(*temp) = pid;
                        }
                        cpyStat.relToId.at(cpyStat.relationMap.find(relNameRight)->first) = pid; 
                        
                        //update relation names in idToRel
                        
                        vector<string> prev_vector = cpyStat.idToRel.at(prev_pid);

                        for(auto temp2 =prev_vector.begin(); temp2 != prev_vector.end(); temp2++ ) {
                            // cout<<"temp "<<*temp2<<endl;
                            cpyStat.idToRel.at(pid).push_back(*temp2);
                        }
                        //remove the old entry of right side relation name from idToRel
                        // cout<<"Debug prints before delete"<<endl;
                        // cpyStat.printDicts();
                        cpyStat.idToRel.erase(prev_pid);
                    }
                    // cout<<"Debug prints"<<endl;
                    // cpyStat.printDicts();
                    cpyStat.relationMap.at(relNameLeft).num_tuples = orResult;
                    cpyStat.relationMap.at(relNameRight).num_tuples = orResult; //redundant since this happens in loop below
                    // cout<<"Debug prints"<<endl;
                    // cpyStat.printDicts();
                    vector<string> temp_vector = cpyStat.idToRel.at(pid);
                    
                    //if relations were already joined, update the relations in the partition for left
                    for(auto outerit = temp_vector.begin(); outerit != temp_vector.end(); outerit++) {
                        cpyStat.relationMap.at((*outerit)).num_tuples = orResult;
                        //now update their attributes too
                        for(auto innerit = cpyStat.relationMap.at((*outerit)).innerMap.begin(); innerit != cpyStat.relationMap.at((*outerit)).innerMap.end(); innerit++) 
                            if((*innerit).second > orResult) 
                                (*innerit).second = orResult;
                            
                    }
                }
                // else SELECTION
                else {
                    // cout<<"Debug prints"<<endl;
                    // cpyStat.printDicts();
                    isJoin = false;
                    double numAttrs, numTotal;
                    
                    //if both are literals 
                    if(llcode != NAME && lrcode != NAME){
                        // Error since both are literals 
                    }
                    else {
                        // SET LLVAL LRVAL RELNAME LEFT AND RELNAMERIGHT
                        // Check if left is a NAME
                        if(llcode == NAME){
                            cpyStat.extractValues(llval, relName, numTotal, numAttrs, numToJoin, relNames);
                            
                        }
                        // Check if right is a NAME
                        else if(lrcode == NAME) {
                            cpyStat.extractValues(lrval, relName, numTotal, numAttrs, numToJoin, relNames);
                            
                        }
                        switch(currOr->left->code) {
                            case LESS_THAN:
                                orResult = (double)numTotal/3.0;
                                break;
                            case GREATER_THAN:
                                orResult = (double)numTotal/3.0;
                                break;
                            case EQUALS:
                                orResult = (double)numTotal/numAttrs;
                                break;    
                        }
                        // cout << "Oresult = "<<orResult<< "numtotal = "<< numTotal <<endl;
                        if(andResult == -1)
                            n = numTotal;
                        else
                            n = andResult;                        
                        //update statistics for selection
                        // cpyStat.relationMap.at(relNames[index]).num_tuples = orResult;
                        //if sleceted table was joined, update the values in correspnding relations as well 
                        // int pid = cpyStat.relToId.at(relNames[index]);
                            
                        orVector.push_back(orResult);    
                    }//else validation check
                    
                }//else join or selection
                
                currOr = currOr->rightOr;
            }
            //calculate for combined Or
           
           //if not join calculate the combined or.
            if(!isJoin) {
                 double orCombined = 1.0;
                 for(auto it = orVector.begin(); it != orVector.end(); it++) {
                    // cout << "individual or results = " << (*it) <<endl;
                    orCombined = (double)orCombined*(1 - (*it)/n);
                 }
                //  cout << "\n\nCombined OR = " << orCombined<<endl;
                 andResult = (double)n * (1 - orCombined);
                //  cout <<"andResult = "<<andResult<<endl;       
                //update the values in statistics
                 cpyStat.relationMap.at(relName).num_tuples = andResult;
                 int pid = cpyStat.relToId.at(relName);
                 for(auto outerit = cpyStat.idToRel.at(pid).begin(); outerit != cpyStat.idToRel.at(pid).end(); outerit++) {
                    cpyStat.relationMap.at((*outerit)).num_tuples = andResult;
                            //also update the attribute values
                    for(auto innerit = cpyStat.relationMap.at((*outerit)).innerMap.begin(); innerit != cpyStat.relationMap.at((*outerit)).innerMap.end(); innerit++) 
                         if((*innerit).second > andResult) 
                             (*innerit).second = andResult;
                } 

            }
            //if join then update the result
           else 
             andResult = orResult;
            // cout <<"andResult = "<<andResult<<endl;

            currAnd = currAnd->rightAnd;
            numTuples = andResult;
       }
       
   }
    return numTuples;
    
}

bool Statistics::validateRels(char **relname, int numToJoin){
    
    return true;
}
void Statistics::printDicts(){
    for(auto it = this->relationMap.begin(); it != this->relationMap.end(); it++) {
		cout << (*it).first <<", " << (*it).second.num_tuples <<endl;
		for(auto it2 = 	it->second.innerMap.begin(); it2 != it->second.innerMap.end(); it2++) {
				cout <<" ----" <<(*it2).first <<", " << (*it2).second <<endl;

		}
	}
    cout<<"rel map ________________"<<endl;
    for(auto it = this->relToId.begin(); it != this->relToId.end(); it++) {
		cout << (*it).first <<": " << (*it).second <<endl;
    }

    cout<<"reltoID ________________"<<endl;

    for(auto it = this->idToRel.begin(); it != this->idToRel.end(); it++) {
		cout << (*it).first <<": "<<endl;
        for (auto it1 = (*it).second.begin(); it1 != (*it).second.cend(); it1++)
        {
            cout << *it1 << ' ';
        }
        cout<<'\n';
    }
    cout<<"\nidtorel ________________"<<endl;
    cout<<"********************************************"<<endl;
}

void Statistics::extractValues(string &attName, string &relname , double &totalCount, double &attCount, int numRels, char *relNames[]){
    if (attName.find('.') != std::string::npos){
        relname = attName.substr(0, attName.find('.'));
        attName = attName.substr(attName.find('.')+1);
        attCount = this->relationMap.at(relname).innerMap.at(attName);
        totalCount = this->relationMap.find(relname)->second.num_tuples;
    }
    else{
        int l = 0;
        for(l = 0; l < numRels; l++) {
                if(this->relationMap.find(relNames[l])->second.innerMap.find(attName) != this->relationMap.find(relNames[l])->second.innerMap.end()) {
                    try{
                        attCount = this->relationMap.at(relNames[l]).innerMap.at(attName);
                    }
                    catch(const std::out_of_range& e){
                    }
                    
                    totalCount = this->relationMap.find(relNames[l])->second.num_tuples;
                    break;
                }
            }
            relname = relNames[l];
    }
    
}