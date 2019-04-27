
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "TreeNode.h"
#include <vector>
#include <string>
#include <algorithm> 
// #include <bits/stdc++.h> 


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
unordered_map <string, TreeNode> operations_tree;
unordered_map <string, string> alias_to_pipeId;

struct joinMapStruct  {
	double cost;
	Statistics * stat;

};

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

void getTableAndAliasNames(vector <string> &tableName, vector <string> &aliasAs, unordered_map <string, string> &aliasToRel){
	struct TableList *currtables = tables;
	while(currtables){
		tableName.push_back(currtables->tableName);
		aliasAs.push_back(currtables->aliasAs);
		aliasToRel.insert({currtables->aliasAs, currtables->tableName});
		currtables = currtables->next;
	}
}
// helper class to enumerate the permutations of the joins Dynamic programming
void getPermutations(vector <OrList*> &joinVector, vector<string> *permArray) {
	int size = joinVector.size();
	string str = "";
	for(int i =0; i< size; i++)
		str = str + to_string(i);
	//sort the string to create permutatios
	sort(str.begin(), str.end());
	do {
		permArray->push_back(str);
	   } while(next_permutation(str.begin(),str.end()));
}

double processJoin (vector <OrList*> &joinVector, Statistics *s, string sequence, unordered_map<string, joinMapStruct> *joinMap) {
	
	// loop to get costs of every possible subset of joins
	struct AndList temp;

	string relStrings[s->relationMap.size()];
	char *relnames[s->relationMap.size()];
	// char *relnames[100];
	double cost = 0.0;
	// populate relnames array
	int rel_index= 0;

	relnames[0] = "nation";
	relnames[1] = "customer";
	relnames[2] = "supplier";
	relnames[3] = "n1";
	relnames[4] = "n2";
	relnames[5] = "s";
	relnames[6] = "c";
	// for(auto it = s->relationMap.begin(); it != s->relationMap.end(); it++) {
	// 	// char * relation;
	// 	// strcpy(relation,(*it).first.c_str());
	// 	// strcpy(relnames[rel_index] , relation);
	// 	relStrings[rel_index] = (*it).first;
	// 	// cout << "Relation name:" <<relnames[rel_index]<< "relIndex: "<<rel_index<<endl;
	// 	rel_index++;
	// }
	// for(int i =0;i < rel_index;i++) {
	// 	strcpy(relnames[i], relStrings[i].c_str());
	// }

	Statistics dummy(*s);
	for(int index = 0; index < sequence.size(); index++) {
		struct AndList *cnf = &temp;	
		string subset;
		subset = sequence.substr(0,index+1);
		// cout << "\n\nPrint jointMap:-------------------BEGIN---------------------------------"<<endl;
		// for(auto it = joinMap->begin(); it!= joinMap->end(); it++) {
		// 	cout << (*it).first << "  cost ="<< (*it).second.cost <<"  size="<<(*it).second.stat->relationMap.size()<<endl;
		// }
		// if not in joinMap, create cnf and apply on subset-1
		if(joinMap->find(subset) == joinMap->end()) {
			cnf->left = joinVector[(int)(sequence[index]) - 48];
			cnf->rightAnd = NULL;
			
			//get state of subset - 1 and apply
			if(index > 0) {
				string prev_subset = subset.substr(0,subset.length()-1); 
				if(joinMap->find(prev_subset) != joinMap->end()) {
									
					//store cost and statistics in dummy structure and add it to the ew subset entry
					joinMapStruct jmStruct;
					jmStruct.stat = new Statistics(*joinMap->at(prev_subset).stat);
					cost = cost + joinMap->at(prev_subset).stat->Estimate(cnf, relnames,joinMap->at(prev_subset).stat->relationMap.size());
					jmStruct.cost = cost;
					
					jmStruct.stat->Apply(cnf, relnames,joinMap->at(prev_subset).stat->relationMap.size());
										
					joinMap->insert({subset, jmStruct});
					
				} 
			}
			//apply on dummy Statistics object
			else {
				joinMapStruct jmStruct;
				
				cost = cost + dummy.Estimate(cnf, relnames, dummy.relationMap.size());
				jmStruct.cost = cost;
				jmStruct.stat = new Statistics(dummy);
				jmStruct.stat->Apply(cnf, relnames, dummy.relationMap.size());
						
				joinMap->insert({subset, jmStruct});
			}
		
		}
		else {
			cost = cost + joinMap->at(subset).cost;
		}

		std::cout << "\n Cost till now = "<<cost<<endl;
		// 	cout << "\n\nPrint jointMap:-------------------END---------------------------------"<<endl;
		// for(auto it = joinMap->begin(); it!= joinMap->end(); it++) {
		// 	cout << (*it).first << "  size="<<(*it).second->relationMap.size()<<endl;
		// }
	}
	return cost;
	
}

// TreeNode generateNode(string nodeType, string alias, string rel_name){
// 	if(nodeType=="join"){

// 	}
// 	else if(nodeType=="selection"){
		
// 	}
// }
// class for node of the tree
// tree class with all operations
Schema get_join_schema(Schema rel1_sch, Schema rel2_sch){
	
	int numatts_rel1 = rel1_sch.GetNumAtts();
	int numatts_rel2 = rel2_sch.GetNumAtts();
	int outAtts = numatts_rel1+numatts_rel2;
	Attribute joinAtt [outAtts];
	Attribute *rel1_atts =  rel1_sch.GetAtts();
	Attribute *rel2_atts =  rel2_sch.GetAtts();
	
	for(int i=0; i<numatts_rel1;i++){
		joinAtt[i] = rel1_atts[i];
	}

	int i =0;
	for(int j=numatts_rel1; j<outAtts;j++){
		joinAtt[j] = rel2_atts[i];
		i++;
	}
	Schema joinSch("join_sch", outAtts, joinAtt);

	return joinSch;
	
}
string getOperandFromCode(int code){
	switch(code) {
			case LESS_THAN:
				return "<";
				break;
			case GREATER_THAN:
				return ">";
				break;
			case EQUALS:
				return "=";
				break;    
		}
}
TreeNode generateSelectNode(vector <string> aliases, 
                                      vector <string> aliasAs, 
                                      int num_rels, 
                                      vector <string> tableName,
                                      unordered_map<string, string> aliasToRel){
        if (num_rels==1){
            SelectFile_node sf_node;
            sf_node.node_type = "select";
            // sf_node.selOp = 
            // sf_node.literal =  
            string rel_name;
            std::sort(aliases.begin(), aliases.end());
            string out_pipe = "";
            for(int i =0; i<aliases.size(); i++){
                out_pipe = out_pipe+ "_"+aliases[i];
                rel_name = aliasToRel.at(aliases[i]);
	        }
            Schema outSch("catalog", (char*)rel_name.c_str());
            sf_node.output_schema = &outSch;
            sf_node.out_pipe_name = out_pipe;
            return sf_node;
        }
        else{
            SelectPipe_node sp_node;
            std::sort(aliases.begin(), aliases.end());
            string out_pipe = "";
            for(int i =0; i<aliases.size(); i++){
                out_pipe = out_pipe+ "_"+aliases[i];
	        }
            return sp_node;
        }
        

}

void getSelectFileNodes(vector <OrList*> selectionVector, 
						vector <OrList*> &joinVector, 
						vector <string> tableName, 
						vector <string> aliasAs,
						unordered_map <string, string> aliasToRel){
	string attName;
	int code;
	string alias;
	std::string relName;
	vector <string> temp_aliases;
	std::string last_alias = "";
	std::string condition = "(";
	std::unordered_map <std::string, bool> selection;

	for(int i =0; i< selectionVector.size();i++){
		struct OrList *currOr = selectionVector[i];
		TreeNode select_node;
		int loop_ind = 0;
		while(currOr){
			
			attName = currOr->left->left->value;
			
			alias = attName.substr(0, attName.find('.'));
			
			if (loop_ind != 0){
				condition = condition + " OR ";
			}
			if(last_alias !=alias){
				temp_aliases.push_back(alias);
				loop_ind++;
			}
			condition=condition+attName+getOperandFromCode(currOr->left->code)+currOr->left->right->value;
			
			currOr = currOr->rightOr;
			last_alias = alias;
		}
		condition = condition + ")";
		if (loop_ind==1){
			selection.insert({alias, true});
		}
		select_node = generateSelectNode(temp_aliases, aliasAs, loop_ind, tableName, aliasToRel);
		operations_tree.insert({select_node.out_pipe_name, select_node});
	}
	string attNameLeft;
	string attNameRight;
	string aliasLeft;
	string aliasRight;
	// for(int i=0; i<joinVector.size();i++){
		
	// 	attNameLeft = joinVector[i]->left->left->value;
	// 	aliasLeft = attNameLeft.substr(0, attNameLeft.find('.'));
	// 	attNameLeft = attNameLeft.substr(attNameLeft.find('.')+1);
	// 	attNameRight = joinVector[i]->left->right->value;
	// 	aliasRight = attNameRight.substr(0, attNameRight.find('.'));
	// 	attNameRight = attNameRight.substr(attNameRight.find('.')+1);
	// 	int index = 0;
	// 	while(aliasAs.size()>0){

	// 	}
	// 	for(int j =0; j< aliasAs.size();j++){
	// 		TreeNode *select_node = new TreeNode();
	// 		if (aliasLeft == aliasAs[j]){
	// 			select_node->generateSelectNode(aliasLeft, tableName[j]);
	// 			operations_tree.push_back(*select_node);
	// 			tableName.erase(tableName.begin()+j);
	// 			aliasAs.erase(aliasAs.begin()+j);
	// 			j--;
	// 		}
	// 		else if (aliasRight == aliasAs[j]){
	// 			select_node->generateSelectNode(aliasRight, tableName[j]);
	// 			operations_tree.push_back(*select_node);
	// 			tableName.erase(tableName.begin()+j);
	// 			aliasAs.erase(aliasAs.begin()+j);
	// 			j--;
	// 		}
	// 		if(tableName.size() ==0){
	// 				break;
	// 			}
	// 	}
	// 	if(tableName.size() ==0){
	// 		break;
	// 	}
	// }
}

// helper class to enumerate the permutations of the joins Dynamic programming
void getOptimalJoinSequence(vector <OrList*> &joinVector, Statistics s) {
	
	vector<string> permutations;
	// vector <string> permutations;

	unordered_map<string, joinMapStruct > joinMap;
	getPermutations(joinVector, &permutations);
	double cost;
	string minCostPermutation;
	double min_cost = 0.0;
	
	for(auto it_perm = permutations.begin(); it_perm != permutations.end(); it_perm++) {
		cost = processJoin(joinVector, &s, (*it_perm), &joinMap);
		if(min_cost == 0.0 || min_cost > cost) {
			min_cost = cost;
			minCostPermutation = (*it_perm);
		}
		cout << "Cost for sequence: " << (*it_perm)<< " = " <<cost<<endl;
	}
	cout << "\n\n\n+++++++++++++++++++++++++++++FinalAnswer+++++++++++++++++++++++++"<<endl;
	cout << "Cost for sequence: " << minCostPermutation << " = " <<min_cost<<endl;
}
 
int main () {

	// Schema rel1_sch("catalog", "partsupp");
	// Schema rel2_sch("catalog", "nation");
	
	// // Attribute *test =  mySchema.GetAtts();
	// Schema jnSch = get_join_schema(rel1_sch, rel2_sch);
	// cout << "Num atts "<< jnSch.GetNumAtts()<<endl;

	cout<<"Enter the query"<<endl;
	yyparse();

	// Getting the split of joins and selections 
	vector <OrList*> joinVector;
	vector <OrList*> selectionVector;
    
	Statistics s;
	// char *relName[] = { "part",  "partsupp","supplier"};

	// s.AddRel(relName[0],200000);
	// s.AddAtt(relName[0], "p_partkey",200000);
	// s.AddAtt(relName[0], "p_name", 199996);

	// s.AddRel(relName[1],800000);
	// s.AddAtt(relName[1], "ps_partkey",200000);
	// s.AddAtt(relName[1], "ps_suppkey",10000);
	
	// s.AddRel(relName[2],10000);
	// s.AddAtt(relName[2], "s_suppkey",10000);

	// s.CopyRel("part","p");
	// s.CopyRel("partsupp","ps");
	// s.CopyRel("supplier","s");
	
char *relName[] = {"supplier","customer","nation"};

	s.AddRel(relName[0],10000);
	s.AddAtt(relName[0], "s_nationkey",25);
	s.AddRel(relName[1],150000);
	s.AddAtt(relName[1], "c_custkey",150000);
	s.AddAtt(relName[1], "c_nationkey",25);
	
	s.AddRel(relName[2],25);
	s.AddAtt(relName[2], "n_nationkey",25);

	s.CopyRel("nation","n1");
	s.CopyRel("nation","n2");
	s.CopyRel("supplier","s");
	s.CopyRel("customer","c");

	separateJoinSelection(joinVector, selectionVector);
	
	// Getting the table names and aliases 
	vector <string> tableName;
	vector <string> aliasAs;
	unordered_map <string, string> aliasToRel;
	getTableAndAliasNames(tableName, aliasAs, aliasToRel);

	getSelectFileNodes(selectionVector, joinVector, tableName, aliasAs, aliasToRel);

	cout<<"size of ops "<< operations_tree.size()<<endl;

	// cout<<"outpipe name "<< operations_tree[0].out_pipe_name<<endl;
	// cout<<"size of tabname "<< tableName.size()<<endl;
	// cout<<"size of alias "<< aliasAs.size()<<endl;

	// // cout<<"Table name "<<tables->tableName<<endl;
	
	// // cout<<"Name list grouping atts "<<groupingAtts->name<<endl;
	// cout<<"Name list attsToSelect 1 "<<attsToSelect->name<<endl;
	// cout<<"Name list attsToSelect 2 "<<attsToSelect->next->name<<endl;
	// if(attsToSelect->next->next == NULL){
	// 	cout << "only two atts present"<<endl;
	// }
	// cout<<"Distinct Atts "<<distinctAtts<<endl;
	// cout<<"Distinct Funcs "<<distinctFunc<<endl;
	// cout<<"Distinct Funcs "<<boolean->left->left->code<<endl;
	getOptimalJoinSequence(joinVector, s);

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
	
	
	// cout<<"Table name "<<tables->tableName<<endl;
	// // cout<<"Name list grouping atts "<<groupingAtts->name<<endl;
	// cout<<"Name list attsToSelect 1 "<<attsToSelect->name<<endl;
	// cout<<"Name list attsToSelect 2 "<<attsToSelect->next->name<<endl;
	// if(attsToSelect->next->next == NULL){
	// 	cout << "only two atts present"<<endl;
	// }
	// cout<<"Distinct Atts "<<distinctAtts<<endl;
	// cout<<"Distinct Funcs "<<distinctFunc<<endl;
	// cout<<"Distinct Funcs "<<boolean->left->left->code<<endl;
	
	if (finalFunction == NULL){
		cout<<"NO SUM PRESENT"<<endl;
	}
	else{
		cout<<"SUM PRESENT"<<endl;
	}

	
	// if (finalFunction == NULL){
	// 	cout<<"NO SUM PRESENT"<<endl;
	// }
	// else{
	// 	cout<<"SUM PRESENT"<<endl;
	// }

	// cout<<"selection vector size"<<endl;
	// cout<<selectionVector[0]->left->left->value<<endl;
	// if (tempAnd->left==NULL){
		// cout<<"selection vector size"<<endl;

	// }
	// tempAnd->left = selectionVector[0];
	// tempAnd->rightAnd = NULL;
	// double res = s.Estimate(tempAnd, relName, 3);
	// cout<< res<<endl;
	TreeNode n;
	Join_node j;
	Project_node p;
	p.left_child = &j;
	
	// struct AndList A;
	// struct AndList *tempAnd = &A;

	// cout<<"selection vector size"<<endl;
	// cout<<selectionVector[0]->left->left->value<<endl;
	// if (tempAnd->left==NULL){
	// 	cout<<"selection vector size"<<endl;

	// }
	// tempAnd->left = selectionVector[0];
	// tempAnd->rightAnd = NULL;
	// double res = s.Estimate(tempAnd, relName, 3);
	// cout<< res<<endl;
	// TreeNode n;
	// Join_node j;
	// Project_node p;
	// p.left_child = &j;
	
	// // tempAnd->left = 
	// // cout<< res<<endl;


	// cout<<"Table name "<<tables->tableName<<endl;
	// if (groupingAtts!=NULL){
	// 	cout<<"Name list grouping atts "<<groupingAtts->name<<endl;
	// }
	
	// cout<<"Name list attsToSelect "<<attsToSelect->name<<endl;
	// cout<<"Distinct Atts "<<distinctAtts<<endl;
	// cout<<"Distinct Funcs "<<distinctFunc<<endl;
}


