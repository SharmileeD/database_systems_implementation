
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "TreeNode.h"
#include <vector>
#include <string>
#include <algorithm> 
#include <bits/stdc++.h> 


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
unordered_map <string, TreeNode*> operations_tree;
unordered_map <string, string> alias_to_pipeId;
vector<TreeNode*> join_tree;

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

double processJoin (vector <OrList*> &joinVector, Statistics *s, string sequence, unordered_map<string, joinMapStruct> *joinMap, char **relnames, int size) {
	
	// loop to get costs of every possible subset of joins
	struct AndList temp;

	// char *relnames[100];
	double cost = 0.0;
	// populate relnames array
	int rel_index= 0;
	
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
					
					cost = cost + joinMap->at(prev_subset).stat->Estimate(cnf, relnames,size);
					jmStruct.cost = cost;
					
					jmStruct.stat->Apply(cnf, relnames,size);
					joinMap->insert({subset, jmStruct});
					
				} 
			}
			//apply on dummy Statistics object
			else {
				joinMapStruct jmStruct;
				jmStruct.stat = new Statistics(dummy);

				cost = cost + dummy.Estimate(cnf, relnames, size);
				jmStruct.cost = cost;
								
				jmStruct.stat->Apply(cnf, relnames, size);
				joinMap->insert({subset, jmStruct});
			}
		
		}
		else {
			cost = cost + joinMap->at(subset).cost;
		}

		// std::cout << "\n Cost till now = "<<cost<<endl;
		// 	cout << "\n\nPrint jointMap:-------------------END---------------------------------"<<endl;
		// for(auto it = joinMap->begin(); it!= joinMap->end(); it++) {
		// 	cout << (*it).first << "  size="<<(*it).second->relationMap.size()<<endl;
		// }
	}
	return cost;
	
}

Schema get_join_schema(vector <string> tables){
	int outAtts = 0;
	vector <Attribute> join_atts;
	for(int k=0; k<tables.size();k++){
		Schema temp("catalog", (char*)tables[k].c_str());
		int numatts = temp.GetNumAtts();
		outAtts = outAtts + numatts;
		Attribute *rel1_atts =  temp.GetAtts();
		for(int i=0; i<numatts;i++){
			join_atts.push_back(rel1_atts[i]);
		}
	}
	
	Attribute joinAtt [join_atts.size()];
	
	for(int i=0; i<join_atts.size();i++){
		joinAtt[i] = join_atts[i];
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
TreeNode *generateSelectNode(vector <string> aliases, 
                                      vector <string> aliasAs, 
                                      int num_rels, 
                                      vector <string> tableName,
                                      unordered_map<string, string> aliasToRel,
									  string cnf_str
									  ){
        if (num_rels==1){
            SelectFile_node *sf_node = new SelectFile_node(); 
            sf_node->node_type = "select";
            // sf_node.selOp = 
            // sf_node.literal =  
            string rel_name;
			vector<string> tables;
            std::sort(aliases.begin(), aliases.end());
            string out_pipe = "";
            for(int i =0; i<aliases.size(); i++){
                out_pipe = out_pipe+ "_"+aliases[i];
                rel_name = aliasToRel.at(aliases[i]);
				tables.push_back(aliasToRel.at(aliases[i]));
	        }

            sf_node->tables = tables;
            sf_node->out_pipe_name = out_pipe;
			sf_node->cnf_str = cnf_str;
            return sf_node;
        }
        else{
            SelectPipe_node *sp_node = new SelectPipe_node(); 
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
		TreeNode *select_node = new TreeNode();
		int loop_ind = 0;
		temp_aliases.clear();
		condition = "(";
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
			cout << "Condition:"<<condition<<endl;
			currOr = currOr->rightOr;
			last_alias = alias;
		}
		condition = condition + ")";
		if (loop_ind==1){
			selection.insert({alias, true});
		}
		select_node = generateSelectNode(temp_aliases, aliasAs, loop_ind, tableName, aliasToRel, condition);
		operations_tree.insert({select_node->out_pipe_name, select_node});
	}
	string attNameLeft;
	string attNameRight;
	string aliasLeft;
	string aliasRight;
	for(int i=0; i<joinVector.size();i++){
		
		attNameLeft = joinVector[i]->left->left->value;
		aliasLeft = attNameLeft.substr(0, attNameLeft.find('.'));
		// attNameLeft = attNameLeft.substr(attNameLeft.find('.')+1);
		attNameRight = joinVector[i]->left->right->value;
		aliasRight = attNameRight.substr(0, attNameRight.find('.'));
		// attNameRight = attNameRight.substr(attNameRight.find('.')+1);
		int index = 0;
		
		if(selection.count(aliasLeft)==0){
			temp_aliases.clear();
			condition = "("+attNameLeft+"="+attNameLeft+")";
			selection.insert({aliasLeft, true});
			TreeNode *select_node = new TreeNode();
			temp_aliases.push_back(aliasLeft);
			select_node = generateSelectNode(temp_aliases, aliasAs, 1, tableName, aliasToRel, condition);
			operations_tree.insert({select_node->out_pipe_name, select_node});

		}
		if(selection.count(aliasRight)==0){
			temp_aliases.clear();
			condition = "("+attNameRight+"="+attNameRight+")";
			selection.insert({aliasRight, true});
			TreeNode *select_node = new TreeNode();
			temp_aliases.push_back(aliasRight);
			select_node = generateSelectNode(temp_aliases, aliasAs, 1, tableName, aliasToRel, condition);
			operations_tree.insert({select_node->out_pipe_name, select_node});

		}
	}
}


// helper class to enumerate the permutations of the joins Dynamic programming
string getOptimalJoinSequence(vector <OrList*> &joinVector, Statistics s, vector <string> aliasAs) {
	
	vector<string> permutations;
	// vector <string> permutations;

	unordered_map<string, joinMapStruct > joinMap;
	getPermutations(joinVector, &permutations);
	double cost;
	string minCostPermutation;
	double min_cost = 0.0;
	vector<char*> cstrings;
	for(auto it_perm = permutations.begin(); it_perm != permutations.end(); it_perm++) {

		for(int i = 0; i < aliasAs.size(); i++) {
			cstrings.push_back(const_cast<char*>(aliasAs[i].c_str()));
		}
	
		cost = processJoin(joinVector, &s, (*it_perm), &joinMap, &cstrings[0], aliasAs.size());
		if(min_cost == 0.0 || min_cost > cost) {
			min_cost = cost;
			minCostPermutation = (*it_perm);
		}
		// cout << "Cost for sequence: " << (*it_perm)<< " = " <<cost<<endl;
	}
	cout << "Cost for sequence: " << minCostPermutation << " = " <<min_cost<<endl;
	return minCostPermutation;
}
void printPlan(){
	for ( auto it = operations_tree.begin(); it != operations_tree.end(); ++it ){
			cout<< "SF"+it->second->cnf_str+" => "+it->second->out_pipe_name<<endl;
	}
}

TreeNode* getJoinNode (string lPipe, string rPipe, string opPipe, vector <OrList*> &joinVector, int index, vector<string> joinedTables) {
	
	Join_node *node = new Join_node();
	
	node->node_type = "join";
	node->input_pipe_l = lPipe;
	node->input_pipe_r = rPipe;
	node->out_pipe_name = opPipe;
	node->right_child = NULL;
	node->left_child = NULL;
	node->tables = joinedTables;

	string leftVal = joinVector[index]->left->left->value;
	string rightVal = joinVector[index]->left->right->value;
	string cnf_Str = "(" + leftVal+" = " + rightVal + ")";
	cout << "Built cnf: "<<cnf_Str<<endl;
	node->cnf_str = cnf_Str;

	return node;
}

void createJoinTree(vector <OrList*> &joinVector, string minCostPermutation, vector <string> aliasAs) {
	
	unordered_map<string,int> alias_to_pipeId;
	unordered_map<int, vector<string>> pipeId_to_alias;

	string aliasLeft, aliasRight, attName, leftPipe, rightPipe, opPipe;
	vector<string> tempLeft;
	vector<string> tempRight;
	int lpid, rpid;

	for(int i = 0; i < aliasAs.size(); i++) {
		vector<string> dummy;
	 	dummy.push_back(aliasAs[i]);
		alias_to_pipeId.insert({aliasAs[i], i});
		pipeId_to_alias.insert({i, dummy});
	}

	// get nodes for every join operation and store them in operations_tree
	for(int index = 0; index < minCostPermutation.length(); index++) {
		TreeNode *joinNode =  new TreeNode();	
		struct OrList *currOr =  joinVector[(int)minCostPermutation[index] - 48];
		//get left and right aliases 
		attName = currOr->left->left->value; 
		aliasLeft = attName.substr(0, attName.find('.'));
		attName = currOr->left->right->value; 
		aliasRight = attName.substr(0, attName.find('.'));
		 
		leftPipe =""; rightPipe=""; opPipe = ""; 
		
		//form corresponding input pipe names and set them
		lpid = alias_to_pipeId.at(aliasLeft);
		rpid = alias_to_pipeId.at(aliasRight);
		
		sort(pipeId_to_alias.find(lpid)->second.begin(), pipeId_to_alias.find(lpid)->second.end());
		sort(pipeId_to_alias.find(rpid)->second.begin(), pipeId_to_alias.find(rpid)->second.end());

		//make a leftpipeId and rightPipeId also track updates to be done in the alias_to_pipeid
		for(int i = 0; i< pipeId_to_alias.find(lpid)->second.size(); i++) 
			leftPipe.append("_"+pipeId_to_alias.find(lpid)->second[i]);
		
		for(int i = 0; i< pipeId_to_alias.find(rpid)->second.size(); i++) 
			rightPipe.append("_"+pipeId_to_alias.find(rpid)->second[i]);
		
		cout <<"Left pipe:" <<leftPipe<<endl;
		cout <<"Right pipe:" <<rightPipe<<endl;

		//--------------------Update for the join operation performed------------------------
		vector<string> right_vector = pipeId_to_alias.at(rpid);
		//update pipeid of all aliases to lpid
		if(lpid != rpid){
			for(auto itr = right_vector.begin(); itr != right_vector.end(); itr++) {
			alias_to_pipeId.at(*itr) = lpid;
			}		

			for(auto itr = right_vector.begin(); itr != right_vector.end(); itr++) {
			//if the corresponding relation not present then add
				if(find(pipeId_to_alias.at(lpid).begin(), pipeId_to_alias.at(lpid).end(), *itr)==pipeId_to_alias.at(lpid).end())
					pipeId_to_alias.at(lpid).push_back(*itr);
			}		
		//delete the right side entry from pipeId_to_alias
			pipeId_to_alias.erase(rpid);
		} //that is not already joined
		
		//create outputPipe id
		sort(pipeId_to_alias.at(lpid).begin(), pipeId_to_alias.at(lpid).end());
		for(auto itop = pipeId_to_alias.at(lpid).begin(); itop != pipeId_to_alias.at(lpid).end(); itop++) {
			opPipe.append("_" + (*itop));
		}
		cout << "Output Pipe:" << opPipe <<endl;
		joinNode = getJoinNode(leftPipe, rightPipe, opPipe, joinVector, ((int)minCostPermutation[index] - 48), pipeId_to_alias.at(lpid));
		operations_tree.insert({joinNode->out_pipe_name, joinNode}); 
		join_tree.push_back(joinNode);
	}

}


int main () {

	// Schema rel1_sch("catalog", "partsupp");
	// Schema rel2_sch("catalog", "nation");
	

	// // Attribute *test =  mySchema.GetAtts();
	// vector<string> test;
	// test.push_back("part");
	// test.push_back("supplier");
	// Schema jnSch = get_join_schema(test);
	// cout << "Num atts "<< jnSch.GetNumAtts()<<endl;

	cout<<"Enter the query"<<endl;
	yyparse();

	// Getting the split of joins and selections 
	vector <OrList*> joinVector;
	vector <OrList*> selectionVector;
    
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
	
	// char *relName[] = {"supplier","customer","nation"};

	// s.AddRel(relName[0],10000);
	// s.AddAtt(relName[0], "s_nationkey",25);
	// s.AddRel(relName[1],150000);
	// s.AddAtt(relName[1], "c_custkey",150000);
	// s.AddAtt(relName[1], "c_nationkey",25);
	
	// s.AddRel(relName[2],25);
	// s.AddAtt(relName[2], "n_nationkey",25);

	// s.CopyRel("nation","n1");
	// s.CopyRel("nation","n2");
	// s.CopyRel("supplier","s");
	// s.CopyRel("customer","c");

	separateJoinSelection(joinVector, selectionVector);
	
	// Getting the table names and aliases 
	vector <string> tableName;
	vector <string> aliasAs;
	unordered_map <string, string> aliasToRel;
	getTableAndAliasNames(tableName, aliasAs, aliasToRel);

	getSelectFileNodes(selectionVector, joinVector, tableName, aliasAs, aliasToRel);
	printPlan();
	cout<<"size of ops "<< operations_tree.size()<<endl;

	string optimalJoinSeq = getOptimalJoinSequence(joinVector, s, aliasAs);
	createJoinTree(joinVector, optimalJoinSeq, aliasAs);
	printPlan();
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
	// getOptimalJoinSequence(joinVector, s);

	

	// cout<<"size of join "<< joinVector.size()<<endl;
	// cout<<"size of selection "<< selectionVector.size()<<endl;

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


