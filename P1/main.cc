
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "TreeNode.h"
#include <vector>
#include <algorithm>

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
void getOptimalJoinSequence() {
	//TODO
}
 
void calculateCost() {

}

// helper to print query plan
void printQueryPlan() {
	
}

TreeNode generateNode(string nodeType, string alias, string rel_name){
	if(nodeType=="join"){

	}
	else if(nodeType=="selection"){
		
	}
}
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
		select_node.generateSelectNode(temp_aliases, aliasAs, loop_ind, tableName, aliasToRel);
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
	
	// if (finalFunction == NULL){
	// 	cout<<"NO SUM PRESENT"<<endl;
	// }
	// else{
	// 	cout<<"SUM PRESENT"<<endl;
	// }

	// Statistics s;
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


