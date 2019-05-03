
#include <iostream>
#include "ParseTree.h"
#include "Statistics.h"
#include "TreeNode.h"
#include "Function.h"
#include "Pipe.h"
#include <vector>
#include <string>
#include <algorithm> 
#include <stack>
#include <bits/stdc++.h> 
#include "RelOp.h"
#include "DBFile.h"
extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);

// #include <bits/stdc++.h> 


using namespace std;
extern "C" {
	int yyparse(void);   // defined in y.tab.c
	int yyfuncparse(void);   // defined in yyfunc.tab.c
	void init_lexical_parser (char *); // defined in lex.yy.c (from Lexer.l)
	void close_lexical_parser (); // defined in lex.yy.c
	void init_lexical_parser_func (char *); // defined in lex.yyfunc.c (from Lexerfunc.l)
	void close_lexical_parser_func (); // defined in lex.yyfunc.c
}

extern struct TableList *tables;
extern struct NameList *groupingAtts; //groupby
extern struct AndList *boolean;
extern struct AndList *final;
extern  struct NameList *attsToSelect; //projection
extern struct FuncOperator *finalFunction; // NULL if Sum not present
extern int distinctAtts; //only distinct
extern int distinctFunc; //Sum and distinct
extern int operType; 
extern struct CreateList *create; 
extern struct Insert *insertQuery;
extern char* dropTab;
extern char* setOp;
extern char* createTab;
extern char* createTabType;
unordered_map <string, TreeNode*> operations_tree;
unordered_map <string, string> alias_to_pipeId;
vector<Join_node*> join_tree;
vector<SelectPipe_node*> selectPipeVector;
GroupBy_node* groupByNode;
Sum_node* sumNode;
Project_node* projectNode;
WriteOut_node* writeOutNode;
unordered_map <string, Pipe*> pipeMap;
Schema *rschema;
Schema* schema () { return rschema;}
unordered_map <string, Schema*> schemaMap;
string plan;

unordered_map<string, int> relToPid;
unordered_map<int, vector<string> > pidToRel;
unordered_map <string, string> aliasToRel;
unordered_map <string, char*> relToAlias;



string last_out_pipe;
struct joinMapStruct  {
	double cost;
	Statistics * stat;
};

void processAndList(AndList *final) {
	AndList * temp = final;
	AndList * currAnd = final;
	while(currAnd) {
		struct OrList *currOr = currAnd->left;
		while(currOr) {

			string str = currOr->left->left->value;
			if(str.find('.') != string::npos){
				string to_copy = str.substr(str.find('.')+1, str.length());
				strcpy(currOr->left->left->value , to_copy.c_str());
			}
			currOr = currOr->rightOr;
		}	
		currAnd = currAnd -> rightAnd;
	}
	final = temp;
}

void processFunction(FuncOperator * finalFunction) {
	struct FuncOperator *funcOp = finalFunction;
	struct FuncOperator *currFun = finalFunction;

	while(currFun != NULL) {
		if(currFun->leftOperator != NULL) {
			string temp = funcOp->leftOperator->leftOperand->value;
			string attrname = temp.substr(temp.find('.')+1, temp.length()-1);
			strcpy(currFun->leftOperator->leftOperand->value, attrname.c_str());
		}
		else {
			cout << currFun->leftOperand->value<<")" <<endl;
			string temp = currFun->leftOperand->value;
			string attrname = temp.substr(temp.find('.')+1, temp.length()-1);
			strcpy(currFun->leftOperand->value, attrname.c_str());
		}
		currFun = currFun->right;
	}
	finalFunction = funcOp;
}	

void get_cnf (char *input, Schema *left, Function &fn_pred) {
		init_lexical_parser_func (input);
  		if (yyfuncparse() != 0) {
			cout << " Error: can't parse your arithmetic expr. " << input << endl;
			exit (1);
		}
		processFunction(finalFunction);
		fn_pred.GrowFromParseTree (finalFunction, *left); // constructs CNF predicate
		close_lexical_parser_func ();
}

void get_cnf (char *input, CNF &cnf_pred, Record &literal, Schema sch) {
	init_lexical_parser (input);
  	if (yyparse() != 0) {
		cout << " Error: can't parse your CNF.\n";
		exit (1);
	}
	// processAndList(final);
	cnf_pred.GrowFromParseTree (final, &sch, literal); // constructs CNF predicate
	close_lexical_parser ();
}

void get_cnf (char *input, CNF &cnf_pred, Record &literal, Schema sch, struct AndList *mod_andList) {
	init_lexical_parser (input);
  	if (yyparse() != 0) {
		cout << " Error: can't parse your CNF.\n";
		exit (1);
	}
	// processAndList(final);
	cnf_pred.GrowFromParseTree (mod_andList, &sch, literal); // constructs CNF predicate
	close_lexical_parser ();
}
void get_cnf (char *input, Function &fn_pred, Schema sch) {
		init_lexical_parser_func (input);
  		if (yyfuncparse() != 0) {
			cout << " Error: can't parse your CNF.\n";
			exit (1);
		}
		processFunction(finalFunction);
		fn_pred.GrowFromParseTree (finalFunction, sch); // constructs CNF predicate
		close_lexical_parser_func ();
	}

//Store selection and join in separate vectors
void separateJoinSelection(vector <OrList*> &joinVector, vector <OrList*> &selectionVector) {
	struct AndList *currAnd = boolean;
	while(currAnd) {
		struct OrList *currOr = currAnd->left;
		int llcode = currOr->left->left->code;
        int lrcode = currOr->left->right->code;
			// its a join
		if(llcode == NAME && lrcode == NAME) {
			joinVector.push_back(currOr);
		}	
		// its a selection	
		else {
			selectionVector.push_back(currOr);
		}
		currAnd = currAnd->rightAnd;
	}//and while
}

void getTableAndAliasNames(vector <string> &tableName, vector <string> &aliasAs, 
							unordered_map <string, string> &aliasToRel,
							unordered_map <string, char*> &relToAlias){
	struct TableList *currtables = tables;
	while(currtables){
		tableName.push_back(currtables->tableName);
		aliasAs.push_back(currtables->aliasAs);
		aliasToRel.insert({currtables->aliasAs, currtables->tableName});
		relToAlias.insert({currtables->tableName, currtables->aliasAs});
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
        vector<string> tables;
		if (num_rels==1){
            SelectFile_node *sf_node = new SelectFile_node(); 
            sf_node->node_type = SF;
            // sf_node.selOp = 
            // sf_node.literal =  
			string lpart;
			string rpart;
            std::sort(aliases.begin(), aliases.end());
            string out_pipe = "";
            for(int i =0; i<aliases.size(); i++){
                out_pipe = out_pipe+ "_"+aliases[i];
				tables.push_back(aliases[i]);
	        }
			Pipe *outpipe = new Pipe(100);
            sf_node->tables = tables;
            sf_node->out_pipe_name = out_pipe;
			string delimeter = "";
			if(cnf_str.find("=") != string::npos)
				delimeter = "=";
			else if(cnf_str.find(">") != string::npos)
				delimeter = ">";
			else if(cnf_str.find("<") != string::npos)
				delimeter = "<";
			if(delimeter != "") {
				string left_cnf_str = cnf_str.substr(0,cnf_str.find(delimeter));
				string right_cnf_str = cnf_str.substr(cnf_str.find(delimeter)+1, cnf_str.length());

				lpart = left_cnf_str.substr(left_cnf_str.find('.')+1,left_cnf_str.length());
				rpart = right_cnf_str.substr(right_cnf_str.find('.')+1,right_cnf_str.length());

				sf_node->cnf_str = "(" + lpart + delimeter +rpart;
			}
			else {
				sf_node->cnf_str = "("+cnf_str.substr(cnf_str.find('.')+1,cnf_str.length()-1);
			}
			char tempcnfstr[(sf_node->cnf_str).length()];
		
			strcpy(tempcnfstr, (sf_node->cnf_str).c_str());
			string to_copy = aliasToRel.at((cnf_str.substr(cnf_str.find('(')+1, cnf_str.find('.')-1)));
			char relname[to_copy.length()];
			strcpy(relname, to_copy.c_str());
			Schema sch("catalog", relname);
			lpart = lpart+")";
			if(lpart == rpart){
				struct AndList  * newAnd = (AndList * ) malloc(sizeof(AndList));
				struct OrList  * newOr = (OrList *) malloc(sizeof(OrList));
				struct Operand  * left = (Operand *) malloc(sizeof(Operand));
				struct Operand  * right = (Operand *) malloc(sizeof(Operand));
				struct ComparisonOp  * compOp = (ComparisonOp *) malloc(sizeof(ComparisonOp));
				left->code = NAME;
				left->value = (char *)lpart.substr(0, lpart.length()-1).c_str();
				right->code = NAME;
				right->value = (char *)rpart.substr(0, rpart.length()-1).c_str();
				compOp->code = EQUALS;
				compOp->left = left;
				compOp->right = right;
				newOr->rightOr = NULL;
				newOr->left = compOp;
				newAnd->left = newOr;
				newAnd->rightAnd = NULL;
				get_cnf(tempcnfstr, sf_node->selOp, sf_node->literal, sch, newAnd);
			}
			else
			{
				get_cnf(tempcnfstr, sf_node->selOp, sf_node->literal, sch);
			}
			
			

			// get_cnf(tempcnfstr, sf_node->selOp, sf_node->literal, sch);
			pipeMap.insert({out_pipe, outpipe});
            return sf_node;
        }
        else{
            SelectPipe_node *sp_node = new SelectPipe_node(); 
            std::sort(aliases.begin(), aliases.end());
			
            string out_pipe = "";
            for(int i =0; i<aliases.size(); i++){
                out_pipe = out_pipe+ "_"+aliases[i];
				tables.push_back(aliasToRel.at(aliases[i]));
	        }
			sp_node->tables = tables;
			sp_node->node_type = SP;
			sp_node->out_pipe_name = out_pipe;
			sp_node->cnf_str = cnf_str;
		    return sp_node;
        }

}

TreeNode *generateProjectNode(string cnf_str, string last_outpipe, int numAttsOutput, vector<string> projtables, vector<string> columns){
	Project_node *p_node = new Project_node(); 
	p_node->node_type = P;
	p_node->tables = projtables;
	p_node->numAttsOutput = numAttsOutput;
 	p_node->out_pipe_name = "_"+last_outpipe;
	p_node->cnf_str = cnf_str;
	// p_node->numAttsInput = 
	p_node->numAttsOutput = numAttsOutput;
	p_node->columns = columns;
	return p_node; 

}
TreeNode *generateSumNode(string last_outpipe, string sum_on, vector<string> tabnames){
	Sum_node *s_node = new Sum_node(); 
	s_node->node_type = S;
	s_node->out_pipe_name = "_"+last_outpipe;

	s_node->cnf_str = "("+sum_on+")";
	s_node->tables = tabnames;

	Schema sch = get_join_schema(s_node->tables);
	char to_pass[s_node->cnf_str.length()];
	strcpy(to_pass, s_node->cnf_str.c_str());
	
	get_cnf(to_pass, s_node->computeMe, sch);
	return s_node; 

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
		last_alias = "";
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
			condition=condition+attName+getOperandFromCode(currOr->left->code);
			if(currOr->left->right->code==STRING){
				condition = condition + "'"+currOr->left->right->value+"'";
			}else
			{
				condition = condition + currOr->left->right->value;
			}
			

			currOr = currOr->rightOr;
			last_alias = alias;
		}
		condition = condition + ")";
		if (loop_ind==1){
			if (selection.count(alias)==0){
				selection.insert({alias, true});
				select_node = generateSelectNode(temp_aliases, aliasAs, loop_ind, tableName, aliasToRel, condition);
				operations_tree.insert({select_node->out_pipe_name, select_node});
			}
			else{
				TreeNode *t = operations_tree.at("_"+alias);
				t->cnf_str = t->cnf_str + " AND " + condition;
			}
		}
		else{
			SelectPipe_node *select_node = (SelectPipe_node*)generateSelectNode(temp_aliases, aliasAs, loop_ind, tableName, aliasToRel, condition);
			operations_tree.insert({select_node->out_pipe_name, select_node});
			selectPipeVector.push_back(select_node);
		}
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
		string strleft, strright;
		if(selection.count(aliasLeft)==0){
			temp_aliases.clear();
			strleft = attNameLeft.substr(attNameLeft.find('.')+1, attNameLeft.length());
			// condition = "("+strleft+"="+strleft+")";
			condition = "("+attNameLeft+"="+attNameLeft+")";
			selection.insert({aliasLeft, true});
			TreeNode *select_node = new TreeNode();
			temp_aliases.push_back(aliasLeft);
			select_node = generateSelectNode(temp_aliases, aliasAs, 1, tableName, aliasToRel, condition);
			operations_tree.insert({select_node->out_pipe_name, select_node});

		}
		if(selection.count(aliasRight)==0){
			temp_aliases.clear();
			strright = attNameRight.substr(attNameRight.find('.')+1, attNameRight.length());
			// condition = "("+strright+"="+strright+")";
			condition = "("+attNameRight+"="+attNameRight+")";
			selection.insert({aliasRight, true});
			TreeNode *select_node = new TreeNode();
			temp_aliases.push_back(aliasRight);
			select_node = generateSelectNode(temp_aliases, aliasAs, 1, tableName, aliasToRel, condition);
			operations_tree.insert({select_node->out_pipe_name, select_node});

		}
	}
	if(selectionVector.size()==0 && joinVector.size()==0){
		cout<<"NOTHING PRESENT"<<endl;
	}
}
TreeNode *generateWriteOutNode(string last_outpipe){
	
	WriteOut_node *w_node = new WriteOut_node(); 
	w_node->node_type = W;
	w_node->out_pipe_name = "_"+last_outpipe;
	return w_node; 

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
	// cout << "Cost for sequence: " << minCostPermutation << " = " <<min_cost<<endl;
	return minCostPermutation;
}

void printPlan(){
	for ( auto it = operations_tree.begin(); it != operations_tree.end(); ++it ){
			plan = plan + "SF"+it->second->cnf_str+" => "+it->second->out_pipe_name+"\n";
			last_out_pipe = it->second->out_pipe_name;
	}
}

TreeNode* getJoinNode (string lPipe, string rPipe, string opPipe, vector <OrList*> &joinVector, int index, vector<string> joinedTables, unordered_map <string, string> aliasToRel) {
	
	Join_node *node = new Join_node();
	node->node_type = J;
	node->input_pipe_l = lPipe;
	node->input_pipe_r = rPipe;
	node->out_pipe_name = opPipe;
	node->right_child = NULL;
	node->left_child = NULL;
	node->tables = joinedTables;
	Pipe *outpipe = new Pipe(100000);
	pipeMap.insert({opPipe, outpipe});
	string leftVal = joinVector[index]->left->left->value;
	string rightVal = joinVector[index]->left->right->value;
	// cout << "leftVal: "<<leftVal<<endl;
	// cout << "rightVal: "<<rightVal<<endl;
	string left_cnf = leftVal.substr(leftVal.find('.')+1, leftVal.length());
	string right_cnf = rightVal.substr(rightVal.find('.')+1, rightVal.length());
	node->cnf_str= "(" + left_cnf+" = " + right_cnf + ")";
	
	vector <string> tableNames;
	for (int k =0; k<node->tables.size();k++){
		tableNames.push_back(aliasToRel.at(node->tables[k]));
	}
	Schema sch = get_join_schema(tableNames);

	char cnf_Str[node->cnf_str.length()];
	strcpy(cnf_Str, node->cnf_str.c_str());
	get_cnf(cnf_Str, node->selOp, node->literal, sch);

	return node;
}

void createJoinTree(vector <OrList*> &joinVector, string minCostPermutation, vector <string> aliasAs, unordered_map <string, string> aliasToRel) {
	
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
		// cout << "Output Pipe:" << opPipe <<endl;
		joinNode = getJoinNode(leftPipe, rightPipe, opPipe, joinVector, ((int)minCostPermutation[index] - 48), pipeId_to_alias.at(lpid), aliasToRel);
		operations_tree.insert({joinNode->out_pipe_name, joinNode}); 
		join_tree.push_back((Join_node*)joinNode);
	}

}


double calculateSFCost(Statistics s, unordered_map <string, char*> relToAlias){
	double cost = 0.0;
	
	for ( auto it = operations_tree.begin(); it != operations_tree.end(); ++it ){
			TreeNode *t = it->second;
			if(t->node_type==SF){
				char *relName[1];
				cout << t->tables[0]<<endl;
				// strcpy(relName[0], t->tables[0].c_str());
				relName[0] = (char*)t->tables[0].c_str();
				char *cnf = (char*)t->cnf_str.c_str(); 
				yy_scan_string(cnf);
				if (yyparse() != 0) {
						std::cout << "Can't parse your CNF.\n";
						exit (1);
					}
				double result = s.Estimate(final, relName, 1);
				cost = cost + result;
			}
	}
	return cost;
}

TreeNode* generateGroupByNode(OrderMaker * groupOrder, 
							  string cnf_str,
							  Function *func, 
							  vector<string> group_tables, 
							  string ip_pipe, 
							  string op_pipe) {
	GroupBy_node *node =  new GroupBy_node;
	node->groupOrder = groupOrder;
	node->node_type = G;
	node->cnf_str = cnf_str;
	node->computeMe  = func;
	node->left_child = NULL;
	node->right_child = NULL;
	node->tables = group_tables;
	node->input_pipe = ip_pipe;
	node->out_pipe_name = op_pipe;

	return node;
}

void make_group_by() {

	vector<string> group_tables;
	vector <string> tableName;
	vector <string> aliasAs;
	unordered_map <string, string> aliasToRel;
	unordered_map <string, char*> relToAlias;
	TreeNode *group_by_node = new TreeNode();
	Function func;
	string str;
	NameList *tempGroupingAtts = groupingAtts;
	getTableAndAliasNames(tableName, aliasAs, aliasToRel, relToAlias);
	
	while(tempGroupingAtts) {
		string attr = tempGroupingAtts->name;
		string tablealias = attr.substr(0,attr.find('.'));
		group_tables.push_back(aliasToRel.at(tablealias));
		tempGroupingAtts = tempGroupingAtts->next;
	}
	
	Schema groupSchema = get_join_schema(group_tables);
	OrderMaker groupOrder (&groupSchema);

	str = finalFunction->leftOperand->value;
	string tbname = str.substr(0,str.find('.'));
	// string str1 = aliasToRel.at(tbname) +"." + str.substr(str.find(".")+1,str.length()-1);
	string str1 = str.substr(str.find(".")+1,str.length());
	char * cnf_str = (char*)malloc(str1.length());
	strcpy(cnf_str, str1.c_str());
	get_cnf ((char*)cnf_str, &groupSchema, func);
	
	sort(group_tables.begin(), group_tables.end());
	string ip_pipe = "";
	for(auto it = group_tables.begin(); it != group_tables.end(); it++)
		ip_pipe = ip_pipe+(*it);

	string op_pipe = "_"+ip_pipe;
	group_by_node = generateGroupByNode(&groupOrder, cnf_str, &func, group_tables, ip_pipe, op_pipe);
	operations_tree.insert({op_pipe, group_by_node});
	groupByNode = (GroupBy_node*)group_by_node;
}

TreeNode* createTree() {
	//loop through joinVector and find corresponding input pipes an thus the child nodes
	string last_ip, last_op;
	bool empty_last_op =  false;
	TreeNode * root = new TreeNode();
	if(join_tree.size() > 0){
		for(auto join_it = join_tree.begin(); join_it != join_tree.end(); join_it++) {
			// int pid = relToPid.find(leftVal.substr(0, leftVal.find('.')))->second;
			// for(int it = 0; it < joinedTables.size(); it++) {
				// relToPid.at(joinedTables[it]) = pid;
			// }
			//get left and right child for all join nodes;
			(*join_it)->left_child = operations_tree.find((*join_it)->input_pipe_l)->second;
			(*join_it)->right_child = operations_tree.find((*join_it)->input_pipe_r)->second;

			//if sf then update sch of both and get cnf for both.

			root = *join_it;
			last_op = (*join_it)->out_pipe_name;
		}
	} else
	{
		empty_last_op = true;
	}
	
	//add all select pipes if there are any
	for(auto sp_it = selectPipeVector.begin(); sp_it != selectPipeVector.end(); sp_it++) {
		
		if(empty_last_op) {
			last_op = "_"+(*sp_it)->tables[0];
			(*sp_it)->right_child = operations_tree.find(last_op)->second;
			empty_last_op =  false;
		}else
		{
			(*sp_it)->right_child = root;
		}
		
		(*sp_it)->input_pipe = last_op;
		(*sp_it)->out_pipe_name ="_"+last_op;
		last_op = (*sp_it)->out_pipe_name;
		Pipe *spoutpipe = new Pipe(100000);
		pipeMap.insert({last_op, spoutpipe});
		root = *sp_it;
	}
	//group by , if group by is empty then add sum node
	if(groupingAtts != NULL) {
		
		if(empty_last_op) {
			last_op = "_"+groupByNode->tables[0];
			groupByNode->right_child = operations_tree.find(last_op)->second;
			empty_last_op = false;
		}
		else {
			groupByNode->right_child = root;
		}
		groupByNode->input_pipe = last_op;
		groupByNode->out_pipe_name = "_"+last_op;
		last_op = groupByNode->out_pipe_name;
		Pipe *grpoutpipe = new Pipe(100000);
		pipeMap.insert({last_op, grpoutpipe});
		groupByNode->left_child = NULL;
		root = groupByNode;
	}
	else{
		 if(finalFunction != NULL){
			sumNode->left_child = NULL;
			if(empty_last_op) {
				last_op = "_"+sumNode->tables[0];
				sumNode->right_child = operations_tree.find(last_op)->second;
				empty_last_op = false;
			}
			else
			{
				sumNode->right_child = root;
			}
			
			sumNode->input_pipe = last_op;
			sumNode->out_pipe_name = "_"+last_op;
			last_op = sumNode->out_pipe_name;
			Pipe *sumoutpipe = new Pipe(100000);
			pipeMap.insert({last_op, sumoutpipe});
			root = sumNode;
		 }
	}
	if (attsToSelect!=NULL){
		projectNode->left_child = NULL;
		if(empty_last_op) {
			last_op = "_"+projectNode->tables[0];
			projectNode->right_child = operations_tree.find(last_op)->second;
			empty_last_op = false;
		}
		else
		{
			projectNode->right_child = root;
		}
		
		projectNode->input_pipe = last_op;
		projectNode->out_pipe_name = "_"+last_op;
		last_op = projectNode->out_pipe_name;
		Pipe *projoutpipe = new Pipe(100000);
		pipeMap.insert({last_op, projoutpipe});
		root = projectNode;
	}
	writeOutNode->right_child = root;
	writeOutNode->left_child = NULL;
	writeOutNode->input_pipe = last_op;
	writeOutNode->out_pipe_name = "_"+last_op;
	Pipe *writeoutpipe = new Pipe(100000);
	pipeMap.insert({last_op, writeoutpipe});
	root = writeOutNode;

	return root;
}
int clear_pipe (Pipe &in_pipe, Schema *schema, bool print) {
	cout<<"Inside clear pipe"<<endl;
	Record rec;
	int cnt = 0;
	while (in_pipe.Remove (&rec)) {
		if (print) {
			rec.Print (schema);
		}
		cnt++;
	}
	return cnt;
}
void executeTree(TreeNode* root, unordered_map<string,string>aliasToRelation) {
	
	stack<TreeNode*> stack1;
	stack<TreeNode*> stack2;
	stack<TreeNode*> stack3;
	TreeNode * temp = root;
	stack1.push(temp);
	while(!stack1.empty()) {
		TreeNode * node = stack1.top();
		stack1.pop();
		stack2.push(node);
		stack3.push(node);
		if(node->right_child != NULL){
			stack1.push(node->right_child);
		}
			
		if(node->left_child != NULL){
			stack1.push(node->left_child);
		}
	}
	int lastSchNumatts;
	Schema * lastSchema;
	while(!stack2.empty()) {
		TreeNode *node = stack2.top();
		//Process each of these nodes
		if(node->node_type == SF)
		{
			SelectFile_node *sfnode = (SelectFile_node *)node;
			DBFile dbfsf;
			
			string fname = aliasToRelation.find(sfnode->tables[0])->second + ".bin";
			dbfsf.Open(fname.c_str());
			
			char cnf_to_pass[sfnode->cnf_str.length()];
			strcpy(cnf_to_pass, sfnode->cnf_str.c_str());

			string sch_str = aliasToRelation.find(sfnode->tables[0])->second;
			char sch_to_pass[sch_str.length()];
			strcpy(sch_to_pass, sch_str.c_str());
			Schema sch("catalog",sch_to_pass);
			lastSchema->myAtts = sch.GetAtts();
			lastSchema->numAtts = sch.GetNumAtts();
			get_cnf(cnf_to_pass, sfnode->selOp, sfnode->literal, sch);
			sfnode->sf.Run(dbfsf, *pipeMap.at(sfnode->out_pipe_name), sfnode->selOp, sfnode->literal);
			// int cnt = clear_pipe (*pipeMap.at(sfnode->out_pipe_name), &sch, false);
			
			// while()
		}
		else if(node->node_type == P) 
		{
			cout<<"last sch count"<<lastSchema->GetNumAtts()<<endl;
			Project_node *pnode = (Project_node *)node; 
			pnode->numAttsInput = lastSchema->GetNumAtts();
			cout<<"project cnf "<< pnode->cnf_str<<endl;
			cout<<"Find me in project"<<lastSchema->Find("n_nationkey");
			int numCols = pnode->columns.size();
			int *keepMe = new int[numCols] ;
			cout<<"pnode_cols"<<endl;
			cout<<pnode->columns.size()<<endl;
			for(int i = 0; i < numCols; i++)
			{
					keepMe[i] = lastSchema->Find((char*)pnode->columns[i].c_str());
			}
			Attribute * atts = lastSchema->GetAtts();
			Attribute att[numCols];
			for(int i = 0; i < pnode->columns.size(); i++){
				att[i] = atts[keepMe[i]];
			}
			Schema mySchema("project_sch", pnode->columns.size(), att);

			lastSchema->myAtts = mySchema.GetAtts();
			lastSchema->numAtts = mySchema.GetNumAtts();
			
			pnode->keepMe = keepMe;
			pnode->p_relops.Run(*pipeMap.at(pnode->input_pipe), *pipeMap.at(pnode->out_pipe_name) ,keepMe, pnode->numAttsInput,pnode->numAttsOutput);
			
		}
		else if(node->node_type == S)
		{
			Sum s;
			Sum_node *snode = (Sum_node *)node;
			s.Run(*pipeMap.at(snode->input_pipe), *pipeMap.at(snode->out_pipe_name), snode->computeMe);
			s.WaitUntilDone();
		}	
		else if(node->node_type == J) {
			cout<<"Entered join sch"<<endl;
			Join_node *jnode = (Join_node*)node;
			vector <string> tableNames;
			cout<<"size of tables"<<endl;
			cout<<jnode->tables.size()<<endl;
			for (int k =0; k<jnode->tables.size();k++){
				string alias = jnode->tables[k];
				cout<<"Alias is "<<alias;
				string test = aliasToRelation.find(alias)->second;
				tableNames.push_back(test);
			}
			Schema mySchema = get_join_schema(tableNames);
			cout<<"join sch count"<<mySchema.GetNumAtts()<<endl;
			cout<<"Find me "<<mySchema.Find("n_nationkey");
			lastSchema->myAtts = mySchema.GetAtts();
			lastSchema->numAtts = mySchema.GetNumAtts();
			jnode->j_rel.Run(*pipeMap.at(jnode->input_pipe_l), *pipeMap.at(jnode->input_pipe_r), *pipeMap.at(jnode->out_pipe_name), jnode->selOp, jnode->literal);
			lastSchNumatts = mySchema.GetNumAtts();
		}
		else if(node->node_type == G) {
			GroupBy g;
			GroupBy_node * gnode = (GroupBy_node *)node;
			g.Run(*pipeMap.at(gnode->input_pipe), *pipeMap.at(gnode->out_pipe_name), *gnode->groupOrder, *gnode->computeMe);
			g.WaitUntilDone();
		}
		else if(node->node_type == SP) {
			SelectPipe sp;
			SelectPipe_node *spnode = (SelectPipe_node *)node;
			sp.Run(*pipeMap.at(spnode->input_pipe), *pipeMap.at(spnode->out_pipe_name), spnode->selOp, spnode->literal);
			sp.WaitUntilDone();
		}
		else if(node->node_type == W) {
			// std::string setOpstr(setOp);
			// if(setOpstr=="STDOUT"){

			// }
			// else if (setOpstr=="NONE"){

			// }
			// else{
				WriteOut_node *wnode = (WriteOut_node *)node;
				char *fwpath = "testing_tushar.txt";
				FILE *writefile = fopen (fwpath, "w");
				wnode->wo_relOps.Run(*pipeMap.at(wnode->input_pipe), writefile, *lastSchema);
				
			// }
			
		}
		// cout << node->node_type<<endl;
		// sf.WaitUntilDone();
		
		stack2.pop();		
	}
	while(!stack3.empty()) {
		TreeNode *node = stack3.top();
		if(node->node_type == SF)
		{
			SelectFile_node *sfnode = (SelectFile_node *)node;
			sfnode->sf.WaitUntilDone();
		}
		else if(node->node_type == P) 
		{
			
			Project_node *pnode = (Project_node *)node; 
			pnode->p_relops.WaitUntilDone();
			
		}
		else if(node->node_type == J) {
			Join_node *jnode = (Join_node*)node;
			jnode->j_rel.WaitUntilDone();
			
		}
		else if(node->node_type == W) {
			// std::string setOpstr(setOp);

			// if(setOpstr=="STDOUT"){

			// }
			// else if (setOpstr=="NONE"){

			// }
			// else{
				WriteOut_node *wnode = (WriteOut_node *)node;
				wnode->wo_relOps.WaitUntilDone();
			// }
			
		}
		stack3.pop();	
	}
	
}

Type getAttrType(char * input){
	string ipStr(input);
	if (ipStr=="STRING"){
		return String;
	}
	else if(ipStr=="INTEGER"){
		return Int;
	}
	else if(ipStr=="DOUBLE"){
		return Double;
	}
	
}

void showPlan () {
	alias_to_pipeId.clear();
	operations_tree.clear();
	join_tree.clear();
	selectPipeVector.clear();
	pipeMap.clear();
	schemaMap.clear();
	relToPid.clear();
	pidToRel.clear();
	aliasToRel.clear();
	relToAlias.clear();


	// Getting the split of joins and selections 
	vector <OrList*> joinVector;
	vector <OrList*> selectionVector;
    
	Statistics s;
	
	char *relName[] = {"supplier","customer","nation", "part", "partsupp", "lineitem"};
	s.Read("Statistics.txt");
	
	separateJoinSelection(joinVector, selectionVector);
	
	// Getting the table names and aliases 
	vector <string> tableName;
	vector <string> aliasAs;
	
	getTableAndAliasNames(tableName, aliasAs, aliasToRel, relToAlias);
	for(int i = 0; i< aliasAs.size(); i++) {
		relToPid.insert({aliasAs[i], i});
		vector<string> dummy;
		dummy.push_back(aliasAs[i]);
		pidToRel.insert({i,dummy});
	}

	getSelectFileNodes(selectionVector, joinVector, tableName, aliasAs, aliasToRel);
	printPlan();

	string optimalJoinSeq = getOptimalJoinSequence(joinVector, s, aliasAs);
	createJoinTree(joinVector, optimalJoinSeq, aliasAs, aliasToRel);
	
	// Print Joins
	for(int i =0;i<join_tree.size();i++){
		Join_node *t = join_tree[i];
		plan= plan + "On "+t->input_pipe_l+" and "+ t->input_pipe_r+":"+"\n";
		plan= plan+ "J"+t->cnf_str+" => "+t->out_pipe_name+"\n";
		plan = plan + "OutputSchema:"+"\n";
		vector <string> tableNames;
		for (int k =0; k<t->tables.size();k++){
			tableNames.push_back(aliasToRel.at(t->tables[k]));
		}
		Schema mySchema = get_join_schema(tableNames);
		Attribute *rel1_atts =  mySchema.GetAtts(); 
		for(int j = 0;j<mySchema.GetNumAtts();j++){
			plan = plan + rel1_atts[j].name+": "+to_string(rel1_atts[j].myType)+" ";
		}
		cout<<endl;
		last_out_pipe = t->out_pipe_name;
	}
	
	// double sf_cost = calculateSFCost(s, relToAlias);

//DISTINCT
	if(distinctAtts == 1) {
		int numAttsp =0;
		string distinct = "(";
		plan = plan + "On: " +  last_out_pipe +"\n";
		if (attsToSelect!=NULL){
			struct NameList *selectAtt = attsToSelect;
			while(selectAtt){
				numAttsp++;
				if(numAttsp == 1){
					distinct = distinct +selectAtt->name;
				}
				else{
					distinct = distinct +","+selectAtt->name;
				}

			}
			distinct = distinct + ")";	
			plan = plan + "D: " + distinct+ " =>"+ last_out_pipe+"\n";
			last_out_pipe = "_"+last_out_pipe;
			TreeNode *project_node = new TreeNode();
		}
	}

	string projection = "(";
	int numAttsp = 0;
	// Checking if Projection is present
	if (attsToSelect!=NULL){
		vector<string> projtables;
		vector<string> finalprojtables;
		vector<string> columns;
		struct NameList *selectAtt = attsToSelect;
		while(selectAtt){
			numAttsp++;
			if(numAttsp == 1){
				projection = projection +selectAtt->name;
			}
			else{
				projection = projection +", "+selectAtt->name;
			}
			string temp = selectAtt->name;
			string to_push = temp.substr(0,temp.find('.')); 
			string columnName = temp.substr(temp.find('.')+1); 
			columns.push_back(columnName);
			projtables.push_back(to_push);
			selectAtt = selectAtt->next;
		}
		projection = projection + ")";

		//generate tables
		for(int i =0; i<projtables.size(); i++) {
			// int pid = relToPid()
		}

		TreeNode *project_node = new TreeNode();
		project_node = generateProjectNode(projection, last_out_pipe, numAttsp, projtables, columns);
		plan = plan +"On "+last_out_pipe+":"+"\n";
		plan = plan +"P "+projection+" => _"+last_out_pipe+"\n";
		last_out_pipe = project_node->out_pipe_name;
		projectNode = (Project_node*)project_node;
		operations_tree.insert({last_out_pipe, project_node});

	}	
	//only print
	if(finalFunction != NULL) {
		cout<<"Function: (";		
		struct FuncOperator *funcOp = finalFunction;
		struct FuncOperator *preFunc;
		while(funcOp != NULL) {
			if(funcOp->leftOperator != NULL) {
				plan = plan + funcOp->leftOperator->leftOperand->value + " " + to_string(funcOp->code);
				string temp = funcOp->leftOperator->leftOperand->value;
				// string attrname = temp.substr(temp.find('.')+1, temp.length()-1);
				// strcpy(funcOp->leftOperator->leftOperand->value, attrname.c_str());
			}
			else {
				plan = plan + funcOp->leftOperand->value + ")" +"\n";
				string temp = funcOp->leftOperand->value;
				// string attrname = temp.substr(temp.find('.')+1, temp.length()-1);
				// strcpy(funcOp->leftOperand->value, attrname.c_str());
			}
			funcOp = funcOp->right;
		}
	}
	 
	 vector<string> group_tables;

	 if(groupingAtts != NULL) {
		 NameList * tempGroupingAtts = groupingAtts;
		plan = plan +"G On: (";
		while(tempGroupingAtts) {
			plan = plan +tempGroupingAtts->name + ", ";
			string attr = tempGroupingAtts->name;
			string tablealias = attr.substr(0,attr.find('.'));
			group_tables.push_back(aliasToRel.at(tablealias));
			tempGroupingAtts = tempGroupingAtts->next;
		}
		plan = plan + ")"+"\n";
		//print op pipes from SF nodes?
		//print fuction using inorder traversal if not null
		if(finalFunction != NULL) {
			plan = plan+"Function: (";		
			struct FuncOperator *funcOp = finalFunction;
			struct FuncOperator *preFunc;

			while(funcOp != NULL) {
				if(funcOp->leftOperator != NULL) {
					plan = plan+ funcOp->leftOperator->leftOperand->value + " "+to_string(funcOp->code);
					string temp = funcOp->leftOperator->leftOperand->value;
					// string attrname = temp.substr(temp.find('.')+1, temp.length()-1);
					// strcpy(funcOp->leftOperator->leftOperand->value, attrname.c_str());
				}
				else {
					plan = plan+  funcOp->leftOperand->value + ")" +"\n";
					string temp = funcOp->leftOperand->value;
					// string attrname = temp.substr(temp.find('.')+1, temp.length()-1);
					// strcpy(funcOp->leftOperand->value, attrname.c_str());
				}
				funcOp = funcOp->right;
			}
		}
		Schema groupSchema = get_join_schema(group_tables);
		OrderMaker groupOrder (&groupSchema);
		groupOrder.Print();
		make_group_by();
		
	}
	//if no group by then check if sum present
	else {
		if (finalFunction != NULL){
			TreeNode *sum_node = new TreeNode();

			string tempsumstr = finalFunction->leftOperand->value;
			string relname = tempsumstr.substr(0, tempsumstr.find('.'));
			vector<string> tabnames;
			tabnames.push_back(aliasToRel.find(relname)->second);
			
			string sum_on = tempsumstr.substr(tempsumstr.find('.')+1, tempsumstr.length());
			sum_node = generateSumNode(last_out_pipe, sum_on, tabnames);
			last_out_pipe = sum_node->out_pipe_name;
			operations_tree.insert({last_out_pipe, sum_node});
			sumNode = (Sum_node*)sum_node;
		}
	}
	
	// Finally printing out writeout
	TreeNode *writeout_node = new TreeNode();
	writeout_node = generateWriteOutNode(last_out_pipe);
	operations_tree.insert({"root", writeout_node});
	writeOutNode = (WriteOut_node*)writeout_node;
	plan = plan+"On "+last_out_pipe+":"+"\n";
	plan = plan+"W ( "+last_out_pipe+" )"+"\n";

}



int main(){
	bool run = true;
	string ip;
	while(run){
		cout<<"Enter next query"<<endl;
		getline (cin,ip);
		yy_scan_string(ip.c_str());
		yyparse();
		if (operType == 1){
			cout<<"Create query"<<endl;
			operType = 0;
			struct CreateList * currCr = create;
			vector <Attribute*> new_table_sch;
			struct Attribute * attr;
			while(currCr){
				attr = (Attribute *) malloc(sizeof(Attribute));
				attr->name = currCr->attName;
				attr->myType = getAttrType(currCr->attType);
				new_table_sch.push_back(attr);
				currCr=currCr->rightcrt;
			}
			Attribute atts [new_table_sch.size()];
			for(int i=0; i<new_table_sch.size();i++){
				atts[i] = *new_table_sch[i];
			}
			Schema *new_tab_sch = new Schema (createTab, new_table_sch.size(), atts);

			schemaMap.insert({createTab, new_tab_sch});
			DBFile dbf;
			std::string table_name(createTab);
			dbf.Create((table_name+".bin").c_str(), heap, NULL);
		}
		else if (operType == 2){
			DBFile dbf;
			 std::string table_name(insertQuery->tableName);

			dbf.Open((table_name+".bin").c_str());
			dbf.Load(*schemaMap.at(insertQuery->tableName), insertQuery->fileName);
			dbf.Close();
		}
		else if (operType == 3){
			schemaMap.erase(dropTab);
			std::string table_name(dropTab);
			remove((table_name+".bin").c_str());
			remove((table_name+"_lpage.txt").c_str());
			remove((table_name+"_dpage.txt").c_str());
			remove((table_name+"_type.txt").c_str());
		}
		else if (operType == 5){
			string setOpstr(setOp);
			// string setOpstr = "STDOUT";
			showPlan();
			if(setOpstr == "NONE"){
				cout << plan<<endl;
				plan.clear();
				last_out_pipe.clear();
			}
			else{
				TreeNode * finalTree = createTree();
				executeTree(finalTree, aliasToRel);
				plan.clear();
				last_out_pipe.clear();
			}
						
			// TreeNode * finalTree = createTree();
			// executeTree(finalTree, aliasToRel);
			
		}
		else if(operType == 6){
			run = false;
		}
		
	}
	

	
}

