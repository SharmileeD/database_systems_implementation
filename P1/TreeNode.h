#ifndef NODE_
#define NODE_
#include "string"
#include "Schema.h"
#include "Function.h"
#include <iostream>
#include <unordered_map>
#include <vector>


using namespace std;

class TreeNode
{
	public:

        std::string node_type;
        Schema *output_schema;
        TreeNode *left_child;
        TreeNode *right_child;
        std::string out_pipe_name;
	    
        TreeNode();
        TreeNode generateSelectNode(vector<string> aliases, 
                                    vector <string> aliasAs, 
                                    int num_rels, 
                                    vector <string> tableName,
                                    unordered_map<string, string> aliasToRel);
};

class SelectFile_node : public TreeNode{
    public:
        CNF selOp;
        Record literal;
};

class SelectPipe_node : public TreeNode{
    public:
        CNF selOp;
        Record literal;
        string input_pipe;
};

class Join_node : public TreeNode{
    public:
        CNF selOp;
        Record literal;
        string input_pipe_l;
        string input_pipe_r;

};

class Project_node : public TreeNode{
    public:
        int keepMe;
        int numAttsInput;
        int numAttsOutput;
};

class DuplicateRemoval_node : public TreeNode{
    public:
};

class Sum_node : public TreeNode{
    public:
        Function computeMe;
};

class GroupBy_node : public TreeNode{
    public:
        OrderMaker groupAtts;
        Function computeMe;
};

class WriteOut_node : public TreeNode{
    public:
};


#endif