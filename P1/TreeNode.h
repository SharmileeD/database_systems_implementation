#ifndef NODE_
#define NODE_
#include "string"
#include "Schema.h"
#include "Function.h"
#include <iostream>
#include <unordered_map>
#include <vector>


using namespace std;

enum NODE_TYPE {
        SF,
        SP,
        J,
        D,
        G,
        S,
        P,
        W
    };

class TreeNode
{
   	public:

        NODE_TYPE node_type;
        vector<string> tables;
        TreeNode *left_child;
        TreeNode *right_child;
        std::string out_pipe_name;
        std::string cnf_str;
	    
        TreeNode();
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
        string input_pipe;
};
//distinc
class DuplicateRemoval_node : public TreeNode{
    public:
        int distinctAtts;
        int distinctFunc;
        string input_pipe;
};

class Sum_node : public TreeNode{
    public:
        Function computeMe;
        string input_pipe;
};

class GroupBy_node : public TreeNode{
    public:
        OrderMaker *groupOrder;
        Function *computeMe;
        string input_pipe;
};

class WriteOut_node : public TreeNode{
    public:
        string input_pipe;
};


#endif