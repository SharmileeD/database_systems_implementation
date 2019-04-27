#ifndef NODE_
#define NODE_
#include "string"
#include "Schema.h"
#include "Function.h"

class TreeNode
{
	public:

        std::string node_type;
        Schema *output_schema;
        TreeNode *left_child;
        TreeNode *right_child;
	    
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
};

class Join_node : public TreeNode{
    public:
        CNF selOp;
        Record literal;

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