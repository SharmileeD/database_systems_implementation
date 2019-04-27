#include "TreeNode.h"
#include <vector>
#include <algorithm>
#include <iostream>
#include <unordered_map>

using namespace std;

TreeNode::TreeNode (){
}

TreeNode TreeNode::generateSelectNode(vector <string> aliases, 
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