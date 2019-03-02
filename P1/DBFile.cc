#include "TwoWayList.h"
#include "TwoWayList.cc"	
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "iostream"
#include <fstream>
#include <typeinfo>
#include <cstring>
#include <vector>
#include "BigQ.h"
#include "Pipe.h"


using namespace std;

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile (){
    // this->buffer_page = Page();
}
Heap hp_obj;
Sorted srt_obj;
struct SortInfo { OrderMaker *myOrder; int runLength; };
//Method to Create a DBFile store
int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    try
    {   
        
        this->SetMetaDataFileName((char *)f_path);
        ofstream myfile;
        myfile.open (this->meta_type_name);
        // Check what is the 
        struct SortInfo * input_args;
	    input_args = (struct SortInfo *)startup;	
        if (f_type == heap){
            myfile << "heap\n";
            this->instVar = &hp_obj;
            
        }
        if (f_type == sorted){
            cout<<"Filetype is sorted"<<endl;
            myfile << "sorted\n";
            // Write the order maker here
            myfile << "sortorder"<<endl;
            myfile << input_args->myOrder->returnOrderMaker();
            myfile << "runlength"<<endl;
            myfile << input_args->runLength<<endl;
            this->instVar = &srt_obj;
        }
        myfile.close();
        this->SetMetaDataFileName((char *)f_path);
        this->SetValueFromTxt(this->meta_lpage_name, 0);
        this->SetValueFromTxt(this->meta_dpage_name, 1);
        int ret_val = this->instVar->Create(f_path, f_type, startup);
        
	    return ret_val;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        return 0;
    }	
}

// Method to bulk load the DBFile from a text file
// This method essentially calls the Add method of the DBFile class for each record read from the text file 
void DBFile::Load (Schema &f_schema, const char *loadpath) {
    try
    {
        this->instVar->Load(f_schema, loadpath);
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
}

//Method to open a file stored at f_path assuming there exists one and it has data inside
int DBFile::Open (const char *f_path) {
	try
	{   off_t dirty = 0;
        ifstream infile; 
        //Boolean variables to identify the part of file we are reading
        bool om_started = false;
        bool rl_started = false;

        this->SetMetaDataFileName((char *)f_path);
        this->SetValueFromTxt(this->meta_dpage_name, dirty);
        infile.open(this->meta_type_name);
        //Variable to store the line we just read
        string line;
        getline (infile,line);
        if(line == "heap"){            
            this->instVar = &hp_obj;
        }
        vector<int> wch_atts; 
        vector<Type> wch_types; 
        if(line == "sorted"){
            while ( getline (infile,line) )
                {
                    if (line=="sortorder"){
                        om_started = true;
                        continue;
                    }
                        
                    
                    if (line=="runlength"){
                            om_started = false;
                            rl_started = true;
                            continue;
                        }
                // cout << line << '\n';
                    if(om_started){
                        //write the order maker to two arrays
                        string token = line.substr(line.find(":")+2);
                        string att = token.substr(0,token.find(" "));
                        string ty = token.substr(token.find(" ")+1);
                        wch_atts.push_back(atoi(att.c_str()));
                        if(ty == "Int")
                            wch_types.push_back(Int);
                        else if(ty == "Double")
                            wch_types.push_back(Double);
                        else
                            wch_types.push_back(String);

                    }
                    if(rl_started){
                        srt_obj.run_length = atoi(line.c_str());
                    }

                }
                
                int wch_atts_arr [wch_atts.size()];
                Type wch_type_arr [wch_types.size()];
                cout<<"Printing the arrays"<<endl;
                for(int i =0; i<wch_atts.size();i++){
                    wch_type_arr[i] = wch_types[i];
                    wch_atts_arr[i] = wch_atts[i];
                }
                // srt_obj.odr_mkr = getOrderMaker(wch_atts_arr, wch_type_arr, wch_atts.size());
                Schema mySchema ("catalog", "lineitem");
                srt_obj.odr_mkr = OrderMaker(&mySchema);
            this->instVar = &srt_obj;


        }
        int ret_val = this->instVar->Open(f_path);
        
		return ret_val;	
	}
	catch(const std::exception& e)
	{
		std::cerr << e.what() << '\n';
        return 0;
	}
}

//Move the record pointer to the first record of the file
void DBFile::MoveFirst () {
    this->instVar->MoveFirst();
    // // Set Record offset to 0
    // this->record_offset = 0;
    // // Set current page to 0
    // this->current_page = 0;

}
// Method to Close the DBFile
// This method flushes the buffer_page to the file on disk
int DBFile::Close () {
	try
	{
        int return_val = this->instVar->Close();
		return return_val;		
	}
	catch(const std::exception& e) {
		std::cerr << e.what() << '\n';
		return 0;
	}
}

// Method to Add a record to the DBFile instance. 
// This essentially adds the record to the page buffer and 
// if the page buffer is full it writes the page out to disk 
// and after emptying the buffer it writes the record to the buffer
void DBFile::Add (Record &rec) {
    try {

        this->instVar->Add(rec);
	    
    }
    catch(exception e){
       cerr << e.what() <<"Inside Add DBFile" <<'\n';
    }
    
}

//Function to get the record in the next position
int DBFile::GetNext (Record &fetchme) {
    int ret_val = this->instVar->GetNext(fetchme);
    return ret_val;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    int ret_val = this->instVar->GetNext(fetchme, cnf, literal);
    return ret_val;
}

// Method to get the value of the metadata stored in the auxiliary text file
off_t DBFile::GetValueFromTxt(char file_name []){
    
    FILE * file;
    off_t target = 0;
    const char * filename_ptr;
    filename_ptr = file_name;
    file = fopen(filename_ptr, "r");
    fread(&target, sizeof(off_t),1, file);
    fclose(file);
    return target;

}
// Method to set the value of the metadata stored in the auxiliary text file
void DBFile::SetValueFromTxt(char file_name [], off_t set_value ){

    FILE * file;
    const char * filename_ptr;
    filename_ptr = file_name;
    file = fopen(filename_ptr, "w");
    fwrite(&set_value, sizeof(off_t), 1, file);
    fclose(file);
}

// Method to set the meta_lpage_name and meta_dpage_name variables by extracting the name of the Schema file
void DBFile:: SetMetaDataFileName(char tblpath []){
    char * pch;
    char meta_file_name[100];
    pch = strtok (tblpath,"/");
    char test [100];
    while (pch != NULL)
    {
    if(pch!=NULL){
        strcpy(test, pch);
    }
    pch = strtok (NULL, "/");
    }

    test[strlen(test)-4] = '\0';
    sprintf (meta_file_name, "%s_lpage.txt", test);
    strcpy(this->meta_lpage_name, meta_file_name);
    sprintf (meta_file_name, "%s_dpage.txt", test);
    strcpy(this->meta_dpage_name, meta_file_name);
    sprintf(meta_file_name, "%s_type.txt", test);
    strcpy(this->meta_type_name, meta_file_name);
    
}
GenericDBFile::GenericDBFile () 
{
    // this->buffer_page = Page();
}

//Method to Create a DBFile store
int GenericDBFile::Create (const char *f_path, fType f_type, void *startup) {return 0;}

// Method to bulk load the DBFile from a text file
// This method essentially calls the Add method of the DBFile class for each record read from the text file 
void GenericDBFile::Load (Schema &f_schema, const char *loadpath) {
}

//Method to open a file stored at f_path assuming there exists one and it has data inside
int GenericDBFile::Open (const char *f_path) {return 0;}

//Move the record pointer to the first record of the file
void GenericDBFile::MoveFirst () {

}
// Method to Close the DBFile
// This method flushes the buffer_page to the file on disk
int GenericDBFile::Close () {return 0;}

// Method to Add a record to the DBFile instance. 
// This essentially adds the record to the page buffer and 
// if the page buffer is full it writes the page out to disk 
// and after emptying the buffer it writes the record to the buffer
void GenericDBFile::Add (Record &rec) {    
}

//Function to get the record in the next position
int GenericDBFile::GetNext (Record &fetchme) {return 0;}

//Function to get the record after the current record which matches the cnf
int GenericDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) { return 0;}

// Method to get the value of the metadata stored in the auxiliary text file
off_t GenericDBFile::GetValueFromTxt(char file_name []){

}
// Method to set the value of the metadata stored in the auxiliary text file
void GenericDBFile::SetValueFromTxt(char file_name [], off_t set_value ){
}

// Method to set the meta_lpage_name and meta_dpage_name variables by extracting the name of the Schema file
void GenericDBFile:: SetMetaDataFileName(char tblpath []){
    
}

void GenericDBFile::GetPage(Page *putItHere, off_t whichPage){
    // this->file_instance.GetPage(putItHere, whichPage);
}
void GenericDBFile::testoutpipe (){}
Heap::Heap () 
{
    // this->buffer_page = Page();
}
void Heap::testoutpipe (){}

//Method to Create a DBFile store
int Heap::Create (const char *f_path, fType f_type, void *startup) {
    try
    {
        this->SetMetaDataFileName((char *)f_path);
        this->SetValueFromTxt(this->meta_lpage_name, 0);
        this->SetValueFromTxt(this->meta_dpage_name, 1);
    	this->file_instance.Open(0,(char*)f_path);
	    return 1;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        return 0;
    }	
}

// Method to bulk load the DBFile from a text file
// This method essentially calls the Add method of the DBFile class for each record read from the text file 
void Heap::Load (Schema &f_schema, const char *loadpath) {
    try
    {
        FILE *tableFile = fopen (loadpath, "r");

        Record temp;
        while (temp.SuckNextRecord (&f_schema, tableFile) == 1){
            this->Add(temp);
        } 
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
}

//Method to open a file stored at f_path assuming there exists one and it has data inside
int Heap::Open (const char *f_path) {
	try
	{   
        off_t dirty = 0;
        this->SetMetaDataFileName((char *)f_path);
        this->SetValueFromTxt(this->meta_dpage_name, dirty);
        this->file_instance.Open(1,(char*)f_path);
        if (this->file_instance.GetLength()!=0){
            this->file_instance.GetPage(&this->buffer_page,0);
            this->current_page = 0;
            this->record_offset = 0;
        }
        
		return 1;	
	}
	catch(const std::exception& e)
	{
		std::cerr << e.what() << '\n';
        return 0;
	}
}

//Move the record pointer to the first record of the file
void Heap::MoveFirst () {
    
    // Set Record offset to 0
    this->record_offset = 0;
    // Set current page to 0
    this->current_page = 0;

}
// Method to Close the DBFile
// This method flushes the buffer_page to the file on disk
int Heap::Close () {
	try
	{
      // Here before closing the file we will write out the records present in the page buffer if any to the file
       off_t dirty = 0;
        off_t last_page = 0;
        off_t test;
        // Record tempRec;
        Schema mySchema ("catalog", "lineitem");
        dirty = this->GetValueFromTxt(this->meta_dpage_name);
        if (dirty == 1){
              
            last_page = GetValueFromTxt(this->meta_lpage_name);
            test = this->file_instance.GetLength();
            if (this->file_instance.GetLength() != 0){
                
               last_page = this->file_instance.GetLength()-1;
                
           }
            // this->buffer_page.GetFirst(&tempRec);
            // tempRec.Print(&mySchema);
            this->file_instance.AddPage(&this->buffer_page, last_page);
            last_page++;
            this->SetValueFromTxt(this->meta_lpage_name, last_page);
                
        }
        
        this->SetValueFromTxt(this->meta_dpage_name, 0);
		this->file_instance.Close();
		return 1;		
	}
	catch(const std::exception& e) {
		std::cerr << e.what() << '\n';
		return 0;
	}
}

// Method to Add a record to the DBFile instance. 
// This essentially adds the record to the page buffer and 
// if the page buffer is full it writes the page out to disk 
// and after emptying the buffer it writes the record to the buffer
void Heap::Add (Record &rec) {
    try {
        off_t last_page_added = 0;
        this->SetValueFromTxt(this->meta_dpage_name, 1);
        // This if statement checks if the page_buffer is full
        if(this->buffer_page.Append(&rec)!=1){
            
            //if its not the first page we're reading the page out of a txt file so as to maim=ntain persistence 
           if (this->file_instance.GetLength() != 0){
                
               last_page_added = GetValueFromTxt(this->meta_lpage_name);
                
           }
		
            // Here we write the page to file and empty it out and 
            // add record to the new empty page buffer
            int page_num = this->file_instance.GetLength();
            this->file_instance.AddPage(&this->buffer_page, last_page_added);
            last_page_added++;
            this->SetValueFromTxt(this->meta_lpage_name, last_page_added);
            this->buffer_page.EmptyItOut();
            int test = this->buffer_page.Append(&rec);
        }
	    
    }
    catch(exception e){
       cerr << e.what() <<"Inside Add DBFile" <<'\n';
    }
    
}

//Function to get the record in the next position
int Heap::GetNext (Record &fetchme) {
	int page_num = 0;
    if (this->current_page == this->file_instance.GetLength()-1){
        return 0;
    }

    off_t last_page = 0;
    int dirty_page = this->GetValueFromTxt(this->meta_dpage_name);

    //If this action follows a write(Add) then we move the contents of the buffer to the file
    if(dirty_page==1){
        last_page = this->GetValueFromTxt(this->meta_lpage_name);
        this->file_instance.AddPage(&this->buffer_page, last_page-1);
        this->buffer_page.EmptyItOut();
    }

    // 2. Set meta data dirty value to 0
    this->SetValueFromTxt(this->meta_dpage_name, 0);
    
    // 3. Load current_page from file
    this->file_instance.GetPage(&this->buffer_page, this->current_page);
    
    //Here we call the MoveMyRecsPointer to advance the myrecs pointer by one position
    if(this->buffer_page.MoveMyRecsPointer(this->record_offset, fetchme)){
        this->record_offset++;
		return 1;
    }
    else{
        page_num = this->current_page + 1;
        if (page_num == this->file_instance.GetLength()){
            //This is where we've reached the last record of last page
            this->current_page++;
            return 1;
        }
        this->current_page++;
        this->record_offset = 0;
        return 1;
    }
    return 0;  
}

//Function to get the record after the current record which matches the cnf
int Heap::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
	bool found = true;
    int page_num = 0;
    if (this->current_page == this->file_instance.GetLength()-1){
        return 0;
    }
	// 1. Move page contents to file
	off_t last_page = 0;
	int dirty_page = this->GetValueFromTxt(this->meta_dpage_name);
    if(dirty_page==1){
        last_page = this->GetValueFromTxt(this->meta_lpage_name);
        this->file_instance.AddPage(&this->buffer_page, last_page-1);
        this->buffer_page.EmptyItOut();
    }
	// 2. Set meta data dirty value to 1
	this->SetValueFromTxt(this->meta_dpage_name, 0);

    //Looping through the records to find records that match the CNF and return the record that is found
    while(1){
        if (this->current_page == this->file_instance.GetLength()-1){
            return 0;
        }
        // 3. Load current_page from file
        this->file_instance.GetPage(&this->buffer_page, this->current_page);
        if(this->buffer_page.MoveMyRecsPointer(this->record_offset, fetchme)){
            this->record_offset++;
            if(comp.Compare(&fetchme, &literal, &cnf)){
                return 1;
            }
        }
        else{
            //All we know here is it is the last record of some page
            page_num = this->current_page + 1;
            if (page_num == this->file_instance.GetLength()){
                //This is where we've reached the last record of last page
                this->current_page++;
                if(comp.Compare(&fetchme, &literal, &cnf)){
                    return 1;
                }
                else{
                    return 0;
                }
            }
            if(comp.Compare(&fetchme, &literal, &cnf)){
                    this->current_page++;
                    this->record_offset = 0;
                    return 1;
                }
                else{
                    this->current_page++;
                    this->record_offset = 0;
                }
        }
    }
    return 0;
}

// Method to get the value of the metadata stored in the auxiliary text file
off_t Heap::GetValueFromTxt(char file_name []){
    
    FILE * file;
    off_t target = 0;
    const char * filename_ptr;
    filename_ptr = file_name;
    file = fopen(filename_ptr, "r");
    fread(&target, sizeof(off_t),1, file);
    fclose(file);
    return target;

}
// Method to set the value of the metadata stored in the auxiliary text file
void Heap::SetValueFromTxt(char file_name [], off_t set_value ){

    FILE * file;
    const char * filename_ptr;
    filename_ptr = file_name;
    file = fopen(filename_ptr, "w");
    fwrite(&set_value, sizeof(off_t), 1, file);
    fclose(file);
}

// Method to set the meta_lpage_name and meta_dpage_name variables by extracting the name of the Schema file
void Heap:: SetMetaDataFileName(char tblpath []){
    char * pch;
    char meta_file_name[100];
    pch = strtok (tblpath,"/");
    char test [100];
    while (pch != NULL)
    {
    if(pch!=NULL){
        strcpy(test, pch);
    }
    pch = strtok (NULL, "/");
    }

    test[strlen(test)-4] = '\0';
    sprintf (meta_file_name, "%s_lpage.txt", test);
    strcpy(this->meta_lpage_name, meta_file_name);
    sprintf (meta_file_name, "%s_dpage.txt", test);
    strcpy(this->meta_dpage_name, meta_file_name);
    sprintf (meta_file_name, "%s_type.txt", test);
    strcpy(this->meta_type_name, meta_file_name);
    
}

void Heap::GetPage(Page *putItHere, off_t whichPage){
    this->file_instance.GetPage(putItHere, whichPage);
}
Sorted::Sorted () 
{
    // this->buffer_page = Page();
}

//Method to Create a DBFile store
int Sorted::Create (const char *f_path, fType f_type, void *startup) {
 try
    {
        struct SortInfo * input_args;
	    input_args = (struct SortInfo *)startup;

        this->SetMetaDataFileName((char *)f_path);
        this->SetValueFromTxt(this->meta_lpage_name, 0);
        this->SetValueFromTxt(this->meta_dpage_name, 1);
        this->run_length = input_args->runLength;
        this->odr_mkr = *input_args->myOrder;
        this->file_instance.Open(0,(char*)f_path);
        return 1;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
        return 0;
    }      
}

// Method to bulk load the DBFile from a text file
// This method essentially calls the Add method of the DBFile class for each record read from the text file 
void Sorted::Load (Schema &f_schema, const char *loadpath) {}

//Method to open a file stored at f_path assuming there exists one and it has data inside
int Sorted::Open (const char *f_path) {
    off_t dirty = 0;
    this->SetMetaDataFileName((char *)f_path);
    this->SetValueFromTxt(this->meta_dpage_name, dirty);
    this->mode = "reading";
    this->file_instance.Open(1,(char*)f_path);
    if (this->file_instance.GetLength()!=0){
        this->file_instance.GetPage(&this->buffer_page,0);
        this->current_page = 0;
        this->record_offset = 0;
    }
    
    return 1;	
}

//Move the record pointer to the first record of the file
void Sorted::MoveFirst () {

}
// Method to Close the DBFile
// This method flushes the buffer_page to the file on disk
int Sorted::Close () {}

// Method to Add a record to the DBFile instance. 
// This essentially adds the record to the page buffer and 
// if the page buffer is full it writes the page out to disk 
// and after emptying the buffer it writes the record to the buffer
void Sorted::Add (Record &rec) {    
    try {
        Record tempRec;
        Schema mySchema ("catalog", "lineitem");
        Record ins;
        ins.Copy(&rec);
        if(this->mode == "reading"){
            // DO something
            BigQ bq (this->input_pipe, this->output_pipe, this->odr_mkr, this->run_length);
            this->mode = "writing";
            this->input_pipe.Insert(&rec);
            // this->input_pipe.Remove(&tempRec);
            // tempRec.Print(&mySchema);
            

        }
        if(this->mode == "writing"){
            // BigQ bq (this->input_pipe, this->output_pipe, this->odr_mkr, this->run_length);
            this->input_pipe.Insert(&ins);
            
        }
        // off_t last_page_added = 0;
        // this->SetValueFromTxt(this->meta_dpage_name, 1);
        // // This if statement checks if the page_buffer is full
        // if(this->buffer_page.Append(&rec)!=1){
            
        //     //if its not the first page we're reading the page out of a txt file so as to maim=ntain persistence 
        //    if (this->file_instance.GetLength() != 0){
                
        //        last_page_added = GetValueFromTxt(this->meta_lpage_name);
                
        //    }
		
        //     // Here we write the page to file and empty it out and 
        //     // add record to the new empty page buffer
        //     int page_num = this->file_instance.GetLength();
        //     this->file_instance.AddPage(&this->buffer_page, last_page_added);
        //     last_page_added++;
        //     this->SetValueFromTxt(this->meta_lpage_name, last_page_added);
        //     this->buffer_page.EmptyItOut();
        //     int test = this->buffer_page.Append(&rec);
        // }
	    
    }
    catch(exception e){
       cerr << e.what() <<"Inside Add DBFile" <<'\n';
    }
}

//Function to get the record in the next position
int Sorted::GetNext (Record &fetchme) {

}

void Sorted::testoutpipe () {
    Record rec;
    Schema mySchema ("catalog", "lineitem");
    this->input_pipe.ShutDown();
    while (this->output_pipe.Remove (&rec)) {
		rec.Print(&mySchema);
	}
}
//Function to get the record after the current record which matches the cnf
int Sorted::GetNext (Record &fetchme, CNF &cnf, Record &literal) {}

// Method to get the value of the metadata stored in the auxiliary text file
off_t Sorted::GetValueFromTxt(char file_name []){

}
// Method to set the value of the metadata stored in the auxiliary text file
void Sorted::SetValueFromTxt(char file_name [], off_t set_value ){
}

// Method to set the meta_lpage_name and meta_dpage_name variables by extracting the name of the Schema file
void Sorted:: SetMetaDataFileName(char tblpath []){
    
}