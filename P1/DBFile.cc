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

using namespace std;

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
	rec_ptr_page = 0;
	latest_page = 0;		
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    try
    {
	
    	this->file_instance.Open(0,(char*)f_path);
    	this->file_instance.Close();
	return 1;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }	
}

// Method to bulk load the DBFile from a text file
// This method essentially calls the Add method of the DBFile class for each record read from the text file 
void DBFile::Load (Schema &f_schema, const char *loadpath) {
    try
    {
        FILE *tableFile = fopen (loadpath, "r");
        Record temp;
        while (temp.SuckNextRecord (&f_schema, tableFile) == 1){
            Add(temp);
        } 
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    
    
}

int DBFile::Open (const char *f_path) {
	try
	{
		this->file_instance.Open(this->file_instance.GetLength(), (char*)f_path);
		return 1;	
	}
	catch(const std::exception& e)
	{
		std::cerr << e.what() << '\n';
	}
}

void DBFile::MoveFirst () {
//In this case we just flush the page buffer -> load the first page and set the pointer to the first record
	this->file_instance.GetPage(&this->buffer_page,0);
	TwoWayList <Record>* recordList = buffer_page.GetMyRecs();
        Record * new_rec = recordList->Current(0);
	rec_pointer = new_rec;
	rec_ptr_page = 0;	
}

int DBFile::Close () {
	try
	{
		this->file_instance.Close();
		return 1;		
	}
	catch(const std::exception& e) {
		std::cerr << e.what() << '\n';
	}
}

// Method to Add a record to the DBFile instance. 
// This essentially adds the record to the page buffer and 
// if the page buffer is full it writes the page out to disk 
// and after emptying the buffer it writes the record to the buffer
void DBFile::Add (Record &rec) {
   // try {
//       long last_page_added = 0;
        // This if statement checks if the page_buffer is full
        if(this->buffer_page.Append(&rec)!=1){
            
            //if its not the first page we're reading the page out of a txt file so as to maim=ntain persistence 
            //TODO: Consider adding helpers for the same so they can be used from other functions
//            if (this->file_instance.GetLength() != 0){
                
//                GetValueFromTxt(0, "aux_text_file.txt", latest_page);
                
//            }
		
            // Here we write the page to file and empty it out and 
            // add record to the new empty page buffer
            int page_num = this->file_instance.GetLength();
            this->file_instance.AddPage(&this -> buffer_page, latest_page);
            latest_page++;
            this->buffer_page.EmptyItOut();
            this->buffer_page.Append(&rec);
        
        }
        
        
	    
    // }
    //catch(exception e){
    //    cerr << e.what() <<"Inside Add DBFile" <<'\n';
    //}
    
}

int DBFile::GetNext (Record &fetchme) {
	int page_num = 0;
        this->file_instance.GetPage(&this->buffer_page, rec_ptr_page);
        TwoWayList <Record>* recordList = buffer_page.GetMyRecs();
	Record * new_rec = recordList->Current(0);
        char* bits;
	new_rec->GetRecordBits(bits);
	fetchme.SetRecordBits(bits);		
	//strncpy(fetchme., new_rec -> GetRecordBits(), sizeof(new_rec));	
	//fetchme = any_rec;
	//cout << typeid(fetchme).name() << endl;
	//cout << typeid(*any_rec).name() <<endl;
	//fetchme = *any_rec;
	recordList->Advance ();
	return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
	int page_num = 0;
	bool found = true;

	while(rec_ptr_page < file_instance.GetLength()) {
		this->file_instance.GetPage(&this->buffer_page, rec_ptr_page);
	        TwoWayList <Record>* recordList = buffer_page.GetMyRecs();
		if(recordList->RightLength() > 0)
                	rec_pointer = recordList->Current(0);
	        else
                	return 0;

		while(!comp.Compare(rec_pointer, &literal, &cnf)){
			if(recordList->RightLength() > 0) {
				recordList->Advance ();
				rec_pointer = recordList->Current(0);
			} else {
				cout <<"Record was not found on this page";
				found = false;
				if(file_instance.GetLength() > rec_ptr_page) {
					rec_ptr_page++;
					
				}	
				break;
			}//if		
		}
		if(found){
			cout << "Record foud!";
			return 1;	
		}
	}
}

void DBFile::GetValueFromTxt(int property, string text_store , long &return_value ){
   /* string temp_string;
    ifstream auxfile_in;
    std::string::size_type sz;
    auxfile_in.open(text_store);
    for(size_t i = 0; i < 3; i++)
    {
        auxfile_in >> temp_string;
        if (property == i){
            break;   
        }

    }
    
    return_value = std::stoi (temp_string,&sz);
    auxfile_in.close();
    */
}

void DBFile::SetValueFromTxt(int property, string text_store , long set_value ){
    ofstream auxfile_out;
    long old_values [3];
    for(size_t i = 0; i < 3; i++){
         GetValueFromTxt(i, "aux_text_file.txt", old_values[i]);
    } 
    
    auxfile_out.open("aux_text_file.txt", ios::trunc);
    for(size_t i = 0; i < 3; i++){
        
        if (property == i){
            auxfile_out << set_value << endl;
        }
        else{
            auxfile_out << old_values[i] << endl;
        }
    }
    auxfile_out.close();
}

File* DBFile::GetFileInstance(){
     return &this->file_instance;
}


