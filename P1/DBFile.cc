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

DBFile::DBFile () 
{
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
        this->file_instance.Open(1,(char*)f_path);
        if (this->file_instance.GetLength()!=0){
            this->file_instance.GetPage(&this->buffer_page,0);
            
        }
        
        
		return 1;	
	}
	catch(const std::exception& e)
	{
		std::cerr << e.what() << '\n';
	}
}

void DBFile::MoveFirst () {
    
    // 1. Move page contents to file
    off_t last_page = 0;
    this->GetValueFromTxt("l_page.txt", last_page);
    this->file_instance.AddPage(&this->buffer_page, last_page-1);

    // 2. Set meta data dirty value to 1
    this->SetValueFromTxt("d_page.txt", 1);
    
    // 3. Load first page from file
    this->file_instance.GetPage(&this->buffer_page, 0);
    
    // 4. Set offset to 0
    this->record_offset = 0;
    
    // 5. Set current page to 0
    this->current_page = 0;
    Record temp;
    // 6. Call function in Page to set myrecs of buffer_page to current(offset(0 in this case))
    this->buffer_page.MoveMyRecsPointer(this->record_offset, temp);

}

int DBFile::Close () {
	try
	{
        //TODO copy buffer records to file before closing
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
void DBFile::Add (Record &rec) {
   // try {
        off_t last_page_added = 0;
        // This if statement checks if the page_buffer is full
        if(this->buffer_page.Append(&rec)!=1){
            
            //if its not the first page we're reading the page out of a txt file so as to maim=ntain persistence 
           if (this->file_instance.GetLength() != 0){
                
               GetValueFromTxt("l_page.txt", last_page_added);
                
           }
		
            // Here we write the page to file and empty it out and 
            // add record to the new empty page buffer
            int page_num = this->file_instance.GetLength();
            this->file_instance.AddPage(&this->buffer_page, last_page_added);
            last_page_added++;
            SetValueFromTxt("l_page.txt", last_page_added);
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
    
    // 1. Move page contents to file
    off_t last_page = 0;
    this->GetValueFromTxt("l_page.txt", last_page);
    this->file_instance.AddPage(&this->buffer_page, last_page-1);

    // 2. Set meta data dirty value to 1
    this->SetValueFromTxt("d_page.txt", 1);
    // 3. Load current_page from file
    this->file_instance.GetPage(&this->buffer_page, this->current_page);

    // 4. Call function in Page to set myrecs of buffer_page to current(offset( in this case))
    this->buffer_page.MoveMyRecsPointer(this->record_offset+1, fetchme);
	return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
	int page_num = 0;
	bool found = true;

/*	while(rec_ptr_page < file_instance.GetLength()) {
		this->file_instance.GetPage(&this->buffer_page, current_page);
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
				if(file_instance.GetLength() > current_page) {
					rec_ptr_page++;
					
				}	
				break;
			}//if		
		}
		if(found){
			cout << "Record foud!";
			return 1;	
		}
	}*/
	return 1;
}

void DBFile::GetValueFromTxt(char file_name [], off_t &return_value ){
    
    FILE * file;
    off_t target = 0;
    const char * filename_ptr;
    
    filename_ptr = file_name;
    file = fopen(filename_ptr, "r");
    fread(&target, sizeof(off_t),1, file);
    return_value = target;
    fclose(file);

}

void DBFile::SetValueFromTxt(char file_name [], off_t set_value ){
    
    FILE * file;
    const char * filename_ptr;
    filename_ptr = file_name;
    file = fopen(filename_ptr, "w");
    fwrite(&set_value, sizeof(off_t), 1, file);
    fclose(file);
}

File* DBFile::GetFileInstance(){
     return &this->file_instance;
}


