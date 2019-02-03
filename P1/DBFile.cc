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

//Method to Create a DBFile store
int DBFile::Create (const char *f_path, fType f_type, void *startup) {
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
void DBFile::Load (Schema &f_schema, const char *loadpath) {
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
int DBFile::Open (const char *f_path) {
	try
	{   off_t dirty = 0;
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
void DBFile::MoveFirst () {
    
    // Set Record offset to 0
    this->record_offset = 0;
    // Set current page to 0
    this->current_page = 0;

}
// Method to Close the DBFile
// This method flushes the buffer_page to the file on disk
int DBFile::Close () {
	try
	{
      // Here before closing the file we will write out the records present in the page buffer if any to the file
       off_t dirty = 0;
        off_t last_page = 0;
        dirty = this->GetValueFromTxt(this->meta_dpage_name);
        if (dirty == 1){
              
            last_page = GetValueFromTxt(this->meta_lpage_name);
            if (last_page != 0){
                last_page = last_page -1;
            }
            
            this->file_instance.AddPage(&this->buffer_page, last_page);
                
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
void DBFile::Add (Record &rec) {
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
            this->buffer_page.Append(&rec);
        }
	    
    }
    catch(exception e){
       cerr << e.what() <<"Inside Add DBFile" <<'\n';
    }
    
}

//Function to get the record in the next position
int DBFile::GetNext (Record &fetchme) {
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
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
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
    
}
