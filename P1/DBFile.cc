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
        this->SetSchemaName((char *)f_path);
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
        this->SetSchemaName((char *)f_path);
        this->SetValueFromTxt(this->meta_dpage_name, dirty);
        this->file_instance.Open(1,(char*)f_path);
        cout << "curr length " << this->file_instance.GetLength()<<endl;
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
	}
}
//Move the record pointer to the first record of the file
void DBFile::MoveFirst () {
    
    // 1. Move page contents to file
    // off_t last_page = 0;
    // int dirty_page = this->GetValueFromTxt(this->meta_dpage_name);
    // if(dirty_page==1){
    //     last_page = this->GetValueFromTxt(this->meta_lpage_name);
    //     this->file_instance.AddPage(&this->buffer_page, last_page-1);
    // }

    // 2. Set meta data dirty value to 0
    // this->SetValueFromTxt(this->meta_dpage_name, 0);
    
    // 3. Load first page from file

    //TODO Check for the case where there is no data in file 
    // if (this->file_instance.GetLength()!=0){
    //     this->file_instance.GetPage(&this->buffer_page, 0);
    // }
    // 4. Set offset to 0
    this->record_offset = 0;
    
    // 5. Set current page to 0
    this->current_page = 0;
    // Record temp;
    // 6. Call function in Page to set myrecs of buffer_page to current(offset(0 in this case))
    //Commented this out so as to 
   // this->buffer_page.MoveMyRecsPointer(this->record_offset, temp);

}
// Method to Close the DBFile
// This method flushes the buffer_page to the file on disk
// and empties the page out.
int DBFile::Close () {
	try
	{
       // off_t dirty = 0;
        //off_t last_page = 0;
        // dirty = this->GetValueFromTxt(this->meta_dpage_name);
        // if (dirty == 1){
              
        //     last_page = GetValueFromTxt(this->meta_lpage_name);
        //     if (last_page != 0){
        //         last_page = last_page -1;
        //     }
            
        //     this->file_instance.AddPage(&this->buffer_page, last_page);
                
        // }
        
        //this->buffer_page.EmptyItOut();
        //this->SetValueFromTxt(this->meta_dpage_name, 0);
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

int DBFile::GetNext (Record &fetchme) {
	int page_num = 0;
    // cout << "Current page: "<< thfile is->current_page << endl;
    // cout << "File Length:" <<  file_instance.GetLength() <<endl;
    // cout << "Offset===============================>" << this->record_offset << endl; 
    // 1. Move page contents to file
    off_t last_page = 0;
    int dirty_page = this->GetValueFromTxt(this->meta_dpage_name);

    //If tis action follows a write(Add) then we move the contents of the buffer to the file
    if(dirty_page==1){
        last_page = this->GetValueFromTxt(this->meta_lpage_name);
        this->file_instance.AddPage(&this->buffer_page, last_page-1);
        this->buffer_page.EmptyItOut();
    }

    // 2. Set meta data dirty value to 0
    this->SetValueFromTxt(this->meta_dpage_name, 0);
    
    // 3. Load current_page from file
    this->file_instance.GetPage(&this->buffer_page, this->current_page);

    if(this->buffer_page.MoveMyRecsPointer(this->record_offset, fetchme)){
        this->record_offset++;
		return 1;
    }
    else{
        this->current_page++;
        this->record_offset = 0;
        return 1;
    }

   return 0;
       
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
	bool found = true;

	cout << "Record offset in masala getNext:i " <<	this->record_offset  << "Current page :" << this->current_page<<endl;	
	// 1. Move page contents to file
	off_t last_page = 0;
	int dirty_page = this->GetValueFromTxt(this->meta_dpage_name);
    if(dirty_page==1){
        last_page = this->GetValueFromTxt(this->meta_lpage_name);
        this->file_instance.AddPage(&this->buffer_page, last_page-1);
    }
	// 2. Set meta data dirty value to 1
	this->SetValueFromTxt(this->meta_dpage_name, 0);

	while(this->current_page < this->file_instance.GetLength()-1) {

	    this->file_instance.GetPage(&this->buffer_page, this->current_page);
		cout << "Record offset in masala getNext:i " << this->record_offset  <<endl;	
		//If last record reached, read and move to next page 
		if(!(this->buffer_page.MoveMyRecsPointer(this->record_offset, fetchme))) {
			cout << "Reached last record" << endl;
			this->current_page++;
			if(comp.Compare(&fetchme, &literal, &cnf)){
				this->record_offset = 0;
				return 1;
			}
		} 
		else {
			while(!comp.Compare(&fetchme, &literal, &cnf)){
				record_offset ++;
				//If end of page is reached increment current_page and reset the record_offset
				if(!(this->buffer_page.MoveMyRecsPointer(this->record_offset, fetchme)))
				{
					this->current_page++;
					this->record_offset = 0;
					found = false;
					break;	
				}		
			}
			this->record_offset ++;
			if(found){
				return 1;	
			}
		} //else
	}//outer while
	return 0;
}

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

void DBFile:: SetSchemaName(char tblpath []){
    char * pch;
    char meta_file_name[30];
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
