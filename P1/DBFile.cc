#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "iostream"
#include <fstream>
using namespace std;

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
	
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
  /*  this->file_instance.GetPage((off_t)0,this->buffer_page);
    this->buffer_page.GetFirst(this->rec_pointer);*/
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
    try
    {   int last_page_added = 0;
	off_t last_pg;
        // This if statement checks if the page_buffer is full
        if(this->buffer_page.Append(&rec)!=1){
            ofstream auxfile_out;
            ifstream auxfile_in;
            std::string::size_type sz;
            string last_page;
            auxfile_in.open("aux_text_file.txt");
            //if its not the first page we're reading the page out of a txt file so as to maim=ntain persistence 
            //TODO: Consider adding helpers for the same so they can be used from other functions
            if (this->file_instance.GetLength() != 0){
                auxfile_in >> last_page;
                last_page_added = std::stoi (last_page,&sz);
                last_pg = last_page_added;
		auxfile_in.close();
            }
		
            // Here we write the page to file and empty it out and 
            // add record to the new empty page buffer
            int page_num = this->file_instance.GetLength();
            this->file_instance.AddPage(&this->buffer_page, last_pg);
            last_pg = last_pg + 1;
            auxfile_out.open("aux_text_file.txt", ios::trunc);
            auxfile_out << last_pg << endl;
            auxfile_out.close();
            this->buffer_page.EmptyItOut();
            this->buffer_page.Append(&rec);
        
        }
        
        
	    
     }
    catch(exception e){
        cerr << e.what() << '\n';
    }
    
}

int DBFile::GetNext (Record &fetchme) {
	/*int page_num = 0;
	Record *fetch_record;
	this->file_instance->GetPage(this->buffer_page, page_num);
        //this->buffer_page->GetFirst(this->rec_pointer);	
	fetch_record->Copy(rec_pointer);
	//cout << fetch_record;
	return 1;*/
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
