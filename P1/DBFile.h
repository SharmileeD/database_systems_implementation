#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "string"
typedef enum {heap, sorted, tree} fType;

// stub DBFile header..replace it with your own DBFile.h 

class DBFile {
private:
//	Page  buffer_page;
	Record*  rec_pointer;
//	char  aux_text_file [];
	
public:
	DBFile (); 
	Page  buffer_page;
	File  file_instance;
	int record_offset;
	off_t current_page;
	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	void GetValueFromTxt(char file_name[], off_t &return_value);
	void SetValueFromTxt(char file_name[], off_t set_value);
	File* GetFileInstance();


};
#endif
