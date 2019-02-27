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

class GenericDBFile{
	public:
		GenericDBFile();
		
		virtual int Create (const char *fpath, fType file_type, void *startup);
		virtual int Open (const char *fpath);
		virtual int Close ();

		virtual void Load (Schema &myschema, const char *loadpath);

		virtual void MoveFirst ();
		virtual void Add (Record &addme);
		virtual int GetNext (Record &fetchme);
		virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);
		virtual off_t GetValueFromTxt(char file_name[]);
		virtual void SetValueFromTxt(char file_name[], off_t set_value);
		virtual void SetMetaDataFileName(char tblpath []);
};
class Heap: public GenericDBFile{
	public:
		Heap (); 
		Page  buffer_page;
		File  file_instance;
		char meta_lpage_name[100];
		char meta_dpage_name[100];
		char meta_type_name[100];
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
		off_t GetValueFromTxt(char file_name[]);
		void SetValueFromTxt(char file_name[], off_t set_value);
		void SetMetaDataFileName(char tblpath []);
};

class Sorted: public GenericDBFile{
	public:
		Sorted ();
		Page  buffer_page;
		File  file_instance;
		char meta_lpage_name[100];
		char meta_dpage_name[100];
		char meta_type_name[100];
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
		off_t GetValueFromTxt(char file_name[]);
		void SetValueFromTxt(char file_name[], off_t set_value);
		void SetMetaDataFileName(char tblpath []);
};
class DBFile {
private:
	
public:
	DBFile (); 
	Page  buffer_page;
	File  file_instance;
	char meta_lpage_name[100];
	char meta_dpage_name[100];
	char meta_type_name[100];
	int record_offset;
	off_t current_page;
	GenericDBFile * instVar;

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
	off_t GetValueFromTxt(char file_name[]);
	void SetValueFromTxt(char file_name[], off_t set_value);
	void SetMetaDataFileName(char tblpath []);

};
#endif
