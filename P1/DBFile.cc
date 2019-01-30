#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <fstream>
// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
	Page buffer_page;
	File dbFile;
}

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
	//File newFile;
	dbFile.Open(0,(char*)f_path);
	dbFile.Close();
	//Try catch pending
	return 1;

}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
}

int DBFile::Open (const char *f_path) {
//	File dbFile;
//	(int)dbFile.GetLength();
	dbFile.Open((int)dbFile.GetLength(), (char*)f_path);
	return 1;	
}

void DBFile::MoveFirst () {
}

int DBFile::Close () {
	dbFile.Close();
	return 1;		
}

void DBFile::Add (Record &rec) {
}

int DBFile::GetNext (Record &fetchme) {
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
