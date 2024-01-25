#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "GenericDBFile.h"
#include "HeapFile.h"
#include "SortedFile.h"
#include "MetaData.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

//added the enum fType to Defs.h
//added the enum fMode to Defs.h... //It is to indicate the mode in which the file is read or write... Useful for sorted and B+ files...

// stub DBFile header..replace it with your own DBFile.h

struct SortInfo
{
    OrderMaker *myOrder;
    int runLength;
};

class DBFile  {

public:
	DBFile (); 

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
        //friend Class MetaData;
private:
    GenericDBFile *myInternalVar;
    //OrderMaker sortedOrder;

};
#endif
