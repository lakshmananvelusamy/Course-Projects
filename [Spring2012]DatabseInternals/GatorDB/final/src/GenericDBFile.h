#ifndef _GENERICDBFILE_H
#define	_GENERICDBFILE_H

//#include "sortedfile.h"
#include "Defs.h"
#include "Schema.h"
# include "Record.h"


class GenericDBFile {
public:
    GenericDBFile();
    GenericDBFile(const GenericDBFile& orig);
    virtual ~GenericDBFile();
    virtual int Create (char *fpath, fType file_type, void *startup);
    virtual int Open (char *fpath);
    virtual int Close ();

    virtual void Load (Schema &myschema, char *loadpath);

    virtual void MoveFirst ();
    virtual void Add (Record &addme);
    virtual int GetNext (Record &fetchme);
    virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);

private:

};

#endif	/* _GENERICDBFILE_H */

