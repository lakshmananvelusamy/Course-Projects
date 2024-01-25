#include "GenericDBFile.h"

GenericDBFile::GenericDBFile() {
}

GenericDBFile::GenericDBFile(const GenericDBFile& orig) {
}

GenericDBFile::~GenericDBFile() {
}

int GenericDBFile::Create (char *fpath, fType file_type, void *startup)
{

}
int GenericDBFile::Open (char *fpath)
{

}
int GenericDBFile::Close ()
{

}

void GenericDBFile::Load (Schema &myschema, char *loadpath)
{

}
void GenericDBFile::MoveFirst ()
{

}
void GenericDBFile::Add (Record &addme)
{

}
int GenericDBFile::GetNext (Record &fetchme)
{

}
int GenericDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal)
{

}

