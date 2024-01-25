#ifndef _SORTEDFILE_H
#define	_SORTEDFILE_H

#include"Defs.h"
#include "HeapFile.h"
#include "GenericDBFile.h"
#include "Pipe.h"
#include "BigQ.h"


class SortedFile : public GenericDBFile
{
public:
    SortedFile();
    SortedFile(OrderMaker sortOrder, int runLen);
    SortedFile(const SortedFile& orig);
    virtual ~SortedFile();
    int Open(char* name);
    int Create(char* name, fType myType, void *startup);
    void Add(Record &addMe);
    void Load(Schema &mySchema, char* loadMe);
    void MoveFirst();
    int GetNext(Record &fetchMe);
    int GetNext(Record &fetchMe, CNF &applyMe, Record &literal);
    int Close();

private:
    OrderMaker sortedOrder;
    int runLength;
    int SwitchMode();
    int MergeWith(Pipe &consumer);
    int MergeIntoOne(Pipe &producer, Pipe &consumer, Pipe &newconsumer);
    int MergeUsingBigQ();
    int JustAdd(Record &addMe);
    int BinarySearch(int low, int high, Record &literal);
    fMode f_mode;
    char* fpath;
    BigQ *differentialQ;
    Pipe *inPipe;
    Pipe *outPipe;
    OrderMaker queryOm;
    OrderMaker literalOm;
    //HeapFile *differentialFile;
    long int differentialLength;     //this will keep a track of the number of records currently in the differential file
    File* currentFile;
    Page* currentPage;
    Record* currentRecord;
    int flag;
    int isMoveFirst;
    int currentPageNumber;
    BigQ *sortFile;

};

#endif	/* _SORTEDFILE_H */

