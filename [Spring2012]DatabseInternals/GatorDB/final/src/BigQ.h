#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;


static int RecCompare(const void *, const void *);
static void* ExternalMergeSort(void *);  //just a test function for pthreads
static OrderMaker* sortord;

// structure used to push the record information in the priority queue
struct RecordInfo
{
    int pageNumber;
    Record* record;
};

class PriorityQueueComparison
{
    private:
        OrderMaker* sortOrder;
    public:
        PriorityQueueComparison(OrderMaker* sortOrder);
        bool operator()(RecordInfo leftRecord, RecordInfo rightRecord);
        
};

class SortComparison
{
    private:
        OrderMaker* sortOrder;
    public:
        SortComparison(OrderMaker* sortOrder);
        bool operator() (Record* left, Record* right);
};


class BigQ {
    pthread_t thread;
    struct passtothread
    {
        Pipe* inpipe;
        Pipe* outpipe;
        OrderMaker* sorter;
        int length;
    };
    // Record *recordArray;
    
    Record** records;
    passtothread externalSort;
    char* dbFileName;
    int numberOfRuns;
    // DBFile* intermediateFile;
    File* intermediateFile;
    Schema *mySchema; //For printing and testing purposes
    
    RecordInfo GetRecordInfo(int pageNumber, Record* record);
    Page* GetPageFromFile(int pageNumber);

public:
    void GenerateRuns(int randomValue);
    // Will merge the runs
    void MergeRuns();
    //int recCompare(const void* elem1, const void* elem2);
	BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ();
         
};

#endif
