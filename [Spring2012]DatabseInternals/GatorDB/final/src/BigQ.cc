
#include <stdlib.h>
#include <pthread.h>
#include <vector>
#include <queue>
#include <iostream>
#include <algorithm>
#include "BigQ.h"
#include "DBFile.h"
using namespace std;


//Function to compare two records... used by the qsort function
int compare(Record *elem1, Record *elem2)
{
    //Record* ptr1 = ((Record*) elem1);
    //Record* ptr2 = ((Record*) elem2);
    //Schema *newSchema = new Schema("catalog","nation");
    int ans = 0;
    //elem1->Print(newSchema);
    //elem2->Print(newSchema);
    ComparisonEngine *compEng = new ComparisonEngine();
    OrderMaker *order = new OrderMaker();
    order = sortord;
    ans = compEng->Compare(elem1, elem2, order);
    if(ans==-1)
    {
        return(1);
    }
    else
    {
        return(0);
    }
    //return(ans);
}

//Function that is executed by the worker thread, does the entire external sorting
void* ExternalMergeSort(void* tid)
{
    BigQ *bigq;
    bigq =(BigQ*) tid;
    char c;
    // cout<<"random no is"<<rand()<<endl;
    //cin>>c;
    int randomValue = rand() % 10000;
    // cout<<"random value is"<<randomValue<<endl;
    //cin>>c;
    // cout<<"Before Generate Call in thread function"<<endl;
    bigq->GenerateRuns(randomValue);
    // cout<<"After Generate Call in thread function"<<endl;
    // cout<<"Before Merge Call in thread function"<<endl;
    bigq->MergeRuns();
    // cout<<"After Merge Call in thread function"<<endl;

//    int val[] = {1,2,3};
//    qsort(val, 3,sizeof(int), compare);
//    cout << "In File Loading Function" <<threadarg->length << endl;
}

//Constructor to BigQ creates the worker thread
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
{
	// read data from in pipe sort them into runlen pages

    //int *i;
    //i=&runlen;
    /*struct passtothread
    {
        Pipe inpipe(Pipe &in);
        Pipe outpipe(Pipe &out);
        OrderMaker sorter(OrderMaker &sortorder);
        int length;
    }externalsort;
     */
    externalSort.inpipe = &in;
    externalSort.outpipe = &out;
    externalSort.sorter = &sortorder;
    sortord = &sortorder;
    
    externalSort.length = runlen;
   
    numberOfRuns = 0;
    
   
    //TODO: Remove, used for testing
    //mySchema = new Schema("catalog", "customer");
    //externalsort.inpipe(in);
   // externalsort.outpipe(out);
   // externalsort.sorter(sortorder);
    //int i=runlen;
    //cout << "in constructor: i = " << runlen << endl;
    pthread_create(&thread, NULL, &ExternalMergeSort, (void*) this );
    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    //pthread_join(thread, NULL);

    
}

BigQ::~BigQ ()
{
    //TODO: Delete internal structures
}

//Generates the runs
void BigQ::GenerateRuns(int randomValue)
{

    /*
     *
     * */
     dbFileName = new char[30];
    sprintf(dbFileName, "tempsortfile%d.bin", randomValue);
    

    int totalRecords = 0;
    int isPageFull = FALSE;
    int recordCount = 0;
    int recordApproximation;
    float approximationValue = 1.10;
    int recordCountInPage = 0;
    int filePageCount = 0;
    int fileRecordCount = 0;
    int leftOverRecordStartIndex = recordApproximation;
     // Generate Runs
    numberOfRuns = 0;
    Record *tempRecord = new Record();
    Page* tempPage = new Page();
    File* file = new File();
    file->Open(0, dbFileName);
                
    // mySchema = new Schema("catalog","customer");
    //reads records from input pipe
    while(TRUE == externalSort.inpipe->Remove(tempRecord))
    {
        if(FALSE == isPageFull)
        {
            // Fill a single page with records, until it is full
            
            if(FALSE == tempPage->Append(tempRecord))
            {
                // Page is Full
                isPageFull = TRUE;
                
                // Once the page is full, approximate the no. of records required for the run
                // approximate value = approximation factor * page capacity
                //                                              * run length
                recordApproximation = (int) (approximationValue * recordCountInPage * externalSort.length);
                // cout<<"rec app ="<<recordApproximation<<endl;
                // int abc;
                // cin>>abc;
                leftOverRecordStartIndex = recordApproximation;
                records = new Record*[recordApproximation];
                Record* removedRecord = new Record();
                while(tempPage->GetFirst(removedRecord))
                {
                    // Fill the array, with records
                    records[recordCount] = removedRecord;
                    recordCount++;
                    totalRecords++;
                    removedRecord = new Record();
                }
                records[recordCount] = tempRecord;
                recordCount++;
                delete tempPage;
                // delete removedRecord;
                tempPage = NULL;
                removedRecord = NULL;

            }
            else
            {
                // Page is not full
                recordCountInPage++;
            }
        }
        else
        {
            // Fill the array, with records
            
            records[recordCount] = tempRecord;
            // (records[recordCount]).Consume(tempRecord);
            recordCount++;
            totalRecords++;
            // check if the run length is full

            if(recordCount == (leftOverRecordStartIndex) || recordCount == recordApproximation)
            {
                // end of run

                // Sort the array of record pointers
                SortComparison sortObject(externalSort.sorter);
                
                sort(records, records + recordApproximation, sortObject);

                // Insert the sorted records into the dbfile / file, till the run length
                // number of pages are filled

                Page* filePage = new Page();
                
                // Enter into the page till there are more records available or
                // the run length has been reached
                fileRecordCount = 0;
                int runPageCount = (filePageCount % externalSort.length);
                while(runPageCount < externalSort.length && fileRecordCount < recordApproximation)
                {
                    // records[fileRecordCount]->Print(mySchema);
                    if(TRUE == filePage->Append(records[fileRecordCount]))
                    {
                        fileRecordCount++;
                    }
                    else
                    {
                        file->AddPage(filePage, filePageCount);
                        filePageCount++;
                        runPageCount++;
                        delete filePage;
                        filePage = NULL;
                        filePage = new Page();
                    }
                }
                // Preserve the remaining (if any) records in the array for the next run
                leftOverRecordStartIndex = fileRecordCount;
                // If the run contains less than run length number of pages then add blank pages
                while(filePageCount < externalSort.length)
                {
    
                    file->AddPage(filePage, filePageCount++);
                    delete filePage;
                    filePage = NULL;
                    filePage = new Page();
                }

                // file->Close();
                delete filePage;
                filePage = NULL;
                // delete file;
                // file = NULL;
    
                recordCount = 0;
                numberOfRuns++;
            }
            // Fill the array again from the pipe and continue
            
            
        }
        
        tempRecord = new Record();
    }

    // If very few records exist

    if(FALSE == isPageFull)
    {
        recordApproximation = (int) (2.0f * recordCountInPage * externalSort.length);
        isPageFull = TRUE;
        leftOverRecordStartIndex = recordApproximation;
        records = new Record*[recordApproximation];
        Record* removedRecord = new Record();
        while(tempPage->GetFirst(removedRecord))
        {
            // Fill the array, with records
            records[recordCount] = removedRecord;
            // records[recordCount].Consume(removedRecord);
            recordCount++;
            totalRecords++;
            removedRecord = new Record();
        }
        delete tempPage;
        // delete removedRecord;
        tempPage = NULL;
        removedRecord = NULL;
    }

    // for the last run


    
    if(0 < recordCount && recordApproximation > recordCount)
    {
        int difference = leftOverRecordStartIndex - recordCount;
        
        if(0 < difference && leftOverRecordStartIndex < recordApproximation)
        {
            
            int leftOverRecordsIterator = leftOverRecordStartIndex;
            while(leftOverRecordsIterator < recordApproximation)
            {
                int differenceIndex = leftOverRecordsIterator - difference;
            
                records[differenceIndex] = records[leftOverRecordsIterator];
                leftOverRecordsIterator++;
            }
            recordApproximation = leftOverRecordsIterator - difference;
            
        }
        else if(leftOverRecordStartIndex == recordApproximation && recordCount < recordApproximation)
        {
            recordApproximation = recordCount;
        }

        SortComparison mySortObject(externalSort.sorter);
        // Sort the array of record pointers
        
        sort(records, records + (recordApproximation), mySortObject);

        // Insert the sorted records into the dbfile / file, till the run length
        // number of pages are filled

        Page* filePage = new Page();
     
        
        // Enter into the page till there are more records available or
        // the run length has been reached
        
        fileRecordCount = 0;
        int runPageCount = (filePageCount % externalSort.length);
        while(runPageCount < externalSort.length && fileRecordCount < recordApproximation)
        {
            // If it is the last page then, do not add blank page
            // records[fileRecordCount]->Print(mySchema);
            if(TRUE == filePage->Append(records[fileRecordCount]))
            {
                 fileRecordCount++;
            }
            else
            {
                file->AddPage(filePage, filePageCount);
                filePageCount++;
                runPageCount++;
                delete filePage;
                filePage = NULL;
                filePage = new Page();
            }
         }

        if(0 < filePage->GetNumberOfRecords())
        {
            file->AddPage(filePage, filePageCount);
            filePageCount++;
            delete filePage;
            filePage = NULL;
        }

        
        numberOfRuns++;

    }

    file->Close();
    delete file;

}


// Gets the record info structure from individual record
RecordInfo BigQ::GetRecordInfo(int pageNumber, Record* record)
{
    RecordInfo recordInfo;
    
    recordInfo.pageNumber = pageNumber;
    recordInfo.record = record;

    return recordInfo;
}

Page* BigQ::GetPageFromFile(int pageNumber)
{
    Page* page = NULL;
    if(NULL != intermediateFile)
    {
        int numberOfPagesInFile = intermediateFile->GetLength();
       //  cout<<"Page Length = "<<numberOfPagesInFile<<endl;
        if(pageNumber < (numberOfPagesInFile -  1))
        {
            
            page = new Page();
            intermediateFile->GetPage(page, pageNumber);
            if(NULL == page)
            {

                cout<<"ERROR : Could not retrieve page"<<endl;
            }
        }
    }
    else
    {
        cout<<"ERROR : File not open"<<endl;
    }
    return page;
}

void BigQ::MergeRuns()
{
      
    int runLength = externalSort.length;
    
    int numberOfPages = runLength * numberOfRuns;
    int pageNumbers[numberOfRuns];
    Page** pages = new Page*[numberOfRuns];
    
    // First page of the first run
    pageNumbers[0] = 0;
    numberOfPages--;
    int runCount = 0;
    int isRunEmpty[numberOfRuns];
    intermediateFile = new File();
    // File* file = new File();
    intermediateFile->Open(1, dbFileName);
    int pageCountInFile = intermediateFile->GetLength();
    /*int currentPageNumber = 0;
    int recordCountInFile = 0;
    while(currentPageNumber < pageCountInFile)
    {
        Page* page = GetPageFromFile(currentPageNumber);
        if(NULL != page)
        {
            int recs = page->GetNumberOfRecords();
            
            recordCountInFile += recs;
        }
        currentPageNumber++;
    }
    cout<<"Total Number of records read while merging = "<<recordCountInFile<<endl;*/

    if(0 < pageCountInFile)
    {

        // Gets the initial number of pages
        while(runCount < numberOfRuns)
        {
            // Greater than 0 as the page at index 0 is already set
            if(0 < runCount)
            {
                pageNumbers[runCount] = pageNumbers[runCount - 1] + runLength;
                numberOfPages--;
            }
            //function to read pages from the temp file and insert them into an array...
            // cout<<"Retrieving page number = "<<pageNumbers[runCount]<<endl;
            pages[runCount] = GetPageFromFile(pageNumbers[runCount]);
            // cout<<"Retrieved page number = "<<pageNumbers[runCount]<<endl;
            isRunEmpty[runCount] = FALSE;
            runCount++;
        }

        priority_queue< RecordInfo, vector<RecordInfo>, PriorityQueueComparison > priorityQueue(PriorityQueueComparison(externalSort.sorter));

        //function to encapsulate a record into a structure with the run number that it belongs to

        // get one record from each page and insert it into the priority queue
        // remove a record from the priority queue, check the page number and get another record
        // from that page to insert into the priority queue

        // first get the initial records

        runCount = 0;
        Record* record = NULL;
        // mySchema = new Schema("catalog","customer");
        // cout<<"Number of Runs = "<<numberOfRuns<<endl;
        // Gets the first record from each run
        while(runCount < numberOfRuns)
        {
            // cout<<"Run "<<runCount<<endl;
            if(NULL != pages[runCount])
            {
                record = new Record();
                (pages[runCount])->GetFirst(record);
                // if(NULL != record && FALSE == record->IsDataNull())
                // {
                RecordInfo info = GetRecordInfo(pageNumbers[runCount], record);
                // Insert into the priority queue
                priorityQueue.push(info);
                // record->Print(mySchema);
            }
     
            runCount++;
        }
        int numberOfEmptyPages = 0;
        
        runCount = 0;

        // Continue till all the records are retrieved from the priority queue
        int testCount = 0;
        while(FALSE == priorityQueue.empty())
        {
            // Extracted object is of RecordInfo type
            RecordInfo poppedElement = priorityQueue.top();
            priorityQueue.pop();

            // Insert into the output pipe
            externalSort.outpipe->Insert(poppedElement.record);

            int extractedRecordPageNumber = poppedElement.pageNumber;
            int runNumberOfExtractedPage = extractedRecordPageNumber / runLength;

            // get record from the Page
            Record* record = new Record();

            if(NULL != pages[runNumberOfExtractedPage])
            {
                if(FALSE == (pages[runNumberOfExtractedPage])->GetFirst(record))
                {
                    // current page is empty

                    // Check if the page is the last page of the run
                    if((1 == runLength) || ((0 < extractedRecordPageNumber) && ((runLength - 1) == (extractedRecordPageNumber % runLength))))
                    {
                        // cout<<"Run "<<runNumberOfExtractedPage<<" is empty"<<endl;
                        // last page of the run
                        isRunEmpty[runNumberOfExtractedPage] = TRUE;

                        delete (pages[runNumberOfExtractedPage]);
                        record = NULL;
                        pages[runNumberOfExtractedPage] = NULL;
                        numberOfEmptyPages++;
                    }

                    // Get the next page of the run unless the run is empty
                    if(FALSE == isRunEmpty[runNumberOfExtractedPage])
                    {
                        // deletes the page from memory
                        delete (pages[runNumberOfExtractedPage]);

                        // increments the page number within the run
                        ++(pageNumbers[runNumberOfExtractedPage]);
                        
                        // gets a new (next) page from the same run
                        pages[runNumberOfExtractedPage] = GetPageFromFile(pageNumbers[runNumberOfExtractedPage]);
                        // cout<<pageNumbers[runNumberOfExtractedPage]<<" ";
                        // cout<<"Current Page = "<<pageNumbers[runNumberOfExtractedPage]<<endl;
                        
                        // decrements the total number of pages to be retrieved
                        numberOfPages--;

                        // get the first record
                        if(NULL != pages[runNumberOfExtractedPage])
                        {
                            (pages[runNumberOfExtractedPage])->GetFirst(record);
                        }
                        else
                        {
                            // If the run does not contain runlength number of pages
                            isRunEmpty[runNumberOfExtractedPage] = TRUE;
                            numberOfEmptyPages++;
                            delete record;
                            record = NULL;
                        }
                    }


                }
            }

            // If a new record is extracted from a new / existing page
            if(NULL != record)
            {
                RecordInfo info = GetRecordInfo(pageNumbers[runNumberOfExtractedPage], record);

                // Insert into the priority queue
                priorityQueue.push(info);
                // record->Print(mySchema);
            }

            //if(0 == testCount % 10000)
             //{
                // cout<<"In Merge while "<<++testCount<<endl;
             //}
        }

        
    }
    else
    {
        cout<<"In Big Q Merge Count not open db file instance"<<endl;
    }

    intermediateFile->Close();
    delete intermediateFile;
    // cout<<"Before big q pipe shut down"<<endl;
    // finally shut down the out pipe
    externalSort.outpipe->ShutDown ();
    // cout<<"After big q pipe shut down"<<endl;
    
}

PriorityQueueComparison:: PriorityQueueComparison(OrderMaker* sortOrder)
{
    this->sortOrder = sortOrder;
}

bool PriorityQueueComparison:: operator ()(RecordInfo leftRecord, RecordInfo rightRecord)
{
     ComparisonEngine comparisonEngine;
     int returnValue = comparisonEngine.Compare(leftRecord.record, rightRecord.record, sortOrder);
     // cout<<"Left = "<<leftRecord.record->IsDataNull()<<endl;
     // cout<<"Right = "<<rightRecord.record->IsDataNull()<<endl;
     if(returnValue < 0)
     {
        returnValue = 0;
     }
     else
     {
        returnValue = 1;
     }

     return returnValue;
}

SortComparison :: SortComparison(OrderMaker* sortOrder)
{
    this->sortOrder = sortOrder;
}

bool SortComparison :: operator ()(Record* left, Record* right)
{
    int ans = 0;
    //elem1->Print(newSchema);
    //elem2->Print(newSchema);
    ComparisonEngine *compEng = new ComparisonEngine();

    ans = compEng->Compare(left, right, sortOrder);
    if(ans==-1)
    {
        return(1);
    }
    else
    {
        return(0);
    }
}


