#include <stdlib.h>
#include <stdio.h>
#include <sstream>
#include <vector>
#include<algorithm>
#include "ComparisonEngine.h"

#include "RelOp.h"


//Function to compare two records... used by the qsort function
int compare(int elem1, int elem2)
{
    int ans = 0;

    return (elem1 < elem2);
    
}

void* selectFile_t(void *t_args)
{
    
    SelectFile *selFile = (SelectFile*) t_args;
    int abc = selFile->Start();
    
    //ComparisonEngine *ced = new ComparisonEngine();
    
    pthread_exit(NULL);

}

int SelectFile::Start()
{
    
    Record *tempRec = new Record();
    int i=0;
    t_inFile->MoveFirst();
    //while((t_inFile->GetNext(*tempRec, *t_selOp, *t_literal))==1)
    //{
    
     while(t_inFile->GetNext(*tempRec))
    {
        ComparisonEngine *ced = new ComparisonEngine();
        if(ced->Compare(tempRec, t_literal, t_selOp)!=0)
    
        {
            // Schema *sch = new Schema("catalog", "supplier");
            // tempRec->Print(sch);
            t_outPipe->Insert(tempRec);
            i++;
        }
    }
    
    t_outPipe->ShutDown();
    return(201);
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal)
{
    
    t_inFile = &inFile;
    t_outPipe = &outPipe;
    t_selOp = &selOp;
    t_literal = &literal;
    pthread_create(&selFilethread, NULL, selectFile_t, (void*) this);
}

void SelectFile::WaitUntilDone () {
    
	pthread_join (selFilethread, NULL);
    
}

void SelectFile::Use_n_Pages (int runlen)
{
    noOfPages = runlen;
}



void* selectPipe_t(void *t_args)
{
    SelectPipe *selPipe = (SelectPipe*) t_args;
    selPipe->Start();
    pthread_exit(NULL);
}

void SelectPipe::Start()
{
    ComparisonEngine *ced = new ComparisonEngine();
    Record *tempRec = new Record();
    while(t_inPipe->Remove(tempRec))
    {
        if(ced->Compare(tempRec, t_literal, t_selOp)==0)
        {
            t_outPipe->Insert(tempRec);
        }
    }
    t_outPipe->ShutDown();
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal)
{
    t_inPipe = &inPipe;
    t_outPipe = &outPipe;
    t_selOp = &selOp;
    t_literal = &literal;
    pthread_create(&selPipethread, NULL, selectPipe_t, (void*) this);
}

void SelectPipe::WaitUntilDone () {
	pthread_join (selPipethread, NULL);
}

void SelectPipe::Use_n_Pages (int runlen)
{
    noOfPages = runlen;
}


void* project_t(void* t_args)
{
    Project *proj = (Project*) t_args;
    proj->Start();

    pthread_exit(NULL);
}

void Project::Start()
{
    
    int i=0;
    Record *tempRec = new Record();
    while(t_inPipe->Remove(tempRec))
    {
        /*
        Record *newRec = new Record();
        newRec->Copy(tempRec);
        newRec->Project(t_keepMe, t_numAttsOutput, t_numAttsInput);
        t_outPipe->Insert(newRec);
         */
        tempRec->Project(t_keepMe, t_numAttsOutput, t_numAttsInput);
        t_outPipe->Insert(tempRec);
        i++;
    }
    
    t_outPipe->ShutDown();
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput)
{
    t_inPipe = &inPipe;
    t_outPipe = &outPipe;
    t_keepMe = keepMe;
    t_numAttsInput = numAttsInput;
    t_numAttsOutput = numAttsOutput;
    pthread_create(&projectthread, NULL, project_t, (void*) this);
}

void Project::WaitUntilDone () {
	pthread_join (projectthread, NULL);
}

void Project::Use_n_Pages (int runlen)
{
    noOfPages = runlen;
}


void* duplicateRemoval_t(void* t_args)
{
    DuplicateRemoval *dupRemove = (DuplicateRemoval*) t_args;
    dupRemove->Start();

    pthread_exit(NULL);
}

DuplicateRemoval::DuplicateRemoval()
{
    noOfPages = 10;
}

void DuplicateRemoval::Start()
{

    OrderMaker *schemaOrder = new OrderMaker(t_mySchema);
    Pipe *intermediatePipe = new Pipe(100);
    
    BigQ *bigq = new BigQ(*t_inPipe, *intermediatePipe, *schemaOrder, noOfPages);
    
    /*
    Pipe *intermedPipe = new Pipe(100);
    BigQ *bigq = new BigQ(*intermedPipe, *intermediatePipe, *schemaOrder, noOfPages);
    Record *tempRec = new Record();
    while(t_inPipe->Remove(tempRec))
    {
        tempRec->Print(t_mySchema);
        intermedPipe->Insert(tempRec);
    }
     */
    Record *tempRec1 = new Record();
    Record *tempRec2 = new Record();
    ComparisonEngine *comparisonEng = new ComparisonEngine();
    
    intermediatePipe->Remove(tempRec1);
    
    int cnt1=0;
    int cnt2 = 0;
    
    while(intermediatePipe->Remove(tempRec2))
    {
        //cout<<"removing from intermediate pipe"<<endl;
        if(comparisonEng->Compare(tempRec1, tempRec2, schemaOrder))
        {
            cnt1++;
    
            t_outPipe->Insert(tempRec1);
            tempRec1->Copy(tempRec2);
        }
        else
        {
    
            //tempRec1->Print(t_mySchema);
            //tempRec2->Print(t_mySchema);
            //delete tempRec2;
            //tempRec2 = new Record();
        }
    }
    
    if(tempRec1->IsDataNull()!=1)
    {
        t_outPipe->Insert(tempRec1);
    }
    t_outPipe->ShutDown();
}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema)
{
    t_inPipe = &inPipe;
    t_outPipe = &outPipe;
    t_mySchema = &mySchema;
    pthread_create(&duplicateRemovalthread, NULL, duplicateRemoval_t, (void*) this);
}

void DuplicateRemoval::WaitUntilDone () {
	pthread_join (duplicateRemovalthread, NULL);
}

void DuplicateRemoval::Use_n_Pages (int runlen)
{
    noOfPages = runlen;
}

void* sum_thread(void* args)
{
    Sum* sum = (Sum*) args;
    sum->Start();

    pthread_exit(NULL);
}


void Sum :: WaitUntilDone()
{
    pthread_join(workerThread, NULL);
}

void Sum :: Use_n_Pages(int n)
{
    runLength = n;
}

void Sum :: Run(Pipe& inPipe, Pipe& outPipe, Function& computeMe)
{
    inputPipe = &inPipe;
    outputPipe = &outPipe;
    compute = &computeMe;

    pthread_create(&workerThread, NULL, sum_thread, (void*) this);
}

Record* Sum :: GetResultRecord(Type outputType, int integerResult, double doubleResult)
{
    Record* resultRecord = NULL;
    // Create an attribute to convert the result into a record
    Attribute resultAttribute;
    resultAttribute.name = "result";
    // value in char* format
    const char* value;
    stringstream valueStream;

    switch(outputType)
    {
        case Int:
            resultAttribute.myType = Int;
            valueStream << integerResult;

            // convert int value to string
            break;
        case Double:
            resultAttribute.myType = Double;
            // convert double value to string
            valueStream << doubleResult;
            break;
        default:
            cout<<"Invalid output type"<<endl;
            exit(1);
            break;
    }

    string valueString = valueStream.str();
    valueString.append("|");
    value = valueString.c_str();
    char* schemaFileName = "resultSchema";
    Schema resultSchema(schemaFileName, 1, &resultAttribute);
    resultRecord = new Record();
    resultRecord->ComposeRecord(&resultSchema, value);

    return resultRecord;
}

void Sum :: Start()
{
    int integerResult = 0;
    double doubleResult = 0.0;

    Record* pipeRecord = new Record();
    Type outputType;
    // Continue till the input pipe is empty
    while(TRUE == inputPipe->Remove(pipeRecord))
    {
        // Schema *sch = new Schema("catalog", "supplier");
        // pipeRecord->Print(sch);
        // Compute the result for each record
        int intermediateIntegerResult = 0;
        double intermediateDoubleResult = 0.0;
        outputType = compute->Apply(*pipeRecord, intermediateIntegerResult, intermediateDoubleResult);
        // combine the results
        integerResult += intermediateIntegerResult;
        doubleResult += intermediateDoubleResult;

        delete pipeRecord;
        pipeRecord = new Record();
    }

    Record* resultRecord = GetResultRecord(outputType, integerResult, doubleResult);

    outputPipe->Insert(resultRecord);
    outputPipe->ShutDown();
}

void* groupby_thread(void* args)
{
    GroupBy* groupBy = (GroupBy*) args;
    groupBy->Start();

    pthread_exit(NULL);
}


void GroupBy :: WaitUntilDone()
{
    pthread_join(workerThread, NULL);
}

void GroupBy :: Use_n_Pages(int n)
{
    runLength = n;
}

void GroupBy :: Run(Pipe& inPipe, Pipe& outPipe, OrderMaker& groupAtts, Function& computeMe)
{
    inputPipe = &inPipe;
    outputPipe = &outPipe;
    groupAttributes = &groupAtts;
    compute = &computeMe;

    pthread_create(&workerThread, NULL, groupby_thread, (void*) this);
}

Record* GroupBy :: GetResultRecord(Type outputType, int integerResult, double doubleResult)
{
    Record* resultRecord = NULL;
    // Create an attribute to convert the result into a record
    Attribute resultAttribute;
    resultAttribute.name = "result";
    // value in char* format
    const char* value;
    stringstream valueStream;

    switch(outputType)
    {
        case Int:
            resultAttribute.myType = Int;
            valueStream << integerResult;

            // convert int value to string
            break;
        case Double:
            resultAttribute.myType = Double;
            // convert double value to string
            valueStream << doubleResult;
            break;
        default:
            cout<<"Invalid output type"<<endl;
            exit(1);
            break;
    }

    string valueString = valueStream.str();
    valueString.append("|");
    value = valueString.c_str();
    char* schemaFileName = "resultSchema";
    Schema resultSchema(schemaFileName, 1, &resultAttribute);
    resultRecord = new Record();
    resultRecord->ComposeRecord(&resultSchema, value);
    // cout<<"Result = "<<value<<endl;
    return resultRecord;
}

void GroupBy :: SetTotalNumberOfAttributes(int n)
{
    totalNumberOfAttributes = n;
}

void GroupBy :: Start()
{
    
    // assign the input pipe to the big q
    // create an intermediate pipe for the big q output
    // big q output will be sorted, so all the records will be grouped together
    // the big q uses the order maker to sort the data
    Pipe intermediatePipe(100);

    BigQ* queue = new BigQ(*inputPipe, intermediatePipe, *groupAttributes, runLength);
    
    // once the records are sorted, we compare each record as we remove from the internmediate pipe

    Record* intermediatePipeRecord = new Record();
    Record* previousRecord = NULL;
    ComparisonEngine* cEngine = new ComparisonEngine();
    int integerResult = 0;
    double doubleResult = 0.0;
    int isFirstRecord = TRUE;
    Type outputType;
    int intermediateIntResult = 0;
    double intermediateDoubleResult = 0.0;
    while(TRUE == intermediatePipe.Remove(intermediatePipeRecord))
    {
                
        if(FALSE == isFirstRecord)
        {
            if(0 != cEngine->Compare(previousRecord, intermediatePipeRecord, groupAttributes))
            {
               
                // if the record is not a match, we consider the previous result as the total sum

                // Call function that returns the result record
                Record* resultRecord = GetResultRecord(outputType, integerResult, doubleResult);

                int groupNumberOfAttributes;
                groupNumberOfAttributes = groupAttributes->GetNumberOfAttributes();
                
                int* groupAttributeIndices = groupAttributes->GetAttributes();
                
                // sort the group attributes
                sort(groupAttributeIndices, groupAttributeIndices + groupNumberOfAttributes, compare);


                int* combinedAttributes = new int[groupNumberOfAttributes + 1];

                int i = 0;
                combinedAttributes[0] = 0;
                for(i = 1; i < (groupNumberOfAttributes + 1); i++)
                {

                    combinedAttributes[i] = groupAttributeIndices[i - 1];
                }

                int recordNumberOfAttributes = previousRecord->GetNumberOfAttributes();
                // cout<<"num of atts in record = "<<recordNumberOfAttributes<<endl;
                Record* mergedRecord = new Record();
                // merge with the previous record
                mergedRecord->MergeRecords(resultRecord, previousRecord, 1, recordNumberOfAttributes, combinedAttributes, groupNumberOfAttributes + 1, 1);
                // insert the result into the output pipe
                outputPipe->Insert(mergedRecord); 

                // outputPipe->Insert(resultRecord);
                
                delete resultRecord;
                integerResult = 0;
                doubleResult = 0.0;

            
            }
        }

        // set first record flag as false
        isFirstRecord = FALSE;
        
        delete previousRecord;
        previousRecord = new Record();
        previousRecord->Copy(intermediatePipeRecord);

        intermediateDoubleResult = 0.0;
        intermediateIntResult = 0;
        
        // compute for the previous record
        outputType = compute->Apply(*previousRecord, intermediateIntResult, intermediateDoubleResult);
        integerResult += intermediateIntResult;
        doubleResult += intermediateDoubleResult;
        // integerResult += intermediateIntResult;
        // doubleResult += intermediateDoubleResult;
        // intermediateIntResult = 0;
        // intermediateDoubleResult = 0.0;

        delete intermediatePipeRecord;
        intermediatePipeRecord = new Record();
    }

    if(NULL != previousRecord)
    {
        Record* resultRecord = GetResultRecord(outputType, integerResult, doubleResult);
        outputPipe->Insert(resultRecord);
        integerResult = 0;
        doubleResult = 0.0;
    }
    
    // continue till all the records in the intermediate pipe are entered into a group
    // and its result is computed

    outputPipe->ShutDown();
}

void* writeout_thread(void* args)
{
    WriteOut* writeOut = (WriteOut*) args;
    writeOut->Start();

    pthread_exit(NULL);
}

void WriteOut :: WaitUntilDone()
{
    pthread_join(workerThread, NULL);
}

void WriteOut :: Use_n_Pages(int n)
{
    runLength = n;
}

void WriteOut :: Run(Pipe& inPipe, FILE* outFile, Schema& mySchema)
{
    inputPipe = &inPipe;
    outputFile = outFile;
    this->mySchema = &mySchema;
    pthread_create(&workerThread, NULL, writeout_thread, (void*) this);
}

void WriteOut :: Start()
{
    // remove each record from the input pipe
    // write each record from the pipe into the file, continue till the pipe is empty

    Record* removedRecord = new Record();
    while(TRUE == inputPipe->Remove(removedRecord))
    {
        removedRecord->PrintToFile(outputFile, mySchema);
        delete removedRecord;
        removedRecord = new Record();
    }
    
}

void* join_thread(void* args)
{
    Join* join = (Join*) args;
    join->Start();

    pthread_exit(NULL);
}

void Join :: WaitUntilDone()
{
    pthread_join(workerThread, NULL);
}

void Join :: Use_n_Pages(int n)
{
    runLength = n;
}

int Join :: GetNextRecordFromPage(Page*& getFromMe, Record* fillMe, int recordCount)
{
    int recordExists = FALSE;
    if(0 == recordCount)
    {
        recordExists = getFromMe->RetrieveFirstRecord(fillMe);
    }
    else
    {
        recordExists = getFromMe->RetrieveNextRecord(fillMe);
    }

    return recordExists;
}

void Join :: Start()
{
    // check for the CNF.
    
    OrderMaker left;
    OrderMaker right;
    int randomValue = rand() % 10000;
    fileName = new char[30];
    sprintf(fileName, "tempjoinfile%d.bin", randomValue);
    // File tempFile;
    // tempFile.Open(0, fileName);
    // tempFile.Close();
    // build the combined attribute array for merge
    int leftNumAtts = leftSchema->GetNumAtts();
    
    // Attribute* leftAtts = leftSchema->GetAtts();

    int rightNumAtts = rightSchema->GetNumAtts();
    
    // Attribute* rightAtts = rightSchema->GetAtts();
    numJoinAttributes = leftNumAtts + rightNumAtts;
    joinAttributes = new int[numJoinAttributes];
    int i;
    int count = 0;
    for(i = 0; i < leftNumAtts; i++)
    {
        joinAttributes[count++] = i;
    }
    startOfRightTableAttribute = count;

    for(i = 0;i < rightNumAtts; i++)
    {
        joinAttributes[count++] = i;
    }
    int k = joinCNF->GetSortOrders(left, right);
    // k = FALSE;
    if(FALSE == k)
    {
        // If equality does not exist, then we perform block nested loops join
        PerformBlockNestedJoin();
    }
    else
    {
        // If equality exists, then its a nested loop join
        PerformNestedLoopJoin(left, right);
    }

    
}

void Join :: PerformBlockNestedJoin()
{
    // Block Nested Join
    
    // Create the db file of heap type
    DBFile* tempFile = new DBFile();
    tempFile->Create(fileName, heap, NULL);
    tempFile->Open(fileName);
    // tempFile->MoveFirst();
    // Insert all the records in the right pipe into a dbfile
    Record* rightPipeRecord = new Record();
    while(TRUE == rightInputPipe->Remove(rightPipeRecord))
    {
        
        tempFile->Add(*rightPipeRecord);
        delete rightPipeRecord;
        rightPipeRecord = new Record();
    }

    
    Record* leftPipeRecord = new Record();
    int isLeftBufferFull = FALSE;
    tempFile->MoveFirst();
    Page** leftPageBuffer = new Page*[runLength];
    int recordCount = 0;
    int pageIndex = 0;
    Record* preservedRecord = NULL;
    ComparisonEngine cEngine;
    while(TRUE == leftInputPipe->Remove(leftPipeRecord))
    {
        // get some records from the left pipe into runlength number of pages

        if(FALSE == isLeftBufferFull)
        {
            if(0 == pageIndex && 0 == recordCount)
            {
                leftPageBuffer[pageIndex] = new Page();
            }

            if(NULL != preservedRecord)
            {
                leftPageBuffer[pageIndex]->Append(preservedRecord);
                preservedRecord = NULL;
            }

            // fill till page is full
            if(FALSE == leftPageBuffer[pageIndex]->Append(leftPipeRecord))
            {
                // if the page is full, check if the no of pages have exceeded the run length
                if((pageIndex + 1) < runLength)
                {
                    // if within the run length, allocate new page
                    pageIndex++;
                    recordCount = 0;
                    leftPageBuffer[pageIndex] = new Page();
                    leftPageBuffer[pageIndex]->Append(leftPipeRecord);
                }
                else
                {
                    // if exceeded the run length
                    isLeftBufferFull = TRUE;
                    
                    if(NULL != preservedRecord)
                    {
                        delete preservedRecord;
                    }
                    // preserve the current removed record
                    preservedRecord = new Record();
                    preservedRecord->Consume(leftPipeRecord);
                    leftPipeRecord = NULL;
                }

            }
            else
            {
                recordCount++;
            }

            
            if(TRUE ==  isLeftBufferFull)
            {
                // check with dbfile record
                tempFile->MoveFirst();
                int i = 0;
                // i less than number of pages stored
                

                    Record* rightFileRecord = new Record();
                    // access every record in the db file
                    while(TRUE == tempFile->GetNext(*rightFileRecord))
                    {
                        // remove a record from the db file
                        // till all the pages in the buffer are traversed
                        while(i <= pageIndex)
                        {
                            int j = 0;
                            Record* comparisonRecord = new Record();
                            // continue to read till all the records in the page are read
                            while(TRUE == GetNextRecordFromPage(leftPageBuffer[i], comparisonRecord, j++))
                            {
                                // compare the record with each record in the left buffer
                                // compare the records

                                if(0 != cEngine.Compare(comparisonRecord, rightFileRecord, literal, joinCNF))
                                {
                                    // join the record
                                    Record* joinRecord = new Record();
                                    joinRecord->MergeRecords(comparisonRecord, rightFileRecord, leftSchema->GetNumAtts(), rightSchema->GetNumAtts(), joinAttributes, numJoinAttributes, startOfRightTableAttribute);

                                    outputPipe->Insert(joinRecord);
                                    
                                }

                                delete comparisonRecord;
                                comparisonRecord = new Record();
                            }

                            i++;
                    
                        }

                        delete rightPipeRecord;
                        rightPipeRecord = new Record();

                    }
                    
                    // empty the left buffer
                    
                    for(i = 0; i <= pageIndex; i++)
                    {
                        leftPageBuffer[i]->EmptyItOut();
                        delete leftPageBuffer[i];
                    }
                    pageIndex = 0;
                    isLeftBufferFull = FALSE;
                }

            


        }
        
    }

    // if the left pipe is empty before the left buffer is full

    if(FALSE == isLeftBufferFull && (pageIndex < runLength))
    {
        // check with dbfile record
        tempFile->MoveFirst();
        int i = 0;
        // i less than number of pages stored


        Record* rightFileRecord = new Record();
        // access every record in the db file

        while(TRUE == tempFile->GetNext(*rightFileRecord))
        {
            // remove a record from the db file
            // till all the pages in the buffer are traversed
            while(i <= pageIndex)
            {
                int j = 0;
                Record* comparisonRecord = new Record();
                // continue to read till all the records in the page are read
                while(TRUE == GetNextRecordFromPage(leftPageBuffer[i], comparisonRecord, j++))
                {
                    // compare the record with each record in the left buffer
                    // compare the records

                    if(0 != cEngine.Compare(comparisonRecord, rightFileRecord, literal, joinCNF))
                    {
                        // join the record
                        Record* joinRecord = new Record();
                        joinRecord->MergeRecords(comparisonRecord, rightFileRecord, leftSchema->GetNumAtts(), rightSchema->GetNumAtts(), joinAttributes, numJoinAttributes, startOfRightTableAttribute);

                        outputPipe->Insert(joinRecord);

                    }

                    delete comparisonRecord;
                    comparisonRecord = new Record();
                }
                i++;

            }
            delete rightPipeRecord;
            rightPipeRecord = new Record();

        }

        // empty the left buffer

        for(i = 0; i <= pageIndex; i++)
        {
            leftPageBuffer[i]->EmptyItOut();
            delete leftPageBuffer[i];
        }
        pageIndex = 0;
    }
   
    tempFile->Close();

    delete tempFile;

    // check for cases, when the db file is empty
    // then end the join
    // if the left pipe is empty end the join
    
}

void Join :: PerformNestedLoopJoin(OrderMaker left, OrderMaker right)
{
    
    testCount = 0;
    Pipe leftSortedPipe(100);

    if(0 >= runLength)
    {
        runLength = 1;
    }

    BigQ leftQueue(*leftInputPipe, leftSortedPipe, left, runLength);

    sleep(1);
    Pipe rightSortedPipe(100);

    BigQ rightQueue(*rightInputPipe, rightSortedPipe, right, runLength);

    
    
    Record* leftPipeRecord = new Record();
    Record* previousLeftPipeRecord = NULL;
    Record* rightPipeRecord = NULL;
    Record* greaterThanRightRecord = NULL;
    Page** pages = new Page*[runLength];
    ComparisonEngine* cEngineLeft = new ComparisonEngine();
    ComparisonEngine* cEngineLeftRight = new ComparisonEngine();
    int isRightPipeNotEmpty = TRUE;
    int isLeftRecordFirstInGroup = TRUE;
    int pageCount = 0;
    // right record page index for the current left record
        int pageIndex = 0;
    File* tempFile = new File();
    tempFile->Open(0, fileName);
    while(TRUE == leftSortedPipe.Remove(leftPipeRecord))
    {
        // cout<<"New Left"<<endl;
        
        // right record index for the current right record page
        int recordIndex = 0;

        
        if(NULL != previousLeftPipeRecord)
        {
            // check for equality between the previous and current record
            // change the flag for the first left record
            if(0 == (cEngineLeft->Compare(previousLeftPipeRecord, leftPipeRecord, &left)))
            {
                isLeftRecordFirstInGroup = FALSE;
            }
            else
            {
                isLeftRecordFirstInGroup = TRUE;
                pageCount = 0;
                // delete the file
                // tempFile->Open(0, fileName);
                // tempFile->Close();
                
                // assign the right record as the preserver record

                //rightPipeRecord = greaterThanRightRecord;
                rightPipeRecord = new Record();
                rightPipeRecord->Copy(greaterThanRightRecord);
                delete greaterThanRightRecord;
            }
            
        }


        // check if the right pipe record is null
        // this indicates that this is the first time we check for right pipe record
        if(NULL == rightPipeRecord)
        {
            // get record from the right pipe
            rightPipeRecord = new Record();
            isRightPipeNotEmpty = rightSortedPipe.Remove(rightPipeRecord);
        }
        int compareValue;
        // compare the records from the left and right pipe
        while(TRUE == isRightPipeNotEmpty && 0 <= (compareValue = cEngineLeftRight->Compare(leftPipeRecord, &left, rightPipeRecord, &right)))
        {
            // cout<<testCount++<<endl;
            // case where left is greater than right value i.e. compareValue > 0
            // in this case we need to continue removing the record from the pipe
            // without any action
            if(0 == compareValue)
            {
                // cout<<"Equal"<<endl;
                // 1st record for the current page
                if(0 == recordIndex && 0 == pageIndex)
                {
                    pages[pageIndex] = new Page();
                }


                if(TRUE == isLeftRecordFirstInGroup)
                {
                    // if the left record is the 1st of the same type
                    Record* pageRecord = new Record();
                    pageRecord->Copy(rightPipeRecord);
                    // records are equal, insert the record in the page
                    while(FALSE == pages[pageIndex]->Append(pageRecord))
                    {
                        // if the run length number of pages are full
                        if((pageIndex + 1) < runLength)
                        {
                            // if the page is full, insert into the next page
                            pageIndex++;
                            recordIndex = 0;
                            pages[pageIndex] = new Page();
                        }
                        else
                        {
                            // add pages to the file
                            // tempFile = new File();
                            // tempFile->Open(pageCount, fileName);
                            int i = 0;
                            for(i = 0; i < pageIndex; i++)
                            {
                                // cout<<"Adding page to file"<<endl;
                                tempFile->AddPage(pages[i], pageCount++);
                            }
                            // tempFile->Close();
                            pageIndex = 0;
                            recordIndex = 0;
                        }
                    }

                    recordIndex++;


                }

                else
                {
                    // if the left record is not the 1st of the same type, then retrieve record
                    // from the page instead of the pipe

                    if(pageCount <= runLength)
                    {
                        // pages have not been written into the file yet
                        int i = 0;

                        int isPageRetrieved = FALSE;
                        while(i < pageCount)
                        {
                            int j = 0;
                            Record* retrievedRecord = new Record();
                            if(0 == j)
                            {
                                // first record in the page
                                isPageRetrieved = pages[i]->RetrieveFirstRecord(retrievedRecord);
                                j++;
                            }
                            else
                            {
                                if(FALSE == (isPageRetrieved = pages[i]->RetrieveNextRecord(retrievedRecord)))
                                {
                                    // go to the next page
                                    j = 0;
                                    i++;
                                }
                            }

                            if(FALSE != isPageRetrieved)
                            {
                                // page is retrieved
                                // merge with the left record

                                Record* mergedRecord = new Record();
                                mergedRecord->MergeRecords(leftPipeRecord, retrievedRecord, leftSchema->GetNumAtts(), rightSchema->GetNumAtts(), joinAttributes, numJoinAttributes, startOfRightTableAttribute);
                                outputPipe->Insert(mergedRecord);
                            }

                            delete retrievedRecord;

                        }

                        break;
                    }
                    else
                    {
                        int i = 0;
                        // pages have been written into the file
                        Page* retrievedPage = NULL;
                        // tempFile->Open(1, fileName);
                        int numberOfPages = (int) tempFile->GetLength();
                        while(i < (numberOfPages - 1))
                        {
                            retrievedPage = new Page();
                            tempFile->GetPage(retrievedPage, i);
                            Record* retrievedRecord = new Record();
                            while(FALSE != retrievedPage->GetFirst(retrievedRecord))
                            {
                                // merge the record with left record
                                Record* mergedRecord = new Record();
                                mergedRecord->MergeRecords(leftPipeRecord, retrievedRecord, leftSchema->GetNumAtts(), rightSchema->GetNumAtts(), joinAttributes, numJoinAttributes, startOfRightTableAttribute);
                                outputPipe->Insert(mergedRecord);
                                
                                delete retrievedRecord;
                                retrievedRecord = new Record();
                            }

                            delete retrievedRecord;
                            delete retrievedPage;
                            // increase the page number
                            i++;
                        }
                        // tempFile->Close();
                        
                        break;
                    }

                }

                // simultaneously join the record and insert into the output pipe
                // merge the records
                Record* mergedRecord = new Record();
                mergedRecord->MergeRecords(leftPipeRecord, rightPipeRecord, leftSchema->GetNumAtts(), rightSchema->GetNumAtts(), joinAttributes, numJoinAttributes, startOfRightTableAttribute);
                outputPipe->Insert(mergedRecord);


            }
            

                          
            

            // retrieve the next right pipe record
            // get record from the right pipe
            delete rightPipeRecord;
            rightPipeRecord = new Record();
            isRightPipeNotEmpty = rightSortedPipe.Remove(rightPipeRecord);
            // cout<<"Is Pipe Empty ="<<isRightPipeNotEmpty<<endl;
            // cout<<"is null ? = "<<rightPipeRecord->IsDataNull()<<endl;
            if(FALSE == isRightPipeNotEmpty)
            {
                // cout<<"Deleting the right record"<<endl;
                delete rightPipeRecord;
                rightPipeRecord = NULL;
                break;
            }
        }
        // cout<<"Compare Value = "<<compareValue<<endl;
        // delete greaterThanRightRecord;
        
        greaterThanRightRecord = new Record();
        // check if the compare value.. left value is less than right value
        if(NULL != rightPipeRecord && 0 > compareValue)
        {
            // cout<<"is null ? = "<<rightPipeRecord->IsDataNull()<<endl;
            // records are not equal.. right greater than left
            // preserve the current record
            greaterThanRightRecord->Copy(rightPipeRecord);
        }

        // add the unwritten pages to the disk
        int pageIterator = 0;
        
        // tempFile->Open(1, fileName);
        for(pageIterator = 0; pageIterator <= pageIndex; pageIterator++)
        {
            // cout<<"Adding page "<<pageCount<<" to file"<<endl;
            tempFile->AddPage(pages[pageIterator], pageCount++);
        }
        // tempFile->Close();
        

        previousLeftPipeRecord = new Record();
        previousLeftPipeRecord->Copy(leftPipeRecord);
        delete leftPipeRecord;
        leftPipeRecord = new Record();

        
        
    }
    tempFile->Close();
    outputPipe->ShutDown();
    
    
}

void Join :: Run(Pipe& inPipeL, Pipe& inPipeR, Pipe& outPipe, CNF& selOp, Record& literal)
{
    leftInputPipe = &inPipeL;
    rightInputPipe = &inPipeR;
    outputPipe = &outPipe;
    joinCNF = &selOp;
    this->literal = &literal;

    pthread_create(&workerThread, NULL, join_thread, (void*) this);
}

void Join :: SetSchema(Schema& leftSchema, Schema& rightSchema)
{
    this->leftSchema = &leftSchema;
    this->rightSchema = &rightSchema;
}

Join :: Join()
{
    runLength = 3;
}