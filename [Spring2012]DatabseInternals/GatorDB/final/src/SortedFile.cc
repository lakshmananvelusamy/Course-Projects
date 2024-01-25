#include <string.h>

#include "Record.h"

#include "File.h"
#include "SortedFile.h"
#include "MetaData.h"
#include "Pipe.h"
//#include "DBFile.h"
//#include "BigQ.h"

SortedFile::SortedFile() {
    currentFile = NULL;
}

SortedFile::SortedFile(OrderMaker sortOrder, int runLen)
{
    sortedOrder = sortOrder;
    runLength = runLen;
    currentFile = NULL;
}

SortedFile::SortedFile(const SortedFile& orig) {

}

SortedFile::~SortedFile() {
}

int SortedFile::Open(char* name)
{
    int isFileOpen = FALSE;
//    char* tempName = new char[80];
//    sprintf(tempName, "%s.srt", name);
    fpath = name;
    if(NULL == currentFile)
    {
        currentFile = new File();
        currentFile->Open(TRUE, name);
        isFileOpen = TRUE;
        int fileLength = currentFile->GetLength();
        if(0 < fileLength)
        {
            currentPageNumber = 0;
            currentPage = NULL;
            currentRecord = NULL;
        }
        flag = 1;
        f_mode = READ;
    }
    else
    {
        cout<<"Error: File is already open"<<endl;
    }
    return isFileOpen;
}

int SortedFile::Create(char* name, fType myType, void* startup)
{
    int isFileCreated = FALSE;
    f_mode = READ;
    flag = 1;
    fpath = name;
    if(NULL == currentFile)
    {
        currentFile = new File();
    }
//    char* tempName = new char[80];
//    sprintf(tempName, "%s.srt", name);
    /*else
    {
        this->Close();
    }*/

    // FileData* fileData = NULL;
    // int fileExists = GetFileDetailsFromMetaFile(fileData, string(name));
    int fileExists = FALSE;
    if(FALSE == fileExists)
    {

        currentFile = new File();
        currentFile->Open(FALSE, name);

        // TODO: Add to Meta data
        //FileData fData;
        //fData.name = name;
        //fData.type = myType;

        // AddFileDetailsToMetaFile(fData);

        /*currentFile->Close();

        delete currentFile;
        currentFile = NULL;*/

    }
    else
    {
        cout<<"Cannot create file, another file with the same name exists"<<endl;
    }

    return isFileCreated;
}

int SortedFile::Close()
{
    if(f_mode == WRITE)
    {
        SwitchMode();
    }
    int isFileClosed = FALSE;
    // Check if file is open
    if(NULL != currentFile)
    {
        // Check if the page exists in memory
        /*if(NULL != currentPage)
        {
            if(TRUE == dirtyFlag)
            {
                // Save the current unsaved data
                WriteCurrentPageToFile();
            }
            // discard data
            // delete currentRecord;
            delete currentPage;
            currentPage = NULL;
            currentRecord = NULL;
        }*/
        // Sets the current page number to -1 indicating that the file is closed
        currentPageNumber = -1;
        isFileClosed = currentFile->Close();
        delete currentFile;
        currentFile = NULL;
    }
    else
    {
        cout<<"File is not open"<<endl;
    }
    return isFileClosed;
}

void SortedFile::MoveFirst()
{
   if(f_mode == WRITE)
    {
        SwitchMode();
    }
    //cout<<"1file length is:"<<currentFile->GetLength();
   /*currentPage = new Page();
   currentFile->GetPage(currentPage, 0);
   return;*/
   // check if the file is opened
	if(NULL != currentFile)
	{
            cout<<"Move First : File Exists"<<endl;
            cout<<"file length is:"<<currentFile->GetLength();
            cout<<"current page number = "<<currentPageNumber<<endl;
            if(NULL == currentPage && 0 <= currentPageNumber)
            {
                cout<<"New Page"<<endl;
                currentPage = new Page();
                currentFile->GetPage(currentPage, 0);
                Record temp;
                currentPage->GetFirst(&temp);
                cout<<"Record fetched properly";
                //getchar();
            }
            // cout<<"Number of Records ="<<currentPage->GetNumberOfRecords()<<endl;
            /*if(NULL != currentPage && 0 <= currentPage->GetNumberOfRecords())
            {
		if(0 == currentPageNumber)
		{

                    if(FALSE == currentPage->IsCurrentRecordFirst())
                    {
			// We need to update just the record pointer
                        // TODO: Remove
                        currentRecord = new Record();
			currentPage->RetrieveFirstRecord(currentRecord);
                        isMoveFirst = TRUE;
                    }
		}
		else
		{
                    // Save the current record and page and then retrieve the
                    // 1st page and record
                    if(TRUE == dirtyFlag)
                    {
                        // Save the current record and page into the file
                        WriteCurrentPageToFile();
                        dirtyFlag = FALSE;
			// Discard the current data
                        delete currentPage;
			// delete currentRecord;
                        currentPage = NULL;
                        currentRecord = NULL;

                    }

                    // Retrieves the 1st page
//                    if(NULL != currentPage)
//                    {
//                        currentPage->EmptyItOut();
//                    }

                    currentFile->GetPage(currentPage, 0);
                    currentPageNumber = 0;
                    // TODO: Remove
                       // currentRecord = new Record();
                    currentRecord = new Record();
                    currentPage->RetrieveFirstRecord(currentRecord);
                    isMoveFirst = TRUE;
		}
            }*/
            else
            {
                cout<<"Error: No Data in file"<<endl;
            }
	}
        else
        {
            cout<<"Error: No File is not created or opened"<<endl;
        }
}

int SortedFile::GetNext(Record& fetchMe)
{
    if(f_mode == WRITE)
    {
        SwitchMode();
    }
    // currentRecord = &fetchMe;
    if (currentFile !=NULL)								//checks if file is open
    {
        //cout<<"File length"<<currentFile->curLength;
        int currentFileLength = currentFile->GetLength();	 			//determine length of the current file

        // of TwoWayList use IsCurrentRecordLast() instead
        if(currentPage->IsCurrentRecordLast() == TRUE)	 				//if the record pointer is currently at the last record in the current page
        {

            if(currentPageNumber == (currentFileLength - 2))	 			//if page pointer is currently at the last page in the current file
            {
                //fetchMe = NULL;
                return (0);							//no next records!!! so return 0
            }
            else
            {
                //need to save current page	 				//no more records in the same page

                currentFile->GetPage(currentPage, ++currentPageNumber);		//get next page in putItHere
                //currentPage = putItHere;
                // currentPageNumber++;
                // currentRecord = &fetchMe;//update the current page index
                if(0 != (currentPage->RetrieveFirstRecord(&fetchMe)))	//retrieve first record of the next page
                {
                    currentRecord = &fetchMe;

                    return(1);
                }
                else
                {

                    return(0);
                }
            }
        }
        // [Comment:Akshay] : Try to use paranthesis even if there
        //  is a single line of code in the block. it makes the code more readable
        else
        {
            if(0 == currentPageNumber && TRUE == isMoveFirst)
            {
                if(0 != (currentPage->RetrieveFirstRecord(&fetchMe)))
                {
                    currentRecord = &fetchMe;
                    isMoveFirst = FALSE;
                    // currentPage->MoveToNextRecord();
                    // currentRecord = &fetchMe;
                    return 1;
                }
                else
                {
                    isMoveFirst = FALSE;
                    return 0;
                }
            }
            else
            {
                if(0 != (currentPage->RetrieveNextRecord(&fetchMe)))			//retrieve next record of the current page
                {

                    currentRecord = &fetchMe;

                    return(1);
                }
                else
                {

                    return(0);
                }
            }
        }
    }
    else										//cannot do a getnext if file is not open
    {

	return(0);
    }
}

void SortedFile::Add(Record& addMe)
{
    cout<<"In Add"<<endl;
    if(f_mode == READ)
    {
        cout<<"fmode is"<<f_mode<<endl;
        SwitchMode();
    }
//    else
//    {
//        cout<<"haven't called switchmode"<<endl;
//    }
    
    if(addMe.IsDataNull())
    {
        return;
    }
    //Schema *msch = new Schema("catalog","lineitem");
    Schema *sch = new Schema("catalog","lineitem");
    addMe.Print(sch);
    inPipe->Insert(&addMe);
    //addMe.Print(sch);

    //differentialFile->Add(addMe);
    //differentialLength++;
}

void SortedFile::Load(Schema& mySchema, char* loadMe)
{
    if(f_mode == READ)
    {
        SwitchMode();
    }
    FILE* tableFile = fopen(loadMe, "r");					//Opens the table file provided
    if(tableFile!=NULL)
    {
                //cout<<"File opened"<<endl;

		/*if(currentFile!=NULL)						//Checks if a file is currently open or not
		{
			this->Close();						//Closes the currently opened file
			currentFile=NULL;					//assigns null to the current file pointer
		}
        	char* dataFileName = "/COP6726/GROUPS/BA/Tables/region.bin";
        	// if(TRUE == mySchema.GetFileName(dataFileName))			//Gets the name of the data file from the Schema object
                if(NULL != dataFileName)
        	{*/
                        Record *addMe = new Record();					//Creates a Record object
                        // char* filePathTemp;
                        // strcat(filePathTemp, OUTPUTPATH);
                        // strcat(filePathTemp, dataFileName);
                        // clock_t begin=clock();
			/*this->Create(OUTPUTPATH, Heap, NULL);            	//Creates a File with the name dataFileName and type Heap
                        cout<<"Created"<<endl;
			this->Open(OUTPUTPATH);            */            //Opens the File object created
                        if(NULL != tableFile)
                        {
                            int i = 0;
                            while(addMe->SuckNextRecord(&mySchema, tableFile))	//Uses SuckNextRecord to take in a record
                            {
                                    inPipe->Insert(addMe);				//Adds the record to the DBFile
                                    differentialLength++;
                                    i++;
                            }
                        }
                        // this->Close();
                        // TestLoad();
                        // clock_t end =clock();
                        // double timeInTicks = end - begin;
                        // double timeInms = (timeInTicks*10) / CLOCKS_PER_SEC;

                        // cout<<"Time = "<<timeInms<<endl;
        	//}
                /*else
                {
                    cout<<"No data file found"<<endl;
                }*/
	}
        else
        {
            cout<<"File "<<loadMe<<" could not be opened"<<endl;
        }

        fclose(tableFile);

}

 /*int SortedFile::GetNext(Record &fetchMe, CNF &applyMe, Record &literal)
 {
     cout << "In CNF GetNext"<<endl;
     ComparisonEngine compareEng;
     if(flag==0)
     {
         applyMe.GetQueryOrderMaker(queryOm,literalOm,sortedOrder);
         applyMe.Print();
         queryOm.Print();
         literalOm.Print();
         if(BinarySearch(currentPageNumber,currentFile->GetLength(), literal)==0)
         {
             
         }
     }
         do
         {
             if(GetNext(fetchMe)==0)
             {
                break;
             }
             if(compareEng.Compare(currentRecord, &fetchMe, &queryOm))
             {
                break;
             }
         }while(!compareEng.Compare(&fetchMe, &literal, &applyMe));
         if(compareEng.Compare(&fetchMe, &literal, &applyMe))
         {
             flag=1;
             return(1);
         }
         flag=1;
         return(0);
 }*/

 int SortedFile :: GetNext (Record& fetchMe, CNF& applyMe, Record& literal)
 {
    ComparisonEngine* compare = new ComparisonEngine();	 //instantiates the ComparsionEngine class
    while(TRUE == this->GetNext(fetchMe))	 //checks if there is a next record in the file and fetches it using the GetNext method
    {
        if(1==compare->Compare(&fetchMe, &literal, &applyMe))	 //checks if the current record satisfies the given CNF...
        {
            return(1);	 //sucess!!! so returns 1
        }
    }
    return(0);	 //no more records in file... could not find a CNF-satisfying record... returns 0
 }

 int SortedFile::BinarySearch(int low, int high, Record &literal)
 {
     int mid = (low+high)/2;
     if(mid==low||mid==high-1)
     {
         return 0;
     }
     currentFile->GetPage(currentPage, mid);
     currentPage->RetrieveFirstRecord(currentRecord);
     ComparisonEngine compEng;
     if(compEng.Compare(currentRecord,&queryOm,&literal,&literalOm)>0)
     {
         BinarySearch(low,mid,literal);
     }
     else if(compEng.Compare(currentRecord,&queryOm,&literal,&literalOm)<0)
     {
         BinarySearch(mid,high,literal);
     }
     else
         return 1;
 }

int SortedFile::SwitchMode()
{
    cout<<"In Switch Mode"<<endl;
    cout<<f_mode<<endl;
    if(f_mode == WRITE)
    {
         //cout<<"@@@Changing mode from Write to Read"<<endl;
         //inPipe.ShutDown();
        //change mode from write to read
//        int bufferSize = differentialLength;
//        //TODO: what should be the buffer size???
//        Pipe producer(bufferSize);
//        Pipe consumer(bufferSize);
//        Record *currRecord = new Record();
//        while(differentialFile->GetNext(*currRecord)==1)
//        {
//            producer.Insert(currRecord);
//        }
//        producer.ShutDown();
//        //TODO: is it required to shutdown the Producer pipe???
//        BigQ differentialQ(producer, consumer, sortedOrder, runLength);
//        //records are now in sorted form in the consumer pipe...
//        //can just pull them out page by page and merge with the original file
//        MergeWith(consumer);
        MergeUsingBigQ();
        //resetting everything
        delete differentialQ;
//        delete inPipe;
//        delete outPipe;
        //delete differentialFile;
        //differentialFile = NULL;
        differentialQ = NULL;
//        inPipe = NULL;
//        outPipe = NULL;
        differentialLength = -1;
        f_mode = READ;
    }
    else
    {
        cout<<"Changing mode from Read to Write"<<endl;
       //change mode frm read to write
        inPipe = new Pipe(BUFSIZ);
        outPipe = new Pipe(BUFSIZ);
        //differentialFile = new HeapFile();
        differentialQ = new BigQ(*inPipe, *outPipe, sortedOrder, runLength);
//        inPipe = new Pipe(BUFSIZ);
//        outPipe = new Pipe(BUFSIZ);
       char* diffFilePath = new char[80];
       sprintf(diffFilePath, "%s.diff", fpath);
       cout<<"diffFilePath is"<<diffFilePath<<endl;
       //differentialFile->Create(diffFilePath, heap, NULL);
       differentialLength = 0;
       f_mode = WRITE;
    }
}
int SortedFile::MergeUsingBigQ()
{
    Record *tempRec = new Record();
    cout<<"Trying to Merge"<<endl;
//    differentialFile->MoveFirst();
//    if(differentialFile->GetNext(*tempRec)==0)
//    {
//        cout<<"diffFile is empty"<<endl;
//        return(1);
//    }
//
//    int bufsize = 500;
//    Pipe producer(bufsize);
//    producer.Insert(tempRec);
////    while(producer.Remove(tempRec))
////    {
////        cout<<"Checkin records inserted into producer"<<endl;
////        Schema *sch = new Schema("catalog","nation");
////        tempRec->Print(sch);
////        producer.Insert(tempRec);
////    }
//    Pipe consumer(bufsize);
//    sortFile = new BigQ(producer, consumer, sortedOrder, runLength);
//    cout<<"Created BigQ"<<endl;
//    while(differentialFile->GetNext(*tempRec))
//    {
//        cout<<"Inserting Records to producer"<<endl;
//
//        producer.Insert(tempRec);
//    }
    if(currentFile!=NULL)
    {
        if(currentFile->GetLength()!=0)
        {
            cout<<"Found recs in current file"<<endl;
            currentPageNumber = 0;
            currentFile->GetPage(currentPage, currentPageNumber);
            while(1)
            {
                if(currentPage->GetFirst(currentRecord))
                {
                    if(currentRecord->IsDataNull())
                    {
                        cout<<"Found record NULL"<<endl;
                        continue;
                    }
                    Schema *msch = new Schema("catalog","lineitem");
                    currentRecord->Print(msch);
                    inPipe->Insert(currentRecord);
                }
                else
                {
                    currentPageNumber++;
                    if(currentPageNumber>=currentFile->GetLength())
                    {
                        break;
                    }
                    currentFile->GetPage(currentPage, currentPageNumber);
                }
            }
        }
    }
    inPipe->ShutDown();
    cout<<"Producer shut down"<<endl;
    if(currentFile==NULL)
    {
        currentFile = new File();
    }
    currentFile->Open(0, fpath);
    cout<<"Created file at"<<fpath<<endl;
    currentPageNumber = 0;
    currentPage = new Page();
    currentRecord = new Record();
    while(outPipe->Remove(currentRecord))
    {
        if(!currentPage->Append(currentRecord))
        {
            currentFile->AddPage(currentPage, currentPageNumber);
            currentPageNumber++;
            currentPage = new Page();
        }
    }
    return(1);
}
//int SortedFile::MergeWith(Pipe &consumer)
//{
//    Record *tempRec = new Record();
//    this->MoveFirst();
//    int checkLength = this->GetNext(*tempRec);
//    //check for cases when consumer is empty or sorted file instance is empty
//    if(checkLength==0)
//    {
//        while(consumer.Remove(tempRec)!=0)
//        {
//            JustAdd(*tempRec);
//        }
//        return(1);
//    }
//    else if(consumer.Remove(tempRec)==0)
//    {
//        return(1);
//    }
//    else
//    {
//        //insert records from sorted file to producer pipe
//        MergeIntoOne(producer, consumer, newconsumer);
//    }
//}
//
//int SortedFile::JustAdd(Record &addMe)
//{
//    // TODO: Code to parse the meta data file. This is to check if the file is a heap file
//             off_t numberOfPages;
//            // TODO: Check if the file is open else return a 0 with an error message
//            // get the number of pages in the file. return type is off_t
//            if(NULL != currentFile && (0 <= (numberOfPages = currentFile->GetLength())))
//            {
//
//                    // TODO: Check if the current page is the last page in the file
//                    if(NULL != currentPage)
//                    {
//
//                        if(currentPageNumber < ((int)numberOfPages - 2))
//                        {
//                            if(TRUE == dirtyFlag)
//                            {
//                                //TODO: Code to save the current record and page into
//                                //the file if the flag is dirty
//                                WriteCurrentPageToFile();
//                                dirtyFlag = FALSE;
//                            }
//
//                            // Discard the data
//                            delete currentPage;
//                            // delete currentRecord;
//
//                            // TODO: delete or EmptyItOut() ?
//
//                            // Get the last page
//                            currentPage = NULL;
//                            currentRecord = NULL;
//                            currentFile->GetPage(currentPage, numberOfPages - 1);
//                            currentPageNumber = numberOfPages - 1;
//                            // Reset the dirty flag since the new page is retrieved
//                        }
//                    }
//                    else
//                    {
//                        // Create a new page
//                        currentPage = new Page();
//                        // currentPageNumber++;
//                        dirtyFlag = FALSE;
//                    }
//
//                    // Append the record
//                    // Set the dirty flag since the current page is updated
//                    if(NULL != currentPage)
//                    {
//                            int successfullyAdded = currentPage->Append(&addMe);
//
//                            // Failure due to Page overflow
//                            if(FALSE == successfullyAdded)
//                            {
//                                // TODO: Check if the the page is dirty and store to disk
//                                if(TRUE == dirtyFlag)
//                                {
//                                    // TODO: Save to disk
//                                    if(-1 == currentPageNumber)
//                                    {
//                                        currentPageNumber++;
//                                    }
//                                    WriteCurrentPageToFile();
//                                    dirtyFlag = FALSE;
//                                }
//                                //Discard data
//
//                                delete currentPage;
//
//                                //delete currentRecord;
//                                currentPage = NULL;
//                                currentRecord = NULL;
//                                // Creates a new page
//                                currentPage = new Page();
//                                currentPageNumber++;
//                                successfullyAdded = currentPage->Append(&addMe);
//
//                                // Update the current record pointer
//
//                                // TODO: Remove
//                                currentRecord = &addMe;
//
//                            }
//                            else
//                            {
//                                currentRecord = &addMe;
//                            }
//
//                            dirtyFlag = successfullyAdded;
//                    }
//
//            }
//            else
//            {
//                    // TODO: Do we need to open the file? or log the error?
//                cout<<"Error: No File exists. Create a file first"<<endl;
//            }
//}
//

