
#include "File.h"

#include "HeapFile.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <time.h>
using namespace std;

HeapFile::HeapFile()
{
    dirtyFlag = FALSE;
    currentPageNumber = -1;
    currentFile = NULL;
    currentPage = NULL;
    currentRecord = NULL;
    isMoveFirst = FALSE;
}

HeapFile :: ~HeapFile()
{
    // delete currentRecord;
    delete currentPage;
    delete currentFile;
    currentPage = NULL;
    currentRecord = NULL;
    currentFile = NULL;
}

/*int HeapFile :: GetFileDetailsFromMetaFile(FileData* fileData, string fname)
{
	// TODO: Handle Sections in Meta File
	string line;
	fstream file (METADATAFILE, fstream::in | fstream::out);
        int isDataRetrieved = FALSE;
	// Check if tDBFile.cc:643: error: ‘CurrentPage’ was not declared in this scopehe file is open
	if(TRUE == file.is_open())
	{
		// Till the end of file is traversed
		while(FALSE == file.eof())
		{
			// gets the current line
			getline(file, line);
			
			// Finds for an occurence of the file name in the retrieved line
			size_t position = line.find(fname);

			// checks if a match is found, if not found then returned value is npos
			if(position != string::npos)
			{
				// assigns the file name to the name field
				fileData->name = fname.data();
				// Finds for an occurence of the delimiter
				size_t delimiterPosition = line.find('|', position);
				if(delimiterPosition != string::npos)
				{
					int length = line.length();
					// type data starts after the delimiter
					int typeStartPosition = delimiterPosition + 2;
					// creates a new string for type
					string type (line, delimiterPosition + 2, length - typeStartPosition);
					// checks the file type in the meta file and assigns the appropriate enum value
					if(type.compare("heap"))
					{
						fileData->type = heap;
					}
					else if(type.compare("Sorted"))
					{
						fileData->type = sorted;
					}
					else if(type.compare("Tree"))
					{
						fileData->type = tree;
					}
                                        isDataRetrieved = TRUE;
					break;
				}
			}
		}
	}
	return isDataRetrieved;
}

int HeapFile :: AddFileDetailsToMetaFile(FileData fileData)
{
        string line;
        int isDataWritten = FALSE;
        if(NULL != fileData.name && 0 <= fileData.type)
        {
            fstream file (METADATAFILE, fstream::app | fstream::out);

            if(TRUE == file.is_open())
            {
                if(heap == fileData.type)
                {
                    string fileType("Heap");

                    line = string(fileData.name);
                    line = line + string(" | ");
                    line = line + string(fileType);

                    file.write(line.data(), line.size());
                    isDataWritten = TRUE;
                }
            }
        }

        return isDataWritten;
}*/

int HeapFile :: WriteCurrentPageToFile()
{
    int isSuccessfulWrite = FALSE;
    // Check if File is open
    if(NULL != currentFile)
    {
        if(NULL != currentPage)// && 0 <= currentPageNumber)
        {
            if(0 > currentPageNumber)
            {
                currentPageNumber = 0;
            }
            currentFile->AddPage(currentPage, (off_t) currentPageNumber);
            isSuccessfulWrite = TRUE;
            dirtyFlag = FALSE;
            delete currentPage;
            // delete currentRecord;
            currentPage = NULL;
            currentRecord = NULL;
        }
    }
    return isSuccessfulWrite;
}

void HeapFile :: Add(Record &addMe)
{

	// TODO: Code to parse the meta data file. This is to check if the file is a heap file
        off_t numberOfPages;
	// TODO: Check if the file is open else return a 0 with an error message
        // get the number of pages in the file. return type is off_t
	if(NULL != currentFile && (0 <= (numberOfPages = currentFile->GetLength())))
	{
		
		// TODO: Check if the current page is the last page in the file
                if(NULL != currentPage)
                {

                    if(currentPageNumber < ((int)numberOfPages - 2))
                    {
                        if(TRUE == dirtyFlag)
                        {
                            //TODO: Code to save the current record and page into
                            //the file if the flag is dirty
                            WriteCurrentPageToFile();
                            dirtyFlag = FALSE;
                        }
                        
                        // Discard the data
                        delete currentPage;
                        // delete currentRecord;

                        // TODO: delete or EmptyItOut() ?
                        
                        // Get the last page
                        currentPage = NULL;
                        currentRecord = NULL;
                        currentFile->GetPage(currentPage, numberOfPages - 1);
                        currentPageNumber = numberOfPages - 1;
                        // Reset the dirty flag since the new page is retrieved
                    }
                }
                else
                {
                    // Create a new page
                    currentPage = new Page();
                    // currentPageNumber++;
                    dirtyFlag = FALSE;
                }

                // Append the record
                // Set the dirty flag since the current page is updated
		if(NULL != currentPage)
		{
			int successfullyAdded = currentPage->Append(&addMe);
			
			// Failure due to Page overflow
			if(FALSE == successfullyAdded)
			{
                            // TODO: Check if the the page is dirty and store to disk
                            if(TRUE == dirtyFlag)
                            {
                                // TODO: Save to disk
                                if(-1 == currentPageNumber)
                                {
                                    currentPageNumber++;
                                }
                                WriteCurrentPageToFile();
                                dirtyFlag = FALSE;
                            }
                            //Discard data

                            delete currentPage;

                            //delete currentRecord;
                            currentPage = NULL;
                            currentRecord = NULL;
                            // Creates a new page
                            currentPage = new Page();
                            currentPageNumber++;
                            successfullyAdded = currentPage->Append(&addMe);

                            // Update the current record pointer

                            // TODO: Remove
                            currentRecord = &addMe;

			}
			else
			{
                            currentRecord = &addMe;
			}

                        dirtyFlag = successfullyAdded;
		}

	}
	else
	{
		// TODO: Do we need to open the file? or log the error?
            cout<<"Error: No File exists. Create a file first"<<endl;
	}
}

void HeapFile :: MoveFirst()
{
    // check if the file is opened
	if(NULL != currentFile)
	{
            // cout<<"Move First : File Exists"<<endl;
            // cout<<"current page number = "<<currentPageNumber<<endl;
            if(NULL == currentPage && 0 <= currentPageNumber)
            {
                // cout<<"New Page"<<endl;
                currentPage = new Page();
                currentFile->GetPage(currentPage, 0);
            }
            // cout<<"Number of Records ="<<currentPage->GetNumberOfRecords()<<endl;
            if(NULL != currentPage && 0 <= currentPage->GetNumberOfRecords())
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
                    /*if(NULL != currentPage)
                    {
                        currentPage->EmptyItOut();
                    }
                    */
                    currentPage = new Page();
                    currentFile->GetPage(currentPage, 0);
                    currentPageNumber = 0;
                    // TODO: Remove
                       // currentRecord = new Record();
                    currentRecord = new Record();
                    currentPage->RetrieveFirstRecord(currentRecord);
                    isMoveFirst = TRUE;
		}
            }
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

int HeapFile :: Open(char* name)
{
    int isFileOpen = FALSE;
//    char* tempName = new char[80];
//    sprintf(tempName, "%s.hp", name);
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
    }
    else
    {
        cout<<"Error: File is already open"<<endl;
    }
    return isFileOpen;
}

int HeapFile :: Close()
{
    int isFileClosed = FALSE;
    // Check if file is open
    if(NULL != currentFile)
    {
        // Check if the page exists in memory
        if(NULL != currentPage)
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
        }
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

void HeapFile::Load(Schema& mySchema, char* loadMe)
{
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
                                    this->Add(*addMe); 				//Adds the record to the DBFile
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

int HeapFile :: Create (char* name, fType myType, void* startup)
{
    int isFileCreated = FALSE;

    if(NULL == currentFile)
    {
        currentFile = new File();
    }
//    char* tempName = new char[80];
//    sprintf(tempName, "%s.hp", name);
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
        FileData fData;
        fData.name = name;
        fData.type = myType;
        
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

int HeapFile :: GetNext (Record &fetchMe)
{
    
    // currentRecord = &fetchMe;
    if (currentFile !=NULL)								//checks if file is open
    {
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

int HeapFile :: GetNext (Record& fetchMe, CNF& applyMe, Record& literal)
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

int HeapFile :: GetPageToMerge(Page* pageToMerge, int pageNumber)
{
    int isRetrieved = FALSE;
    if(NULL != currentFile)
    {
         if(NULL != currentPage)
        {
            delete currentPage;
            currentPage = NULL;
            currentRecord = NULL;
        }

         int pageLength = currentFile->GetLength();
         cout<<"Page Length in file = "<<pageLength<<endl;
         if(0 <= pageNumber && pageLength > pageNumber)
         {
             cout<<"Page Number = "<<pageNumber<<endl;
             // TODO: Replace pageNumber+1 with pageNumber
             currentFile->GetPage(pageToMerge, pageNumber);
             isRetrieved = TRUE;
         }
         else
         {
             cout<<"Invalid Page Number"<<endl;
         }
    }
    else
    {
        cout<<"Error: File not open"<<endl;
    }

    return isRetrieved;
}

int HeapFile :: Add(Record &addMe, int addPage)
{
    cout<<"In heap file add"<<endl;
    int returnValue = 0;
	// TODO: Code to parse the meta data file. This is to check if the file is a heap file
        off_t numberOfPages;
	// TODO: Check if the file is open else return a 0 with an error message
        // get the number of pages in the file. return type is off_t
	if(NULL != currentFile && (0 <= (numberOfPages = currentFile->GetLength())))
	{
            //cout<<"Add : File Exists"<<endl;
            //cout<<"Add Page = "<<addPage<<endl;
		// TODO: Check if the current page is the last page in the file
                if(NULL != currentPage)
                {
                    cout<<"@@@@@@@@@@Current Page is not null"<<endl;
                    if(currentPageNumber < ((int)numberOfPages - 2))
                    {
                        if(TRUE == dirtyFlag)
                        {
                            //TODO: Code to save the current record and page into
                            //the file if the flag is dirty
                            WriteCurrentPageToFile();
                            dirtyFlag = FALSE;
                        }

                        // Discard the data
                        delete currentPage;
                        // delete currentRecord;

                        // TODO: delete or EmptyItOut() ?

                        // Get the last page
                        currentPage = NULL;
                        currentRecord = NULL;
                        cout<<"@@@@@@@@@@@number of pages = "<<numberOfPages<<endl;
                        currentFile->GetPage(currentPage, numberOfPages-1);
                        currentPageNumber = numberOfPages-1;
                        // Reset the dirty flag since the new page is retrieved
                    }
                }
                else if(addPage > 0)
                {
                    cout<<"@@@@@@@@@@Add: New Page"<<endl;
                    // Create a new page
                    currentPage = new Page();
                    // currentPageNumber++;
                    dirtyFlag = FALSE;
                }
                else
                {
                    cout<<"Returning 0"<<endl;
                    return(0);
                }

                // Append the record
                // Set the dirty flag since the current page is updated
		if(NULL != currentPage)
		{
			int successfullyAdded = currentPage->Append(&addMe);

			// Failure due to Page overflow
			if(FALSE == successfullyAdded)
			/*{
                            // TODO: Check if the the page is dirty and store to disk
                            cout<<"@@@@@@@@@@sucessfullyadded = false"<<endl;
                            if(TRUE == dirtyFlag)
                            {
                                // TODO: Save to disk
                                if(-1 == currentPageNumber)
                                {
                                    currentPageNumber++;
                                }
                                WriteCurrentPageToFile();
                                dirtyFlag = FALSE;
                            }
                            //Discard data

                            delete currentPage;

                            //delete currentRecord;
                            currentPage = NULL;
                            currentRecord = NULL;
                            // Creates a new page
                            currentPage = new Page();
                            currentPageNumber++;
                            successfullyAdded = currentPage->Append(&addMe);

                            // Update the current record pointer

                            // TODO: Remove
                            currentRecord = &addMe;

			}
			else
			{
                            currentRecord = &addMe;
			}*/
                        //cout<<"Add: Successfully Added = "<<successfullyAdded<<endl;
                        returnValue = successfullyAdded;
                        dirtyFlag = successfullyAdded;
		}

	}
	else
	{
		// TODO: Do we need to open the file? or log the error?
            cout<<"Error: No File exists. Create a file first"<<endl;
	}

        return returnValue;
}
int HeapFile :: MoveToNextPage()
{
    int writtenPageNumber = currentPageNumber;
    if(WriteCurrentPageToFile()==0)
    {
        cout<<"unable to write current page to file" << endl;
        return(0);
    }
    Page *currentPage = new Page();
    currentFile->AddPage(currentPage, writtenPageNumber+1);
    currentFile->GetPage(currentPage, writtenPageNumber+1);
    Record *currentRecord = new Record();
    currentPage->RetrieveFirstRecord(currentRecord);
    currentPageNumber = writtenPageNumber+1;
    return(1);
}

int HeapFile :: PrintFileContent(int option, Schema *mySchema)
{
    int tempPageNumber = 0;
    int noOfRecords = 0;
    int totNoOfRecords=0;
    if(option==1)
    {
        MoveFirst();
        noOfRecords++;
        do
        {
            tempPageNumber++;
            cout << "Moving to Page Number " << tempPageNumber << endl;
            while(currentPage->RetrieveNextRecord(currentRecord)!=0)
            {
                noOfRecords++;
            }
            cout<<"Found " << noOfRecords << " on Page Number "<<tempPageNumber<<endl;
            totNoOfRecords += noOfRecords;
            noOfRecords = 0;
        }
        while(GetNext(*currentRecord)!=0);
        cout << " Found " << totNoOfRecords << " over " << tempPageNumber << " Pages "<<endl;
    }
    else
    {
        MoveFirst();
        noOfRecords++;
        do
        {
            tempPageNumber++;
            cout << "Moving to Page Number " << tempPageNumber << endl;
            currentRecord->Print(mySchema);
            noOfRecords++;
            while(currentPage->RetrieveNextRecord(currentRecord)!=0)
            {
                noOfRecords++;
                currentRecord->Print(mySchema);
            }
            cout<<"Found " << noOfRecords << " on Page Number "<<tempPageNumber<<endl;
            totNoOfRecords += noOfRecords;
            noOfRecords = 0;
        }
        while(GetNext(*currentRecord)!=0);
        cout << " Found " << totNoOfRecords << " over " << tempPageNumber << " Pages "<<endl;
    }   
}

int HeapFile :: GetPageLength()
{
        int numberOfPages = 0;
        if(NULL != currentFile)
        {
            numberOfPages = currentFile->GetLength();
            // as the first page is blank
            numberOfPages--;

        }
        return numberOfPages;
}
int HeapFile :: Add(Page &addMe)
{
    //CAUTION:::: This function should not be used if you are adding records and pages to the DBFile simultaneously
    //It is not written to handle that kind of stuff... Heavy modifications may be needed for that kind of stuff...
    int whichPage = currentPageNumber;
    if(NULL!=currentFile)
    {
        whichPage = (int) currentFile->GetLength();
        if(&addMe != NULL)
        {
            currentFile->AddPage(&addMe, ((off_t) whichPage+1));
            currentPageNumber++;
            //currentFile->GetPage(currentPage, (off_t) whichPage+1);
            //currentPage->RetrieveFirstRecord(currentRecord);
        }
        else
        {
            Page *newPage = new Page();
            currentFile->AddPage(newPage, whichPage+1);
            currentPageNumber++;
            //currentFile->GetPage(currentPage, (off_t) whichPage+2);
            //currentPage->RetrieveFirstRecord(currentRecord);
        }
    }
}
