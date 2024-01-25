#include "File.h"
#include "TwoWayList.cc"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <iostream>

using namespace std;

Page :: Page () {
	curSizeInBytes = sizeof (int);
	numRecs = 0;

	myRecs = new (std::nothrow) TwoWayList<Record>;
	if (myRecs == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}
}

Page :: ~Page () {
	delete myRecs;
}


void Page :: EmptyItOut () {

	// get rid of all of the records
	while (1) {
		Record temp;
		if (!GetFirst (&temp))
			break;
	}	

	// reset the page size
	curSizeInBytes = sizeof (int);
	numRecs = 0;
}


int Page :: GetFirst (Record *firstOne) {
	
	/* <Akshay> - gets the first record and then removes it from the two way list */

	// move to the first record
	myRecs->MoveToStart ();

	// make sure there is data 
	if (!myRecs->RightLength ()) {
		return 0;
	}

	// and remove it
	myRecs->Remove (firstOne);
	numRecs--;

	char *b = firstOne->GetBits();
	/* <Akshay> - Removes the number of bytes held by the first record 
		from the current size */
	curSizeInBytes -= ((int *) b)[0];

	return 1;
}

// Gets the first record on the page without removing it. 
// If there are no records on the page, then the 0 is returned else 1 is returned
int Page :: RetrieveFirstRecord(Record* first)
{
	int isRetrievedSuccessfully = FALSE;
	// move to the first record
	myRecs->MoveToStart ();
        
	// make sure there is data 
	if (0 < myRecs->RightLength ()) 
	{
           
		// gets the 1st record
                myRecs->GetCurrent(first);

                if(NULL != first)
                {
                    isRetrievedSuccessfully = TRUE;
                }
	}
	
	return isRetrievedSuccessfully;
}

// Gets the next record in the two way list
// Retrieves a pointer to the next record on the page
// (i.e. the one immediately after the current record pointer) and stores it in next
// Record* Page:: RetrieveNextRecord()
int Page:: RetrieveNextRecord(Record* next)
{
     int isNextRetrieved = FALSE;
    // [Akshay]
    // Checks if there are any more records on the page
    // There is possibility in the current node that the right length
    // becomes negative, if the methods are not called appropriately,
    // hence we need to check for greater than 0
    // if(myRecs->RightLength()!=0)
    
        //Advances the current record pointer
    if(1 < myRecs->RightLength())
    {
        myRecs->Advance();
        myRecs->GetCurrent(next);
        if(NULL != next)
        {
            isNextRetrieved = TRUE;
        }
    }

    return isNextRetrieved;
}


// Checks if the current record is the first record on the page
int Page :: IsCurrentRecordFirst()
{
	// checks if the Left Length is 0
	int isFirst = (0 == myRecs->LeftLength());

	return isFirst;
}

// Checks if the current record is the last record on the page
int Page :: IsCurrentRecordLast()
{
	// Checks if the Right Length is 0
	int isLast = (1 >= myRecs->RightLength());
	return isLast;
}

// Returns the number of records
int Page :: GetNumberOfRecords()
{
    return numRecs;
}

int Page :: MoveToNextRecord()
{
    int isMoveSuccessful = FALSE;
    if(1 < myRecs->RightLength())
    {
        myRecs->Advance();
        isMoveSuccessful = TRUE;
    }

    return isMoveSuccessful;
}

int Page :: Append (Record *addMe) {
    
	char *b = addMe->GetBits();
    
	/* <Akshay> - Checks if the size after appending stays within 
		the limit of the maximum size */
	// first see if we can fit the record
    
    
	if (curSizeInBytes + ((int *) b)[0] > PAGE_SIZE) {
    
		return 0;
	}

    
	// move to the last record
	myRecs->MoveToFinish ();
    
	// and add it
	curSizeInBytes += ((int *) b)[0];
       
	myRecs->Insert(addMe);
       
	numRecs++;

	return 1;	
}


void Page :: ToBinary (char *bits) {
	/* <Akshay> - Converts all the records on the page to
		a character array
		Binary Representation: Initial 4 bytes represent 
		the number of records followed by the records binary representation
	*/

	// first write the number of records on the page
	((int *) bits)[0] = numRecs;
	char *curPos = bits + sizeof (int);

	// and copy the records one-by-one
	myRecs->MoveToStart ();
	for (int i = 0; i < numRecs; i++) {	
		char *b = myRecs->Current(0)->GetBits();
		
		// copy over the bits of the current record
		memcpy (curPos, b, ((int *) b)[0]);
		curPos += ((int *) b)[0];

		// and traverse the list
		myRecs->Advance ();
	}
}


void Page :: FromBinary (char *bits) {

    
	// first read the number of records on the page
	numRecs = ((int *) bits)[0];
	// sanity check
	if (numRecs > 1000000 || numRecs < 0)
        {
		cerr << "This is probably an error.  Found " << numRecs << " records on a page.\n";
		exit (1);
	}

	// and now get the binary representations of each
	char *curPos = bits + sizeof (int);

	// first, empty out the list of current records
	myRecs->MoveToStart ();
	while (myRecs->RightLength ()) {
		Record temp;
		myRecs->Remove(&temp);
	}

	// now loop through and re-populate it
	Record *temp = new (std::nothrow) Record();
	if (temp == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}

	curSizeInBytes = sizeof (int);
	for (int i = 0; i < numRecs; i++) {

		/* <Akshay> - gets the length of each record from the 1st 
			4 bytes in the record header */

		// get the length of the current record
		int len = ((int *) curPos)[0];
		curSizeInBytes += len;

		// create the record
		temp->CopyBits(curPos, len);

		// add it
		myRecs->Insert(temp);

		// and move along
		myRecs->Advance ();

		curPos += len;
	}

	delete temp;
}




File :: File () {
    // cout<<"Constructor called/n";
    //getchar();
}

File :: ~File () {
    // cout<<"Destructor called/n";
    //getchar();
}


void File :: GetPage (Page *putItHere, off_t whichPage) {

    // cout<<whichPage<<endl;
	// this is because the first page has no data
	whichPage++;
	if (whichPage >= curLength) {
		cerr << "BAD: you tried to read past the end of the file\n";
		exit (1);
	}

	// read in the specified page
	char *bits = new (std::nothrow) char[PAGE_SIZE];
	if (bits == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}
	/* <Akshay> - reads from the desired page file 
		Read in binary format
	*/
	lseek (myFilDes, PAGE_SIZE * whichPage, SEEK_SET);
	read (myFilDes, bits, PAGE_SIZE);
	/* <Akshay> - Converts from binary to page
	*/
        
	putItHere->FromBinary (bits);
	delete [] bits;
	
}


void File :: AddPage (Page *addMe, off_t whichPage) {

	/* <Akshay> - First Page does not contain data. If the 
		specified page is greater than the current size, then 
		a new page is added along with more number of blank pages
		i.e pages with values = 0. If the page number is lesser than
		the current size then the new page data over-writes the old 
		page data
	*/
	// this is because the first page has no data
	whichPage++;
	
	// if we are trying to add past the end of the file, then
	// zero all of the pages out
	if (whichPage >= curLength) {
		
		// do the zeroing
		for (off_t i = curLength; i < whichPage; i++) {
			int foo = 0;
			/* <Akshay> - Writes to the file the value 0 
				(whichPage - currLength) times
			*/
			lseek (myFilDes, PAGE_SIZE * i, SEEK_SET);
			write (myFilDes, &foo, sizeof (int));
		}

		// set the size
		curLength = whichPage + 1;	
	}

	// now write the page
	char *bits = new (std::nothrow) char[PAGE_SIZE];
	if (bits == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}
	addMe->ToBinary (bits);
	/* <Akshay> - Writes the binary converted page data
		to the file */
	lseek (myFilDes, PAGE_SIZE * whichPage, SEEK_SET);
	write (myFilDes, bits, PAGE_SIZE);
	delete [] bits;

        
}


void File :: Open (int fileLen, char *fName) {

	// figure out the flags for the system open call
        int mode;
        if (fileLen == 0)
                mode = O_TRUNC | O_RDWR | O_CREAT;
        else
                mode = O_RDWR;

	// actually do the open
        myFilDes = open (fName, mode, S_IRUSR | S_IWUSR);

#ifdef verbose
	cout << "Opening file " << fName << " with "<< curLength << " pages.\n";
#endif

	// see if there was an error
	if (myFilDes < 0) {
		cerr << "BAD!  Open did not work for " << fName << "\n";
		exit (1);
	}
	// read in the buffer if needed
	if (fileLen != 0) {

		// read in the first few bits, which is the page size
		lseek (myFilDes, 0, SEEK_SET);
		read (myFilDes, &curLength, sizeof (off_t));

	} else {
		curLength = 0;
	}

}


off_t File :: GetLength () {
	return curLength;
}


int File :: Close () {

	// write out the current length in pages
	lseek (myFilDes, 0, SEEK_SET);
	write (myFilDes, &curLength, sizeof (off_t));

	// close the file
	close (myFilDes);

	// and return the size
	return curLength;
	
}


