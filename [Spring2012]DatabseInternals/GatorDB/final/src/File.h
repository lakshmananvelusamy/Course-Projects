#ifndef FILE_H
#define FILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

class Record;
class DBFile;
class Schema;
using namespace std;

class Page {
private:
	TwoWayList <Record> *myRecs;
	
	int numRecs;
	int curSizeInBytes;

public:
	// constructor
	Page ();
	virtual ~Page ();
	// this takes a page and writes its binary representation to bits
	void ToBinary (char *bits);

	// this takes a binary representation of a page and gets the
	// records from it
	void FromBinary (char *bits);

	// the deletes the first record from a page and returns it; returns
	// a zero if there were no records on the page
	int GetFirst (Record *firstOne);

	// this appends the record to the end of a page.  The return value
	// is a one on success and a aero if there is no more space
	// note that the record is consumed so it will have no value after
	int Append (Record *addMe);

	// empty it out
	void EmptyItOut ();

	/*
	[Akshay]
	We need to have a function to get the 1st record of a page which does not consume the record, 
	since using toBinary to get a particular record would be too much of work everytime.
	*/
	int RetrieveFirstRecord(Record* first);

	/*
	[Akshay]
	We need a method to check if the current pointer in the two way list is pointing to the first record in the page
	Similarly, we need a method to check if the current pointer is at the last record in the page
	*/
	// Checks if the current record is the first record on the page
	int IsCurrentRecordFirst();

	// Checks if the current record is the last record on the page
	int IsCurrentRecordLast();

        /* [Akshay]
         *  We need a method to move to the next record in a page. This will also
         *  update the current pointer of the Two Way list
         */
        int RetrieveNextRecord(Record* next);

        /* [Akshay]
         * A method to get the number of records in the page
         * It returns 0 if the page is empty. This is required to check
         * during move first function
         */
        int GetNumberOfRecords();

        /*
         * Required as we need to move to the next record, when we retrieve
         * the first record during GetNext()
         */
        int MoveToNextRecord();
};


class File {
private:
	/* [Akshay] - Current length of the file - number of pages */
	off_t curLength;
	int myFilDes;

public:
	File ();
	~File ();

	// returns the current length of the file, in pages
	off_t GetLength ();

	// opens the given file; the first parameter tells whether or not to
	// create the file.  If the parameter is zero, a new file is created
	// the file; if notNew is zero, then the file is created and any other
	// file located at that location is erased.  Otherwise, the file is
	// simply opened
	void Open (int length, char *fName);

	// allows someone to explicitly get a specified page from the file
	void GetPage (Page *putItHere, off_t whichPage);

	// allows someone to explicitly write a specified page to the file
	// if the write is past the end of the file, all of the new pages that
	// are before the page to be written are zeroed out
	void AddPage (Page *addMe, off_t whichPage);

	// closes the file and returns the file length (in number of pages)
	int Close ();

};



#endif
