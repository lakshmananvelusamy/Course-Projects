#ifndef HEAPFILE_H
#define HEAPFILE_H

#include <stdio.h>
#include <iostream>
using namespace std;

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"

struct FileData
{
	const char* name;
	fType type;
};

class HeapFile : public GenericDBFile
{
	private:
		File* currentFile;
		Page* currentPage;
		Record* currentRecord;
                int isMoveFirst;
		/* [Akshay]
		We need to keep the current page number from which the current page has been retrieved from the file.
		This would be helpful in MoveFirst() as it would avoid unnecessary saving and retrieving if the pointer 
		is already at the 1st position or in the 1st page
		*/
		// Indicates the current page number. Make sure to update this whenever a page is retrieved
		int currentPageNumber;
                /* [Akshay]
                 *  We need a dirty flag which will indicate if the current file
                 *  being read from has been written into after being brought
                 *  out from disk
                 */
                int dirtyFlag;
                int WriteCurrentPageToFile();
//		int GetFileDetailsFromMetaFile(FileData* fileData, string fname);
//                int AddFileDetailsToMetaFile(FileData fileData);

                
	public:
                //TODO: Remove AttachSchema method.. used only for test proj 1
                void AttachSchema(Schema* schema);
                HeapFile();

                ~HeapFile();
                
		// Moves to the first record in the first page of the file
		void MoveFirst();

		// Adds the record to the end of the file
		void Add(Record &addMe);

		int GetNext(Record &fetchMe);

		int GetNext(Record &fetchMe, CNF &applyMe, Record &literal);

		int Create(char* name, fType myType, void* startup);

		int Open(char* name);

		int Close();

		// Loads data from the loadMe file which has a schema as specified by mySchema
		void Load(Schema &mySchema, char* loadMe);

                int Add(Record &addMe, int addPage);

                // Gets the specified page from the file into memory
                int GetPageToMerge(Page* pageToMerge, int pageNumber);

                int MoveToNextPage();

                int PrintFileContent(int, Schema*);

                int GetPageLength();

                int Add(Page& addMe);
};

#endif


