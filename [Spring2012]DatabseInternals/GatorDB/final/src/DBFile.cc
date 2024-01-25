#include <string.h>
//#include <string.h>
#include <fstream>
#include "DBFile.h"
#include "Defs.h"
using namespace std;


// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {

}

int DBFile::Create (char *fPath, fType f_type, void *startup)
{
    cout<<"In Create"<<endl;
        //open the file file_name for writing objects.fstream object used for both input and output operations with respect to files
        int isFileCreated = FALSE;
        OrderMaker *sortedOrder;
        char* metaFilePath = new char[80];
        //char* metaFilePath = fPath;
        //char* extension = ".meta";
        //string metaFilePath = (string) f_path;
        //SortInfo *newSortInfo = (SortInfo*) startup;
        //onechar[0]='a';
        //onechar[1]='b';
        //char* twochar = "abc";
        //char* mn = abc + ".lp";
        cout<<"about to concatenate"<<endl;
        //cout<<fPath<<endl;
        
        //cout<<extension<<endl;
        //strcat(metaFilePath, extension);
        sprintf(metaFilePath, "%s.meta", fPath);
        cout <<metaFilePath<<endl;
        cout<<f_type<<endl;
        fstream fobj (metaFilePath,fstream::out|fstream::binary);
	//to write in to file
	//cout<<"enter the employee details"<<endl;
        if(f_type == sorted)
        {
            cout<<"Into Sorted"<<endl;
            //sortedOrder = newSortInfo->myOrder;
            MetaData newMetaData(f_type, *(((SortInfo*)startup)->myOrder), ((SortInfo*)startup)->runLength);
            //cout<<"the created object is:"<<endl;
            //newMetaData.print();
            //using write method we can write in to the file
            fobj.write ((char *)(&newMetaData) , sizeof(newMetaData));
        }
        else
        {
            OrderMaker tempom;
            MetaData newMetaData(f_type, tempom, -1);
            //cout<<"the created object is:"<<endl;
            //newMetaData.print();
            //using write method we can write in to the file
            fobj.write ((char *)(&newMetaData) , sizeof(newMetaData));
        }
        //delete &nc;
	fobj.close();
        if(f_type == sorted)
        {
            myInternalVar = new SortedFile(*(((SortInfo*)startup)->myOrder), ((SortInfo*)startup)->runLength);
        }
        else
        {
            myInternalVar = new HeapFile();
        }
        isFileCreated = myInternalVar->Create(fPath, f_type, startup);
        return(isFileCreated);
}

int DBFile::Open (char *fPath)
{
    //fstream fobj ("abc.meta",fstream::out|fstream::app);
    int isFileOpen = FALSE;
    OrderMaker *om = new OrderMaker();
    MetaData tempMetaData;
   //MetaData tempMetaData(heap, *om, -1);
   char* metaFileName = new char[80];
   sprintf(metaFileName, "%s.meta", fPath);
   //cout<<metaFileName<<endl;
   fstream fobj(metaFileName,fstream::in|fstream::binary);
   if(!fobj.is_open())
   {
       cout<<"Cannot open Metadata file"<<endl;
       return(0);
   }
   //read all the objects using read method and displaying.when read encounters end of file returns null.
   fobj.read ((char *)&tempMetaData , sizeof(tempMetaData) );
   //fobj.Print();
   tempMetaData.Print();
   fobj.close();
   fType f_type = tempMetaData.fileType;
   if(f_type == sorted)
   {
       myInternalVar = new SortedFile(tempMetaData.sortedOrder, tempMetaData.runLength);
   }
   else
   {
       myInternalVar = new HeapFile();
   }
   isFileOpen = myInternalVar->Open(fPath);
   return(isFileOpen);
}

void DBFile::MoveFirst ()
{
    myInternalVar->MoveFirst();
}

int DBFile::Close ()
{
    int isFileClosed = FALSE;
    isFileClosed = myInternalVar->Close();
    return(isFileClosed);
}

void DBFile::Load(Schema& myschema, char* loadpath)
{
    myInternalVar->Load(myschema, loadpath);
}

void DBFile::Add (Record &rec)
{
    //cout<<"myInternalVar is"<<myInternalVar<<endl;
    //Schema *sch = new Schema("catalog", "nation");
    //rec.Print(sch);
    myInternalVar->Add(rec);
}

int DBFile::GetNext (Record &fetchme)
{
    int getNext = FALSE;
    getNext = myInternalVar->GetNext(fetchme);
	return(getNext);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal)
{
    int getNext = FALSE;
    getNext = myInternalVar->GetNext(fetchme, cnf, literal);
    return getNext;
}

