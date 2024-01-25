#include <stdlib.h>
#include <iostream>
#include <string>

//#include "newclass.h"
//#include "newclass.cc"
#include "DBFile.h"


using namespace std;



/*
 * 
 */
int main(int argc, char** argv) {

//    //open the file file_name for writing objects.fstream object used for both input and output operations with respect to files
//	fstream fobj("file_name.meta",ios::out|ios::app);
//
//
//        NewClass nc;
//	//char ch;
//
//	//to write in to file
//
//	//this do while loop will take the details of employee to an object and writes in to the file until u press 'n' or than 'y'
//
//	cout<<"enter the employee details"<<endl;
//        //String str = "abcpqr";
//        nc = new NewClass(12.24);
//        cout<<"the created object is:"<<nc<<endl;
//        nc.print();
//	//using write method we can write in to the file
//	fobj.write ( (char *)(&nc) , sizeof(nc) );
//
//
//	fobj.close();
//
//	//to read from the file
//
//	fobj.open("file_name.meta",ios::read);
//
//	//read all the objects using read method and displaying.when read encounters end of file returns null.
//
//	NewClass mn = fobj.read (  (char *)&mn , sizeof(mn) );
//        mn.print();
//	fobj.close();
    OrderMaker *tempom = new OrderMaker();
    //int runLen = 12;
    SortInfo si;
    si.myOrder = tempom;
    si.runLength = 12;
    DBFile *testDBFile = new DBFile();
    cout<<"DBFile constructed"<<endl;
    fType ftype = heap;
    cout<<ftype<<endl;
    ftype = sorted;
    cout<<ftype<<endl;
    testDBFile->Create("srttest1.bin", ftype, (void*) &si);
    testDBFile->Close();
    testDBFile->Open("srttest1.bin");
    testDBFile->Close();
    //Schema mySchema = new Schema();
    

    return (EXIT_SUCCESS);
}



