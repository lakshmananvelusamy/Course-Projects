#include "MetaData.h"
#include "Comparison.h"
#include <stdlib.h>
#include <iostream>
using namespace std;

MetaData::MetaData(fType fType, OrderMaker om, int runlen)
{
    cout<<"In metadata constructor"<<endl;
    fileType = fType;
    sortedOrder = om;
    runLength = runlen;
    this->Print();
}

MetaData::MetaData(const MetaData& orig) {
}

MetaData::MetaData() {
}

MetaData::~MetaData() {
}

void MetaData::Print()
{
    cout<<"File type is"<<fileType<<endl;
    cout<<"Order Maker is"<<endl;
    sortedOrder.Print();
    cout<<"Run length is"<<runLength<<endl;
}

