#ifndef _METADATA_H
#define	_METADATA_H

#include "Defs.h"
#include "Comparison.h"

class MetaData {
public:
    fType fileType;
    OrderMaker sortedOrder;
    int runLength;

    MetaData(fType fileType, OrderMaker om, int runLength);
    MetaData(const MetaData& orig);
    MetaData();
    void Print();
    ~MetaData();
    //friend Class DBFile;
private:


};

#endif	/* _METADATA_H */

