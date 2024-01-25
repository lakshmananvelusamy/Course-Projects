#ifndef _RECORDBUFFER_H
#define	_RECORDBUFFER_H



#endif	/* _RECORDBUFFER_H */

class RecordBuffer
{
    private:
        Page** pageBuffer;
        int pageIndex;
        int runLength;
        int retrievePageIndex;
    public:
        
        RecordBuffer(int runLength);
        
        ~RecordBuffer();

        int AddRecord(Record* addMe);

        int GetNextRecord(Record* fillMe);

        int RetrieveNextRecord(Record* fillMe);

        int MoveToBufferStart();

        int DeleteBuffer();
    
};