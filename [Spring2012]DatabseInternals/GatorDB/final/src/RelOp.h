#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp {
	public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp {

	private:
	pthread_t selFilethread;
	Record *buffer;
        DBFile *t_inFile;
        Pipe *t_outPipe;
        CNF *t_selOp;
        Record *t_literal;
        int noOfPages;

	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
        int Start();

};

class SelectPipe : public RelationalOp
{
        private:
        pthread_t selPipethread;
        Pipe *t_inPipe;
        Pipe *t_outPipe;
        CNF *t_selOp;
        Record *t_literal;
        int noOfPages;

	public:

	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
        void Start();
};

class Project : public RelationalOp
{
        private:
        pthread_t projectthread;
        Pipe *t_inPipe;
        Pipe *t_outPipe;
        int *t_keepMe;
        int t_numAttsInput;
        int t_numAttsOutput;
        int noOfPages;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
        void Start();
};

class Join : public RelationalOp
{
    private:
        pthread_t workerThread;
        Pipe* leftInputPipe;
        Pipe* rightInputPipe;
        Pipe* outputPipe;
        CNF* joinCNF;
        Record* literal;
        int runLength;
        char* fileName;
        Schema* leftSchema;
        Schema* rightSchema;
        int* joinAttributes;
        int startOfRightTableAttribute;
        int numJoinAttributes;
        int testCount;
        void PerformNestedLoopJoin(OrderMaker left, OrderMaker right);
        void PerformBlockNestedJoin();
        int GetNextRecordFromPage(Page*& getFromMe, Record* fillMe, int recordCount);
    public:
        Join();
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
        void Start();
        void SetSchema(Schema &leftSchema, Schema &rightSchema);
};

class DuplicateRemoval : public RelationalOp
{
    private:
    pthread_t duplicateRemovalthread;
    Pipe *t_inPipe;
    Pipe *t_outPipe;
    Schema *t_mySchema;
    int noOfPages;


	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
        void Start();
        DuplicateRemoval();
};

class Sum : public RelationalOp
{
    private:
        pthread_t workerThread;
        Pipe* inputPipe;
        Pipe* outputPipe;
        Schema* mySchema;
        int runLength;
        Function* compute;
        Record* GetResultRecord(Type outputType, int integerResult, double doubleResult);
    public:

	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
        void Start();
};

class GroupBy : public RelationalOp
{
    private:
        pthread_t workerThread;
        Pipe* inputPipe;
        Pipe* outputPipe;
        OrderMaker* groupAttributes;
        Schema* mySchema;
        int runLength;
        Function* compute;
        int totalNumberOfAttributes;
        Record* GetResultRecord(Type outputType, int integerResult, double doubleResult);
    public:

	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
        void Start();
        void SetTotalNumberOfAttributes(int n);
        
};

class WriteOut : public RelationalOp
{
    private:
        pthread_t workerThread;
        Pipe* inputPipe;
        FILE* outputFile;
        Schema* mySchema;
        int runLength;
    public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
        void Start();
};
#endif
