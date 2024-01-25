#ifndef DEFS_H
#define DEFS_H


#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072
// [Akshay] Added for condition check 
#define TRUE 1
#define FALSE 0
//#define METADATAFILE "meta.header"
#define INPUTPATH "/COP6726/DATA/10M/region.tbl"
#define OUTPUTPATH "/COP6726/GROUPS/BA/Tables/region.bin"

// const char* METADATAFILE = "meta.header";
enum Target {Left, Right, Literal};
enum CompOperator {LessThan, GreaterThan, Equals};
enum Type {Int, Double, String};
// [Akshay] Added enum fror file type
enum fType {heap, sorted, tree };
enum fMode {READ, WRITE};

unsigned int Random_Generate();


#endif

