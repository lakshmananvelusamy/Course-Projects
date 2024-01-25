#include <string.h>
#include <tr1/unordered_map>
#include <vector>
using namespace std;
// using namespace __gnu_cxx;
using namespace tr1;

#ifndef _STATISTICS_H
#define	_STATISTICS_H



struct KeyComparison
{
    bool operator()(const char* s1, const char* s2) const
    {
        return strcmp(s1, s2) == 0;
    }
};

namespace std
{
    namespace tr1
    {
        template<>
        struct hash<const char *> : public std::unary_function<const char *, size_t>
        {
            size_t operator()(const char* str) const
            {
                size_t h = 0;
                for (; *str; ++str)
                    h += *str;
                return h;
            }
        };
    }

}

class Relation;
typedef unordered_map<const char*, Relation*, hash<const char*>, KeyComparison> RelationMap;
typedef unordered_map<const char*, int, hash<const char*>, KeyComparison > AttributeMap;
typedef unordered_map<const char*, const char*, hash<const char*>, KeyComparison> JoinMap;

class Relation
{
    private:
        double numberOfTuples;
        AttributeMap attributes;
        vector<const char*> joinedRelations;

    public:
        Relation();
        // returns the number of tuples
        double GetNumberOfTuples();
        // returns the number of distincts for the attribute
        int GetNumberOfDistincts(char* attributeName);
        // sets the number of tuples
        void SetNumberOfTuples(double numberOfTuples);
        // sets the number of distincts for the given attribute
        void SetNumberOfDistincts(char* attributeName, int numberOfDistincts);
        // gets the attributes in the relation
        // hash_map<const char*, int, hash<const char*>, KeyComparison > GetAttributes();
        AttributeMap GetAttributes();
        // sets the attributes in the relation
        // void SetAttributes(const hash_map<const char*, int, hash<const char*>, KeyComparison > newAttributes);
        void SetAttributes(AttributeMap newAttributes);
        // Get the relations with which this relation has been joined
        vector<const char*> GetJoinedRelations();

        void SetJoinedRelations(vector<const char*> joinedRelations);

        
};

class Statistics
{
    private:
        // hash_map<const char*, Relation*, hash<const char*>, KeyComparison > relations;
        RelationMap relations;
        JoinMap joinedRelations;
        vector<const char*> GetVectorFromJoinString(const char* joinString);
        const char* GetJoinStringFromVector(vector<const char*> joinVector);
        Relation* CalculateOperand(struct Operand *pOp, char** relName, int numToJoin);
        double CalculateComparisonOp(struct ComparisonOp *pCom, char** relName, int numToJoin);
        double ComputeOrList(struct OrList *pOr, char** relName, int numToJoin);
        double ComputeAndList(struct AndList *pAnd, char** relName, int numToJoin);
    public:
        // hash_map<const char*, Relation*, hash<const char*>, KeyComparison > GetRelations();
        RelationMap GetRelations();
        void AddRel(char* relName, int numTuples);
        void AddAtt(char* relName, char* attName, int numDistincts);
        void CopyRel(char* oldName, char* newName);
        void Read(char* fromWhere);
        void Write(char* fromWhere);
        Statistics();
        ~Statistics();
        Statistics(Statistics &copyMe);
        void Apply(struct AndList *parseTree, char **relNames, int numToJoin);
        double Estimate(struct AndList *parseTree, char **relNames,int numToJoin);

};



#endif	/* _STATISTICS_H */

