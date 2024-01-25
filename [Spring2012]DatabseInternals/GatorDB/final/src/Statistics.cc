#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <fstream>
#include <iostream>
#include <vector>
#include "Statistics.h"
#include "Defs.h"
#include "ParseTree.h"
#define ATTRIBUTE_END_MARKER '$'
#define RELATION_END_MARKER '*'
#define JOIN_SEPERATOR ','
#define ATTRIBUTE_CODE 4

using namespace std;
using namespace tr1;

Relation :: Relation()
{
    numberOfTuples = 0;
}

double Relation :: GetNumberOfTuples()
{
    return numberOfTuples;
}

int Relation :: GetNumberOfDistincts(char* attributeName)
{
    int numberOfDistincts = attributes[attributeName];
    return numberOfDistincts;
}

void Relation :: SetNumberOfTuples(double numberOfTuples)
{
    this->numberOfTuples = numberOfTuples;
}

void Relation :: SetNumberOfDistincts(char* attributeName, int numberOfDistincts)
{
    if(NULL != attributeName && 0 < numberOfDistincts)
    {
        attributes[attributeName] = numberOfDistincts;
    }
}

// hash_map<const char*, int, hash<const char*>, KeyComparison > Relation :: GetAttributes()
AttributeMap Relation :: GetAttributes()
{
    return attributes;
}

vector<const char*> Relation :: GetJoinedRelations()
{
    return joinedRelations;
}

// void Relation :: SetAttributes(hash_map<const char*,int,hash<const char*>,KeyComparison> newAttributes)
void Relation :: SetAttributes(AttributeMap newAttributes)
{

    attributes = newAttributes;
}

void Relation :: SetJoinedRelations(vector<const char*> joinedRelations)
{
    this->joinedRelations = joinedRelations;
}

void Statistics :: AddRel(char* relName, int numTuples)
{
    if(NULL != relName && 0 < numTuples)
    {
        
        Relation* retrievedRelation = relations[relName];

        if(NULL != retrievedRelation)
        {
            // already existing relation

            retrievedRelation->SetNumberOfTuples(numTuples);
            // int currentNumberOfTuples = retrievedRelation->GetNumberOfTuples();
            // currentNumberOfTuples += numTuples;
            // retrievedRelation->SetNumberOfTuples(currentNumberOfTuples);
            relations[relName] = retrievedRelation;
        }
        else
        {
            // new relation
            Relation* newRelation = new Relation();
            newRelation->SetNumberOfTuples(numTuples);
            newRelation->SetJoinedRelations(GetVectorFromJoinString(relName));
            relations[relName] = newRelation;
        }
        
        joinedRelations[relName] = relName;
    }
}

void Statistics :: AddAtt(char* relName, char* attName, int numDistincts)
{
    if(NULL != relName)
    {
        Relation* retrievedRelation = relations[relName];
        if(NULL != retrievedRelation)
        {
            int currentNumberOfDistincts = retrievedRelation->GetNumberOfDistincts(attName);

            if(0 == currentNumberOfDistincts)
            {
                // new attribute
                retrievedRelation->SetNumberOfDistincts(attName, numDistincts);
            }
            else if(0 < currentNumberOfDistincts)
            {
                // existing attribute
                currentNumberOfDistincts = numDistincts;
                retrievedRelation->SetNumberOfDistincts(attName, currentNumberOfDistincts);
            }

            relations[relName] = retrievedRelation;
        }
    }
}

void Statistics :: CopyRel(char* oldName, char* newName)
{
    if(NULL != oldName && NULL != newName)
    {
        
        Relation* oldRelation = (Relation*) relations[oldName];
        // Relation* newRelationCheck = relations[newName];
        if(NULL != oldRelation)
        {
           int oldRelationTuples = oldRelation->GetNumberOfTuples();
           //cout<<"old tup = "<<oldRelationTuples<<endl;
           AttributeMap oldAttributes = oldRelation->GetAttributes();
            
           Relation* newRelation = new Relation();
           
           newRelation->SetNumberOfTuples(oldRelationTuples);
           AttributeMap newAttributes;
           AttributeMap::iterator attIter;
           for(attIter = oldAttributes.begin(); attIter != oldAttributes.end(); attIter++)
           {
               char* fullAttributeName = new char[30];
               fullAttributeName[0] = '\0';
               strcat(fullAttributeName, newName);
               strcat(fullAttributeName, ".");
               
               char* attributeName = (char*) attIter->first;
               strcat(fullAttributeName, attributeName);
               int numOfDuplicates = (int) attIter->second;
               //cout<<"old attr = "<<attributeName<<endl;
               //cout<<"new attr = "<<fullAttributeName<<endl;
               //cout<<"num of Dup = "<<numOfDuplicates<<endl;
               newAttributes[fullAttributeName] = numOfDuplicates;
           }
           
           newRelation->SetAttributes(newAttributes);
           vector<const char*> oldVector = oldRelation->GetJoinedRelations();

           vector<const char*>::iterator iter;
           vector<const char*> newVector;
           //cout<<"New join vector = "<<endl;
           for(iter = oldVector.begin(); iter != oldVector.end(); iter++)
           {
               if(0 == strcmp(*iter, oldName))
               {
                   //cout<<newName<<endl;
                   newVector.push_back(newName);
               }
               else
               {
                   //cout<<*iter<<endl;
                   newVector.push_back(*iter);
               }
           }

           newRelation->SetJoinedRelations(newVector);
           
           relations[newName] = newRelation;
           
           // const char* oldJoinedRelation = joinedRelations[oldName];
           //cout<<"New join string = "<<GetJoinStringFromVector(newVector)<<endl;
           joinedRelations[newName] = GetJoinStringFromVector(newVector);
       
        }
        
    }
}

void Statistics :: Write(char* fromWhere)
{
    if(NULL != fromWhere)
    {
        fstream fileObj;
        fileObj.open (fromWhere, fstream::out);

        RelationMap::iterator iter;
        for(iter = relations.begin(); iter != relations.end(); iter++)
        {
            char* relName = (char*) iter->first;
            
            Relation* relation = (Relation*) iter->second;
            fileObj<<relName<<":"<<relation->GetNumberOfTuples()<<endl;
            AttributeMap atts = relation->GetAttributes();
            AttributeMap::iterator attIter;
            for(attIter = atts.begin(); attIter != atts.end(); attIter++)
            {
                fileObj<<attIter->first<<":"<<attIter->second<<endl;
            }
            fileObj<<ATTRIBUTE_END_MARKER<<endl;
            JoinMap::iterator joinIter;
            for(joinIter = joinedRelations.begin(); joinIter != joinedRelations.end(); joinIter++)
            {
                fileObj<<joinIter->first<<":"<<joinIter->second<<endl;
            }
            fileObj<<RELATION_END_MARKER<<endl;

        }
            
        fileObj.close();
    }
}

vector<const char*> Statistics :: GetVectorFromJoinString(const char* joinString)
{
    vector<const char*> returnVector;
    if(NULL != joinString)
    {
        char* temp = new char[20];
        int j = 0;
        for(int i = 0; joinString[i] != '\0'; i++)
        {
            if(JOIN_SEPERATOR == joinString[i])
            {
                temp[j] = '\0';
                returnVector.push_back(temp);
                temp = new char[20];
                j = 0;
            }
            else
            {
                temp[j++] = joinString[i];
            }
        }
        temp[j] = '\0';
        returnVector.push_back(temp);
        
    }

    return returnVector;
}

const char* Statistics :: GetJoinStringFromVector(vector<const char*> joinVector)
{
    char* joinString = new char[60];
    joinString[0] = '\0';
    vector<const char*>::iterator iter;
    int i;
    for(i = 0, iter = joinVector.begin(); iter != joinVector.end(); i++, iter++)
    {
         if(0 < i)
         {
             char* joinSep = new char[2];
             joinSep[0] = JOIN_SEPERATOR;
             joinSep[1] - '\0';
             strcat(joinString, joinSep);
            // joinString.append(1, JOIN_SEPERATOR);
         }
         strcat(joinString, *iter);
         // joinString.append(*iter);
         
    }
    return joinString;//.data();
}

void Statistics :: Read(char* fromWhere)
{
    if(NULL != fromWhere)
    {
        string line;
        fstream fileObj (fromWhere, fstream::in);
        if(0 != fileObj.is_open())
        {
            int isRelation = TRUE;
            int isNew = TRUE;
            int isAttributeEndEncountered = FALSE;
            int shouldJoinBeRecorded = FALSE;
            relations.clear();
            Relation* newRelation = NULL;
            AttributeMap newAttributes;
            
            vector<const char*> newJoinRelationsVector;
            char* relationName = new char[50];
            char* attributeName = new char[50];
            char* joinRelName = new char[50];
            while(!fileObj.eof())
            {
                getline(fileObj, line);
                
                string::iterator strIter = line.begin();
                char* temp = new char[50];
                int i = 0;
                
                
                if(TRUE == isRelation)
                {
                    newAttributes.clear();
                    isNew = FALSE;
                }
                else
                {
                    attributeName = new char[50];
                }
                while(strIter != line.end())
                {
                    char ch = *strIter;
                    if(RELATION_END_MARKER == ch)
                    {
                        
                        newRelation->SetAttributes(newAttributes);
                        newRelation->SetJoinedRelations(newJoinRelationsVector);
                        relations[relationName] = newRelation;
                        
                        isRelation = TRUE;
                        isAttributeEndEncountered = FALSE;
                        shouldJoinBeRecorded = FALSE;
                        isNew = TRUE;
                        strIter++;
                        break;
                    }
                    else if(ATTRIBUTE_END_MARKER == ch)
                    {
                        isAttributeEndEncountered = TRUE;
                        break;
                    }
                    
                    
                    if(':' == ch)
                    {
                        temp[i] = '\0';
                        if(TRUE == isRelation)
                        {
                            newRelation = new Relation();
                            
                            relationName = temp;
                        }
                        else
                        {
                            if(TRUE == isAttributeEndEncountered)
                            {
                                joinRelName = temp;
                            }
                            else
                            {
                                attributeName = temp;
                            }
                        }

                        i = 0;
                        temp = new char[50];
                    }
                    else
                    {
                        temp[i++] = ch;
                    }

                    strIter++;
                }

                if(FALSE == isNew)
                {
                    temp[i] = '\0';
                    
                    if(TRUE == isRelation)
                    {
                        int numOfTuples = atoi(temp);
                        if(0 < numOfTuples)
                        {
                            newRelation->SetNumberOfTuples(numOfTuples);
                            isRelation = FALSE;
                        }
                    }
                    else
                    {

                        if(TRUE == isAttributeEndEncountered)
                        {
                            if(FALSE == shouldJoinBeRecorded)
                            {
                                shouldJoinBeRecorded = TRUE;
                            }
                            else
                            {                    
                                //cout<<joinRelName<<endl;
                                joinedRelations[joinRelName] = temp;
                                joinRelName = new char[50];
                                newJoinRelationsVector = GetVectorFromJoinString(temp);
                                
                            }
                        }
                        int numOfDistincts = atoi(temp);
                        if(0 != numOfDistincts)
                        {
                            newAttributes[attributeName] = numOfDistincts;
                        }
                        

                    }
                }
            }

            fileObj.close();
        }
    }
}

// hash_map<const char*, Relation*, hash<const char*>, KeyComparison> Statistics :: GetRelations()
RelationMap Statistics :: GetRelations()
{
    return relations;
}

Statistics :: Statistics()
{
    
}

Statistics :: ~Statistics()
{
    
}

Statistics :: Statistics(Statistics& copyMe)
{
    this->relations.clear();
    RelationMap relationFromCopyMe = copyMe.GetRelations();
    RelationMap::iterator iter;
        for(iter = relationFromCopyMe.begin(); iter != relationFromCopyMe.end(); iter++)
        {
            char* relName = (char*) iter->first;
            Relation* relObj = (Relation*) iter->second;
            Relation* newRelation = new Relation();

            newRelation->SetNumberOfTuples(relObj->GetNumberOfTuples());
            AttributeMap copyMeAttributes = relObj->GetAttributes();

            AttributeMap::iterator attrIter;

            for(attrIter = copyMeAttributes.begin(); attrIter != copyMeAttributes.end(); attrIter++)
            {
                newRelation->SetNumberOfDistincts((char*)attrIter->first, (int)attrIter->second);
            }

            this->relations[relName] = newRelation;

        }
}

void Statistics :: Apply(AndList* parseTree, char** relNames, int numToJoin)
{
    // Things to check for:

    // Relations are valid
    // Attributes are valid

    vector<const char*> attributesFromRelations;
    AttributeMap finalAttributeMap;
    vector<const char*> newRelNames;
    vector<const char*> originalRelNames;

    for(int i = 0; i < numToJoin; i++)
    {
        originalRelNames.push_back(relNames[i]);
        //cout<<"orig = "<<relNames[i]<<endl;
    }

    JoinMap::iterator itt;
    for(itt = joinedRelations.begin(); itt != joinedRelations.end(); itt++)
    {
        //cout<<"join"<<endl;
        //cout<<itt->first<<endl;
        //cout<<itt->second<<endl;
    }

    newRelNames.clear();
    int isSelect;
    for(int i = 0; i < numToJoin; i++)
    {
        char* relationName = relNames[i];
        // cout<<"at start = "<<relationName<<endl;
        if(NULL != relationName)
        {
            const char* relationsInJoin = (joinedRelations.find(relNames[i]))->second;//  [relNames[i]];
            //cout<<"at start join = "<<relationsInJoin<<endl;
            if(NULL != relationsInJoin)
            {
                Relation* relation = relations[relationsInJoin];
                if(NULL != relation)
                {
                    
                    vector<const char*> joinedRelations = relation->GetJoinedRelations();
                    vector<const char*>::iterator jIter;
                    for(jIter = joinedRelations.begin(); jIter != joinedRelations.end(); jIter++)
                    {
                        //cout<<"In vector = "<<*jIter<<endl;
                        int isPresent = FALSE;
                        for(int j = 0; j < numToJoin; j++)
                        {
                            if(NULL != relNames[j])
                            {
                                if(0 == strcmp(*jIter, relNames[j]))
                                {
                                    relNames[j] = NULL;
                                    isPresent = TRUE;
                                    break;
                                }
                            }

                        }

                        if(FALSE == isPresent)
                        {
                            cout<<"Not present = "<<*jIter<<endl;
                            cout<<"Invalid Relation Names in Apply"<<endl;
                            exit(1);
                        }
                    }
                    //cout<<"Pushing "<<relationsInJoin<<endl;
                    newRelNames.push_back(relationsInJoin);
                }
            }
        }
    }

    if(1 == newRelNames.size())
    {
        isSelect = TRUE;
    }
    else
    {
        isSelect = FALSE;
    }

    vector<const char*>::iterator newRelIter;

    for(newRelIter = newRelNames.begin(); newRelIter != newRelNames.end(); newRelIter++)
    {
        //cout<<"In New rel = "<<*newRelIter<<endl;
    }
    for(newRelIter = newRelNames.begin(); newRelIter != newRelNames.end(); newRelIter++)
    {
        //cout<<"rel = "<<*newRelIter<<endl;
        // char* relationName = (char*) *newRelIter;
        Relation* relation = relations[*newRelIter];
        //cout<<"rel = "<<*newRelIter<<endl;
        AttributeMap relationAttributes = relation->GetAttributes();
        //cout<<"rel = "<<*newRelIter<<endl;
        AttributeMap::iterator attrIter;
        
        for(attrIter = relationAttributes.begin(); attrIter != relationAttributes.end(); attrIter++)
        {
            //cout<<"attr = "<<attrIter->first<<endl;
            //cout<<"num = "<<attrIter->second<<endl;
            attributesFromRelations.push_back(attrIter->first);
            finalAttributeMap[attrIter->first] = attrIter->second;
        }
        
    }

    int isNewRelationRequired = FALSE;
    double estimatedNumberOfTuples;
    int k;
    vector<const char*>::iterator iter;
    for(k = 0, iter = originalRelNames.begin(); iter != originalRelNames.end(); iter++, k++)
         {
             relNames[k] = (char*) *iter;
             //cout<<"end = "<<relNames[k]<<endl;
         }

    AndList* tempParseTree = parseTree;
    if(NULL != parseTree)
    {
        //cout<<"Attributes Provided"<<endl;
        vector<const char*> attributeList;
        // get all the attributes from the AndList
        // check if all the attributes belong to the relations
        
        while(NULL != parseTree)
        {
            OrList* left = parseTree->left;
            if(NULL != left)
            {
                do
                {
                    ComparisonOp* operands = left->left;
                    Operand* leftOperand = operands->left;
                    Operand* rightOperand = operands->right;
                    // get the values
                    // operands->code
                    if(ATTRIBUTE_CODE == leftOperand->code)
                    {
                        attributeList.push_back(leftOperand->value);
                    }

                    if(ATTRIBUTE_CODE == rightOperand->code)
                    {
                        attributeList.push_back(rightOperand->value);
                    }

                    left = left->rightOr;
                }while(NULL != left);

            }

            parseTree = parseTree->rightAnd;
        }


        // check for occurrence of attribute

        vector<const char*>::iterator attrIter;

        for(attrIter = attributeList.begin(); attrIter != attributeList.end(); attrIter++)
        {
            int isAttributePresent = FALSE;

            const char* attributeInList = *attrIter;
            //cout<<"Checking for "<<attributeInList<<endl;

            int numOfDist = finalAttributeMap[attributeInList];
            //cout<<numOfDist<<endl;
            if(0 < numOfDist)
            {
                isAttributePresent = TRUE;
            }

            if(FALSE == isAttributePresent)
            {
                AttributeMap::iterator testingIter;
                for(testingIter = finalAttributeMap.begin(); testingIter != finalAttributeMap.end(); testingIter++)
                {
                    //cout<<"attr in check rel = "<<testingIter->first<<endl;
                }

                cout<<*attrIter<<endl;
                cout<<"Attribute not in any of the relations in relNames"<<endl;
                exit(1);
            }
        }

        // call estimate
        // get the number of tuples
        if(NULL == tempParseTree)
        {
            //cout<<"Parse tree NULL _------------------------------"<<endl;
        }
        estimatedNumberOfTuples = this->Estimate(tempParseTree, relNames, numToJoin);
        

        isNewRelationRequired = TRUE;
    }
    else
    {
        //cout<<"No attributes provided"<<endl;
        // If the attributes are null, then perform cartesian product
        // cartesian product if not a select
        if(FALSE == isSelect)
        {
            // join, perform cartesian product
            // call estimate
            estimatedNumberOfTuples = this->Estimate(tempParseTree, relNames, numToJoin);
            isNewRelationRequired = TRUE;
        }
        else
        {
            // select, no cartesian product required
            isNewRelationRequired = FALSE;
        }
    }

    if(TRUE == isNewRelationRequired)
    {
        // Create a new Relation
        char* newRelationName = new char[50];
        newRelationName[0] = '\0';
        
         vector<const char*>::iterator iter;
         int i = 0;
         for(iter = newRelNames.begin(), i = 0; iter != newRelNames.end(); iter++, i++)
         {

            if(0 < i)
            {
                char* joinSep = new char[2];
                joinSep[0] = JOIN_SEPERATOR;
                joinSep[1] = '\0';
                strcat(newRelationName, joinSep);
                // newRelationName.append(1, JOIN_SEPERATOR);
            }

            strcat(newRelationName, *iter);
            // newRelationName.append(*iter);
            //cout<<"Erasing "<<*iter<<endl;
            relations.erase(*iter);

         }

         
         for(iter = originalRelNames.begin(); iter != originalRelNames.end(); iter++)
         {
            joinedRelations[*iter] = newRelationName;//.data();
            //cout<<"Entered joined relation "<<*iter<<","<<newRelationName<<endl;
         }


         if(0 < estimatedNumberOfTuples)
         {
            Relation* newRelation = new Relation();
            //cout<<"Estimate========================="<<estimatedNumberOfTuples<<endl;
            newRelation->SetNumberOfTuples(estimatedNumberOfTuples);
            //cout<<"New Relation****************"<<endl;
            //cout<<newRelationName<<endl;
            
            newRelation->SetAttributes(finalAttributeMap);
            newRelation->SetJoinedRelations(originalRelNames);
            //cout<<"New Relation****************"<<endl;
            //cout<<"new rel = "<<newRelationName<<endl;
            relations[newRelationName] = newRelation;

        }
         
         

    }

}

double Statistics :: Estimate(struct AndList *parseTree, char** relName,  int numToJoin)
{
    double numTuples=0.0;
    int iter=0;
    double product=1.0;
    int included=0;
    char c;
    vector<const char*> joined_relName;
    if(numToJoin == 0);
    else if(numToJoin > 0)
    {
        //delete &joined_relName;
        //joined_relName = *(new vector<char*>);
        iter=0;
        for(iter=0;iter<numToJoin;iter++)
        {
            //prints out the relations in the relName
            //cout<<"relation name and iter : " << relName[iter]<< iter<<endl;
        }
        iter=0;
                //cout<<"abt to enter while "<<iter<<endl;
        while(iter<numToJoin)
        {
            char c;
                //cout<<"relName of iter is "<<relName[iter]<<endl;
                //cin>>c;
                const char* temp = joinedRelations[(relName[iter])];

            //cout<<"inside while "<<iter<<numToJoin<<endl;
            included=0;
            for(int i=0;i<joined_relName.size();i++)
            {
                if(strcmp(temp, joined_relName.at(i))==0)
                {
                    //cout<<"found the relation in the temp vector "<<relName[iter]<<temp<<endl;
                    //cin>>c;
                    included=1;
                    break;
                }
            }
            if(included==1);
            else
            {
                
                //cout<<"joined relations has for supplier "<<joinedRelations["supplier"]<<endl;
                
                //cout<<"in else of while found temp as "<<temp<<iter<<endl;
                joined_relName.push_back(temp);
                //cout<<"relName is "<<temp<<endl;
                // cin>>c;
                //cout<<"product is multiplied by "<<relations[temp]->GetNumberOfTuples()<<endl;
                //cin>>c;
                product = product * (relations[temp]->GetNumberOfTuples());
            }
            iter++;
        }
        //cout<<"product is "<<product<<endl;
        double numSelect = ComputeAndList(parseTree, relName, numToJoin);
        numTuples = product*numSelect;
    }
    return(numTuples);
}

Relation* Statistics :: CalculateOperand(struct Operand *pOp, char** relName, int numToJoin)
{
        int iter=0;
        char c;
                while(iter<numToJoin)
                {
                    JoinMap::iterator iterate_appliedRel = joinedRelations.find(relName[iter]);
                    if(iterate_appliedRel!=joinedRelations.end())
                    {
                        Relation* tempRelation = relations[iterate_appliedRel->second];

                        if(tempRelation!=NULL)
                        {
                            AttributeMap::iterator iterate = (tempRelation->GetAttributes()).find(pOp->value);

                            if(iterate==(tempRelation->GetAttributes()).end())
                            {
                               iter++;
                            }
                            else
                            {
                                //numDist = iterate->second;
                                return(tempRelation);
                            }
                        }
                        else
                        {
                            cout<<"Could not find the relation in the relations map"<<endl;
                            return(NULL);
                        }
                    }
                    else
                    {
                        cout<<"Could not find the relation in the joinedrelations map"<<endl;
                        return(NULL);
                    }
                }
}

double Statistics :: CalculateComparisonOp(struct ComparisonOp *pCom, char** relName, int numToJoin)
{
    Relation* rel_OperandLeft = new Relation();
    Relation* rel_OperandRight = new Relation();
    char c;
    double returnValue;
    int numDistLeft, numDistRight;
        if(pCom!=NULL)
        {
            if(pCom->left->code == 4)
            {
                rel_OperandLeft = CalculateOperand(pCom->left, relName, numToJoin);
                if(rel_OperandLeft==NULL)
                {
                    cout<<"Could not find the attribute in the statistics object"<<endl;
                    return(-1);
                }
                else
                {
//                    if(strcmp(pCom->left->value,"l_shipmode")==0)
//                    {
//                        cout<<"l_shipmode"<<endl;
//                        cout<<rel_OperandLeft<<endl;
//                        cout<<rel_OperandLeft->GetNumberOfTuples()<<endl;
//                        cin>>c;
//                    }

                    AttributeMap attrMap = rel_OperandLeft->GetAttributes();
                     numDistLeft = attrMap[pCom->left->value];
                     /*AttributeMap::iterator it= attrMap.find(pCom->left->value);

                     //cout<<" in compute"<<pCom->left->value<<" = "<<rel_OperandLeft->GetNumberOfDistincts(pCom->left->value)<<endl;

                     if(it != attrMap.end())
                     {
                         //cout<<"Assigning numDistLeft"<<endl;
                        numDistLeft = it->second;

                        // cout<<it->first<<" = "<<numDistLeft<<endl;
                        //numDistLeft = rel_OperandLeft->GetNumberOfDistincts(pCom->left->value);
                     }*/
                     if(strcmp(pCom->left->value,"l_shipmode")==0)
                    {
                        //cout<<rel_OperandLeft<<endl;
                        //cout<<rel_OperandLeft->GetNumberOfTuples()<<endl;
                        //cout<<numDistLeft;
                        // cin>>c;
                    }
                }
            }
            else
            {
                numDistLeft=-2;
            }
            if(pCom->right->code == 4)
            {
                rel_OperandRight = CalculateOperand(pCom->right, relName, numToJoin);
                if(rel_OperandRight==NULL)
                {
                    cout<<"Could not find the attribute in the statistics object"<<endl;
                    return(-1);
                }
                else
                {
                    numDistRight = rel_OperandRight->GetNumberOfDistincts(pCom->right->value);
                }
            }
            else
            {
                numDistRight=-2;
            }
            if(pCom->code==1||pCom->code==2)
            {
                //cout<<pCom->left->value<<" : "<<numDistLeft<<endl;
                //cout<<pCom->right->value<<" : "<<numDistRight<<endl;
                //The condition is greater than or less than
                if(numDistLeft>=0 && numDistRight>=0)
                {
                    if(numDistLeft>0 && numDistRight>0)
                    {
                        returnValue=1.0/3.0;
                        //cout<<"returns 012 "<<returnValue<<endl;
                        return(returnValue);
                    }
                    else
                    {
                       returnValue=0.0;
                        //cout<<"returns 123 "<<returnValue<<endl;
                        return(returnValue);
                    }
                }
                else if(numDistLeft>=0)
                {
                    returnValue=1.0/3.0;
                        //cout<<"returns 345 "<<returnValue<<endl;
                        return(returnValue);
                }
                else if(numDistRight>=0)
                {
                    returnValue=1.0/3.0;
                        //cout<<"returns 456 "<<returnValue<<endl;
                        return(returnValue);
                }
                else
                {
                    cout<<"Cannot compare two constants for a condition"<<endl;
                }
            }
            else
            {
                //cout<<pCom->left->value<<" : "<<numDistLeft<<endl;
                //cout<<pCom->right->value<<" : "<<numDistRight<<endl;
                // cin>>c;
                 //The condition is equality
                 if(numDistLeft>=0 && numDistRight>=0)
                {
                    if(numDistLeft>0 && numDistRight>0)
                    {
                        if(numDistLeft>numDistRight)
                        {
                            returnValue=1.0/(double)numDistLeft;
                        //cout<<"returns 1234 "<<returnValue<<endl;
                        return(returnValue);
                        }
                        else
                        {
                            returnValue=1.0/(double)numDistRight;
                        //cout<<"returns 2345 "<<returnValue<<endl;
                        return(returnValue);
                        }
                    }
                    else
                        return(0);
                }
                else if(numDistLeft>=0)
                {
                    returnValue=1.0/(double)numDistLeft;
                        //cout<<"returns 3456 "<<returnValue<<endl;
                        return(returnValue);
                }
                else if(numDistRight>=0)
                {
                    returnValue=1.0/(double)numDistRight;
                        //cout<<"returns 4567 "<<returnValue<<endl;
                        return(returnValue);
                }
                else
                {
                    cout<<"Cannot compare two constants for a condition"<<endl;
                }
            }
        }
        else
        {
                return 1.0;
        }
}
double Statistics :: ComputeOrList(struct OrList *pOr, char** relName, int numToJoin)
{
        struct ComparisonOp *pCom;
        double numTuples=0.0;
        double temp=0.0;
        double returnValue;
        //cout<<"Compute Or Called and pOr is "<<pOr<<" jus to let u knw"<<endl;
        if(pOr !=NULL)
        {
                pCom = pOr->left;
                numTuples = CalculateComparisonOp(pCom, relName, numToJoin);
                //cout<<"Or gets numTuples as"<<numTuples<<endl;
                if(pOr->rightOr!=NULL)
                {
                        temp = ComputeOrList(pOr->rightOr, relName, numToJoin);
                        //cout<<"Or gets temp as"<<temp<<endl;
                }
                returnValue = (1.0-((1.0-numTuples)*(1.0-temp)));
                //cout<<"Or returns "<<returnValue<<endl;
                return(returnValue);
        }
        else
        {
                return 0.0;
        }
}
double Statistics :: ComputeAndList(struct AndList *pAnd, char** relName, int numToJoin)
{
    struct OrList *pOr;
    double numTuples=1.0;
    double temp=1.0;
    double returnValue;
        if(pAnd !=NULL)
        {
                pOr = pAnd->left;
                numTuples = ComputeOrList(pOr, relName, numToJoin);
                //cout<<"And gets numTuples as"<<numTuples<<endl;
                if(pAnd->rightAnd)
                {
                    temp = ComputeAndList(pAnd->rightAnd, relName, numToJoin);
                    //cout<<"And gets temp as"<<temp<<endl;
                }
                returnValue = numTuples*temp;
                //cout<<"And returns "<<returnValue<<endl;
                return(returnValue);
        }
        else
        {
                return 1.0;
        }
}