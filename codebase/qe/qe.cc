
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
}

// ... the rest of your implementations go here
INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        )
{
    this->leftRelation = leftIn;
    this->rightIndex = rightIn;
    this->joinCond = condition; // create the copies

    // need to store the attributes that we will return for getAttributes
    vector<Attribute> indexAttr;
    vector<Attribute> leftAttr; 
    leftRelation->getAttributes(returnAttr);
    leftRelation->getAttributes(leftAttr);
    rightIndex->getAttributes(indexAttr);
    // join vector to vector 
    returnAttr.insert(std::end(returnAttr), std::begin(indexAttr), std::end(indexAttr));
    leftTuplePage = NULL;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
    attrs = returnAttr;
}

RC INLJoin::getNextTuple(void *data) 
{
    // foreach tuple r in R do
    //      foreach tuple s in S where ri == sj do 
    //          add <r,s> to result
    RC rc;
    void *tableData = malloc(PAGE_SIZE);
    // get the tuple r in R
    if (leftTuplePage == NULL) { // means there hasn't been a r tuple seen before
        // create a page to hold the leftTuple that will get s tuples appended to it
        leftTuplePage = malloc(PAGE_SIZE); 
        memset(leftTuplePage, 0, PAGE_SIZE);
        rc = leftRelation->getNextTuple(tableData); // get first tuple
        if (rc) {// if end then no tuples at all 
            free(leftTuplePage);
            free(tableData);
            return rc;
        }

        // fill the leftTuplePage with the tuples from R
        fillLeftTuples(tableData, leftTuplePage);
    }
    // need someplace to hold the data returned by the index scan
    void *indexData = malloc(PAGE_SIZE);
    memset(indexData, 0, PAGE_SIZE);

    // get the tuple s in S
    rc = rightIndex->getNextTuple(indexData);
    // return and move down the left relation since its every s for r
    if (rc == QE_EOF) { // the scan has reached its end 
        free(indexData);
        // use the method from the header to restart scan
        rightIndex->setIterator(NULL,NULL,true,true); // create an iterator without any restrictions
        // move the left column down one
        rc = leftRelation->getNextTuple(tableData);
        if (rc) {// if at end
            free(tableData); // free memory
            return rc; // return the EOF code
        }
        memset(leftTuplePage, 0, PAGE_SIZE); // reset the tuplesPage
        // fill the leftTuplePage with the tuples from R
        fillLeftTuples(tableData, leftTuplePage);
    }
}

// memory work gonna need RBFM
void INLJoin::fillLeftTuples(void* leftTuple, void* finalTuple) 
{
    RelationManager *rm = RelationManager::instance();
    // fill the final tuple with the left one 
    // need to have null indicator for total number of fields 
    // get the null indicator vector same as before
    int nullIndicatorSize = getNullIndicatorSize(returnAttr.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator,(char*) leftTuple, nullIndicatorSize); // got the null indicators


    map<string, IndexData> dataList;
    vector<IndexData> atrList; // get all the tuples like in rm
    vector<Attribute> leftAtr;
    leftRelation->getAttributes(leftAtr); // get the left attributes 
    rm->formatData(leftAtr, leftTuple, atrList); // get the atrributes into list form
    // fill into the map same as before
    for (IndexData id: atrList) {
        dataList[id.atr.name] = id;
    }
    // for each attribute append it to the finalTuple
    int i=0;
    for (i =0; i < returnAttr.size(); i++) 
    {
        // get the name of the attr and then type
        
    }

}           

// Calculate actual bytes for nulls-indicator for the given field counts
int INLJoin::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}