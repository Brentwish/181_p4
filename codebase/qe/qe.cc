
#include "qe.h"
#include <cstring>

// Calculate actual bytes for nulls-indicator for the given field counts
int getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

//Here we're binding the input iterator that's passed in to Iterator iter
//as well as the Condition
//https://en.cppreference.com/w/cpp/language/reference_initialization
Filter::Filter(Iterator* input, const Condition &condition) : iter(input), cond(condition) {
    //we created a private Attribute vector to hold the attrs associated with
    //the table or index input. use input's getAttributes to populate it
    input->getAttributes(attrs);
}

RC Filter::getNextTuple(void *data) {
    //Use a loop to pass over elements we don't want to include in our filter
    //while getNextTuple isn't EOF or test *data against the comparator
    while (true) {
        RC rc = iter->getNextTuple(data);
        //Check for end of file
        if (rc == QE_EOF)
            return QE_EOF;
        //If the tuple passes the check, return
        if (include(data))
            return SUCCESS;
    }
    return -1;
}

int compareInts(CompOp op, int left, int right) {
    switch (op) {
        case EQ_OP: return left == right;
        case LT_OP: return left < right;
        case GT_OP: return left > right;
        case LE_OP: return left <= right;
        case GE_OP: return left >= right;
        case NE_OP: return left != right;
        case NO_OP: return 1;
        default: return -1;
    }
}

int compareReals(CompOp op, float left, float right) {
    switch (op) {
        case EQ_OP: return left == right;
        case LT_OP: return left < right;
        case GT_OP: return left > right;
        case LE_OP: return left <= right;
        case GE_OP: return left >= right;
        case NE_OP: return left != right;
        case NO_OP: return 1;
        default: return -1;
    }
}

int compareVarChars(CompOp op, char *left, char *right) {
    switch (op) {
        case EQ_OP: return (strcmp(left, right) == 0) ? 1 : 0;
        case LT_OP: return (strcmp(left, right) < 0) ? 1 : 0;
        case GT_OP: return (strcmp(left, right) > 0) ? 1 : 0;
        case LE_OP: return (strcmp(left, right) <= 0) ? 1 : 0;
        case GE_OP: return (strcmp(left, right) >= 0) ? 1 : 0;
        case NE_OP: return (strcmp(left, right) != 0) ? 1 : 0;
        case NO_OP: return 1;
        default: return -1;
    }
}

int Filter::include(void *data) {
    //We need to iterate through (data, attrs) until we find the attr
    //we are comparing on
    int nullSize = getNullIndicatorSize(attrs.size());
    char nullField[nullSize];
    memset(nullField, 0, nullSize);
    memcpy(nullField, data, nullSize);

    //we'll keep a running total for the offset into data
    int offset = nullSize;

    Attribute attr;
    for (unsigned int i = 0; i < attrs.size(); i++) {
        attr = attrs[i];

        if (fieldIsNull(nullField, i)) //data holds nothing
            continue;

        //Once we find the attr we care about, break
        if (attr.name == cond.lhsAttr)
            break;

        switch (attr.type) {
            case TypeInt: offset += INT_SIZE;
                break;
            case TypeReal: offset += REAL_SIZE;
                break;
            case TypeVarChar:
                int size = 0;
                memcpy(&size, (char *)data + offset, INT_SIZE);
                offset += INT_SIZE + size;
                break;
        }
    }

    //Copy the attribute into val
    Value val;
    val.type = attr.type;
    val.data = (char *)data + offset;

    switch (val.type) {
        case TypeInt: {
            int leftIval = 0;
            int rightIval = 0;
            memcpy(&leftIval, val.data, INT_SIZE);
            memcpy(&rightIval, cond.rhsValue.data, INT_SIZE);

            return compareInts(cond.op, leftIval, rightIval);
        }
        case TypeReal: {
            float leftFval = 0;
            float rightFval = 0;
            memcpy(&leftFval, val.data, REAL_SIZE);
            memcpy(&rightFval, cond.rhsValue.data, REAL_SIZE);

            return compareReals(cond.op, leftFval, rightFval);
        }
        case TypeVarChar: {
            int leftSize = 0;
            int rightSize = 0;
            memcpy(&leftSize, val.data, VARCHAR_LENGTH_SIZE);
            memcpy(&rightSize, cond.rhsValue.data, VARCHAR_LENGTH_SIZE);

            char leftSval[leftSize + 1];
            char rightSval[rightSize + 1];

            memset(leftSval, 0, leftSize + 1);
            memset(rightSval, 0, rightSize + 1);
            memcpy(leftSval, (char *) val.data + VARCHAR_LENGTH_SIZE, leftSize);
            memcpy(rightSval, (char *) cond.rhsValue.data + VARCHAR_LENGTH_SIZE, rightSize);

            return compareVarChars(cond.op, leftSval, rightSval);
        }
    }
    return -1;
}

//void Filter::getAttributes(vector<Attribute> &attrs) const {
//
//}

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