
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
    tableData = malloc(PAGE_SIZE);
    indexData = malloc(PAGE_SIZE);
    leftOffset = 0;
}

INLJoin::~INLJoin()
{
    free(leftTuplePage);
    free(tableData);
    free(indexData);
    leftTuplePage = NULL;
    returnAttr.clear();
    leftOffset = 0;
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
    RelationManager *rm = RelationManager::instance();
    RC rc;
    // void *tableData = malloc(PAGE_SIZE); // need to store in the object because it'll be reset everytime

    // get the tuple r in R
    if (leftTuplePage == NULL) { // means there hasn't been a r tuple seen before
        // create a page to hold the leftTuple that will get s tuples appended to it
        leftTuplePage = malloc(PAGE_SIZE); 
        memset(leftTuplePage, 0, PAGE_SIZE);
        rc = leftRelation->getNextTuple(tableData); // get first tuple
        if (rc) {// if end then no tuples at all 
            // free(leftTuplePage); // first time so free this too but not other times 
            return rc;
        }

        // fill the leftTuplePage with the tuples from R
        this->leftOffset = fillLeftTuples(tableData, leftTuplePage); // need to store the offset returned 
    }
    // need someplace to hold the data returned by the index scan
    // void *indexData = malloc(PAGE_SIZE); // trying so maybe memory errors go away?
    memset(indexData, 0, PAGE_SIZE);

    // get the tuple s in S
    rc = rightIndex->getNextTuple(indexData);
    // return and move down the left relation since its every s for r
    if (rc == QE_EOF) { // the scan has reached its end 
        // free(indexData);
        // use the method from the header to restart scan
        rightIndex->setIterator(NULL,NULL,true,true); // create an iterator without any restrictions
        // move the left column down one
        rc = leftRelation->getNextTuple(tableData);
        if (rc) {// if at end
            return rc; // return the EOF code
        }
        memset(leftTuplePage, 0, PAGE_SIZE); // reset the tuplesPage
        // fill the leftTuplePage with the tuples from R
        this->leftOffset = fillLeftTuples(tableData, leftTuplePage);
        rc = rightIndex->getNextTuple(indexData); // get the first tuple for the comparison forward
        if (rc) 
            return rc;
    }
    else if (rc != SUCCESS) {
        return rc;
    }

    // got the s tuple 
    // compare the the s and r attributes based on condition
    // condition has the attribute name for left and right relation

    // get the right hand attributes 
    map<string, IndexData> indexMap;
    vector<IndexData> indexList;
    vector<Attribute> indexAtr; 
    rightIndex->getAttributes(indexAtr); // get the index attributes 
    rm->formatData(indexAtr, indexData, indexList); // get the atrributes into list form
    for (IndexData id : indexList) { // fill the map for easier lookup 
        indexMap[id.atr.name] = id;
    }
    map<string, IndexData> leftMap;
    vector<IndexData> leftList;
    vector<Attribute> leftAtr; 
    leftRelation->getAttributes(leftAtr); // get the index attributes 
    rm->formatData(leftAtr, tableData, leftList); // get the atrributes into list form
    for (IndexData id : leftList) { // fill the map for easier lookup 
        leftMap[id.atr.name] = id;
    }

    // compare
    IndexData leftData = leftMap[joinCond.lhsAttr];
    IndexData rightData = indexMap[joinCond.rhsAttr];
    Attribute attr = leftData.atr;
    int compare; 
    switch (attr.type) {
        case TypeInt: {
            int leftIval = 0;
            int rightIval = 0;
            memcpy(&leftIval, leftData.key, INT_SIZE);
            memcpy(&rightIval, rightData.key, INT_SIZE);

            compare = compareInts(joinCond.op, leftIval, rightIval);
            break;
        }
        case TypeReal: {
            float leftFval = 0;
            float rightFval = 0;
            memcpy(&leftFval, leftData.key, REAL_SIZE);
            memcpy(&rightFval, rightData.key, REAL_SIZE);

            compare = compareReals(joinCond.op, leftFval, rightFval);
            break;
        }
        case TypeVarChar: {
            int leftSize = 0;
            int rightSize = 0;
            memcpy(&leftSize, leftData.key, VARCHAR_LENGTH_SIZE);
            memcpy(&rightSize, rightData.key, VARCHAR_LENGTH_SIZE);

            char leftSval[leftSize + 1];
            char rightSval[rightSize + 1];

            memset(leftSval, 0, leftSize + 1);
            memset(rightSval, 0, rightSize + 1);
            memcpy(leftSval, (char *) leftData.key + VARCHAR_LENGTH_SIZE, leftSize);
            memcpy(rightSval, (char *) rightData.key + VARCHAR_LENGTH_SIZE, rightSize);

            compare = compareVarChars(joinCond.op, leftSval, rightSval);
            break;
        }
    }
    // compare 0 means the comparison was false 
    if (compare == 0 ) // not a valid tuple
    {
        return getNextTuple(data); // recursively call this function for the next tuple 
    }
    // found a valid tuple 
    // join the left and right tuples 
    // start inserting at where the last offset returned in leftTuplePage
    int nullIndicatorSize = getNullIndicatorSize(returnAttr.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, leftTuplePage, nullIndicatorSize); // get the nullIndicator
    int startingNullIdx = returnAttr.size() - leftAtr.size(); // where the nullIndicator index should start to account for the left attributes
    int offset = this->leftOffset; // start at the offset
    for (int i =0; i < indexAtr.size(); i++) { // do all the index attributes
        string fName = indexAtr[i].name;
        IndexData id = indexMap[fName]; // get the data for the key 

        if (id.nullKey) {

            // set the bit to null
            // similar to fieldIsNull but OR instead of AND
            int indicatorIndex = (i + startingNullIdx) / CHAR_BIT;
            int indicatorMask  = 1 << (CHAR_BIT - 1 - ((i + startingNullIdx) % CHAR_BIT)); // can use OR to set 
            nullIndicator[indicatorIndex] |= indicatorMask;
            memcpy(leftTuplePage,nullIndicator, nullIndicatorSize); // insert the new null indicator
            continue; // no need to move offset
        }

        if (id.atr.type == TypeInt) {
            memcpy((char*) leftTuplePage + offset, id.key, INT_SIZE);
            offset += INT_SIZE;
        }
        else if (id.atr.type == TypeReal) {
            memcpy((char*) leftTuplePage + offset, id.key, REAL_SIZE);
            offset += REAL_SIZE;
        }
        else if (id.atr.type == TypeVarChar) {
            int varLen;
            memcpy(&varLen, (char*) id.key, VARCHAR_LENGTH_SIZE); // get the size 
            memcpy((char*) leftTuplePage + offset, id.key, VARCHAR_LENGTH_SIZE + varLen); // copy the total key
            offset += VARCHAR_LENGTH_SIZE;
            offset += varLen;
        }
    }
    // copy into the data 
    memcpy(data, leftTuplePage, PAGE_SIZE);
    return SUCCESS;
}

// need RM for IndexData
// returns offset in finalTuple
int INLJoin::fillLeftTuples(void* leftTuple, void* finalTuple) 
{
    RelationManager *rm = RelationManager::instance();
    // fill the final tuple with the left one 
    // need to have null indicator for total number of fields 
    // get the null indicator vector same as before
    int nullIndicatorSize = getNullIndicatorSize(returnAttr.size());
    char nullIndicator[nullIndicatorSize];
    int offset = 0;
    memset(nullIndicator, 0, nullIndicatorSize);
    memset(leftTuple, 0, nullIndicatorSize);
    memcpy(nullIndicator,(char*) leftTuple, nullIndicatorSize); // got the null indicators
    offset += nullIndicatorSize;

    map<string, IndexData> dataList;
    vector<IndexData> atrList; // get all the tuples like in rm
    vector<Attribute> leftAtr;
    leftRelation->getAttributes(leftAtr); // get the left attributes 
    rm->formatData(leftAtr, leftTuple, atrList); // get the atrributes into list form
    // fill into the map same as before
    for (IndexData id: atrList) {
        dataList[id.atr.name] = id;
    }
    // for each left attribute append it to the finalTuple
    // similar to inserting into index
    int i=0;
    for (i =0; i < leftAtr.size(); i++) 
    {
        // get the name of the attr and then type
        string fName = leftAtr[i].name; // get the name
        IndexData id = dataList[fName];

        if (id.nullKey) { // attr is null
            // set the bit to null
            // similar to fieldIsNull but OR instead of AND
            int indicatorIndex = i / CHAR_BIT;
            int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT)); // can use OR to set 
            nullIndicator[indicatorIndex] |= indicatorMask;
            memcpy(nullIndicator,(char*) leftTuple, nullIndicatorSize); // insert the new null indicator
            continue; // no need to move offset 
        }

        if (id.atr.type == TypeInt) {
            memcpy((char*) finalTuple + offset, id.key, INT_SIZE);
            offset += INT_SIZE;
        }
        else if (id.atr.type == TypeReal) {
            memcpy((char*) finalTuple + offset, id.key, REAL_SIZE);
            offset += REAL_SIZE;
        }
        else if (id.atr.type == TypeVarChar) {
            int varLen;
            memcpy(&varLen, (char*) id.key, VARCHAR_LENGTH_SIZE); // get the size 
            memcpy((char*) finalTuple + offset, id.key, VARCHAR_LENGTH_SIZE + varLen); // copy the total key
            offset += VARCHAR_LENGTH_SIZE;
            offset += varLen;
        }
    }
    return offset;
}           

// Calculate actual bytes for nulls-indicator for the given field counts
int INLJoin::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}
