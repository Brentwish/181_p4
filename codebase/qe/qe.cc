
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
