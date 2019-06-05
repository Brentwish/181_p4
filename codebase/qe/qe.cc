
#include "qe.h"

//Here we're binding the input iterator that's passed in to Iterator iter
//as well as the Condition struct
//https://en.cppreference.com/w/cpp/language/reference_initialization
Filter::Filter(Iterator* input, const Condition &condition) : iter(input), cond(condition) {
    //we created a private Attribute vector to hold the attrs associated with
    //the table or index input
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

int Filter::include(void *data) {
    RelationManager *rm = RelationManager::instance();
    vector<Attribute> attrs;
    rm->getAttributes("left", attrs);
    rm->printTuple(attrs, data);

    return 0;
}


//void Filter::getAttributes(vector<Attribute> &attrs) const {
//
//}

// ... the rest of your implementations go here
