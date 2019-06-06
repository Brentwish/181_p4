
#include "rm.h"

#include <algorithm>
#include <cstring>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
: tableDescriptor(createTableDescriptor()), columnDescriptor(createColumnDescriptor()), indexDescriptor(createIndexDescriptor())
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    // Create both tables and columns tables, return error if either fails
    RC rc;
    rc = rbfm->createFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;
    rc = rbfm->createFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;
    rc = rbfm->createFile(getFileName(INDEXES_TABLE_NAME));

    // Add table entries for both Tables and Columns
    rc = insertTable(TABLES_TABLE_ID, 1, TABLES_TABLE_NAME);
    if (rc)
        return rc;
    rc = insertTable(COLUMNS_TABLE_ID, 1, COLUMNS_TABLE_NAME);
    if (rc)
        return rc;
    // Add the Index table entry
    rc = insertTable(INDEXES_TABLE_ID, 1, INDEXES_TABLE_NAME);
    if (rc) 
        return rc;


    // Add entries for tables and columns to Columns table
    rc = insertColumns(TABLES_TABLE_ID, tableDescriptor);
    if (rc)
        return rc;
    rc = insertColumns(COLUMNS_TABLE_ID, columnDescriptor);
    if (rc)
        return rc;
    rc = insertColumns(INDEXES_TABLE_ID, indexDescriptor);
    if (rc) 
        return rc;
    return SUCCESS;
}

// Just delete the the two catalog files
RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 

    RC rc;

    rc = rbfm->destroyFile(getFileName(TABLES_TABLE_NAME));
    if (rc)
        return rc;

    rc = rbfm->destroyFile(getFileName(COLUMNS_TABLE_NAME));
    if (rc)
        return rc;
    rc = rbfm->destroyFile(getFileName(INDEXES_TABLE_NAME));
    if (rc)
        return rc;

    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 

    // Create the rbfm file to store the table
    if ((rc = rbfm->createFile(getFileName(tableName))))
        return rc;

    // Get the table's ID
    int32_t id;
    rc = getNextTableID(id);
    if (rc)
        return rc;

    // Insert the table into the Tables table (0 means this is not a system table)
    rc = insertTable(id, 0, tableName);
    if (rc)
        return rc;

    // Insert the table's columns into the Columns table
    rc = insertColumns(id, attrs);
    if (rc)
        return rc;

    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    RC rc;

    // If this is a system table, we cannot delete it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Delete the rbfm file holding this table's entries
    rc = rbfm->destroyFile(getFileName(tableName));
    if (rc)
        return rc;

    // Grab the table ID
    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    // Open tables file
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find entry with same table ID
    // Use empty projection because we only care about RID
    RBFM_ScanIterator rbfm_si;
    vector<string> projection; // Empty
    void *value = &id;

    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    RID rid;
    rc = rbfm_si.getNextRecord(rid, NULL);
    if (rc)
        return rc;

    // Delete RID from table and close file
    rbfm->deleteRecord(fileHandle, tableDescriptor, rid);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    // Delete from Columns table
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Find all of the entries whose table-id equal this table's ID
    rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);

    while((rc = rbfm_si.getNextRecord(rid, NULL)) == SUCCESS)
    {
        // Delete each result with the returned RID
        rc = rbfm->deleteRecord(fileHandle, columnDescriptor, rid);
        if (rc)
            return rc;
    }
    if (rc != RBFM_EOF)
        return rc;

    rbfm->closeFile(fileHandle);
    rbfm_si.close();

    // need to delete all the indexes on this table too 
    vector<IndexID> indexList;
    rc = getIndexesForTable(tableName, indexList); // get the indexes associated with this table
    if (rc) 
        return rc;
    
    rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fileHandle); // open the file with all the indexList and their filename
    for(IndexID id : indexList) { // go through each index and destroy it 
        destroyIndex(tableName, id.attrName); 
    }


    return SUCCESS;
}

// Fills the given attribute vector with the recordDescriptor of tableName
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    // Clear out any old values
    attrs.clear();
    RC rc;

    int32_t id;
    rc = getTableID(tableName, id);
    if (rc)
        return rc;

    void *value = &id;

    // We need to get the three values that make up an Attribute: name, type, length
    // We also need the position of each attribute in the row
    RBFM_ScanIterator rbfm_si;
    vector<string> projection;
    projection.push_back(COLUMNS_COL_COLUMN_NAME);
    projection.push_back(COLUMNS_COL_COLUMN_TYPE);
    projection.push_back(COLUMNS_COL_COLUMN_LENGTH);
    projection.push_back(COLUMNS_COL_COLUMN_POSITION);

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Scan through the Column table for all entries whose table-id equals tableName's table id.
    rc = rbfm->scan(fileHandle, columnDescriptor, COLUMNS_COL_TABLE_ID, EQ_OP, value, projection, rbfm_si);
    if (rc)
        return rc;

    RID rid;
    void *data = malloc(COLUMNS_RECORD_DATA_SIZE);

    // IndexedAttr is an attr with a position. The position will be used to sort the vector
    vector<IndexedAttr> iattrs;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // For each entry, create an IndexedAttr, and fill it with the 4 results
        IndexedAttr attr;
        unsigned offset = 0;

        // For the Columns table, there should never be a null column
        char null;
        memcpy(&null, data, 1);
        if (null)
            rc = RM_NULL_COLUMN;

        // Read in name
        offset = 1;
        int32_t nameLen;
        memcpy(&nameLen, (char*) data + offset, VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
        char name[nameLen + 1];
        name[nameLen] = '\0';
        memcpy(name, (char*) data + offset, nameLen);
        offset += nameLen;
        attr.attr.name = string(name);

        // read in type
        int32_t type;
        memcpy(&type, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.type = (AttrType)type;

        // Read in length
        int32_t length;
        memcpy(&length, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.attr.length = length;

        // Read in position
        int32_t pos;
        memcpy(&pos, (char*) data + offset, INT_SIZE);
        offset += INT_SIZE;
        attr.pos = pos;

        iattrs.push_back(attr);
    }
    // Do cleanup
    rbfm_si.close();
    rbfm->closeFile(fileHandle);
    free(data);
    // If we ended on an error, return that error
    if (rc != RBFM_EOF)
        return rc;

    // Sort attributes by position ascending
    auto comp = [](IndexedAttr first, IndexedAttr second) 
        {return first.pos < second.pos;};
    sort(iattrs.begin(), iattrs.end(), comp);

    // Fill up our result with the Attributes in sorted order
    for (auto attr : iattrs)
    {
        attrs.push_back(attr.attr);
    }

    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    IndexManager *im = IndexManager::instance(); 
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
    rbfm->closeFile(fileHandle);

    // need to do work if we have index too
    // find all the indexes on the attributes touched
    vector<IndexID> indexList;
    rc = getIndexesForTable(tableName, indexList);
    if (rc) 
        return rc;

    vector<IndexData> dataList;
    map<string, IndexData> atrList;
    // data must be of record type with null identifier at the beginning
    formatData(recordDescriptor, data, dataList); // returns a list of IndexData for each atr
    for (IndexData i : dataList) { // fill the hashmap so we can access the lists easier
        atrList[i.atr.name] = i;
    }
    // insert into index 
    for (IndexID id : indexList) {
        // insert entry needs fh, attribute, void*, rid
        IndexData idx = atrList[id.attrName];
        if (idx.nullKey) // skip if null 
            continue;
        
        IXFileHandle ixfh;
        // open the file first then insert then close to write it 
        rc = im->openFile(id.fileName, ixfh);
        if (rc) 
            return rc;
        rc = im->insertEntry(ixfh, idx.atr, idx.key, rid); // insert the rid and key into the index
        if (rc) 
            return rc;
        rc = im->closeFile(ixfh);
        if (rc) 
            return rc;
    }
    return rc;
}
// get the attributes with their keys as a list 
void RelationManager::formatData(const vector<Attribute> &recordDescriptor, const void *data, vector<IndexData> &atrList) {
    atrList.clear(); // clear the list

    // get the null indicator vector same as before
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator,(char*) data, nullIndicatorSize); // got the null indicators

    unsigned offset = nullIndicatorSize;

    for (unsigned i =0; i < recordDescriptor.size(); i++) {
        IndexData id;
        id.atr = recordDescriptor[i]; // fill in the attribute
        id.nullKey = fieldIsNull(nullIndicator, i); // fill in if its null
        if (fieldIsNull(nullIndicator, i)) { // if it's null
            id.key = NULL;
            atrList.push_back(id); // go to next one b/c no need to push data 
            continue;
        }
        // atr is not null 
        id.key = (char*) data + offset; // point to where the atr starts

        if (id.atr.type == TypeInt) {
            offset += INT_SIZE;
        }
        else if (id.atr.type == TypeReal) {
            offset += REAL_SIZE;
        }
        else if (id.atr.type == TypeVarChar) {
            // calculate len of varChar then move forward that much
            int varLen;
            memcpy(&varLen, (char*) data + offset, VARCHAR_LENGTH_SIZE); // get the size 
            offset += VARCHAR_LENGTH_SIZE;
            offset += varLen;
        }
        atrList.push_back(id); // append to the atrList
    }
}

RC RelationManager::getIndexesForTable(const string &tableName, vector<IndexID> &indexList){
    indexList.clear();
    RC rc;
    // going to use scan to get all tables with tableName
    // fill a vector with attribute name, filename, and table name
    vector<string> columnsWanted {INDEXES_COL_ATTR_NAME, INDEXES_COL_FILE_NAME, INDEXES_COL_TABLE_NAME};

    // create a value for the scan to work 
    void *val = malloc(tableName.length() + INT_SIZE);
    toAPI(tableName, val); // make the key like before

    RM_ScanIterator scanner; // iterator object
    rc = scan(INDEXES_TABLE_NAME, INDEXES_COL_TABLE_NAME, EQ_OP, val, columnsWanted,  scanner);
    if (rc) 
        return rc;
    
    void *page = malloc(PAGE_SIZE);
    memset(page, 0 , PAGE_SIZE);
    RID rid;

    while(true){
        rc = scanner.getNextTuple(rid, page); // get the next tuple
        int tupleOffset = 0;
        if (rc != SUCCESS) {
            if (rc == RM_EOF){ // if at the end 
                rc = SUCCESS;
                break;
            }
        }

        char nullChar = 0; 
        memcpy(&nullChar, page, 1); // check if null
        if (nullChar) {
            rc = RM_INVALID_COLUMN;
            break;
        }
        tupleOffset++;

        // fill in the indexID with the correct values
        IndexID id;

        int varLen;
        memcpy(&varLen, (char*) page + tupleOffset, VARCHAR_LENGTH_SIZE); // do for the attribute name first 
        tupleOffset += VARCHAR_LENGTH_SIZE;

        char atrName[varLen+1];
        memcpy(atrName, (char*) page + tupleOffset, varLen); // copy in the atr Name 
        atrName[varLen] = '\0';
        tupleOffset += varLen;

        memcpy(&varLen, (char*)page + tupleOffset, VARCHAR_LENGTH_SIZE); // get size of filename
        tupleOffset += VARCHAR_LENGTH_SIZE;

        char fileName[varLen+1];
        memcpy(fileName, (char*) page + tupleOffset, varLen); // copy in the fileName
        fileName[varLen] = '\0';
        tupleOffset += varLen;

        memcpy(&varLen, (char*) page + tupleOffset, VARCHAR_LENGTH_SIZE); // get the size of tableName
        tupleOffset += VARCHAR_LENGTH_SIZE;

        char tableName[varLen+1];
        memcpy(tableName, (char*)page + tupleOffset, varLen); // copy in the tableName
        tableName[varLen] = '\0';
        tupleOffset += varLen;

        // fill in the indexId struct
        id.attrName = atrName;
        id.fileName = fileName;
        id.tableName = tableName;
        id.rid = rid;

        // append to the indexList
        indexList.push_back(id);
    }

    // clean up the memory
    free(page);
    free(val);
    scanner.close();
    return rc;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    IndexManager *im = IndexManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
    rbfm->closeFile(fileHandle);

    // same as insert but with delete 
    // need to do work if we have index too
    // find all the indexes on the attributes touched
    vector<IndexID> indexList;
    rc = getIndexesForTable(tableName, indexList);
    if (rc) 
        return rc;

    vector<IndexData> dataList;
    map<string, IndexData> atrList;
    // data must be of record type with null identifier at the beginning
    void *data = malloc(PAGE_SIZE);
    memset(data, 0 , PAGE_SIZE);
    rbfm->readRecord(fileHandle, recordDescriptor, rid, data); // get the record into data 
    formatData(recordDescriptor, data, dataList); // returns a list of IndexData for each atr
    for (IndexData i : dataList) { // fill the hashmap so we can access the lists easier
        atrList[i.atr.name] = i;
    }
    // delete into index 
    for (IndexID id : indexList) {
        // delete entry needs fh, attribute, void*, rid
        IndexData idx = atrList[id.attrName];
        if (idx.nullKey) // skip if null 
            continue;
        
        IXFileHandle ixfh;
        // open the file first then insert then close to write it 
        rc = im->openFile(id.fileName, ixfh);
        if (rc) 
            return rc;
        rc = im->deleteEntry(ixfh, idx.atr, idx.key, rid); // delete the rid and key into the index
        if (rc) 
            return rc;
        rc = im->closeFile(ixfh);
        if (rc) 
            return rc;
    }
    free(data);
    return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    IndexManager *im = IndexManager::instance();
    RC rc;

    // If this is a system table, we cannot modify it
    bool isSystem;
    rc = isSystemTable(isSystem, tableName);
    if (rc)
        return rc;
    if (isSystem)
        return RM_CANNOT_MOD_SYS_TBL;

    // Get recordDescriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
    rbfm->closeFile(fileHandle);
    
    rc = rbfm->openFile(getFileName(tableName), fileHandle); // open table file 
    if (rc) 
        return rc;
    // need to do work if we have index too
    // find all the indexes on the attributes touched
    vector<IndexID> indexList;
    rc = getIndexesForTable(tableName, indexList);
    if (rc) 
        return rc;

    vector<IndexData> dataList;
    map<string, IndexData> atrList;
    formatData(recordDescriptor, data, dataList); // returns a list of IndexData for each atr

    // original Data  must be of record type with null identifier at the beginning
    vector<IndexData> originalList;
    map<string, IndexData> origMap;
    void *originalRecord = malloc(PAGE_SIZE);
    rbfm->readRecord(fileHandle, recordDescriptor, rid, originalRecord); // read into origRecord
    formatData(recordDescriptor, originalRecord, originalList);// get all attributes for the associated original record 

    for (IndexData i : dataList) { // fill the hashmap so we can access the lists easier
        atrList[i.atr.name] = i;
    }

    for (IndexData i : originalList) { // fill the hashmap so we can access the lists easier
        origMap[i.atr.name] = i;
    }
    // insert into index 
    for (IndexID id : indexList) {
        // insert entry needs fh, attribute, void*, rid
        
        IXFileHandle ixfh;
        // open the file first then insert then close to write it 
        rc = im->openFile(id.fileName, ixfh);
        if (rc)
            return rc;

        IndexData idx = atrList[id.attrName]; // new attribute
        IndexData origIdx = origMap[id.attrName]; // original attribute
        // multiple cases since no updateEntry 
        // 1. Original is null so was never in index
        if (origIdx.nullKey) {
            // check if new is null 
            if (idx.nullKey) { // new is null
                rc = im->closeFile(ixfh); // close file and open new one next loop
                if (rc) 
                    return rc;
                continue;
            }
            else { // new is not null
                rc = im->insertEntry(ixfh, idx.atr, idx.key, rid); // insert
                if (rc) 
                    return rc;
            }
        }
        else // original not null
        {
            // must delete old entry 
            rc = im->deleteEntry(ixfh, origIdx.atr, origIdx.key, rid);
            if (rc) 
                return rc;
            // still need to check and insert new entry
            if (idx.nullKey) { // new is null
                rc = im->closeFile(ixfh); // close file and open new one next loop
                if (rc) 
                    return rc;
                continue;
            }
            else { // new is not null
                rc = im->insertEntry(ixfh, idx.atr, idx.key, rid); // insert
                if (rc) 
                    return rc;
            }
        }
        rc = im->closeFile(ixfh); // close file and open new one next loop
        if (rc) 
            return rc;
    }
    free(originalRecord); // free memory and return
    return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    RC rc;

    // Get record descriptor
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // And get fileHandle
    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    // Let rbfm do all the work
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

// Let rbfm do all the work
RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    RC rc;

    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(tableName), fileHandle);
    if (rc)
        return rc;

    rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    rbfm->closeFile(fileHandle);
    return rc;
}

string RelationManager::getFileName(const char *tableName)
{
    return string(tableName) + string(TABLE_FILE_EXTENSION);
}

string RelationManager::getFileName(const string &tableName)
{
    return tableName + string(TABLE_FILE_EXTENSION);
}

string RelationManager::getIndexName(const string &tName, const string &atrName){
    return atrName + "Of" + tName + string(INDEX_FILE_EXTENSION); 
}

vector<Attribute> RelationManager::createTableDescriptor()
{
    vector<Attribute> td;

    Attribute attr;
    attr.name = TABLES_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_TABLE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_TABLE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_FILE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)TABLES_COL_FILE_NAME_SIZE;
    td.push_back(attr);

    attr.name = TABLES_COL_SYSTEM;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    td.push_back(attr);

    return td;
}

vector<Attribute> RelationManager::createColumnDescriptor()
{
    vector<Attribute> cd;

    Attribute attr;
    attr.name = COLUMNS_COL_TABLE_ID;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)COLUMNS_COL_COLUMN_NAME_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_TYPE;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_LENGTH;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    attr.name = COLUMNS_COL_COLUMN_POSITION;
    attr.type = TypeInt;
    attr.length = (AttrLength)INT_SIZE;
    cd.push_back(attr);

    return cd;
}

// create it for the index 
// needs table name, attribute the index is on, and the file name for that index
vector<Attribute> RelationManager::createIndexDescriptor()
{
    vector<Attribute> id;

    Attribute attr;
    attr.name = INDEXES_COL_TABLE_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)INDEXES_COL_TABLE_NAME_SIZE;
    id.push_back(attr);

    attr.name = INDEXES_COL_ATTR_NAME;
    attr.type = TypeVarChar;
    attr.length = (AttrLength)INDEXES_COL_ATTR_NAME_SIZE;
    id.push_back(attr);

    // used to identify the index file that contains that index 
    attr.name = INDEXES_COL_FILE_NAME;;
    attr.type = TypeInt;
    attr.length = (AttrLength)INDEXES_COL_FILE_NAME_SIZE;
    id.push_back(attr);
    return id;
}

// based off of prepareTablesRecordData
void RelationManager::prepareIndexRecordData(const string &tableName, const string &attributeName, void *indexPage) {
    string fName = getIndexName(tableName, attributeName); // used to calculate the filename length

    unsigned offset = 0;
    int32_t tableLength = tableName.length();
    int32_t attributeLength = attributeName.length();
    int32_t filenameLength = fName.length();

    char null = 0;

    memcpy((char*) indexPage + offset, &null, 1); // place null in 
    offset += 1;

    // place the table name into the page 
    memcpy((char*) indexPage + offset, &tableLength, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) indexPage + offset, tableName.c_str(), tableLength);
    offset += tableLength;

    // place the attribute name afterwards
    memcpy((char*) indexPage + offset, &attributeLength, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) indexPage + offset, attributeName.c_str(), attributeLength);
    offset += attributeLength;

    // finally place the filename into the record 
    memcpy((char*) indexPage + offset, &filenameLength, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) indexPage + offset, fName.c_str(), filenameLength);
    offset += filenameLength;

    // page is filled so done 
}

// Creates the Tables table entry for the given id and tableName
// Assumes fileName is just tableName + file extension
void RelationManager::prepareTablesRecordData(int32_t id, bool system, const string &tableName, void *data)
{
    unsigned offset = 0;

    int32_t name_len = tableName.length();

    string table_file_name = getFileName(tableName);
    int32_t file_name_len = table_file_name.length();

    int32_t is_system = system ? 1 : 0;

    // All fields non-null
    char null = 0;
    // Copy in null indicator
    memcpy((char*) data + offset, &null, 1);
    offset += 1;
    // Copy in table id
    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;
    // Copy in varchar table name
    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, tableName.c_str(), name_len);
    offset += name_len;
    // Copy in varchar file name
    memcpy((char*) data + offset, &file_name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, table_file_name.c_str(), file_name_len);
    offset += file_name_len; 
    // Copy in system indicator
    memcpy((char*) data + offset, &is_system, INT_SIZE);
    offset += INT_SIZE; // not necessary because we return here, but what if we didn't?
}

// Prepares the Columns table entry for the given id and attribute list
void RelationManager::prepareColumnsRecordData(int32_t id, int32_t pos, Attribute attr, void *data)
{
    unsigned offset = 0;
    int32_t name_len = attr.name.length();

    // None will ever be null
    char null = 0;

    memcpy((char*) data + offset, &null, 1);
    offset += 1;

    memcpy((char*) data + offset, &id, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*) data + offset, &name_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;
    memcpy((char*) data + offset, attr.name.c_str(), name_len);
    offset += name_len;

    int32_t type = attr.type;
    memcpy((char*) data + offset, &type, INT_SIZE);
    offset += INT_SIZE;

    int32_t len = attr.length;
    memcpy((char*) data + offset, &len, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char*) data + offset, &pos, INT_SIZE);
    offset += INT_SIZE;
}

// Insert the given columns into the Columns table
RC RelationManager::insertColumns(int32_t id, const vector<Attribute> &recordDescriptor)
{
    RC rc;

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 

    FileHandle fileHandle;
    rc = rbfm->openFile(getFileName(COLUMNS_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *columnData = malloc(COLUMNS_RECORD_DATA_SIZE);
    RID rid;
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        int32_t pos = i+1;
        prepareColumnsRecordData(id, pos, recordDescriptor[i], columnData);
        rc = rbfm->insertRecord(fileHandle, columnDescriptor, columnData, rid);
        if (rc)
            return rc;
    }

    rbfm->closeFile(fileHandle);
    free(columnData);
    return SUCCESS;
}

RC RelationManager::insertTable(int32_t id, int32_t system, const string &tableName)
{
    FileHandle fileHandle;
    RID rid;
    RC rc;
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    void *tableData = malloc (TABLES_RECORD_DATA_SIZE);
    prepareTablesRecordData(id, system, tableName, tableData);
    rc = rbfm->insertRecord(fileHandle, tableDescriptor, tableData, rid);

    rbfm->closeFile(fileHandle);
    free (tableData);
    return rc;
}

// Get the next table ID for creating a table
RC RelationManager::getNextTableID(int32_t &table_id)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // Grab only the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Scan through all tables to get largest ID value
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_ID, NO_OP, NULL, projection, rbfm_si);

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    int32_t max_table_id = 0;
    while ((rc = rbfm_si.getNextRecord(rid, data)) == (SUCCESS))
    {
        // Parse out the table id, compare it with the current max
        int32_t tid;
        fromAPI(tid, data);
        if (tid > max_table_id)
            max_table_id = tid;
    }
    // If we ended on eof, then we were successful
    if (rc == RM_EOF)
        rc = SUCCESS;

    free(data);
    // Next table ID is 1 more than largest table id
    table_id = max_table_id + 1;
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return SUCCESS;
}

// Gets the table ID of the given tableName
RC RelationManager::getTableID(const string &tableName, int32_t &tableID)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // We only care about the table ID
    vector<string> projection;
    projection.push_back(TABLES_COL_TABLE_ID);

    // Fill value with the string tablename in api format (without null indicator)
    void *value = malloc(4 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

    // Find the table entries whose table-name field matches tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    // There will only be one such entry, so we use if rather than while
    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        int32_t tid;
        fromAPI(tid, data);
        tableID = tid;
    }

    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return rc;
}

// Determine if table tableName is a system table. Set the boolean argument as the result
RC RelationManager::isSystemTable(bool &system, const string &tableName)
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    FileHandle fileHandle;
    RC rc;

    rc = rbfm->openFile(getFileName(TABLES_TABLE_NAME), fileHandle);
    if (rc)
        return rc;

    // We only care about system column
    vector<string> projection;
    projection.push_back(TABLES_COL_SYSTEM);

    // Set up value to be tableName in API format (without null indicator)
    void *value = malloc(5 + TABLES_COL_TABLE_NAME_SIZE);
    int32_t name_len = tableName.length();
    memcpy(value, &name_len, INT_SIZE);
    memcpy((char*)value + INT_SIZE, tableName.c_str(), name_len);

    // Find table whose table-name is equal to tableName
    RBFM_ScanIterator rbfm_si;
    rc = rbfm->scan(fileHandle, tableDescriptor, TABLES_COL_TABLE_NAME, EQ_OP, value, projection, rbfm_si);

    RID rid;
    void *data = malloc (1 + INT_SIZE);
    if ((rc = rbfm_si.getNextRecord(rid, data)) == SUCCESS)
    {
        // Parse the system field from that table entry
        int32_t tmp;
        fromAPI(tmp, data);
        system = tmp == 1;
    }
    if (rc == RBFM_EOF)
        rc = SUCCESS;

    free(data);
    free(value);
    rbfm->closeFile(fileHandle);
    rbfm_si.close();
    return rc;   
}

void RelationManager::toAPI(const string &str, void *data)
{
    int32_t len = str.length();
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &len, INT_SIZE);
    memcpy((char*) data + 1 + INT_SIZE, str.c_str(), len);
}

void RelationManager::toAPI(const int32_t integer, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &integer, INT_SIZE);
}

void RelationManager::toAPI(const float real, void *data)
{
    char null = 0;

    memcpy(data, &null, 1);
    memcpy((char*) data + 1, &real, REAL_SIZE);
}

void RelationManager::fromAPI(string &str, void *data)
{
    char null = 0;
    int32_t len;

    memcpy(&null, data, 1);
    if (null)
        return;

    memcpy(&len, (char*) data + 1, INT_SIZE);

    char tmp[len + 1];
    tmp[len] = '\0';
    memcpy(tmp, (char*) data + 5, len);

    str = string(tmp);
}

void RelationManager::fromAPI(int32_t &integer, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    int32_t tmp;
    memcpy(&tmp, (char*) data + 1, INT_SIZE);

    integer = tmp;
}

void RelationManager::fromAPI(float &real, void *data)
{
    char null = 0;

    memcpy(&null, data, 1);
    if (null)
        return;

    float tmp;
    memcpy(&tmp, (char*) data + 1, REAL_SIZE);
    
    real = tmp;
}

// RM_ScanIterator ///////////////

// Makes use of underlying rbfm_scaniterator
RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    // Open the file for the given tableName
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    RC rc = rbfm->openFile(getFileName(tableName), rm_ScanIterator.fileHandle);
    if (rc)
        return rc;

    // grab the record descriptor for the given tableName
    vector<Attribute> recordDescriptor;
    rc = getAttributes(tableName, recordDescriptor);
    if (rc)
        return rc;

    // Use the underlying rbfm_scaniterator to do all the work
    rc = rbfm->scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute,
                     compOp, value, attributeNames, rm_ScanIterator.rbfm_iter);
    if (rc)
        return rc;

    return SUCCESS;
}

// Let rbfm do all the work
RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
    return rbfm_iter.getNextRecord(rid, data);
}

// Close our file handle, rbfm_scaniterator
RC RM_ScanIterator::close()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    rbfm_iter.close();
    rbfm->closeFile(fileHandle);
    return SUCCESS;
}

// based off of createTable
RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	IndexManager *im = IndexManager::instance();
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    string fName = getIndexName(tableName, attributeName);
    RID rid;
    RC rc;

    rc = im->createFile(fName);
    if (rc) 
        return rc;
    
    // need to get attribute to see how big key is and to later determine type 
    Attribute atr; 
    vector<Attribute> atrVec; 
    getAttributes(tableName, atrVec); // get all the attributes
    for (Attribute attr : atrVec) {
        if (attr.name == attributeName){
            atr = attr;
            break; // found the right one 
        }
    }

    IXFileHandle ixfh; // needed to open a file and handle it
    rc = im->openFile(fName, ixfh);
    if (rc) 
        return rc;

    // need to get all the records and only the attribute for this index 
    vector<string> scanAttr;
    scanAttr.push_back(attributeName); // this is the attribute that will be returned from scan
    RM_ScanIterator scanner; // scanner used to go through a table 
    scan(tableName, "", NO_OP, NULL, scanAttr, scanner); // NO OP because we want all tuples
    
    void *data = malloc(atr.length + 10); // for the null terminator in a string
    memset(data, 0, atr.length + 10); // clear the data 

    while(true) { // iterate through the table
        rc = scanner.getNextTuple(rid, data);
        if (rc == IX_EOF) { // if at the end then everything is fine  and done
            rc = 0;
            break;
        } 
        if(data == NULL) { // if the attr is null for this tuple 
            continue;
        }
        rc = im->insertEntry(ixfh, atr, (char*) data + 1, rid); // insert into the index with no null flag
        if (rc) // error then break
            break;
    }

    // clear all the memory
    free(data);
    im->closeFile(ixfh);
    scanner.close();
    if (rc != SUCCESS) 
        return rc;

    
    // finally add index to the catalog
    FileHandle fh; // used to get the INDEX TABLE FILE
    rc = rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fh);
    if (rc)
        return rc;

    void *page = malloc(PAGE_SIZE);
    memset(page, 0, PAGE_SIZE);
    prepareIndexRecordData(tableName, attributeName, page); // create a record for the new index
    rc = rbfm->insertRecord(fh, indexDescriptor, page, rid); // rid now contains the new record
    if (rc) 
        return rc;
    free(page);

    return rc;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
    // similar to insert 
    IndexManager *im = IndexManager::instance();
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();  // for file operations like open file 
    string fName = getIndexName(tableName, attributeName);
    RID rid;
    RC rc;

    // remove the index from the catalog 
    void *val =  malloc(fName.length() + 10); // leave enough room for extension
    memset(val, 0, fName.length() + 10);
    toAPI(fName, val); // fill val with the filename 

    // scan the indexTable to find entry and remove it 
    // setup scanner
    RM_ScanIterator scanner;
    vector<string> scanAttr = {INDEXES_COL_FILE_NAME};
    // scan the index table in the filename attribute for a file name equal to this one 
    scan(INDEXES_TABLE_NAME, INDEXES_COL_FILE_NAME, EQ_OP, val, scanAttr, scanner);

    void *data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE); // data to hold getNextTuple information in
    while(true) {
        rc = scanner.getNextTuple(rid, data); // should be only 1?
        if (rc == IX_EOF) { // if at the end then everything is fine  and done
            rc = 0;
            break;
        }
        FileHandle fh; 
        rbfm->openFile(getFileName(INDEXES_TABLE_NAME), fh); // open the index table 
        rbfm->deleteRecord(fh, indexDescriptor, rid); // remove the record with the format of the indexDescriptor
        rbfm->closeFile(fh); 
    }

    // destroy the file 
    rc = im->destroyFile(fName);
    if (rc)
        return rc;
    free(data);
    free(val);
    scanner.close();
    return rc;

}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
	// index scan needs: filehandle, attribute, lowKey, highkey, boolHKey, boolLKey, ixscanIter
    IndexManager *im = IndexManager::instance(); // needed to do index scan
    RC rc;
    // based on previous implementations
    // get the file name for the index and open it
    // use the indexScanners file handler
    rc = im->openFile(getIndexName(tableName, attributeName), rm_IndexScanIterator.ixfh);
    if (rc) 
        return rc;

    // get the attribute to scan on
    Attribute atr; 
    vector<Attribute> atrVec; 
    getAttributes(tableName, atrVec); // get all the attributes
    for (Attribute attr : atrVec) {
        if (attr.name == attributeName){
            atr = attr;
            break; // found the right one 
        }
    } // now we have the right attr

    rc = im->scan(rm_IndexScanIterator.ixfh, atr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_scan);
    return rc;
}

// "key" follows the same format as in IndexManager::insertEntry()
RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key)// Get next matching entry
{
    return ix_scan.getNextEntry(rid,key); // just use indexManager getNextEntry since we use their scanner anyways
}
RC RM_IndexScanIterator::close()
{
    // make sure to close the scanner and filehandle
    ix_scan.close();
    IndexManager *im = IndexManager::instance();
    im->closeFile(ixfh);
    return SUCCESS;
}

// Calculate actual bytes for nulls-indicator for the given field counts
int RelationManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RelationManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

