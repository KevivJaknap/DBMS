#include "BlockAccess.h"

#include <cstring>
#include <iostream>
RecId BlockAccess::linearSearch(int relId, char attrName[ATTR_SIZE], union Attribute attrVal, int op){
    RecId prevRecId;
    RelCacheTable::getSearchIndex(relId, &prevRecId);

    int block = prevRecId.block;
    int slot = prevRecId.slot;

    if (prevRecId.block == -1 and prevRecId.slot == -1) {
        RelCatEntry relcatEntry;
        RelCacheTable::getRelCatEntry(relId, &relcatEntry);
        block = relcatEntry.firstBlk;
        slot = 0;
    }
    else{
        slot++;
    }
    // printf("Block %d Slot %d\n", block, slot);
    while (block != -1) {
       //get Block using RecBuffer
       RecBuffer recBuffer(block);

       //get Header
       struct HeadInfo head;
       recBuffer.getHeader(&head);

       //get number of attributes and number of slots
       int numAttrs = head.numAttrs;
       int numSlots = head.numSlots;

       //allocate memory for union Attribute* record and read record
       Attribute rec[numAttrs];
       recBuffer.getRecord(rec, slot);

       //allocate memory for slot Map and read slotMap
       unsigned char slotMap[numSlots];
       recBuffer.getSlotMap(slotMap);

       if(slot > numSlots){
        block = head.rblock;
        slot=0;
        continue;
       }

       if(slotMap[slot] == SLOT_UNOCCUPIED){
        slot++;
        continue;
       }
       //get AttrCatEntry
       AttrCatEntry* attrCatBuf = (AttrCatEntry*)malloc(sizeof(AttrCatEntry));
       AttrCacheTable::getAttrCatEntry(relId, attrName, attrCatBuf);
       //get offset and value of attribute
       int offset = attrCatBuf->offset;
       union Attribute value = rec[offset];

       int cmpVal = compareAttrs(value, attrVal, attrCatBuf->attrType);

       if (
        (op == NE && cmpVal != 0) ||
        (op == EQ && cmpVal == 0) ||
        (op == LT && cmpVal < 0) ||
        (op == GT && cmpVal > 0) ||
        (op == LE && cmpVal <= 0) ||
        (op == GE && cmpVal >= 0)
       ){
        RecId recId;
        recId.block = block;
        recId.slot = slot;
        RelCacheTable::setSearchIndex(relId, &recId);
        return recId;
       }
       slot++;
    }
    return RecId{-1, -1};
}

int BlockAccess::renameRelation(char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){
    //reset searchIndex of relation Catalog
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    //set new relation name into an attribute
    Attribute newRelationName;
    strcpy(newRelationName.sVal, newName);

    //check if new relation name already exists
    RecId recId = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, newRelationName, EQ);
    if(recId.block != -1){
        return E_RELEXIST;
    }

    //reset searchIndex again
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    //set old relation name into an attribute
    Attribute oldRelationName;
    strcpy(oldRelationName.sVal, oldName);

    //search for old relation name
    recId = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, oldRelationName, EQ);
    if(recId.block == -1){
        return E_RELNOTEXIST;
    }

    //get relation catalog record
    RecBuffer recBuffer(RELCAT_BLOCK);
    union Attribute relcatRec[RELCAT_NO_ATTRS];
    recBuffer.getRecord(relcatRec, recId.slot);

    //set new relation name into relation catalog record
    strcpy(relcatRec[RELCAT_REL_NAME_INDEX].sVal, newName);

    //update relation catalog record
    recBuffer.setRecord(relcatRec, recId.slot);

    //reset searchIndex of attribute catalog
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    //for each attribute entry change relation name
    int numAttrs = relcatRec[RELCAT_NO_ATTRIBUTES_INDEX].nVal;

    for(int i=0; i<numAttrs; i++){
        //get attribute catalog records
        recId = linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, oldRelationName, EQ);
        RecBuffer recBuffer(recId.block);
        union Attribute attrcatRec[ATTRCAT_NO_ATTRS];
        recBuffer.getRecord(attrcatRec, recId.slot);

        //change name
        strcpy(attrcatRec[ATTRCAT_REL_NAME_INDEX].sVal, newName);

        //use setRecord
        recBuffer.setRecord(attrcatRec, recId.slot);
    }
    return SUCCESS;
}

int BlockAccess::renameAttribute(char relName[ATTR_SIZE], char oldName[ATTR_SIZE], char newName[ATTR_SIZE]){
    //reset searchIndex of relation catalog
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    //set relation name into an attribute
    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);

    //check if relName exists or not
    RecId recId = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, relNameAttr, EQ);
    if(recId.block == -1){
        return E_RELNOTEXIST;
    }

    //reset searchIndex of attribute catalog
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    //declare a RecId to store the record Id of the attribute catalog record to be updated
    RecId attrToRenameRecId{-1, -1};
    Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];


    //iterate over all Attribute Catalog Entries and change stuff

    while(true){
        //linear search to find the next attribute catalog entry
        RecId attrcatRecId = linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);

        //if no more records to be updated, break
        if(attrcatRecId.block == -1){
            break;
        }

        //get the record 
        RecBuffer recBuffer(attrcatRecId.block);
        recBuffer.getRecord(attrCatEntryRecord, attrcatRecId.slot);

        //check if attribute name matches
        if(!strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, oldName)){
            attrToRenameRecId.block = attrcatRecId.block;
            attrToRenameRecId.slot = attrcatRecId.slot;
        }

        //check if any other attribute has the new name
        if(!strcmp(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName)){
            return E_ATTREXIST;
        }
    }

    //if no attribute with old name found, return error
    if(attrToRenameRecId.block == -1){
        return E_ATTRNOTEXIST;
    }

    //get the record
    RecBuffer recBuffer(attrToRenameRecId.block);
    recBuffer.getRecord(attrCatEntryRecord, attrToRenameRecId.slot);

    //change the name
    strcpy(attrCatEntryRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, newName);

    //update the record
    recBuffer.setRecord(attrCatEntryRecord, attrToRenameRecId.slot);

    return SUCCESS;
}

int BlockAccess::insert(int relId, Attribute *record) {
    // get relation catalog entry of relId to find blockNum and
    // some other stuff
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    int blockNum = relCatEntry.firstBlk;

    // recId to store location where new record will be stored
    RecId recId = {-1, -1};

    int numOfSlots = relCatEntry.numSlotsPerBlk;
    int numOfAttributes = relCatEntry.numAttrs;

    int prevBlockNum = relCatEntry.lastBlk;

    // to make note if relation is empty
    bool empty = blockNum == -1;

    while(blockNum != -1){
        // create RecBuffer object for blockNum
        RecBuffer recBuffer(blockNum);

        // get Header
        struct HeadInfo head;
        recBuffer.getHeader(&head);

        //get slot map
        unsigned char slotMap[numOfSlots];
        recBuffer.getSlotMap(slotMap);

        //find free slot
        for(int i=0; i<numOfSlots; i++){
            if(slotMap[i] == SLOT_UNOCCUPIED){
                recId.block = blockNum;
                recId.slot = i;
                break;
            }
        }

        // if free slot is found, break from while
        if(recId.block != -1){
            break;
        }

        prevBlockNum = blockNum;
        blockNum = head.rblock;
    }

    if (recId.block == -1){
        if (!strcmp(relCatEntry.relName, RELCAT_RELNAME)){
            return E_MAXRELATIONS;
        }

        // getting new record block
        RecBuffer recBuffer;
        int ret = recBuffer.getBlockNum();
        if (ret == E_DISKFULL){
            return E_DISKFULL;
        }

        recId.block = ret;
        recId.slot = 0;

        // set Header
        struct HeadInfo head;
        head.blockType = REC;
        head.pblock = -1;
        head.lblock = empty ? -1 : prevBlockNum;
        head.rblock = -1;
        head.numEntries = 0;
        head.numSlots = numOfSlots;
        head.numAttrs = numOfAttributes;

        recBuffer.setHeader(&head);

        //set slotMap as unoccupied for all slots
        unsigned char slotMap[numOfSlots];
        for(int i=0; i<numOfSlots; i++){
            slotMap[i] = SLOT_UNOCCUPIED;
        }
        //set slotMap
        recBuffer.setSlotMap(slotMap);

        if (!empty) {
            // set rblock of previous to new block
            RecBuffer prevBuffer(prevBlockNum);
            struct HeadInfo prevHead;
            prevBuffer.getHeader(&prevHead);
            prevHead.rblock = recId.block;
            prevBuffer.setHeader(&prevHead);
        }
        else {
            // update first block field
            relCatEntry.firstBlk = recId.block;
            RelCacheTable::setRelCatEntry(relId, &relCatEntry);
        }

        //update last block field
        relCatEntry.lastBlk = recId.block;
        RelCacheTable::setRelCatEntry(relId, &relCatEntry);

    }

    RecBuffer recBuffer(recId.block);
    recBuffer.setRecord(record, recId.slot);

    //update slotMap
    unsigned char slotMap[numOfSlots];
    recBuffer.getSlotMap(slotMap);
    slotMap[recId.slot] = SLOT_OCCUPIED;
    recBuffer.setSlotMap(slotMap);

    //update numEntries
    struct HeadInfo head;
    recBuffer.getHeader(&head);
    head.numEntries++;
    recBuffer.setHeader(&head);

    //increment number of records in relation cache entry
    relCatEntry.numRecs++;
    RelCacheTable::setRelCatEntry(relId, &relCatEntry);

    return SUCCESS;
}

int BlockAccess::search(int relId, Attribute *record, char attrName[ATTR_SIZE], Attribute attrVal, int op){
    RecId recId;

    recId = linearSearch(relId, attrName, attrVal, op);

    if(recId.block == -1){
        return E_NOTFOUND;
    }

    RecBuffer recBuffer(recId.block);
    recBuffer.getRecord(record, recId.slot);

    return SUCCESS;
}

int BlockAccess::deleteRelation(char relName[ATTR_SIZE]){
    //check
    bool check = !strcmp(relName, RELCAT_RELNAME) || !strcmp(relName, ATTRCAT_RELNAME);
    if(check){
        return E_NOTPERMITTED;
    }

    //reset searchIndex of relation catalog
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    Attribute relNameAttr;
    strcpy(relNameAttr.sVal, relName);

    //check if relation exists
    RecId recId = linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, relNameAttr, EQ);
    if(recId.block == -1){
        return E_RELNOTEXIST;
    }

    Attribute relCatEntryRecord[RELCAT_NO_ATTRS];
    RecBuffer recBuffer(recId.block);
    int retVal = recBuffer.getRecord(relCatEntryRecord, recId.slot);
    if(retVal != SUCCESS){
        return retVal;
    }

    //get number of attributes
    int numAttrs = relCatEntryRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal;
    //get first block
    int firstBlock = relCatEntryRecord[RELCAT_FIRST_BLOCK_INDEX].nVal;

    int currBlock = firstBlock;
    int nextBlock = -1;
    while(currBlock != -1){
        RecBuffer recBuffer2(currBlock);
        struct HeadInfo head;
        retVal = recBuffer2.getHeader(&head);
        if(retVal != SUCCESS){
            return retVal;
        }
        currBlock = head.rblock;
        recBuffer2.releaseBlock();
    }
    //reset search index of attribute catalog
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);
    int numberOfAttributesDeleted = 0;

    while(true) {
        RecId attrCatRecId = linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, relNameAttr, EQ);
        if(attrCatRecId.block == -1){
            break;
        }
        numberOfAttributesDeleted++;
        RecBuffer attrCatBuffer(attrCatRecId.block);
        struct HeadInfo head;
        retVal = attrCatBuffer.getHeader(&head);
        if(retVal != SUCCESS){
            return retVal;
        }
        Attribute attrCatEntryRecord[ATTRCAT_NO_ATTRS];
        attrCatBuffer.getRecord(attrCatEntryRecord, attrCatRecId.slot);

        //updating slotmap
        unsigned char slotMap[head.numSlots];
        retVal = attrCatBuffer.getSlotMap(slotMap);
        if(retVal != SUCCESS){
            return retVal;
        }
        slotMap[attrCatRecId.slot] = SLOT_UNOCCUPIED;
        retVal = attrCatBuffer.setSlotMap(slotMap);
        if(retVal != SUCCESS){
            return retVal;
        }


        int rootBlock = attrCatEntryRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal;
        

        //decrement the number of entries
        head.numEntries--;
        retVal = attrCatBuffer.setHeader(&head);
        if(retVal != SUCCESS){
            return retVal;
        }

        if(head.numEntries == 0){
            RecBuffer prevBuffer(head.lblock);
            struct HeadInfo prevHead;
            retVal = prevBuffer.getHeader(&prevHead);
            if(retVal != SUCCESS){
                return retVal;
            }
            prevHead.rblock = head.rblock;
            retVal = prevBuffer.setHeader(&prevHead);
            if(retVal != SUCCESS){
                return retVal;
            }
            if(head.rblock != -1){
                RecBuffer nextBuffer(head.rblock);
                struct HeadInfo nextHead;
                retVal = nextBuffer.getHeader(&nextHead);
                if(retVal != SUCCESS){
                    return retVal;
                }
                nextHead.lblock = head.lblock;
                retVal = nextBuffer.setHeader(&nextHead);
                if(retVal != SUCCESS){
                    return retVal;
                }
            }
            else{
                relCatEntryRecord[RELCAT_LAST_BLOCK_INDEX].nVal = head.lblock;
                recBuffer.setRecord(relCatEntryRecord, recId.slot);
            }
            attrCatBuffer.releaseBlock();
        }
        if (rootBlock != -1){
            BPlusTree::bPlusDestroy(rootBlock);
        }
    }
    /* deleting the entry in relation catalog */
    struct HeadInfo relCatHead;
    retVal = recBuffer.getHeader(&relCatHead);
    if(retVal != SUCCESS){
        return retVal;
    }
    relCatHead.numEntries--;
    retVal = recBuffer.setHeader(&relCatHead);
    if(retVal != SUCCESS){
        return retVal;
    }

    //updating slotmap
    unsigned char slotMap[relCatHead.numSlots];
    retVal = recBuffer.getSlotMap(slotMap);
    if(retVal != SUCCESS){
        return retVal;
    }
    slotMap[recId.slot] = SLOT_UNOCCUPIED;
    retVal = recBuffer.setSlotMap(slotMap);

    /* updating relCacheTable of relation catalog */
    RelCatEntry relCatEntryrelCat;
    RelCacheTable::getRelCatEntry(RELCAT_RELID, &relCatEntryrelCat);
    relCatEntryrelCat.numRecs--;
    RelCacheTable::setRelCatEntry(RELCAT_RELID, &relCatEntryrelCat);

    /* updating relCacheTable of attribute catalog */
    RelCatEntry relCatEntryattrCat;
    RelCacheTable::getRelCatEntry(ATTRCAT_RELID, &relCatEntryattrCat);
    relCatEntryattrCat.numRecs -= numberOfAttributesDeleted;
    RelCacheTable::setRelCatEntry(ATTRCAT_RELID, &relCatEntryattrCat);

    return SUCCESS;
}