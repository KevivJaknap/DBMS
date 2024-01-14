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