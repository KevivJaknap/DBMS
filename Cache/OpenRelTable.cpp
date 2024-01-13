#include "OpenRelTable.h"
#include <cstdlib>
#include <cstring>
#include <iostream>

OpenRelTable::OpenRelTable() {
    for(int i=0;i < MAX_OPEN; i++){
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
    }
    RecBuffer relCatBlock(RELCAT_BLOCK);

    Attribute relCatRecord[RELCAT_NO_ATTRS];
    relCatBlock.getRecord(relCatRecord, RELCAT_SLOTNUM_FOR_RELCAT);
    struct RelCacheEntry relCacheEntry;
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
    relCacheEntry.recId.block = RELCAT_BLOCK;
    relCacheEntry.recId.slot = RELCAT_SLOTNUM_FOR_RELCAT;

    RelCacheTable::relCache[RELCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(struct RelCacheEntry));
    *(RelCacheTable::relCache[RELCAT_RELID]) = relCacheEntry;

    Attribute relCatAttrRecord[RELCAT_NO_ATTRS];
    relCatBlock.getRecord(relCatAttrRecord, RELCAT_SLOTNUM_FOR_ATTRCAT);
    
    struct RelCacheEntry relCacheAttrEntry;
    RelCacheTable::recordToRelCatEntry(relCatAttrRecord, &relCacheAttrEntry.relCatEntry);
    relCacheAttrEntry.recId.block = RELCAT_BLOCK;
    relCacheAttrEntry.recId.slot = RELCAT_SLOTNUM_FOR_ATTRCAT;

    RelCacheTable::relCache[ATTRCAT_RELID] = (struct RelCacheEntry*)malloc(sizeof(struct RelCacheEntry));
    *(RelCacheTable::relCache[ATTRCAT_RELID]) = relCacheAttrEntry;
    //verified
    RecBuffer attrCatBlock(ATTRCAT_BLOCK);

    Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    struct AttrCacheEntry* curr;
    struct AttrCacheEntry* head;
    attrCatBlock.getRecord(attrCatRecord, 0);
    struct AttrCacheEntry attrCacheEntry;
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry.attrCatEntry);
    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
    attrCacheEntry.recId.slot = 0;
    head = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
    *head = attrCacheEntry;
    curr = head;
    for(int i = 1; i < 6; i++){
        attrCatBlock.getRecord(attrCatRecord, i);
        struct AttrCacheEntry attrCacheEntry;
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry.attrCatEntry);
        attrCacheEntry.recId.block = ATTRCAT_BLOCK;
        attrCacheEntry.recId.slot = i;
        curr->next = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
        *(curr->next) = attrCacheEntry;
        curr = curr->next;
    }
    curr->next = nullptr;
    AttrCacheTable::attrCache[RELCAT_RELID] = head;
    curr = AttrCacheTable::attrCache[RELCAT_RELID];
    //verified
    attrCatBlock.getRecord(attrCatRecord, 6);
    AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry.attrCatEntry);
    attrCacheEntry.recId.block = ATTRCAT_BLOCK;
    attrCacheEntry.recId.slot = 6;
    
    head = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
    *head = attrCacheEntry;
    curr = head;
    for(int i = 7; i < 12; i++){
        attrCatBlock.getRecord(attrCatRecord, i);
        struct AttrCacheEntry attrCacheEntry;
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry.attrCatEntry);
        attrCacheEntry.recId.block = ATTRCAT_BLOCK;
        attrCacheEntry.recId.slot = i;
        curr->next = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
        *(curr->next) = attrCacheEntry;
        curr = curr->next;
    }
    curr->next = nullptr;
    AttrCacheTable::attrCache[ATTRCAT_RELID] = head;
    curr = AttrCacheTable::attrCache[ATTRCAT_RELID];

    struct HeadInfo headInfo;
    relCatBlock.getHeader(&headInfo);
    int numRecs = headInfo.numEntries;
    for(int i=0; i < numRecs; i++){
        relCatBlock.getRecord(relCatRecord, i);
        struct RelCacheEntry relCacheEntry;
        if(!strcmp(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, "Students")){
            RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry.relCatEntry);
            relCacheEntry.recId.block = RELCAT_BLOCK;
            relCacheEntry.recId.slot = i;
            RelCacheTable::relCache[2] = (struct RelCacheEntry*)malloc(sizeof(struct RelCacheEntry));
            *(RelCacheTable::relCache[2]) = relCacheEntry;
            break;        
        }
    }
    //verified
    struct HeadInfo attrHeadInfo;
    int attrCatBlockNum = ATTRCAT_BLOCK;
    AttrCacheEntry* dummy = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
    dummy->next = nullptr;
    AttrCacheTable::attrCache[2] = dummy;
    do{
        RecBuffer attrCatBlock(attrCatBlockNum);
        attrCatBlock.getHeader(&attrHeadInfo);
        numRecs = attrHeadInfo.numEntries;
        for(int i=0; i < numRecs; i++){
            attrCatBlock.getRecord(attrCatRecord, i);
            struct AttrCacheEntry attrCacheEntry;
            if(!strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, "Students")){
                AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry.attrCatEntry);
                attrCacheEntry.recId.block = attrCatBlockNum;
                attrCacheEntry.recId.slot = i;
                dummy->next = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
                *(dummy->next) = attrCacheEntry;
                dummy = dummy->next;
            }
        }
        attrCatBlockNum = attrHeadInfo.rblock;
    }while(attrHeadInfo.rblock != -1);
    dummy->next = nullptr;
    dummy = AttrCacheTable::attrCache[2];
    AttrCacheTable::attrCache[2] = dummy->next;
    free(dummy);
    //verified
    
    // while(curr != nullptr){
    //     printf("%s\n", curr->attrCatEntry.attrName);
    //     curr = curr->next;
    // }
    // scanf("hello");
    
}

OpenRelTable::~OpenRelTable() {
    // for(int i=0;i < MAX_OPEN; i++){
    //     if(RelCacheTable::relCache[i] != NULL){
    //         free(RelCacheTable::relCache[i]);
    //     }
    //     if(AttrCacheTable::attrCache[i] != NULL){
    //         free(AttrCacheTable::attrCache[i]);
    //     }
    // }
}