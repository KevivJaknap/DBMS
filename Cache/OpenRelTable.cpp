#include "OpenRelTable.h"
#include <cstdlib>
#include <cstring>
#include <iostream>

OpenRelTableMetaInfo OpenRelTable::tableMetaInfo[MAX_OPEN];

OpenRelTable::OpenRelTable() {
    for(int i=0;i < MAX_OPEN; i++){
        RelCacheTable::relCache[i] = nullptr;
        AttrCacheTable::attrCache[i] = nullptr;
        OpenRelTable::tableMetaInfo[i].free = true;
    }

    //setting free=false for RELCAT_RELID and ATTRCAT_RELID
    OpenRelTable::tableMetaInfo[RELCAT_RELID].free = false;
    OpenRelTable::tableMetaInfo[ATTRCAT_RELID].free = false;


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
    struct AttrCacheEntry* dummy = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
    struct AttrCacheEntry* head = dummy;
    for(int i=0; i<6; i++){
        attrCatBlock.getRecord(attrCatRecord, i);
        struct AttrCacheEntry attrCacheEntry;
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry.attrCatEntry);
        attrCacheEntry.recId.block = ATTRCAT_BLOCK;
        attrCacheEntry.recId.slot = i;
        dummy->next = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
        *(dummy->next) = attrCacheEntry;
        dummy = dummy->next;
    }
    dummy->next = nullptr;
    AttrCacheTable::attrCache[RELCAT_RELID] = head->next;

    struct AttrCacheEntry* dummy2 = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
    struct AttrCacheEntry* head2 = dummy2;
    for(int i=6; i<12; i++){
        attrCatBlock.getRecord(attrCatRecord, i);
        struct AttrCacheEntry attrCacheEntry;
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry.attrCatEntry);
        attrCacheEntry.recId.block = ATTRCAT_BLOCK;
        attrCacheEntry.recId.slot = i;
        dummy2->next = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
        *(dummy2->next) = attrCacheEntry;
        dummy2 = dummy2->next;
    }
    dummy2->next = nullptr;
    AttrCacheTable::attrCache[ATTRCAT_RELID] = head2->next;
    // struct AttrCacheEntry* curr;
    // struct AttrCacheEntry* head;
    // attrCatBlock.getRecord(attrCatRecord, 0);
    // struct AttrCacheEntry attrCacheEntry;
    // AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry.attrCatEntry);
    // attrCacheEntry.recId.block = ATTRCAT_BLOCK;
    // attrCacheEntry.recId.slot = 0;
    // head = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
    // *head = attrCacheEntry;
    // curr = head;
    // for(int i = 1; i < 6; i++){
    //     attrCatBlock.getRecord(attrCatRecord, i);
    //     struct AttrCacheEntry attrCacheEntry;
    //     AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry.attrCatEntry);
    //     attrCacheEntry.recId.block = ATTRCAT_BLOCK;
    //     attrCacheEntry.recId.slot = i;
    //     curr->next = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
    //     *(curr->next) = attrCacheEntry;
    //     curr = curr->next;
    // }
    // curr->next = nullptr;
    // AttrCacheTable::attrCache[RELCAT_RELID] = head;
    // curr = AttrCacheTable::attrCache[RELCAT_RELID];
    //verified
    // attrCatBlock.getRecord(attrCatRecord, 6);
    // AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry.attrCatEntry);
    // attrCacheEntry.recId.block = ATTRCAT_BLOCK;
    // attrCacheEntry.recId.slot = 6;
    
    // head = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
    // *head = attrCacheEntry;
    // curr = head;
    // for(int i = 7; i < 12; i++){
    //     attrCatBlock.getRecord(attrCatRecord, i);
    //     struct AttrCacheEntry attrCacheEntry;
    //     AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry.attrCatEntry);
    //     attrCacheEntry.recId.block = ATTRCAT_BLOCK;
    //     attrCacheEntry.recId.slot = i;
    //     curr->next = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
    //     *(curr->next) = attrCacheEntry;
    //     curr = curr->next;
    // }
    // curr->next = nullptr;
    // AttrCacheTable::attrCache[ATTRCAT_RELID] = head;
}

int OpenRelTable::openRel(char relName[ATTR_SIZE]){
    int ret = OpenRelTable::getRelId(relName);
    if(ret != E_RELNOTOPEN){
        return ret;
    }
    int relId = OpenRelTable::getFreeOpenRelTableEntry();
    if(relId == E_CACHEFULL){
        return E_CACHEFULL;
    }
    //reset search index of relation catalog
    RelCacheTable::resetSearchIndex(RELCAT_RELID);

    //search using linear search to find
    //blockid and slot of relName in relation Catalog
    union Attribute relNameattr;
    strcpy(relNameattr.sVal, relName);
    RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, relNameattr, EQ);
    
    if(relcatRecId.block == -1 && relcatRecId.slot == -1){
        return E_RELNOTEXIST;
    }
    //get the record from relation catalog
    RecBuffer relCatBlock(RELCAT_BLOCK);
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    relCatBlock.getRecord(relCatRecord, relcatRecId.slot);
    struct RelCacheEntry* relCacheEntry = (struct RelCacheEntry*)malloc(sizeof(struct RelCacheEntry));
    RelCacheTable::recordToRelCatEntry(relCatRecord, &relCacheEntry->relCatEntry);

    //set the recId of the record in relation catalog
    relCacheEntry->recId.block = RELCAT_BLOCK;
    relCacheEntry->recId.slot = relcatRecId.slot;

    //set the relCacheEntry in relCache
    RelCacheTable::relCache[relId] = relCacheEntry;

    //reset search index of attribute catalog
    RelCacheTable::resetSearchIndex(ATTRCAT_RELID);

    //declare listHead
    struct AttrCacheEntry* listHead;
    struct AttrCacheEntry* dummy = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
    dummy->next = nullptr;
    listHead = dummy;

    //search using linear search to find all attributes of relName
    union Attribute attrCatrelName;
    strcpy(attrCatrelName.sVal, relName);

    while(true){
        RecId attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, attrCatrelName, EQ);
        if(attrcatRecId.block == -1 && attrcatRecId.slot == -1){
            break;
        }
        struct AttrCacheEntry* attrCacheEntry = (struct AttrCacheEntry*)malloc(sizeof(struct AttrCacheEntry));
        RecBuffer attrCatBlock(attrcatRecId.block);
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatBlock.getRecord(attrCatRecord, attrcatRecId.slot);
        AttrCacheTable::recordToAttrCatEntry(attrCatRecord, &attrCacheEntry->attrCatEntry);
        // printf("Here %s \n", attrCacheEntry->attrCatEntry.attrName);
        attrCacheEntry->recId.block = attrcatRecId.block;
        attrCacheEntry->recId.slot = attrcatRecId.slot;
        dummy->next = attrCacheEntry;
        dummy = dummy->next;
    }
    dummy->next = nullptr;
    AttrCacheTable::attrCache[relId] = listHead->next;
    //debug print
    listHead = AttrCacheTable::attrCache[relId];
    while(listHead){
        // printf("%s \n", listHead->attrCatEntry.attrName);
        listHead = listHead->next;
    }
    free(listHead);

    OpenRelTable::tableMetaInfo[relId].free = false;
    strcpy(OpenRelTable::tableMetaInfo[relId].relName, relName);

    return relId;
}

int OpenRelTable::closeRel(int relId){
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID){
        return E_NOTPERMITTED;
    }
    if(relId < 0 || relId >= MAX_OPEN){
        return E_OUTOFBOUND;
    }
    if(OpenRelTable::tableMetaInfo[relId].free){
        return E_RELNOTOPEN;
    }
    //free the allocated memory
    free(RelCacheTable::relCache[relId]);
    RelCacheTable::relCache[relId] = nullptr;
    free(AttrCacheTable::attrCache[relId]);
    AttrCacheTable::attrCache[relId] = nullptr;

    OpenRelTable::tableMetaInfo[relId].free = true;
    return SUCCESS;
}
int OpenRelTable::getRelId(char relName[ATTR_SIZE]){
    for(int i=0; i < MAX_OPEN; i++){
        if(!strcmp(tableMetaInfo[i].relName, relName)){
            return i;
        }
    }
    return E_RELNOTOPEN;
}



int OpenRelTable::getFreeOpenRelTableEntry(){
    for(int i=0; i < MAX_OPEN; i++){
        if(tableMetaInfo[i].free){
            return i;
        }
    }
    return E_CACHEFULL;
}

OpenRelTable::~OpenRelTable() {
    for(int i=0;i < MAX_OPEN; i++){
        if(RelCacheTable::relCache[i] != NULL){
            free(RelCacheTable::relCache[i]);
        }
        if(AttrCacheTable::attrCache[i] != NULL){
            free(AttrCacheTable::attrCache[i]);
        }
    }
    //close all open relations from rel-id 2
    for(int i=2; i < MAX_OPEN; i++){
        if(!tableMetaInfo[i].free) {
            OpenRelTable::closeRel(i);
        }
    }
}