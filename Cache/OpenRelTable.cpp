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
    //setting name for relation catalog and attribute catalog
    strcpy(OpenRelTable::tableMetaInfo[RELCAT_RELID].relName, RELCAT_RELNAME);
    strcpy(OpenRelTable::tableMetaInfo[ATTRCAT_RELID].relName, ATTRCAT_RELNAME);

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
    int x;
    RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, relNameattr, EQ, x);
    
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
        int x;
        RecId attrcatRecId = BlockAccess::linearSearch(ATTRCAT_RELID, ATTRCAT_ATTR_RELNAME, attrCatrelName, EQ, x);
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
    
    //check if modified
    if(RelCacheTable::relCache[relId]->dirty){
        //get relation catalog entry
        RelCatEntry relCatEntry = RelCacheTable::relCache[relId]->relCatEntry;
        //initialize record
        union Attribute relCatRecord[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&relCatEntry, relCatRecord);

        //perform linear search to find the record in relation catalog
        RelCacheTable::resetSearchIndex(RELCAT_RELID);
        union Attribute relNameattr;
        strcpy(relNameattr.sVal, relCatEntry.relName);
        int x;
        RecId relcatRecId = BlockAccess::linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, relNameattr, EQ, x);
        if(relcatRecId.block == -1 && relcatRecId.slot == -1){
            return E_RELNOTEXIST;
        }

        //declare an object of RecBuffer
        RecBuffer relCatBlock(RELCAT_BLOCK);

        //set the record in relation catalog
        relCatBlock.setRecord(relCatRecord, relcatRecId.slot);
    }

    //free the relCacheEntry
    free(RelCacheTable::relCache[relId]);
    RelCacheTable::relCache[relId] = nullptr;

    //free the attrCacheEntry
    struct AttrCacheEntry* dummy = AttrCacheTable::attrCache[relId];
    while(dummy != nullptr){
        struct AttrCacheEntry* temp = dummy;
        dummy = dummy->next;
        free(temp);
    }

    AttrCacheTable::attrCache[relId] = nullptr;

    //set openRelTable entry to free
    OpenRelTable::tableMetaInfo[relId].free = true;
    return SUCCESS;
}

int OpenRelTable::getRelId(char relName[ATTR_SIZE]){
    for(int i=0; i < MAX_OPEN; i++){
        if(!strcmp(tableMetaInfo[i].relName, relName) && !tableMetaInfo[i].free){
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
    //close all open relations from rel-id 2
    for(int i=2; i < MAX_OPEN; i++){
        if(!tableMetaInfo[i].free) {
            OpenRelTable::closeRel(i);
        }
    }
    //checking if relCatentry of attribute catalog is modified
    if (RelCacheTable::relCache[ATTRCAT_RELID]->dirty){
        //get relation catalog entry
        RelCatEntry relCatEntry = RelCacheTable::relCache[ATTRCAT_RELID]->relCatEntry;
        //initialize record
        union Attribute relCatRecord[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&relCatEntry, relCatRecord);

        int blockNum = RELCAT_BLOCK;
        int slotNum = RELCAT_SLOTNUM_FOR_ATTRCAT;

        RecBuffer recBuffer(blockNum);
        recBuffer.setRecord(relCatRecord, slotNum);
    }
    //free the memory allocated to relCacheEntry of attribute catalog
    free(RelCacheTable::relCache[ATTRCAT_RELID]);
    RelCacheTable::relCache[ATTRCAT_RELID] = nullptr;

    //release the relation catalog
    if (RelCacheTable::relCache[RELCAT_RELID]->dirty){
        //get relation catalog entry
        RelCatEntry relCatEntry = RelCacheTable::relCache[RELCAT_RELID]->relCatEntry;
        //initialize record
        union Attribute relCatRecord[RELCAT_NO_ATTRS];
        RelCacheTable::relCatEntryToRecord(&relCatEntry, relCatRecord);

        int blockNum = RELCAT_BLOCK;
        int slotNum = RELCAT_SLOTNUM_FOR_RELCAT;

        RecBuffer recBuffer(blockNum);
        recBuffer.setRecord(relCatRecord, slotNum);
    }
    //free the memory allocated to relCacheEntry of relation catalog
    free(RelCacheTable::relCache[RELCAT_RELID]);
    RelCacheTable::relCache[RELCAT_RELID] = nullptr;

    //release the attribute catalog
    struct AttrCacheEntry* dummy = AttrCacheTable::attrCache[RELCAT_RELID];
    while(dummy != nullptr){
        struct AttrCacheEntry* temp = dummy;
        dummy = dummy->next;
        free(temp);
    }
    AttrCacheTable::attrCache[RELCAT_RELID] = nullptr;

    struct AttrCacheEntry* dummy2 = AttrCacheTable::attrCache[ATTRCAT_RELID];
    while(dummy2 != nullptr){
        struct AttrCacheEntry* temp = dummy2;
        dummy2 = dummy2->next;
        free(temp);
    }
    AttrCacheTable::attrCache[ATTRCAT_RELID] = nullptr;
}