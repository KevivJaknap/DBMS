#include "Schema.h"

#include <cmath>
#include <cstring>

int Schema::openRel(char relName[ATTR_SIZE]){
    int ret = OpenRelTable::openRel(relName);

    if(ret >= 0){
        return SUCCESS;
    }

    return ret;
}

int Schema::closeRel(char relName[ATTR_SIZE]){
    if(!strcmp(relName, RELCAT_RELNAME) || !strcmp(relName, ATTRCAT_RELNAME)){
        return E_NOTPERMITTED;
    }

    int relId = OpenRelTable::getRelId(relName);

    if(relId == E_RELNOTOPEN){
        return E_RELNOTOPEN;
    }

    return OpenRelTable::closeRel(relId);
}

int Schema::renameRel(char oldRelName[ATTR_SIZE], char newRelName[ATTR_SIZE]){
    //check if oldRelName or newRelName is Relation Catalog or Attribute Catalog
    bool checkOld = !strcmp(oldRelName, RELCAT_RELNAME) || !strcmp(oldRelName, ATTRCAT_RELNAME);
    bool checkNew = !strcmp(newRelName, RELCAT_RELNAME) || !strcmp(newRelName, ATTRCAT_RELNAME);
    if(checkOld || checkNew){
        return E_NOTPERMITTED;
    }

    //check if oldRelName is open
    int relId = OpenRelTable::getRelId(oldRelName);
    if(relId != E_RELNOTOPEN){
        return E_RELOPEN;
    }

    int retVal = BlockAccess::renameRelation(oldRelName, newRelName);
    return retVal;
}

int Schema::renameAttr(char relName[ATTR_SIZE], char oldAttrName[ATTR_SIZE], char newAttrName[ATTR_SIZE]){
    //check if relName is Relation Catalog or Attribute Catalog
    bool checkRel = !strcmp(relName, RELCAT_RELNAME) || !strcmp(relName, ATTRCAT_RELNAME);
    if(checkRel){
        return E_NOTPERMITTED;
    }

    //check if relName is open
    int relId = OpenRelTable::getRelId(relName);
    if(relId != E_RELNOTOPEN){
        return E_RELOPEN;
    }

    int retVal = BlockAccess::renameAttribute(relName, oldAttrName, newAttrName);
    return retVal;
}

int Schema::createRel(char relName[ATTR_SIZE], int nAttrs, char attrs[][ATTR_SIZE], int attrtype[]){
    Attribute relNameAsAttr;
    strcpy(relNameAsAttr.sVal, relName);

    RecId targetRecId;
    RelCacheTable::resetSearchIndex(RELCAT_RELID);
    //search for the relation in relation catalog
    targetRecId = BlockAccess::linearSearch(RELCAT_RELID, RELCAT_ATTR_RELNAME, relNameAsAttr, EQ);

    if(targetRecId.block != -1){
        return E_RELEXIST;
    }

    //compare every pair of attributes in attrs array
    for(int i = 0; i < nAttrs; i++){
        for(int j = i + 1; j < nAttrs; j++){
            if(!strcmp(attrs[i], attrs[j])){
                return E_DUPLICATEATTR;
            }
        }
    }

    //declaring record which will be used to store the new record in relation catalog
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    strcpy(relCatRecord[RELCAT_REL_NAME_INDEX].sVal, relName);
    relCatRecord[RELCAT_NO_ATTRIBUTES_INDEX].nVal = nAttrs;
    relCatRecord[RELCAT_NO_RECORDS_INDEX].nVal = 0;
    relCatRecord[RELCAT_FIRST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_LAST_BLOCK_INDEX].nVal = -1;
    relCatRecord[RELCAT_NO_SLOTS_PER_BLOCK_INDEX].nVal = floor((2016/ (16 * nAttrs + 1)));

    //inserting the record in relation catalog
    int retVal = BlockAccess::insert(RELCAT_RELID, relCatRecord);
    if(retVal != SUCCESS){
        return retVal;
    }

    //iterating through attributes and creating attribute catalog records
    for(int i=0; i<nAttrs; i++){
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        strcpy(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relName);
        strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrs[i]);
        attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal = attrtype[i];
        attrCatRecord[ATTRCAT_PRIMARY_FLAG_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_ROOT_BLOCK_INDEX].nVal = -1;
        attrCatRecord[ATTRCAT_OFFSET_INDEX].nVal = i;

        retVal = BlockAccess::insert(ATTRCAT_RELID, attrCatRecord);
        if(retVal != SUCCESS){
            Schema::deleteRel(relName);
            return E_DISKFULL;
        }
    }
    return SUCCESS;
}

int Schema::deleteRel(char relName[ATTR_SIZE]){

    bool checkRel = !strcmp(relName, RELCAT_RELNAME) || !strcmp(relName, ATTRCAT_RELNAME);
    if(checkRel){
        return E_NOTPERMITTED;
    }

    //check if relation is open (not allowed if open)
    int relId = OpenRelTable::getRelId(relName);
    if(relId != E_RELNOTOPEN){
        return E_RELOPEN;
    }

    int ret = BlockAccess::deleteRelation(relName);
    return ret;
}

int Schema::createIndex(char relName[ATTR_SIZE], char attrName[ATTR_SIZE]){
    //check if relName is Relation Catalog or Attribute Catalog
    bool checkRel = !strcmp(relName, RELCAT_RELNAME) || !strcmp(relName, ATTRCAT_RELNAME);
    if(checkRel){
        return E_NOTPERMITTED;
    }

    //check if relName is open
    int relId = OpenRelTable::getRelId(relName);
    if(relId == E_RELNOTOPEN){
        return E_RELNOTOPEN;
    }

    return BPlusTree::bPlusCreate(relId, attrName);
}

int Schema::dropIndex(char *relName, char *attrName){
    //check if relName is Relation Catalog or Attribute Catalog
    bool checkRel = !strcmp(relName, RELCAT_RELNAME) || !strcmp(relName, ATTRCAT_RELNAME);
    if(checkRel){
        return E_NOTPERMITTED;
    }

    //check if relName is open
    int relId = OpenRelTable::getRelId(relName);
    if(relId == E_RELNOTOPEN){
        return E_RELNOTOPEN;
    }

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if(ret != SUCCESS){
        return ret;
    }

    int rootBlock = attrCatEntry.rootBlock;
    if(rootBlock == -1){
        return E_NOINDEX;
    }

    BPlusTree::bPlusDestroy(rootBlock);

    attrCatEntry.rootBlock = -1;
    ret = AttrCacheTable::setAttrCatEntry(relId, attrName, &attrCatEntry);
    if(ret != SUCCESS){
        return ret;
    }
    return SUCCESS;
}