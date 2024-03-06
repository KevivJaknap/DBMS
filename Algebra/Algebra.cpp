#include "Algebra.h"

#include <cstring>
#include <iostream>

bool isNumber(char* str);

int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]){
    int srcRelId = OpenRelTable::getRelId(srcRel);
    if (srcRelId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId, attr, &attrCatEntry);
    if (ret != SUCCESS) {
        return E_ATTRNOTEXIST;
    }
    int type = attrCatEntry.attrType;
    Attribute attrVal;
    if (type == NUMBER) {
        if (isNumber(strVal)) {
            attrVal.nVal = atof(strVal);
        } else {
            return E_ATTRTYPEMISMATCH;
        }
    } else if(type == STRING){
        strcpy(attrVal.sVal, strVal);
    }

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    int src_nAttrs = relCatEntry.numAttrs;
    char attr_names[src_nAttrs][ATTR_SIZE];
    int attr_types[src_nAttrs];

    for (int i=0; i<src_nAttrs; i++) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
        strcpy(attr_names[i], attrCatEntry.attrName);
        attr_types[i] = attrCatEntry.attrType;
    }

    //creating relation using Schema
    ret = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);
    if (ret != SUCCESS) {
        return ret;
    }

    //open the newly created relation
    int targetRelId = OpenRelTable::openRel(targetRel);
    if (targetRelId < 0 || targetRelId > MAX_OPEN){
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    //resetting the search index
    ret = RelCacheTable::resetSearchIndex(srcRelId);
    if (ret != SUCCESS){
        return ret;
    }
    ret = AttrCacheTable::resetSearchIndex(srcRelId, attr);
    if (ret != SUCCESS){
        return ret;
    }

    
    Attribute record[src_nAttrs];
    while(BlockAccess::search(srcRelId, record, attr, attrVal, op) == SUCCESS){
        ret = BlockAccess::insert(targetRelId, record);

        if (ret != SUCCESS){
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    ret = Schema::closeRel(targetRel);
    if (ret != SUCCESS){
        return ret;
    }

    return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]){

    int srcRelId = OpenRelTable::getRelId(srcRel);
    if (srcRelId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    int src_nAttrs = relCatEntry.numAttrs;
    char attr_names[src_nAttrs][ATTR_SIZE];
    int attr_types[src_nAttrs];

    for (int i=0; i<src_nAttrs; i++) {
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
        strcpy(attr_names[i], attrCatEntry.attrName);
        attr_types[i] = attrCatEntry.attrType;
    }

    //creating relation using Schema
    int ret = Schema::createRel(targetRel, src_nAttrs, attr_names, attr_types);
    if (ret != SUCCESS) {
        return ret;
    }

    int targetRelId = OpenRelTable::openRel(targetRel);
    if (targetRelId < 0 || targetRelId > MAX_OPEN){
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    RelCacheTable::resetSearchIndex(srcRelId);

    Attribute record[src_nAttrs];

    while(BlockAccess::project(srcRelId, record) == SUCCESS){
        ret = BlockAccess::insert(targetRelId, record);

        if (ret != SUCCESS){
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    ret = Schema::closeRel(targetRel);
    if (ret != SUCCESS){
        return ret;
    }

    return SUCCESS;
}

int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], int tar_nAttrs, char tar_attrs[][ATTR_SIZE]){
    int srcRelId = OpenRelTable::getRelId(srcRel);
    if (srcRelId == E_RELNOTOPEN) {
        return E_RELNOTOPEN;
    }

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    int src_nAttrs = relCatEntry.numAttrs;
    
    int attr_offset[tar_nAttrs];
    int attr_types[tar_nAttrs];
    for (int i=0; i<tar_nAttrs; i++){
        AttrCatEntry attrCatEntry;
        int ret = AttrCacheTable::getAttrCatEntry(srcRelId, tar_attrs[i], &attrCatEntry);
        if (ret != SUCCESS) {
            return E_ATTRNOTEXIST;
        }
        attr_offset[i] = attrCatEntry.offset;
        attr_types[i] = attrCatEntry.attrType;
    }

    int ret = Schema::createRel(targetRel, tar_nAttrs, tar_attrs, attr_types);
    if (ret != SUCCESS) {
        return ret;
    }

    int targetRelId = OpenRelTable::openRel(targetRel);
    if (targetRelId < 0 || targetRelId > MAX_OPEN){
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    RelCacheTable::resetSearchIndex(srcRelId);
    Attribute record[src_nAttrs];

    while(BlockAccess::project(srcRelId, record) == SUCCESS){
        Attribute proj_record[tar_nAttrs];
        for (int i=0; i<tar_nAttrs; i++){
            if(attr_types[i] == NUMBER){
                proj_record[i].nVal = record[attr_offset[i]].nVal;
            }
            else if(attr_types[i] == STRING){
                strcpy(proj_record[i].sVal, record[attr_offset[i]].sVal);
            }
        }

        ret = BlockAccess::insert(targetRelId, proj_record);
        if (ret != SUCCESS){
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }

    ret = Schema::closeRel(targetRel);
    if (ret != SUCCESS){
        return ret;
    }

    return SUCCESS;
}

int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE]){
    //check if relname is relation catalog or attribute catalog
    bool checkRelName = !strcmp(relName, RELCAT_RELNAME) || !strcmp(relName, ATTRCAT_RELNAME);
    if(checkRelName){
        return E_NOTPERMITTED;
    }

    //get relId
    int relId = OpenRelTable::getRelId(relName);
    if(relId == E_RELNOTOPEN){
        return E_RELNOTOPEN;
    }

    //get relation catalog entry of the relation
    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(relId, &relCatEntry);

    //check if number of attributes match
    if(relCatEntry.numAttrs != nAttrs){
        return E_NATTRMISMATCH;
    }

    //declare record 
    Attribute recordAttr[nAttrs];

    //get attribute catalog entry of each attribute for type
    for(int i=0; i<nAttrs; i++){
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(relId, i, &attrCatEntry);
        
        int type = attrCatEntry.attrType;

        if(type == NUMBER){
            //check if record[i] can be converted to number
            if(isNumber(record[i])){
                recordAttr[i].nVal = atof(record[i]);
            }
            else{
                return E_ATTRTYPEMISMATCH;
            }
        }
        else if(type == STRING){
            strcpy(recordAttr[i].sVal, record[i]);
        }
    }

    //insert the record by calling BlockAccess::insert() function
    int retVal = BlockAccess::insert(relId, recordAttr);

    return retVal;
}

int Algebra::join(char srcRelation1[ATTR_SIZE], char srcRelation2[ATTR_SIZE], char targetRelation[ATTR_SIZE], 
char attribute1[ATTR_SIZE], char attribute2[ATTR_SIZE]){
    //get the srcRelation1's relId
    int srcRelId1 = OpenRelTable::getRelId(srcRelation1);
    if(srcRelId1 == E_RELNOTOPEN){
        return E_RELNOTOPEN;
    }

    //get the srcRelation2's relId
    int srcRelId2 = OpenRelTable::getRelId(srcRelation2);
    if(srcRelId2 == E_RELNOTOPEN){
        return E_RELNOTOPEN;
    }
    //getting RelCache entries
    RelCatEntry relCatEntry1, relCatEntry2;
    RelCacheTable::getRelCatEntry(srcRelId1, &relCatEntry1);
    RelCacheTable::getRelCatEntry(srcRelId2, &relCatEntry2);

    int numOfAttributes1 = relCatEntry1.numAttrs;
    int numOfAttributes2 = relCatEntry2.numAttrs;

    //getting attribute catalog entries
    AttrCatEntry attrCatEntry1, attrCatEntry2;
    int ret = AttrCacheTable::getAttrCatEntry(srcRelId1, attribute1, &attrCatEntry1);
    if(ret != SUCCESS){
        return E_ATTRNOTEXIST;
    }

    ret = AttrCacheTable::getAttrCatEntry(srcRelId2, attribute2, &attrCatEntry2);
    if(ret != SUCCESS){
        return E_ATTRNOTEXIST;
    }

    //checking if the attribute types are same
    if(attrCatEntry1.attrType != attrCatEntry2.attrType){
        return E_ATTRTYPEMISMATCH;
    }

    
    //iterating through all the attributes of srcRelation1 and srcRelation2
    //and checking if the attribute names are same except for the attribute1 and attribute2

    for(int i=0; i<numOfAttributes1; i++){
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId1, i, &attrCatEntry);
        if(strcmp(attrCatEntry.attrName, attribute1) != 0){
            for(int j=0; j<numOfAttributes2; j++){
                AttrCatEntry attrCatEntry_j;
                AttrCacheTable::getAttrCatEntry(srcRelId2, j, &attrCatEntry_j);
                if(strcmp(attrCatEntry_j.attrName, attribute2) != 0){
                    if(strcmp(attrCatEntry.attrName, attrCatEntry_j.attrName) == 0){
                        return E_DUPLICATEATTR;
                    }
                }
            }
        }
    }

    //check if relation2 has an index on attribute2 if not create one
    bool isIndexPresent = (attrCatEntry2.rootBlock != -1);
    if(!isIndexPresent){
        int ret = BPlusTree::bPlusCreate(srcRelId2, attribute2);
        if(ret != SUCCESS){
            return ret;
        }
    }

    int numOfAttributesInTarget = numOfAttributes1 + numOfAttributes2 - 1;

    char targetRelAttrNames[numOfAttributesInTarget][ATTR_SIZE];
    int targetRelAttrTypes[numOfAttributesInTarget];

    //get attributes from srcRelation1
    for(int i=0; i<numOfAttributes1; i++){
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId1, i, &attrCatEntry);
        strcpy(targetRelAttrNames[i], attrCatEntry.attrName);
        targetRelAttrTypes[i] = attrCatEntry.attrType;
    }

    //get attributes from srcRelation2 except for attribute2
    int k=0;
    for(int i=0; i<numOfAttributes2; i++){
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId2, i, &attrCatEntry);
        if(strcmp(attrCatEntry.attrName, attribute2) != 0){
            strcpy(targetRelAttrNames[numOfAttributes1+k], attrCatEntry.attrName);
            targetRelAttrTypes[numOfAttributes1+k] = attrCatEntry.attrType;
            k++;
        }
    }

    //create targetRelation using Schema
    ret = Schema::createRel(targetRelation, numOfAttributesInTarget, targetRelAttrNames, targetRelAttrTypes);
    if(ret != SUCCESS){
        return ret;
    }

    //open the newly created relation
    int targetRelId = OpenRelTable::openRel(targetRelation);

    if(targetRelId < 0 || targetRelId > MAX_OPEN){
        Schema::deleteRel(targetRelation);
        return targetRelId;
    }

    Attribute record1[numOfAttributes1], record2[numOfAttributes2];
    Attribute targetRecord[numOfAttributesInTarget];

    RelCacheTable::resetSearchIndex(srcRelId1);

    while (BlockAccess::project(srcRelId1, record1) == SUCCESS){
        //reset the search index of srcRelation2
        RelCacheTable::resetSearchIndex(srcRelId2);

        //reset the search index of attribute2 in AttrCacheTable
        AttrCacheTable::resetSearchIndex(srcRelId2, attribute2);

        while (BlockAccess::search(srcRelId2, record2, attribute2, record1[attrCatEntry1.offset],
        EQ) == SUCCESS){
            //copy the record1 and record2 (except for attribute2) to targetRecord
            for(int i=0; i<numOfAttributes1; i++){
                targetRecord[i] = record1[i];
            }

            k = 0;
            for(int i=0; i<numOfAttributes2; i++){
                AttrCatEntry attrCatEntry;
                AttrCacheTable::getAttrCatEntry(srcRelId2, i, &attrCatEntry);
                if(strcmp(attrCatEntry.attrName, attribute2) != 0){
                    targetRecord[numOfAttributes1+k] = record2[i];
                    k++;
                }
            }

            //insert the targetRecord to targetRelation
            ret = BlockAccess::insert(targetRelId, targetRecord);
            if(ret != SUCCESS){
                Schema::closeRel(targetRelation);
                Schema::deleteRel(targetRelation);
                return ret;
            }        
        }
        
    }

    ret = Schema::closeRel(targetRelation);
    if(ret != SUCCESS){
        return ret;
    }

    return SUCCESS;
}

bool isNumber(char* str){
    int len;
    float ignore;

    int ret = sscanf(str, "%f %n", &ignore, &len);
    return ret == 1 && len == strlen(str);
}