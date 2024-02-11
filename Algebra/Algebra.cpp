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
    int comps = 0;
    while(BlockAccess::search(srcRelId, record, attr, attrVal, op, comps) == SUCCESS){
        ret = BlockAccess::insert(targetRelId, record);

        if (ret != SUCCESS){
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
    }
    printf("The number of comparisons made is %d\n", comps);

    ret = Schema::closeRel(targetRel);
    if (ret != SUCCESS){
        return ret;
    }

    return SUCCESS;


    // RelCacheTable::resetSearchIndex(srcRelId);

    // RelCatEntry relCatEntry;
    // RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    // int attrTypes[relCatEntry.numAttrs];
    // printf("|");
    // for(int i = 0; i < relCatEntry.numAttrs; i++){
    //     AttrCatEntry attrCatEntry;
    //     AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
    //     printf(" %s |", attrCatEntry.attrName);
    //     attrTypes[i] = attrCatEntry.attrType;
    // }
    // printf("\n");

    // while (true) {
    //     RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);
    //     // printf("SearchRes block %d slot %d\n", searchRes.block, searchRes.slot);
    //     if(searchRes.block != -1 && searchRes.slot != -1){
    //         Attribute rec[relCatEntry.numAttrs];
    //         RecBuffer recBuffer(searchRes.block);
    //         recBuffer.getRecord(rec, searchRes.slot);
    //         for(int i=0; i<relCatEntry.numAttrs; i++){
    //             if(attrTypes[i] == NUMBER){
    //                 printf(" %.2f |", rec[i].nVal);
    //             }
    //             else if(attrTypes[i] == STRING){
    //                 printf(" %s |", rec[i].sVal);
    //             }
    //         }
    //         printf("\n");
    //     }
    //     else {
    //         break;
    //     }
    // }
    // return SUCCESS;
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
        // printf("Inserting record ");
        // for(int i=0; i<tar_nAttrs; i++){
        //     if(attr_types[i] == NUMBER){
        //         printf(" %.2f |", proj_record[i].nVal);
        //     }
        //     else if(attr_types[i] == STRING){
        //         printf(" %s |", proj_record[i].sVal);
        //     }
        // }
        // printf("\n");
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
    RelCatEntry* relCatEntry = (RelCatEntry*)malloc(sizeof(RelCatEntry));
    RelCacheTable::getRelCatEntry(relId, relCatEntry);

    //check if number of attributes match
    if(relCatEntry->numAttrs != nAttrs){
        return E_NATTRMISMATCH;
    }

    //declare record 
    Attribute recordAttr[nAttrs];

    //get attribute catalog entry of each attribute for type
    for(int i=0; i<nAttrs; i++){
        AttrCatEntry* attrCatEntry = (AttrCatEntry*)malloc(sizeof(AttrCatEntry));
        AttrCacheTable::getAttrCatEntry(relId, i, attrCatEntry);
        
        int type = attrCatEntry->attrType;

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

bool isNumber(char* str){
    int len;
    float ignore;

    int ret = sscanf(str, "%f %n", &ignore, &len);
    return ret == 1 && len == strlen(str);
}