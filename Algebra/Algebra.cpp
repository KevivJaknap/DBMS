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
        return E_RELNOTOPEN;
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

    RelCacheTable::resetSearchIndex(srcRelId);

    RelCatEntry relCatEntry;
    RelCacheTable::getRelCatEntry(srcRelId, &relCatEntry);

    int attrTypes[relCatEntry.numAttrs];
    printf("|");
    for(int i = 0; i < relCatEntry.numAttrs; i++){
        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId, i, &attrCatEntry);
        printf(" %s |", attrCatEntry.attrName);
        attrTypes[i] = attrCatEntry.attrType;
    }
    printf("\n");

    while (true) {
        RecId searchRes = BlockAccess::linearSearch(srcRelId, attr, attrVal, op);
        // printf("SearchRes block %d slot %d\n", searchRes.block, searchRes.slot);
        if(searchRes.block != -1 && searchRes.slot != -1){
            Attribute rec[relCatEntry.numAttrs];
            RecBuffer recBuffer(searchRes.block);
            recBuffer.getRecord(rec, searchRes.slot);
            for(int i=0; i<relCatEntry.numAttrs; i++){
                if(attrTypes[i] == NUMBER){
                    printf(" %.2f |", rec[i].nVal);
                }
                else if(attrTypes[i] == STRING){
                    printf(" %s |", rec[i].sVal);
                }
            }
            printf("\n");
        }
        else {
            break;
        }
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



