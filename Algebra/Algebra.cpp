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
        return ret;
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
                    printf(" %d |", (int)rec[i].nVal);
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

bool isNumber(char* str){
    int len;
    float ignore;

    int ret = sscanf(str, "%f %n", &ignore, &len);
    return ret == 1 && len == strlen(str);
}



