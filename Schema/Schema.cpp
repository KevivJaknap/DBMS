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