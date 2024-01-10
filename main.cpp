#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>

void change();
int main(int argc, char *argv[]) {
  /* Initialize the Run Copy of Disk */
  Disk disk_run;
  // StaticBuffer buffer;
  // OpenRelTable cache;

  change();

  RecBuffer relCatBuffer(RELCAT_BLOCK);
  // RecBuffer attrCatBuffer(ATTRCAT_BLOCK);

  HeadInfo relCatHeader;
  // HeadInfo attrCatHeader;

  relCatBuffer.getHeader(&relCatHeader);
  // attrCatBuffer.getHeader(&attrCatHeader);

  // int lblock = attrCatHeader.lblock;
  // int rblock = attrCatHeader.rblock;
  
  for(int i=0;i<relCatHeader.numEntries;i++){
    Attribute relCatRecord[RELCAT_NO_ATTRS];
    relCatBuffer.getRecord(relCatRecord, i);
    printf("Relation: %s\n", relCatRecord[RELCAT_REL_NAME_INDEX].sVal);
    int attrCatBlockNum = ATTRCAT_BLOCK;
    do{
      RecBuffer attrCatBuffer(attrCatBlockNum);
      HeadInfo attrCatHeader;
      attrCatBuffer.getHeader(&attrCatHeader);
      for(int j=0;j<attrCatHeader.numEntries;j++){
        Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
        attrCatBuffer.getRecord(attrCatRecord, j);
        if(!strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relCatRecord[RELCAT_REL_NAME_INDEX].sVal)){
          const char* attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM" : "STR";
          printf(" %s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
        }
      }
      attrCatBlockNum = attrCatHeader.rblock;
    }while(attrCatBlockNum != -1);
    // for(int j=0;j<attrCatHeader.numEntries;j++){
    //   Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
    //   attrCatBuffer.getRecord(attrCatRecord, j);
    //   if(!strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, relCatRecord[RELCAT_REL_NAME_INDEX].sVal)){
    //     const char* attrType = attrCatRecord[ATTRCAT_ATTR_TYPE_INDEX].nVal == NUMBER ? "NUM" : "STR";
    //     printf(" %s: %s\n", attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, attrType);
    //   }
    // }
    printf("\n");
  }

  
  return 0;
  //return FrontendInterface::handleFrontend(argc, argv);
}

void change(){
  unsigned char buffer[BLOCK_SIZE];
  int attrCatIndex = ATTRCAT_BLOCK;

  do{
    int flag = 0;
    RecBuffer attrCatBuffer(attrCatIndex);
    HeadInfo attrCatHeader;
    attrCatBuffer.getHeader(&attrCatHeader);
    for(int i=0;i<attrCatHeader.numEntries;i++){
      Attribute attrCatRecord[ATTRCAT_NO_ATTRS];
      attrCatBuffer.getRecord(attrCatRecord, i);
      if(!strcmp(attrCatRecord[ATTRCAT_REL_NAME_INDEX].sVal, "Students") && !strcmp(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, "Class")){
        strcpy(attrCatRecord[ATTRCAT_ATTR_NAME_INDEX].sVal, "Batch");
        attrCatBuffer.setRecord(attrCatRecord, i);
        flag = 1;
        break;
      }
    }
    if(flag){
      break;
    }
    attrCatIndex = attrCatHeader.rblock;
  }while(attrCatIndex != -1);
}