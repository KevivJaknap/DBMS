#include "Buffer/StaticBuffer.h"
#include "Cache/OpenRelTable.h"
#include "Disk_Class/Disk.h"
#include "FrontendInterface/FrontendInterface.h"
#include <iostream>
#include <cstdlib>

int main(int argc, char *argv[]) {
  /* Initialize the Run Copy of Disk */
  Disk disk_run;
  StaticBuffer buffer;
  OpenRelTable cache;
  for(int i=0;i<2;i++){
    RelCatEntry* relCatBuf = (RelCatEntry*)malloc(sizeof(RelCatEntry));
    RelCacheTable::getRelCatEntry(i, relCatBuf);
    printf("Relation: %s\n", relCatBuf->relName);
    for(int j=0;j<relCatBuf->numAttrs;j++){
      AttrCatEntry *attrCatBuf = (AttrCatEntry*)malloc(sizeof(AttrCatEntry));
      AttrCacheTable::getAttrCatEntry(i, j, attrCatBuf);
      char* type;
      type = (attrCatBuf->attrType == 0) ? (char*)"NUM" : (char*)"STR";
      printf("%s %s\n", attrCatBuf->attrName, type);
    }
    printf("\n");
  }
  return 0;
  //return FrontendInterface::handleFrontend(argc, argv);
}
