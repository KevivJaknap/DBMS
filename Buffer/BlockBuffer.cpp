#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>

BlockBuffer::BlockBuffer(int blockNum) {
    this->blockNum = blockNum;
}

RecBuffer::RecBuffer(int blockNum) : BlockBuffer::BlockBuffer(blockNum) {}

int BlockBuffer::getHeader(struct HeadInfo *head){
    
    // unsigned char buffer[BLOCK_SIZE];
    // Disk::readBlock(buffer, blockNum);
    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if (ret != SUCCESS) {
        return ret;
    }
       
    memcpy(&head->numSlots, bufferPtr+24, 4);
    memcpy(&head->numEntries, bufferPtr+16, 4);
    memcpy(&head->numAttrs, bufferPtr+20, 4);
    memcpy(&head->rblock, bufferPtr+12, 4);
    memcpy(&head->lblock, bufferPtr+8, 4);

    return SUCCESS;
}

int RecBuffer::getRecord(union Attribute *rec, int slotNum){
    struct HeadInfo head;

    getHeader(&head);

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    unsigned char *bufferPtr;
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if (ret != SUCCESS) {
        return ret;
    }
    // unsigned char buffer[BLOCK_SIZE];
    // Disk::readBlock(buffer, blockNum);

    int recordSize = attrCount*ATTR_SIZE;
    unsigned char *slotPointer = bufferPtr + 32 + slotCount + slotNum*recordSize;
    memcpy(rec, slotPointer, recordSize);

    return SUCCESS;
}

int BlockBuffer::loadBlockAndGetBufferPtr(unsigned char **bufferPtr){
    int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

    if(bufferNum == E_BLOCKNOTINBUFFER){
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

        if (bufferNum == E_OUTOFBOUND) {
            return E_OUTOFBOUND;
        }
        
        Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
    }
    *bufferPtr = StaticBuffer::blocks[bufferNum];
    return SUCCESS;
}
int RecBuffer::setRecord(union Attribute *rec, int slotNum){
    unsigned char buffer[BLOCK_SIZE];
    Disk::readBlock(buffer, blockNum);

    struct HeadInfo head;
    getHeader(&head);

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;

    int recordSize = attrCount*ATTR_SIZE;
    unsigned char *slotPointer = buffer + 32 + slotCount + slotNum*recordSize;
    memcpy(slotPointer, rec, recordSize);

    Disk::writeBlock(buffer, blockNum);

    return SUCCESS;
}

int RecBuffer::getSlotMap(unsigned char *slotMap){
    unsigned char *bufferPtr;

    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS){
        return ret;
    }

    struct HeadInfo head;
    getHeader(&head);
    int slotCount = head.numSlots;

    unsigned char *slotMapInBuffer = bufferPtr + HEADER_SIZE;
    memcpy(slotMap, slotMapInBuffer, slotCount);

    return SUCCESS;
}

int compareAttrs(union Attribute attr1, union Attribute attr2, int attrType){
    if(attrType == NUMBER){
        return attr1.nVal - attr2.nVal;
    }
    else if(attrType == STRING){
        return strcmp(attr1.sVal, attr2.sVal);
    }
    else{
        return E_ATTRTYPEMISMATCH;
    }
}
