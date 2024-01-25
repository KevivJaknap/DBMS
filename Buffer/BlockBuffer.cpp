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

    //check if already present
    if(bufferNum != E_BLOCKNOTINBUFFER){
        StaticBuffer::metainfo[bufferNum].timeStamp = 0;
        //increment timestamps
        for(int i=0; i<BUFFER_CAPACITY; i++){
            if(!StaticBuffer::metainfo[i].free){
                StaticBuffer::metainfo[i].timeStamp++;
            }
        }
    }
    else{
        bufferNum = StaticBuffer::getFreeBuffer(this->blockNum);

        if(bufferNum == E_OUTOFBOUND){
            return E_OUTOFBOUND;
        }
        //read the block
        Disk::readBlock(StaticBuffer::blocks[bufferNum], this->blockNum);
    }

    *bufferPtr = StaticBuffer::blocks[bufferNum];
    return SUCCESS;
}
int RecBuffer::setRecord(union Attribute *rec, int slotNum){
    unsigned char* bufferPtr;
    //get starting address of buffer containing the block
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS){
        return ret;
    }

    //get Header of the block
    struct HeadInfo head;
    getHeader(&head);

    int attrCount = head.numAttrs;
    int slotCount = head.numSlots;
    int recordSize = attrCount*ATTR_SIZE;
    
    //check if input slot number is valid
    if(slotNum >= slotCount){
        return E_OUTOFBOUND;
    }

    //offset bufferPtr to the beginning of the record
    bufferPtr = bufferPtr + 32 + slotCount + slotNum*recordSize;

    //copy the record
    memcpy(bufferPtr, rec, recordSize);
    //update dirty bit
    StaticBuffer::setDirtyBit(this->blockNum);

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
