#include "BlockBuffer.h"

#include <cstdlib>
#include <cstring>

BlockBuffer::BlockBuffer(int blockNum) {
    this->blockNum = blockNum;
}

BlockBuffer::BlockBuffer(char blockType){
    int blockNum = getFreeBlock(blockType);

    this->blockNum = blockNum;
}

RecBuffer::RecBuffer(): BlockBuffer::BlockBuffer('R') {}

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

int BlockBuffer::setHeader(struct HeadInfo *head){
    unsigned char *bufferPtr;

    //get starting address of buffer containing the block
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS){
        return ret;
    }

    // cast bufferPtr to type HeadInfo*
    struct HeadInfo *bufferHeader = (struct HeadInfo*) bufferPtr;

    //copy the values from head to bufferHeader
    bufferHeader->numSlots = head->numSlots;
    bufferHeader->numEntries = head->numEntries;
    bufferHeader->numAttrs = head->numAttrs;
    bufferHeader->rblock = head->rblock;
    bufferHeader->lblock = head->lblock;
    bufferHeader->blockType = head->blockType;
    bufferHeader->pblock = head->pblock;

    //update dirty bit
    ret = StaticBuffer::setDirtyBit(this->blockNum);
    if(ret != SUCCESS){
        return ret;
    }

    return SUCCESS;
}

int BlockBuffer::setBlockType(int blockType){
    unsigned char *bufferPtr;

    //get starting address of buffer containing the block
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);
    if(ret != SUCCESS){
        return ret;
    }

    // cast bufferPtr to type int32_t* and then assign blockType
    *((int32_t *)bufferPtr) = blockType;

    //update in blockAllocMap
    StaticBuffer::blockAllocMap[this->blockNum] = blockType;

    //update dirty bit
    ret = StaticBuffer::setDirtyBit(this->blockNum);
    if(ret != SUCCESS){
        return ret;
    }

    return SUCCESS;
}

int BlockBuffer::getFreeBlock(int blockType){
    int blockNum = -1;
    //iterate through blockAllocMap to find free block
    for(int i=0; i<DISK_BLOCKS; i++){
        if(StaticBuffer::blockAllocMap[i] == UNUSED_BLK){
            blockNum = i;
            break;
        }
    }

    //if no block is free, return E_DISKFULL
    if(blockNum == -1){
        return E_DISKFULL;
    }

    //set object's blockNum
    this->blockNum = blockNum;

    //find a free buffer
    int bufferNum = StaticBuffer::getFreeBuffer(blockNum);

    //initialize header
    struct HeadInfo head;
    head.numSlots = 0;
    head.numEntries = 0;
    head.numAttrs = 0;
    head.rblock = -1;
    head.lblock = -1;
    head.pblock = -1;

    //set header
    setHeader(&head);

    //set blockType
    setBlockType(blockType);

    return blockNum;
}

int BlockBuffer::getBlockNum(){
    return this->blockNum;
}

int RecBuffer::setSlotMap(unsigned char *slotMap){
    unsigned char *bufferPtr;

    //get starting address of buffer
    int ret = loadBlockAndGetBufferPtr(&bufferPtr);

    if(ret != SUCCESS){
        return ret;
    }

    //get Header
    struct HeadInfo head;
    getHeader(&head);

    int numSlots = head.numSlots;

    //copy the contents
    //slotMap starts at bufferPtr + HEADER_SIZE of length numSlots
    memcpy(bufferPtr + HEADER_SIZE, slotMap, numSlots);

    //update dirty bit
    ret = StaticBuffer::setDirtyBit(this->blockNum);

    if(ret != SUCCESS){
        return ret;
    }

    return SUCCESS;
}

void BlockBuffer::releaseBlock(){
    if(this->blockNum != INVALID_BLOCKNUM){
        //get buffer number
        int bufferNum = StaticBuffer::getBufferNum(this->blockNum);

        if(bufferNum != E_BLOCKNOTINBUFFER){
            //set free bit
            StaticBuffer::metainfo[bufferNum].free = true;
            
            StaticBuffer::blockAllocMap[this->blockNum] = UNUSED_BLK;
        }
    }
    this->blockNum = INVALID_BLOCKNUM;
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
