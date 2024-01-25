#include "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];

StaticBuffer::StaticBuffer() {
    for(int i=0; i<BUFFER_CAPACITY; i++){
        metainfo[i].free = true;
        metainfo[i].dirty = false;
        metainfo[i].timeStamp = -1;
        metainfo[i].blockNum = -1;
    }
}

StaticBuffer::~StaticBuffer() {
    for(int i=0; i<BUFFER_CAPACITY; i++){
        if(metainfo[i].dirty){
            Disk::writeBlock(blocks[i], metainfo[i].blockNum);
        }
    }
}

int StaticBuffer::getFreeBuffer(int blockNum) {
    if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }
    //increase timestamp of all allocated buffers
    for(int i=0; i<BUFFER_CAPACITY; i++){
        if(!metainfo[i].free){
            metainfo[i].timeStamp++;
        }
    }
    
    int bufferNum = -1; //to store buffer number of free/freed buffer

    //iterate and find free buffer
    for(int i=0; i<BUFFER_CAPACITY; i++){
        if(metainfo[i].free){
            bufferNum = i;
            break;
        }
    }

    //if not found, find buffer with largest timestamp
    if(bufferNum == -1){
        int maxTimeStamp = -1;
        for(int i=0; i<BUFFER_CAPACITY; i++){
            if(metainfo[i].timeStamp > maxTimeStamp){
                maxTimeStamp = metainfo[i].timeStamp;
                bufferNum = i;
            }
        }
    }

    //if dirty, write to disk
    if(metainfo[bufferNum].dirty){
        Disk::writeBlock(blocks[bufferNum], metainfo[bufferNum].blockNum);
    }

    //update metainfo entry
    metainfo[bufferNum].free = false;
    metainfo[bufferNum].dirty = false;
    metainfo[bufferNum].timeStamp = 0;
    metainfo[bufferNum].blockNum = blockNum;

    return bufferNum;
}

int StaticBuffer::getBufferNum(int blockNum) {
    if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }
    for(int index=0; index<BUFFER_CAPACITY; index++){
        if(metainfo[index].blockNum == blockNum){
            return index;
        }
    }
    return E_BLOCKNOTINBUFFER;
}

int StaticBuffer::setDirtyBit(int blockNum){
    int bufferIndex = getBufferNum(blockNum);

    //check if block not in buffer
    if(bufferIndex == E_BLOCKNOTINBUFFER){
        return E_BLOCKNOTINBUFFER;
    }

    //check if out of bound
    if(bufferIndex == E_OUTOFBOUND){
        return E_OUTOFBOUND;
    }

    //else set dirty bit
    metainfo[bufferIndex].dirty = true;
    return SUCCESS;
}