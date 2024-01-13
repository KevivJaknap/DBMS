#include "StaticBuffer.h"

unsigned char StaticBuffer::blocks[BUFFER_CAPACITY][BLOCK_SIZE];
struct BufferMetaInfo StaticBuffer::metainfo[BUFFER_CAPACITY];

StaticBuffer::StaticBuffer() {
    for(int i=0; i<BUFFER_CAPACITY; i++){
        metainfo[i].free = true;
    }
}

StaticBuffer::~StaticBuffer() {}

int StaticBuffer::getFreeBuffer(int blockNum) {
    if (blockNum < 0 || blockNum >= DISK_BLOCKS) {
        return E_OUTOFBOUND;
    }
    int allocatedBuffer;
    for(int index = 0; index < BUFFER_CAPACITY; index++){
        if(metainfo[index].free){
            allocatedBuffer = index;
            break;
        }
    }
    metainfo[allocatedBuffer].free = false;
    metainfo[allocatedBuffer].blockNum = blockNum;

    return allocatedBuffer;
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