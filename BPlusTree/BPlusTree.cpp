#include "BPlusTree.h"

#include <cstring>

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op){
    //declare searchIndex used to store index of search result
    IndexId searchIndex;

    int ret = AttrCacheTable::getSearchIndex(relId, attrName, &searchIndex);
    
    AttrCatEntry attrCatEntry;
    ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);

    int block, index;

    if(searchIndex.block == -1){
        //i.e, search is done for the first time, so we start from first entry of root
        block = attrCatEntry.rootBlock;
        index = 0;

        if(block == -1){
            return RecId{-1, -1};
        }
    } else {
        block = searchIndex.block;
        index = searchIndex.index + 1;

        //load block using constructor
        IndLeaf leaf(block);

        //load header of leaf
        HeadInfo leafHead;
        leaf.getHeader(&leafHead);

        if(index >= leafHead.numEntries){
            //i.e, we have reached end of leaf, so we move to next leaf
            block = leafHead.rblock;
            index = 0;
            if(block == -1){
                return RecId{-1, -1};
            }
        }

    }

    //search for the record if we are starting from a fresh search
    while(StaticBuffer::getStaticBlockType(block) == IND_INTERNAL){
        IndInternal internalBlk(block);

        HeadInfo intHead;
        internalBlk.getHeader(&intHead);

        InternalEntry intEntry;
        if(op == NE || op == LT || op == LE){
            int ret = internalBlk.getEntry(&intEntry, 0);
            block = intEntry.lChild;
        }
        else{
            bool found = false;
            for(int i=0; i<intHead.numEntries; i++){
                int ret = internalBlk.getEntry(&intEntry, i);
                bool EQorGE = (op == EQ || op == GE);
                if(EQorGE && compareAttrs(intEntry.attrVal, attrVal, attrCatEntry.attrType) >= 0){
                    found = true;
                    break;
                }
                if(op == GT && compareAttrs(intEntry.attrVal, attrVal, attrCatEntry.attrType) > 0){
                    found = true;
                    break;
                }
            }
            if(found){
                block = intEntry.lChild;
            }
            else{
                int ret = internalBlk.getEntry(&intEntry, intHead.numEntries-1);
                block = intEntry.rChild;
            }
        }
    }

    while (block != -1){
        IndLeaf leafBlk(block);
        HeadInfo leafHead;
        leafBlk.getHeader(&leafHead);

        Index leafEntry;

        while(index < leafHead.numEntries){
            int ret = leafBlk.getEntry(&leafEntry, index);
            
            int cmpVal = compareAttrs(leafEntry.attrVal, attrVal, attrCatEntry.attrType);

            if(
                (op == EQ && cmpVal == 0) ||
                (op == LT && cmpVal < 0) ||
                (op == GT && cmpVal > 0) ||
                (op == LE && cmpVal <= 0) ||
                (op == GE && cmpVal >= 0) ||
                (op == NE && cmpVal != 0)
            ) {
                searchIndex.block = block;
                searchIndex.index = index;
                AttrCacheTable::setSearchIndex(relId, attrName, &searchIndex);
                return RecId{leafEntry.block, leafEntry.slot};
            } else if((op == EQ || op == LE || op == LT) && cmpVal > 0){
                return RecId{-1, -1};
            }
            index++;
        }
        if (op != NE){
            break;
        }
        block = leafHead.rblock;
        index = 0;
    }
    return RecId{-1, -1};
}

int BPlusTree::bPlusCreate(int relId, char attrName[ATTR_SIZE]){
    if (relId == RELCAT_RELID || relId == ATTRCAT_RELID){
        return E_NOTPERMITTED;
    }

    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if (ret != SUCCESS){
        return ret;
    }

    if(attrCatEntry.rootBlock != -1){
        return SUCCESS;
    }

    //get a free leaf block
    IndLeaf rootBlockBuf;

    int rootBlock = rootBlockBuf.getBlockNum();

    if(rootBlock == E_DISKFULL){
        return E_DISKFULL;
    }

    attrCatEntry.rootBlock = rootBlock;
    ret = AttrCacheTable::setAttrCatEntry(relId, attrCatEntry.offset, &attrCatEntry);
    if(ret != SUCCESS){
        return ret;
    }

    RelCatEntry relCatEntry;
    ret = RelCacheTable::getRelCatEntry(relId, &relCatEntry);
    if (ret != SUCCESS){
        return ret;
    }

    int block = relCatEntry.firstBlk;

    while (block != -1){
        RecBuffer recBuffer(block);

        //load the slotMap
        unsigned char slotMap[relCatEntry.numSlotsPerBlk];
        recBuffer.getSlotMap(slotMap);

        for(int i=0; i<relCatEntry.numSlotsPerBlk; i++){
            if(slotMap[i] == SLOT_OCCUPIED){
                Attribute record[relCatEntry.numAttrs];
                recBuffer.getRecord(record, i);

                RecId recId{block, i};
                ret = bPlusInsert(relId, attrName, record[attrCatEntry.offset], recId);
                if(ret == E_DISKFULL){
                    return E_DISKFULL;
                }
            }
        }
        HeadInfo headInfo;
        recBuffer.getHeader(&headInfo);
        block = headInfo.rblock;
    }
    return SUCCESS;
}

int BPlusTree::bPlusDestroy(int rootBlockNum){
    if(rootBlockNum < 0 || rootBlockNum >= DISK_BLOCKS){
        return E_OUTOFBOUND;
    }

    int type = StaticBuffer::getStaticBlockType(rootBlockNum);

    if(type == IND_LEAF){
        IndLeaf leaf(rootBlockNum);
        leaf.releaseBlock();
        return SUCCESS;
    } 
    else if (type == IND_INTERNAL) {
        IndInternal internal(rootBlockNum);
        HeadInfo headInfo;

        internal.getHeader(&headInfo);
        int numEntries = headInfo.numEntries;
        //delete the lChild of first Entry

        InternalEntry internalEntry;
        internal.getEntry(&internalEntry, 0);
        int ret = bPlusDestroy(internalEntry.lChild);
        if(ret != SUCCESS){
            return ret;
        }

        //delete the rchild of all entries
        for(int i=0; i<numEntries; i++){
            internal.getEntry(&internalEntry, i);
            ret = bPlusDestroy(internalEntry.rChild);
            if(ret != SUCCESS){
                return ret;
            }
        }
        internal.releaseBlock();
        return SUCCESS;
    }
    else {
        return E_INVALIDBLOCK;
    }
}

int BPlusTree::bPlusInsert(int relId, char attrName[ATTR_SIZE], Attribute attrVal, RecId recId){
    //getting attribute cache entry of attribute
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if(ret != SUCCESS){
        return ret;
    }

    int rootBlock = attrCatEntry.rootBlock;
    if(rootBlock == -1){
        return E_NOINDEX;
    }

    int leafBlkNum = findLeafToInsert(rootBlock, attrVal, attrCatEntry.attrType);

    Index leafEntry;
    leafEntry.block = recId.block;
    leafEntry.slot = recId.slot;
    leafEntry.attrVal = attrVal;
    
    ret = insertIntoLeaf(relId, attrName, leafBlkNum, leafEntry);
    if(ret == E_DISKFULL){
        //destroy the B+ tree
        ret = bPlusDestroy(rootBlock);
        if(ret != SUCCESS){
            return ret;
        }
        attrCatEntry.rootBlock = -1;
        ret = AttrCacheTable::setAttrCatEntry(relId, attrCatEntry.offset, &attrCatEntry);
        
        return E_DISKFULL;
    }
    return SUCCESS;
}

int BPlusTree::findLeafToInsert(int rootBlock, Attribute attrVal, int attrType){
    int blockNum = rootBlock;

    while(StaticBuffer::getStaticBlockType(blockNum) != IND_LEAF){
        IndInternal internalBlk(blockNum);
        HeadInfo intHead;
        internalBlk.getHeader(&intHead);

        bool found = false;
        InternalEntry intEntry;
        for(int i=0; i<intHead.numEntries; i++){
            internalBlk.getEntry(&intEntry, i);
            if(compareAttrs(intEntry.attrVal, attrVal, attrType) >= 0){
                blockNum = intEntry.lChild;
                found = true;
                break;
            }
        }
        if(!found){
            internalBlk.getEntry(&intEntry, intHead.numEntries-1);
            blockNum = intEntry.rChild;
        }
    }
    return blockNum;
}

int BPlusTree::insertIntoLeaf(int relId, char attrName[ATTR_SIZE], int blockNum, Index indexEntry){
    //getting attribute cache entry of attribute
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if(ret != SUCCESS){
        return ret;
    }

    IndLeaf leafBlk(blockNum);
    HeadInfo blockHead;
    leafBlk.getHeader(&blockHead);

    Index indices[blockHead.numEntries+1];

    int i = 0, j = 0;
    //iterate and copy all the entries to indices array while inserting the new entry
    //and maintaining the ascending order of entries
    Index leafEntry;
    bool found = false;
    for(int i=0; i<blockHead.numEntries; i++){
        leafBlk.getEntry(&leafEntry, i);
        if(!found && compareAttrs(leafEntry.attrVal, indexEntry.attrVal, attrCatEntry.attrType) >= 0){
            indices[j++] = indexEntry;
            found = true;
        }
        indices[j++] = leafEntry;
    }

    if(!found){
        indices[j] = indexEntry;
    }
    
    if(blockHead.numEntries != MAX_KEYS_LEAF){
        blockHead.numEntries++;
        leafBlk.setHeader(&blockHead);

        for(int i=0; i<blockHead.numEntries; i++){
            leafBlk.setEntry(&indices[i], i);
        }

        return SUCCESS;
    }

    int newRightBlk = splitLeaf(blockNum, indices);

    if(newRightBlk == E_DISKFULL){
        return E_DISKFULL;
    }

    //check if current leaf block was root
    if(blockHead.pblock != -1){
        IndInternal parentBlk(blockHead.pblock);

        struct InternalEntry middleEntry;
        middleEntry.attrVal = indices[MIDDLE_INDEX_LEAF].attrVal;
        middleEntry.lChild = blockNum;
        middleEntry.rChild = newRightBlk;
        ret = insertIntoInternal(relId, attrName, blockHead.pblock, middleEntry);
        if(ret != SUCCESS){
            return ret;
        }
    } else {
        ret = createNewRoot(relId, attrName, indices[MIDDLE_INDEX_LEAF].attrVal, blockNum, newRightBlk);
    }
    return ret;
}

int BPlusTree::splitLeaf(int leafBlockNum, Index indices[]){
    //for new block using constructor 1
    IndLeaf rightBlk;

    //for existing leaf block using constructor 2
    IndLeaf leftBlk(leafBlockNum);

    int rightBlkNum = rightBlk.getBlockNum();
    int leftBlkNum = leftBlk.getBlockNum();

    if(rightBlkNum == E_DISKFULL){
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;
    //get the headers
    leftBlk.getHeader(&leftBlkHeader);
    rightBlk.getHeader(&rightBlkHeader);

    //set the header for right block
    rightBlkHeader.numEntries = (MAX_KEYS_LEAF+1)/2; //32
    rightBlkHeader.pblock = leftBlkHeader.pblock;
    rightBlkHeader.rblock = leftBlkHeader.rblock;
    rightBlkHeader.lblock = leftBlkNum;

    //set the Header
    rightBlk.setHeader(&rightBlkHeader);

    //set the header for left block
    leftBlkHeader.numEntries = (MAX_KEYS_LEAF+1)/2; //32
    leftBlkHeader.rblock = rightBlkNum;

    //set the Header
    leftBlk.setHeader(&leftBlkHeader);

    //set the entries of first 32 in indices to left block
    for(int i=0; i<leftBlkHeader.numEntries; i++){
        leftBlk.setEntry(&indices[i], i);
    }

    //set the entries of next 32 in indices to right block
    for(int i=0; i<rightBlkHeader.numEntries; i++){
        rightBlk.setEntry(&indices[i+leftBlkHeader.numEntries], i);
    }
    return rightBlkNum;    
}

int BPlusTree::insertIntoInternal(int relId, char attrName[ATTR_SIZE], int blockNum, InternalEntry internalEntry){
    //getting attribute cache entry of attribute
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if(ret != SUCCESS){
        return ret;
    }

    IndInternal internalBlk(blockNum);
    HeadInfo blockHead;
    internalBlk.getHeader(&blockHead);

    InternalEntry internalEntries[blockHead.numEntries+1];

    int i = 0, j = 0;
    //iterate and copy all the entries to internalEntries array while inserting the new entry
    //and maintaining the ascending order of entries
    InternalEntry iEntry;
    bool found = false;
    for(int i=0; i<blockHead.numEntries; i++){
        internalBlk.getEntry(&iEntry, i);
        if(!found && compareAttrs(iEntry.attrVal, internalEntry.attrVal, attrCatEntry.attrType) >= 0){
            internalEntries[j++] = internalEntry;
            found = true;
        }
        internalEntries[j++] = iEntry;
    }

    if(!found){
        internalEntries[j] = internalEntry;
    }
    else{
        //update the lChild of next entry
        internalEntries[j].lChild = internalEntries[j-1].rChild;
    }

    if (blockHead.numEntries != MAX_KEYS_INTERNAL){
        blockHead.numEntries++;
        internalBlk.setHeader(&blockHead);

        for(int i=0; i<blockHead.numEntries; i++){
            internalBlk.setEntry(&internalEntries[i], i);
        }

        return SUCCESS;
    }

    int newRightBlk = BPlusTree::splitInternal(blockNum, internalEntries);

    if(newRightBlk == E_DISKFULL){
        bPlusDestroy(internalEntry.rChild);
        return E_DISKFULL;
    }

    //check if current internal block was root
    if(blockHead.pblock != -1){
        IndInternal parentBlk(blockHead.pblock);

        InternalEntry middleEntry;
        middleEntry.attrVal = internalEntries[MIDDLE_INDEX_INTERNAL].attrVal;
        middleEntry.lChild = blockNum;
        middleEntry.rChild = newRightBlk;
        ret = insertIntoInternal(relId, attrName, blockHead.pblock, middleEntry);
        if(ret != SUCCESS){
            return ret;
        }
    }
    else{
        ret = createNewRoot(relId, attrName, internalEntries[MIDDLE_INDEX_INTERNAL].attrVal, blockNum, newRightBlk);
    }
    return ret;
}

int BPlusTree::createNewRoot(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int lChild, int rChild){
    //getting attribute cache entry of attribute
    AttrCatEntry attrCatEntry;
    int ret = AttrCacheTable::getAttrCatEntry(relId, attrName, &attrCatEntry);
    if(ret != SUCCESS){
        return ret;
    }

    IndInternal rootBlk;

    int newRootBlkNum = rootBlk.getBlockNum();
    if(newRootBlkNum == E_DISKFULL){
        bPlusDestroy(rChild);
        return E_DISKFULL;
    }

    HeadInfo rootHead;
    rootBlk.getHeader(&rootHead);
    rootHead.numEntries = 1;
    rootBlk.setHeader(&rootHead);

    InternalEntry rootEntry;
    rootEntry.attrVal = attrVal;
    rootEntry.lChild = lChild;
    rootEntry.rChild = rChild;
    rootBlk.setEntry(&rootEntry, 0);

    //update the parent block of left and right child
    BlockBuffer lChildBlk(lChild);
    BlockBuffer rChildBlk(rChild);

    HeadInfo lChildHead, rChildHead;
    lChildBlk.getHeader(&lChildHead);
    rChildBlk.getHeader(&rChildHead);

    lChildHead.pblock = newRootBlkNum;
    rChildHead.pblock = newRootBlkNum;

    lChildBlk.setHeader(&lChildHead);
    rChildBlk.setHeader(&rChildHead);

    //update the root block in attribute cache table
    attrCatEntry.rootBlock = newRootBlkNum;
    ret = AttrCacheTable::setAttrCatEntry(relId, attrCatEntry.offset, &attrCatEntry);
    if(ret != SUCCESS){
        return ret;
    }
    return SUCCESS;
}

int BPlusTree::splitInternal(int intBlockNum, InternalEntry internalEntries[]){
    IndInternal rightBlk;

    IndInternal leftBlk(intBlockNum);

    int rightBlkNum = rightBlk.getBlockNum();
    int leftBlkNum = leftBlk.getBlockNum();

    if(rightBlkNum == E_DISKFULL){
        return E_DISKFULL;
    }

    HeadInfo leftBlkHeader, rightBlkHeader;
    leftBlk.getHeader(&leftBlkHeader);
    rightBlk.getHeader(&rightBlkHeader);

    rightBlkHeader.numEntries = (MAX_KEYS_INTERNAL)/2; //50
    rightBlkHeader.pblock = leftBlkHeader.pblock;

    rightBlk.setHeader(&rightBlkHeader);

    leftBlkHeader.numEntries = (MAX_KEYS_INTERNAL)/2; //50
    leftBlk.setHeader(&leftBlkHeader);

    for(int i=0; i<leftBlkHeader.numEntries; i++){
        leftBlk.setEntry(&internalEntries[i], i);
    }

    for(int i=0; i<rightBlkHeader.numEntries; i++){
        rightBlk.setEntry(&internalEntries[i+leftBlkHeader.numEntries], i);
    }

    int type = StaticBuffer::getStaticBlockType(internalEntries[0].lChild);

    //change pblock of lChild of first entry
    int lChildBlockNum = internalEntries[leftBlkHeader.numEntries].lChild;
    BlockBuffer lChildBlk(lChildBlockNum);
    HeadInfo lChildHead;
    lChildBlk.getHeader(&lChildHead);
    lChildHead.pblock = rightBlkNum;
    lChildBlk.setHeader(&lChildHead);

    //for each entry in right block, change the pblock of rChild
    for(int i=0; i<rightBlkHeader.numEntries; i++){
        BlockBuffer rChildBlk(internalEntries[i+leftBlkHeader.numEntries].rChild);
        HeadInfo rChildHead;
        rChildBlk.getHeader(&rChildHead);
        rChildHead.pblock = rightBlkNum;
        rChildBlk.setHeader(&rChildHead);
    }
    return rightBlkNum;
}
