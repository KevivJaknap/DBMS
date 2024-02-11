#include "BPlusTree.h"

#include <cstring>

RecId BPlusTree::bPlusSearch(int relId, char attrName[ATTR_SIZE], Attribute attrVal, int op, int &comps){
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
                if(EQorGE){
                    comps++;
                }
                if(EQorGE && compareAttrs(intEntry.attrVal, attrVal, attrCatEntry.attrType) >= 0){
                    found = true;
                    break;
                }
                if(op == GT){
                    comps++;
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
            comps++;
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
