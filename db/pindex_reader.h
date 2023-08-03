#pragma once

#include "blocks/fixed_size_block.h"
#include "allocator/segment_allocator.h"

#include <vector>
class PIndexReader
{
private:
    char* start_addr_;
    PIndexBlockWrapper block512_buf_;

public:
    PIndexReader(SegmentAllocator *allocator);
    ~PIndexReader();

    /**
     * @brief read pindexblock and add the entries into a vector
     *
     * @param pm_offset the pm offset of index block
     * @param kvlist
     * @return size_t the number of entries
     */
    size_t ReadPIndexBlock(uint64_t pm_offset, std::vector<std::pair<uint64_t, uint64_t>> &kvlist);

    /**
     * @brief point query a datablock in pindexblock with key
     *
     * @param pm_offset the pm offset of index block
     * @param key
     * @return size_t the offset of data block in which the target key may exist
     */
    size_t PointQuery(uint64_t pm_offset, Slice key, int entry_num=PIndexBlock::MAX_ENTRIES);

    /**
     * @brief
     *
     * @param pm_offset
     * @return PIndexBlock* shallow copy
     */
    PIndexBlock *ReadPIndexBlock512(uint64_t pm_offset);
};
