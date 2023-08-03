#pragma once

#include "blocks/fixed_size_block.h"
#include "allocator/segment_allocator.h"

#include <vector>
struct DataBlockMeta
{
    char *block_start;
    size_t size;
    uint64_t min_key;
    uint64_t max_key;
    PBlockType type;
};

class DataBlockReader
{

private:
    char* start_addr_;
    char block_buf_pm_[4096];
    uint64_t block_pm_ptr_=INVALID_PTR;
    char block_buf_ssd_[16384];
    FilePtr block_ssd_ptr_=FilePtr::InvalidPtr();

public:
    DataBlockReader(SegmentAllocator *seg_allocator);
    ~DataBlockReader();

    DataBlockMeta TraverseDataBlock(uint64_t pm_offset,std::vector<std::pair<uint64_t,uint64_t>>* results=nullptr);
    DataBlockMeta TraverseDataBlock(FilePtr fptr,std::vector<std::pair<uint64_t,uint64_t>>* results=nullptr);
    bool BinarySearch(uint64_t pm_offset,Slice key,const char* value_out);
    bool BinarySearch(FilePtr ftpr, Slice key,const char *value_out);

private:
    PDataBlock *ReadPmDataBlock(uint64_t pm_offset);
    PSSDBlock *ReadSsdDataBlock(FilePtr fp);
};
