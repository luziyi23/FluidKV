#include "datablock_reader.h"
#include "util/binary_search.h"
#include <algorithm>

DataBlockReader::DataBlockReader(SegmentAllocator *seg_allocator) : start_addr_(seg_allocator->GetStartAddr())
{
}

DataBlockReader::~DataBlockReader()
{
}

DataBlockMeta DataBlockReader::TraverseDataBlock(uint64_t pm_offset, std::vector<std::pair<uint64_t, uint64_t>> *results)
{
    // TODO: check the type of the datablock. Currently the default is 512B pm block.
    // traverse
    PDataBlock *block = ReadPmDataBlock(pm_offset);
    int i;
    size_t last_key = INVALID_PTR;
    for (i = 0; i < PDataBlock::MAX_ENTRIES; i++)
    {
        LOG("read entry %lu:%lu", block->entries[i].key, block->entries[i].value);
        if (block->entries[i].key == INVALID_PTR && block->entries[i].value == INVALID_PTR)
        {
            break;
        }
        if (block->entries[i].key == last_key)
        {
            break;
        }
        last_key = block->entries[i].key;
        if (results)
        {
            results->emplace_back(block->entries[i].key, block->entries[i].value);
        }
    }
    if (i == 0)
    {
        ERROR_EXIT("datablock have no entries");
    }
    DataBlockMeta meta;
    meta.block_start = start_addr_ + pm_offset;
    meta.max_key = block->entries[i - 1].key;
    meta.min_key = block->entries[0].key;
    meta.size = i - 1;
    meta.type = PBlockType::DATABLOCK512;
    return meta;
}
DataBlockMeta DataBlockReader::TraverseDataBlock(FilePtr fptr, std::vector<std::pair<uint64_t, uint64_t>> *results)
{
    PSSDBlock *block = ReadSsdDataBlock(fptr);
    int i;
    for (i = 0; i < PSSDBlock::MAX_ENTRIES; i++)
    {
        LOG("read entry %lu:%lu", block->entries[i].key, block->entries[i].value);
        if (block->entries[i].key == INVALID_PTR && block->entries[i].value == INVALID_PTR)
        {
            break;
        }
        if (results)
        {
            results->emplace_back(block->entries[i].key, block->entries[i].value);
        }
    }
    if (i == 0)
    {
        ERROR_EXIT("datablock have no entries");
    }
    DataBlockMeta meta;
    meta.block_start = block_buf_ssd_;
    meta.max_key = block->entries[i - 1].key;
    meta.min_key = block->entries[0].key;
    meta.size = i - 1;
    meta.type = PBlockType::DATABLOCK4K;
    return meta;
}

bool DataBlockReader::BinarySearch(uint64_t pm_offset, Slice key, const char *value_out)
{
    // TODO： currently, only support 8-byte string key.
    PDataBlock *block = ReadPmDataBlock(pm_offset);
    int index = binarysearch((char *)block->entries, PDataBlock::MAX_ENTRIES, key, sizeof(PDataBlock::Entry));
    if (index >= 0)
    {
        memcpy((void *)value_out, &block->entries[index].value, 8);
        return true;
    }
    else
    {
        index = -(index + 1);
        return false;
    }
}

bool DataBlockReader::BinarySearch(FilePtr fptr, Slice key, const char *value_out)
{
    // TODO： currently, only support 8-byte string key.
    PSSDBlock *block = ReadSsdDataBlock(fptr);
    int index = binarysearch((char *)block->entries, PSSDBlock::MAX_ENTRIES, key, sizeof(PSSDBlock::Entry));
    if (index >= 0)
    {
        memcpy((void *)value_out, &block->entries[index].value, 8);
        return true;
    }
    else
    {
        index = -(index + 1);
        if (index < PSSDBlock::MAX_ENTRIES)
            memcpy((void *)value_out, &block->entries[index].value, 8);
        else
            memcpy((void *)value_out, &block->entries[PSSDBlock::MAX_ENTRIES - 1].value, 8);
        return false;
    }
}

// private
PDataBlock *DataBlockReader::ReadPmDataBlock(uint64_t pm_offset)
{
    if (pm_offset == block_pm_ptr_)
    {
        LOG("read datablock: cache hit %lu", pm_offset);
        return (PDataBlock *)block_buf_pm_;
    }

    PDataBlock *block = nullptr;
    char *addr = start_addr_ + pm_offset;
#ifdef DIRECT_PM_ACCESS
    // direct access
    block = (PDataBlock *)addr;
#else
// copy to buffer
#ifdef ALIGNED_COPY_256
    for (size_t offset = 0; offset < sizeof(PDataBlock); offset += 256)
    {
        memcpy(block_buf_pm_ + offset, addr + offset, 256);
    }
#else
    memcpy(block_buf_pm_, addr, sizeof(PDataBlock));
#endif
    block = (PDataBlock *)block_buf_pm_;
    block_pm_ptr_ = pm_offset;
#endif
    LOG("read datablock: cache miss, read %lu", (uint64_t)addr);
    return block;
}

// private
PSSDBlock *DataBlockReader::ReadSsdDataBlock(FilePtr fp)
{
    if (fp == block_ssd_ptr_)
    {
        return (PSSDBlock *)block_buf_ssd_;
    }

    auto ret = pread(fp.fd, block_buf_ssd_, sizeof(PSSDBlock), fp.offset);
    assert(ret != -1);
    block_ssd_ptr_ = fp;
    return (PSSDBlock *)block_buf_ssd_;
}