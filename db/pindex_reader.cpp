#include "pindex_reader.h"

PIndexReader::PIndexReader(SegmentAllocator *allocator) : start_addr_(allocator->GetStartAddr())
{
    LOG("Pindex reader initialized");
}

PIndexReader::~PIndexReader()
{
}

size_t PIndexReader::ReadPIndexBlock(uint64_t pm_offset, std::vector<std::pair<uint64_t, uint64_t>> &kvlist)
{
    auto data = ReadPIndexBlock512(pm_offset);
    size_t i;
    size_t last_key=INVALID_PTR;
    for (i = 0; i < PIndexBlock::MAX_ENTRIES; i++)
    {
        auto kap = std::make_pair(data->entries[i].key, data->entries[i].leafptr);
        if (kap.first==last_key || kap.second == INVALID_PTR)
        {
            break;
        }
        last_key=kap.first;
        kvlist.emplace_back(kap);
    }
    block512_buf_.size = i;
    return i;
}

PIndexBlock *PIndexReader::ReadPIndexBlock512(uint64_t pm_offset)
{
    char *addr = start_addr_ + pm_offset;
#ifdef DIRECT_PM_ACCESS
    // direct access
    return (PIndexBlock *)addr;
#endif

    // copy to buffer
    if (block512_buf_.pm_page_addr != addr)
    {
        //TODO: 引起compaction错误
#ifdef ALIGNED_COPY_256
        for (size_t offset = 0; offset < sizeof(PIndexBlock); offset += 256)
        {
            memcpy((char *)&block512_buf_.data_buf + offset, addr + offset, 256);
        }
#else
        memcpy(&block512_buf_.data_buf, addr, sizeof(PIndexBlock));
#endif
        block512_buf_.set_page(addr);
    }

    return &block512_buf_.data_buf;
}

size_t PIndexReader::PointQuery(uint64_t pm_offset, Slice key, int entry_num)
{
    auto block = ReadPIndexBlock512(pm_offset);


    // binary search
    int left = 0, right = entry_num - 1;
    int mid, ret;
    while (left <= right)
    {
        mid = (left + right + 1) / 2;
        ret = Slice(&(block->entries[mid].key)).compare(key);
        if (ret == 0)
        {
            right = mid;
            break;
        }
        else if (ret < 0)
        {
            left = mid + 1;
        }
        else
        {
            right = mid - 1;
        }
    }
    if (right >= 0)
        return block->entries[right].leafptr;
    return INVALID_PTR;
}