#include "pst_reader.h"
#include <algorithm>
PSTReader::PSTReader(SegmentAllocator *allocator) : pindex_reader_(allocator), datablock_reader_(allocator)
{
}

PSTReader::~PSTReader()
{
}
PSTMeta PSTReader::RecoverPSTMeta(uint64_t pindex_addr)
{
    PSTMeta meta;
    meta.indexblock_ptr_ = pindex_addr;
    // get datablock list
    std::vector<std::pair<uint64_t, uint64_t>> indexlist;
    std::vector<std::pair<uint64_t, uint64_t>> kvlist;
    size_t size = pindex_reader_.ReadPIndexBlock(pindex_addr, indexlist);
    for (auto &datablock : indexlist)
    {
        uint64_t min_key = datablock.first;
        uint64_t datablock_offset = datablock.second;
        // TODO: if any datablock is not allocated in p_bitmap, it means that the compaction is not done and should be roll back. Try to recycle the pst.
        datablock_reader_.TraverseDataBlock(datablock_offset, &kvlist);
        meta.datablock_num_++;
    }
    meta.min_key_ = indexlist[0].first;
    meta.max_key_ = kvlist.end()->first;
    meta.entry_num_ = kvlist.size();
    return meta;
}
bool cmp(const std::pair<uint64_t, uint64_t> &a, const Slice &b)
{
    uint64_t key = a.first;
    return Slice(&key).compare(b) > 0;
};
bool cmp2(const Slice &b, const std::pair<uint64_t, uint64_t> &a)
{
    uint64_t key = a.first;
    return Slice(&key).compare(b) > 0;
};
bool PSTReader::PointQuery(uint64_t pindex_addr, Slice key, const char *value_out, int *value_size, int datablock_num)
{
    size_t datablock_ptr = pindex_reader_.PointQuery(pindex_addr, key, datablock_num);
    if (datablock_ptr == INVALID_PTR)
        return false;
    bool ret = datablock_reader_.BinarySearch(datablock_ptr, key, value_out);
    *value_size = 8;
    return ret;
}
PSTReader::Iterator *PSTReader::GetIterator(uint64_t pindex_addr)
{
    return new PSTReader::Iterator(this, pindex_addr);
}