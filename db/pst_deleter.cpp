#include "pst_deleter.h"

PSTDeleter::PSTDeleter(SegmentAllocator *seg_allocator) : seg_allocator_(seg_allocator), index_reader_(seg_allocator) {}
PSTDeleter::~PSTDeleter() { PersistCheckpoint(); }

bool PSTDeleter::DeletePST(PSTMeta meta)
{
    //TODO: if indexblock or any datablock not exist, correctly skip
    size_t seg_id = seg_allocator_->TrasformOffsetToId(meta.indexblock_ptr_);
    SortedSegment *index_seg = nullptr;
    LOG("b,used_index_segments_.size()=%lu",used_index_segments_.size());
    for (int i = used_index_segments_.size() - 1; i >= 0; i--)
    {
        if (used_index_segments_[i]->segment_id_ == seg_id)
        {
            index_seg = used_index_segments_[i];
            break;
        }
    }
    if (index_seg == nullptr)
    {
        index_seg = seg_allocator_->GetSortedSegmentForDelete(seg_id,sizeof(PIndexBlock));
        used_index_segments_.push_back(index_seg);
    }
    auto ret = index_seg->RecyclePage(index_seg->TrasformOffsetToPageId(meta.indexblock_ptr_));
    assert(ret);
    std::vector<std::pair<uint64_t, uint64_t>> indexlist;
    std::vector<std::pair<uint64_t, uint64_t>> kvlist;
    size_t size = index_reader_.ReadPIndexBlock(meta.indexblock_ptr_, indexlist);
    for (auto &datablock : indexlist)
    {
        uint64_t datablock_offset = datablock.second;
        size_t data_seg_id = seg_allocator_->TrasformOffsetToId(datablock_offset);
        SortedSegment *data_seg = nullptr;
        for (int i = used_data_segments_.size() - 1; i >= 0; i--)
        {
            if (used_data_segments_[i]->segment_id_ == data_seg_id)
            {
                data_seg = used_data_segments_[i];
                break;
            }
        }
        if (data_seg == nullptr)
        {
            data_seg = seg_allocator_->GetSortedSegmentForDelete(data_seg_id,sizeof(PDataBlock));
            used_data_segments_.push_back(data_seg);
        }
        data_seg->RecyclePage(data_seg->TrasformOffsetToPageId(datablock_offset));
        
    }
    return true;
}

bool PSTDeleter::PersistCheckpoint()
{
    //just persist,don't modify the state
    for (auto &seg : used_data_segments_)
    {
        seg_allocator_->CloseSegmentForDelete(seg);
    }
    used_data_segments_.clear();
    for (auto &seg : used_index_segments_)
    {
        seg_allocator_->CloseSegmentForDelete(seg);
    }
    used_index_segments_.clear();
    return true;
}
