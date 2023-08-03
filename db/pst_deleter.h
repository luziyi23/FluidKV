#pragma once
#include "table.h"
#include "pindex_reader.h"
#include "allocator/segment_allocator.h"

class PSTDeleter
{
private:
    SegmentAllocator *seg_allocator_;
    PIndexReader index_reader_;
    // SortedSegment *current_indexblock_ = nullptr;
    // SortedSegment *current_datablock_ = nullptr;
    std::vector<SortedSegment *> used_index_segments_;
    std::vector<SortedSegment *> used_data_segments_;
    // std::vector<SortedSegmentOnSSD *>used_data_segments_;

public:
    PSTDeleter(SegmentAllocator *seg_allocator);
    ~PSTDeleter();

    bool DeletePST(PSTMeta meta);
    bool PersistCheckpoint();
};
