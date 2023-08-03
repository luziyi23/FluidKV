/**
 * @file pindex_writer.h
 * @author your name (you@domain.com)
 * @brief 负责持久索引块的写入。每个PIndexWriter独享一个类型为PIndex的segment的写入权限，允许用户逐条按顺序写入K-addr对，当buf攒满一个block大小的数据时则从segment中分配一个page来持久化该block。 目前，所有的PIndex都使用512-byte的index block，仅支持8-byte定长key。
 * @version 0.1
 * @date 2022-08-28
 *
 * @copyright Copyright (c) 2022
 *
 */
#pragma once

#include "blocks/fixed_size_block.h"
#include "allocator/segment_allocator.h"
#include <vector>
class PIndexWriter
{
private:
    SegmentAllocator *allocator_;
    SortedSegment *current_segment_; // index segments of which this thread have modify permission. use this segment to allocate new index block.

    PIndexBlockWrapper current_block512_;
    std::vector<SortedSegment*> used_segments_;

public:
    PIndexWriter(SegmentAllocator *allocator);
    ~PIndexWriter();
    /**
     * @brief 
     * 
     * @param key 
     * @param ptr 
     * @return true add success
     * @return false current pindex block is full, add failed
     */
    bool AddEntry(Slice key, uint64_t ptr);
    /**
     * @brief
     *
     * @return uint64_t volatile pmem addr offset of the flushed pindexblock to datapool start. 0 means no data to flush, nothing happened;
     */
    uint64_t Flush();

    int PersistCheckpoint();

private:
    void persist_current_block512();
    void allocate_block512();
};
