/**
 * @file datablock_writer.h
 * @author your name (you@domain.com)
 * @brief 负责持久数据块的写入。管理多种粒度datablock的写入（目前只支持定长类型）。
 * 在需要用到时lazy地通过segment allocator获得对应粒度的data segment的写权限。
 * @version 0.1
 * @date 2022-08-29
 *
 * @copyright Copyright (c) 2022
 *
 */
#pragma once
#include "blocks/fixed_size_block.h"
#include "allocator/segment_allocator.h"
#include <vector>

class DataBlockWriter
{
public:
    virtual bool AddEntry(Slice key, Slice value)=0;
    virtual uint64_t GetCurrentMinKey()=0;
    virtual uint64_t GetCurrentMaxKey()=0;
    virtual uint64_t Flush()=0;
    virtual int PersistCheckpoint()=0;

private:
    virtual void allocate_block()=0;
};


class DataBlockWriterPm: public DataBlockWriter
{
private:
    SegmentAllocator *seg_allocator_;
    SortedSegment *current_segment_;
    PDataBlockPmWrapper blocks_buf_;
    std::vector<SortedSegment*> used_segments_;

public:
    DataBlockWriterPm(SegmentAllocator *allocator);
    ~DataBlockWriterPm();

    virtual bool AddEntry(Slice key, Slice value) override;
    virtual uint64_t GetCurrentMinKey() override;
    virtual uint64_t GetCurrentMaxKey() override;
    virtual uint64_t Flush() override;
    virtual int PersistCheckpoint() override;

private:
    virtual void allocate_block() override;
};

class DataBlockWriterSsd: public DataBlockWriter
{
private:
    SegmentAllocator *seg_allocator_;
    SortedSegmentOnSSD *current_segment_;
    PDataBlockSsdWrapper blocks_buf_;
    std::vector<SortedSegmentOnSSD*> used_segments_;

public:
    DataBlockWriterSsd(SegmentAllocator *allocator);
    ~DataBlockWriterSsd();

    virtual bool AddEntry(Slice key, Slice value) override;
    virtual uint64_t GetCurrentMinKey() override;
    virtual uint64_t GetCurrentMaxKey() override;
    virtual uint64_t Flush() override;
    virtual int PersistCheckpoint() override;

private:
    virtual void allocate_block() override;
};

