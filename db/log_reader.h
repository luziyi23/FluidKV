#pragma once

#include "log_format.h"
#include "db_common.h"
#include "allocator/segment_allocator.h"

class SegmentAllocator;
class LogReader
{
private:
    SegmentAllocator* seg_allocator_;
    char *start_addr_;

public:
    LogReader(SegmentAllocator *allocator);
    ~LogReader();
    Slice ReadLogForValue(const Slice &key, ValuePtr ptr);
    int ReadLogFromSegment(int segment_id, LogEntry32* output);
};
