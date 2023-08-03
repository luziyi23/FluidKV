#pragma once

#include "log_format.h"
#include "db_common.h"

class SegmentAllocator;
class LogSegment;
class LogWriter
{
private:
    SegmentAllocator *allocator_;
    LogSegment *current_segment_;
    int log_segment_group_id_;
    char variable_entry_buffer_[4160];
public:
    LogWriter(SegmentAllocator *allocator, int log_segment_group_id);
    ~LogWriter();
    uint64_t WriteLogPut(Slice key, Slice value, LSN lsn);
    uint64_t WriteLogDelete(Slice key, LSN lsn);
    void SwitchToNewSegment(int log_segment_group_id);

private:
    template <typename T>
    uint64_t append_log(T *data); // T denotes log entry type
    template <typename T>
    uint64_t append_log(T *data, size_t size); //For KV-separate log when value > 48 bytes
};
