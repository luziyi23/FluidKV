#include "log_writer.h"
#include "allocator/segment_allocator.h"
LogWriter::LogWriter(SegmentAllocator *allocator, int log_segment_group_id) : allocator_(allocator), log_segment_group_id_(log_segment_group_id)
{
    current_segment_ = allocator_->AllocLogSegment(log_segment_group_id_);
}

LogWriter::~LogWriter()
{
    if (current_segment_)
        allocator_->CloseSegment(current_segment_);
}

// return log ptr
uint64_t LogWriter::WriteLogPut(Slice key, Slice value, LSN lsnumber)
{
    uint64_t ret = 0;
    // TODO: now only support up to 8-byte key, need to support var key in the future;
    if (value.size() <= 8)
    {
        LogEntry32 log = {
            .valid = 1,
            .lsn = (uint32_t)lsnumber.lsn,
            .key_sz = (uint16_t)key.size(),
            .value_sz = (uint16_t)value.size(),
            .key = *(const uint64_t *)key.data(),
            .value_addr = *(const uint64_t *)value.data()}; // value_addr is the union of value, we use it as a uint64_t
        LOG("put append log value=%lu", log.value_addr);

        ret = append_log<LogEntry32>(&log);
        return ret;
    }
    else if (value.size() <= 48)
    {
        LogEntry64 log = {
            .valid = 1,
            .lsn = (uint32_t)lsnumber.lsn,
            .key_sz = (uint16_t)key.size(),
            .value_sz = (uint16_t)value.size(),
            .key = *(const uint64_t *)key.data()};
        memcpy(log.value, value.data(), value.size());
        LOG("put append log value=%lu", log.value_addr);

        ret = append_log<LogEntry64>(&log);
    }
    else
    {
        LogEntryVar64 *ptr = (LogEntryVar64 *)variable_entry_buffer_;
        (*ptr) = {
            .valid = 1,
            .lsn = (uint32_t)lsnumber.lsn,
            .key_sz = (uint16_t)key.size(),
            .value_sz = (uint16_t)value.size(),
            .key = *(const uint64_t *)key.data()};
        memcpy(ptr->value, value.data(), value.size());
        ret = append_log<LogEntryVar64>(ptr, value.size() + 16);
    }
#ifndef KV_SEPARATE
    ERROR_EXIT("KV_SEPARATE is disabled but value > 8byte");
#endif
    return ret;
}
uint64_t LogWriter::WriteLogDelete(Slice key, LSN lsnumber)
{
    uint64_t ret;
    // TODO: now only support 8-byte key, need to support var key;
    if (key.size() <= 8)
    {

        LogEntry32 log = {
            .valid = 0,
            .lsn = (uint32_t)lsnumber.lsn,
            .key_sz = (uint16_t)key.size(),
            .key = *(const uint64_t *)key.data()};
        ret = append_log<LogEntry32>(&log);
    }
    else
    {
        LogEntryVar64 log = {
            .valid = 0,
            .lsn = (uint32_t)lsnumber.lsn,
            .key_sz = (uint16_t)key.size(),
            .key = *(const uint64_t *)key.data()};
        ret = append_log<LogEntryVar64>(&log);
    }
    assert(ret);
    return ret;
}

void LogWriter::SwitchToNewSegment(int id)
{
    if (current_segment_)
        allocator_->CloseSegment(current_segment_);

    log_segment_group_id_ = id;
    current_segment_ = allocator_->AllocLogSegment(log_segment_group_id_);
}

// return the offset from the start of datapool
template <typename T>
uint64_t LogWriter::append_log(T *data)
{
    uint64_t log_offset;
    int segment_offset;
    // init
    if (current_segment_ == nullptr)
        current_segment_ = allocator_->AllocLogSegment(log_segment_group_id_);

    segment_offset = current_segment_->Append((char *)data, sizeof(T));
    if (segment_offset == -1)
    {
        // segment overflow, allocate another for this logging
        allocator_->CloseSegment(current_segment_);
        current_segment_ = allocator_->AllocLogSegment(log_segment_group_id_);
        segment_offset = current_segment_->Append((char *)data, sizeof(T));
    }
    assert(segment_offset >= 0);
    return current_segment_->segment_id_ * SEGMENT_SIZE + segment_offset;
}


template <typename T>
uint64_t LogWriter::append_log(T *data, size_t size)
{
    uint64_t log_offset;
    int segment_offset;
    // init
    if (current_segment_ == nullptr)
        current_segment_ = allocator_->AllocLogSegment(log_segment_group_id_);

    current_segment_->AlignTailTo64B();
    segment_offset = current_segment_->Append((char *)data, size);
    if (segment_offset == -1)
    {
        // segment overflow, allocate another for this logging
        allocator_->CloseSegment(current_segment_);
        current_segment_ = allocator_->AllocLogSegment(log_segment_group_id_);
        segment_offset = current_segment_->Append((char *)data, size);
    }
    assert(segment_offset >= 0);
    return current_segment_->segment_id_ * SEGMENT_SIZE + segment_offset;
}