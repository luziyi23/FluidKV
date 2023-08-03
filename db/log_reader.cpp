#include "log_reader.h"

LogReader::LogReader(SegmentAllocator *allocator) : seg_allocator_(allocator), start_addr_(allocator->GetStartAddr())
{
}

LogReader::~LogReader()
{
}

Slice LogReader::ReadLogForValue(const Slice &key, ValuePtr valueptr)
{
    char *addr = start_addr_ + (valueptr.detail_.ptr << 6) + sizeof(LogSegment::Header);
    LOG("ReadLogForValue %lu(%s) , addr=%lu", key.ToUint64(), key.ToString().c_str(), (uint64_t)addr);
    LOG("start_addr=%lu,valueptr.detail_.ptr=%lu,sizeof(LogSegment::Header)=%lu", (uint64_t)start_addr_, valueptr.detail_.ptr, sizeof(LogSegment::Header));
#ifndef KV_SEPARATE
    LogEntry32 *record = (LogEntry32 *)addr;
    if (key.size() != record->key_sz || record->valid == 0 || (record->key != *reinterpret_cast<const uint64_t *>(key.data())))
    {
        LOG("try 2nd log entry in a cacheline");
        record = record + 1;
    }
    return Slice(&record->value[0], record->value_sz);
#else
    LogEntryVar64 *record = (LogEntryVar64 *)addr;
    return Slice((char *)record->value, record->value_sz);
#endif
    // TODO: read other log entry format
    ERROR_EXIT("read log error");
}

int LogReader::ReadLogFromSegment(int segment_id, LogEntry32 *output)
{
    // TODO: now we only support 32-byte log entry
    auto seg = seg_allocator_->GetLogSegment(segment_id);
    if (seg == nullptr)
        return 0;
    auto header = seg->GetHeader();
    auto log_head = seg->GetStartAddr();
    memcpy(output, log_head, header.objects_tail_offset);
    return header.objects_tail_offset / sizeof(LogEntry32);
}