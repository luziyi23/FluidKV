#pragma once

#include "segment.h"
#include <filesystem>
#include <unordered_map>
#include <queue>
#include <atomic>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "util/atomic_vector.h"
class SegmentAllocator
{
private:
    std::string pool_path_;
    const size_t pool_size_;
    std::string ssd_path_;
    char *start_addr_;
    BitMap segment_bitmap_;     // persist in the tail of pm pool
    BitMap log_segment_bitmap_; // a backup bitmap of log segments for fast recovery, persisted after segment_bitmap
    // TODO: modify these cache to a bitmap or a segment tree
    std::queue<SortedSegment *> index_segment_cache_; // cache the index segment which is allocated but not full
    std::queue<SortedSegment *> data_segment_cache_;  // ditto
    std::atomic_int ssd_file_counter_;
    std::queue<SortedSegmentOnSSD *> ssd_segment_cache_;
    AtomicVector<uint64_t> log_segment_group_[MAX_MEMTABLE_NUM];
    int current_log_group_;

    SpinLock mtx_i, mtx_d, mtx_s; // lock the cache for poping element

public:
    SegmentAllocator(std::string pool_path, size_t pool_size, std::string ssd_path = "", bool recover=false) : pool_path_(pool_path), pool_size_(pool_size), ssd_path_(ssd_path), start_addr_(nullptr), segment_bitmap_(pool_size_ / SEGMENT_SIZE, true), log_segment_bitmap_(pool_size_ / SEGMENT_SIZE, true), current_log_group_(0)
    {
        // TODO: When recovering, need to get the real pool size instead of using the paramater
        size_t mapped_len;
        start_addr_ = (char *)pmem_map_file(pool_path_.c_str(), pool_size_ + 2 * roundup(segment_bitmap_.SizeInByte(), 64), PMEM_FILE_CREATE, 0666, &mapped_len, nullptr);
        assert(mapped_len == pool_size_ + 2 * roundup(segment_bitmap_.SizeInByte(), 64) && start_addr_);
        DEBUG("segment pool start = %lu, end = %lu, total segment num=%lu", (uint64_t)start_addr_, (uint64_t)(start_addr_ + mapped_len), pool_size_ / SEGMENT_SIZE);
        segment_bitmap_.SetPersistAddr(start_addr_ + pool_size_);
        log_segment_bitmap_.SetPersistAddr(start_addr_ + pool_size_ + roundup(segment_bitmap_.SizeInByte(), 64));
        if (recover)
        {
            // TODO: segment recover
            DEBUG("segment_bitmap_recover");
            segment_bitmap_.Recover();
            log_segment_bitmap_.Recover();
            // TODO: recover ssd_file_counter_
        }
        else
        {
            segment_bitmap_.PersistToPM();
            log_segment_bitmap_.PersistToPM();
            ssd_file_counter_ = 0;
        }

        LOG("new allocator:%lu", (uint64_t)start_addr_);
    };
    ~SegmentAllocator()
    {
        while (!index_segment_cache_.empty())
        {
            auto &seg = index_segment_cache_.front();
            delete seg;
            index_segment_cache_.pop();
        }
        while (!data_segment_cache_.empty())
        {
            auto &seg = data_segment_cache_.front();
            delete seg;
            data_segment_cache_.pop();
        }
        while (!ssd_segment_cache_.empty())
        {
            auto &seg = ssd_segment_cache_.front();
            delete seg;
            ssd_segment_cache_.pop();
        }
        segment_bitmap_.PersistToPM();
    };

    bool RecoverLogSegmentAndGetId(std::vector<uint64_t> &seg_id_list)
    {
        // TODO: for crash-consitency, check if elements of log_segment_bitmap exist in segment_bitamp. Make them consistent.
        bool ret = log_segment_bitmap_.GetUsedBits(seg_id_list);
        DEBUG("used_bits.size=%lu",seg_id_list.size());
        for (auto &id : seg_id_list)
        {
            log_segment_group_[current_log_group_].add(id);
        }
        return ret;
    }
    void PrintLogStats(){
        printf("-------valid log stats------\n");
        std::vector<uint64_t> seg_id_list;
        bool ret = log_segment_bitmap_.GetUsedBits(seg_id_list);
        DEBUG("used_bits.size=%lu(%lu)",seg_id_list.size(),log_segment_bitmap_.GetUsedBitsNum());
        for (auto &id : seg_id_list)
        {
            printf("%lu,",id);
        }
        printf("\n");
    }

    bool RedoFlushLog(std::vector<uint64_t> &deleted_seg_id_list)
    {
        for (auto &id : deleted_seg_id_list)
        {
            DEBUG("clean log segment %lu", id);
            auto log_seg = GetLogSegment(id);
            if (log_seg != nullptr)
            {
#ifdef KV_SEPARATE
                CloseSegment(log_seg, true);
#else
                FreeSegment(log_seg);
#endif
            }
        }
        return true;
    }

    LogSegment *AllocLogSegment(int group_id)
    {
        size_t id = segment_bitmap_.AllocateOne();
        log_segment_bitmap_.AllocatePos(id);
        log_segment_bitmap_.PersistToPM();
        segment_bitmap_.PersistToPM();
        LOG("allocate log segment id=%lu", id);
        // printf("allocate log segment id=%lu\n", id);
        if (id == ERROR_CODE)
        {
            ERROR_EXIT("log segment allocation failed, space not enough");
        }
        log_segment_group_[group_id].add(id);
        return new LogSegment(start_addr_, id);
    };
    SortedSegment *AllocSortedSegment(int page_size, bool is_data = 0)
    {
        // reuse unfilled segment with segment cache
        if (is_data)
        {
            std::lock_guard<SpinLock> lock(mtx_d);
            if (!data_segment_cache_.empty())
            {
                auto p = data_segment_cache_.front();
                data_segment_cache_.pop();
                p->Reuse();
                assert(!p->Full());
                return p;
            }
        }
        else
        {
            std::lock_guard<SpinLock> lock(mtx_i);
            if (!index_segment_cache_.empty())
            {
                auto p = index_segment_cache_.front();
                index_segment_cache_.pop();
                p->Reuse();
                assert(!p->Full());
                return p;
            }
        }
        // alloc new segment
        size_t id = segment_bitmap_.AllocateOne();
        LOG("allocate sorted segment id=%lu, isdata=%d", id, is_data);
        segment_bitmap_.PersistToPM();
        if (id == ERROR_CODE)
        {
            ERROR_EXIT("index segment allocation failed, space not enough!");
        }
        PBlockType type = INVALID_NODE;
        if (is_data)
        {
            type = PBlockType::DATABLOCK512;
        }
        else
        {
            type = PBlockType::INDEX512_TO_BLOCK512;
        }
        SortedSegment *seg = new SortedSegment(start_addr_, id, type, page_size);
        return seg;
    };

    SortedSegmentOnSSD *AllocSortedSegmentOnSSD(int page_size)
    {
        {
            std::lock_guard<SpinLock> lock(mtx_s);
            if (!ssd_segment_cache_.empty())
            {
                auto p = ssd_segment_cache_.front();
                ssd_segment_cache_.pop();
                p->Reuse();
                return p;
            }
        }
        // alloc new segment
        int file_id = ssd_file_counter_.fetch_add(1);
        DEBUG("allocate ssd segment id=%d", file_id);
        if (file_id == -1)
        {
            ERROR_EXIT("ssd segment allocation failed, space not enough!");
        }
        int fd = open((ssd_path_ + std::to_string(file_id) + ".seg").c_str(), O_RDWR | O_CREAT, 0777);
        SortedSegmentOnSSD *seg = new SortedSegmentOnSSD(file_id, fd, page_size);
        return seg;
    }

    bool CloseSegment(LogSegment *&seg, bool avail = 0)
    {
        if (avail)
        {
            seg->Avail();
        }
        else
        {
            seg->Close();
        }
        delete seg;
        seg = nullptr;
        return true;
    };
    bool CloseSegment(SortedSegment *&seg)
    {
        LOG("close segment id=%lu, full=%d", seg->segment_id_, seg->Full());
        if (!seg->Full())
        {
            seg->Freeze();
            auto type = seg->type();
            if (type == PBlockType::INDEX512_TO_BLOCK512)
            {
                std::lock_guard<SpinLock> lock(mtx_i);
                index_segment_cache_.emplace(seg);
            }
            else if (type == PBlockType::DATABLOCK512)
            {
                std::lock_guard<SpinLock> lock(mtx_d);
                data_segment_cache_.emplace(seg);
            }
            return true;
        }
        seg->Close();
        delete seg;
        seg = nullptr;
        return true;
    };
    bool CloseSegmentForDelete(SortedSegment *&seg)
    {
        // TODO: may have bugs when remove this to allow concurrent allocate and free page in a shared segment
        //  while (seg->status() == StatusUsing)
        //  {
        //      usleep(1000);
        //      seg->RecoverHeader();
        //  }

        if (seg->status() == StatusAvailable || seg->status() == StatusUsing)
        {
            seg->PersistBitmapSoft();
            delete seg;
            seg = nullptr;
            return true;
        }
        if (seg->status() == StatusClosed)
        {
            seg->Freeze();
            auto type = seg->type();
            seg->SetForDelete(false);
            if (type == PBlockType::INDEX512_TO_BLOCK512)
            {
                std::lock_guard<SpinLock> lock(mtx_i);
                index_segment_cache_.emplace(seg);
            }
            else if (type == PBlockType::DATABLOCK512)
            {
                std::lock_guard<SpinLock> lock(mtx_d);
                data_segment_cache_.emplace(seg);
            }
            return true;
        }
        ERROR_EXIT("segment open for delete have no data %d", seg->status());
    }

    bool CloseSegment(SortedSegmentOnSSD *&seg)
    {
        if (!seg->Full())
        {
            ssd_segment_cache_.emplace(seg);
            seg->Freeze();
            return true;
        }
        seg->Close();
        close(seg->get_fd());
        delete seg;
        seg = nullptr;
        return true;
    }
    bool FreeSegment(LogSegment *&seg)
    {
        // TODO: need finer-grainded persist I/O for bitmap
        LOG("free log segment id=%lu", seg->segment_id_);
        seg->Free();
        segment_bitmap_.Free(seg->segment_id_);
        segment_bitmap_.PersistToPM();
        auto ret=log_segment_bitmap_.Free(seg->segment_id_);
        assert(ret);
        log_segment_bitmap_.PersistToPM();
        delete seg;
        return true;
    };
    LogSegment *GetLogSegment(size_t id)
    {
        if (!segment_bitmap_.Exist(id))
        {
            // TODO: process return value at each call of this function
            return nullptr;
        }
        return new LogSegment(start_addr_, id, true);
    };
    SortedSegment *GetSortedSegment(size_t id, size_t page_size)
    {
        if (!segment_bitmap_.Exist(id))
        {
            ERROR_EXIT("try to get unallocted log segment %lu", id);
        }
        SortedSegment *seg;
        seg = new SortedSegment(start_addr_, id, INVALID_NODE, page_size, true);
        return seg;
    };

    inline size_t TrasformOffsetToId(size_t pm_offset)
    {
        return pm_offset / SEGMENT_SIZE;
    };

    SortedSegment *GetSortedSegmentForDelete(size_t id, size_t page_size)
    {
        if (!segment_bitmap_.Exist(id))
        {
            ERROR_EXIT("try to get unallocted log segment %lu", id);
        }
        SortedSegment *seg;
        seg = new SortedSegment(start_addr_, id, OPEN_FOR_DELETE, page_size, true);
        return seg;
    }

    char *GetStartAddr() { return start_addr_; };

    void ClearLogGroup(int idx)
    {
        LOG("clear log group: %lu log segment, %lu remaining",log_segment_group_[idx].size(),log_segment_bitmap_.GetUsedBitsNum());
        log_segment_group_[idx].clear();
    }

    void GetElementsFromLogGroup(int idx, std::vector<size_t> *list)
    {
        log_segment_group_[idx].get_elements(list);
    }

private:
};
