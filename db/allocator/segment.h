#pragma once

#include "bitmap.h"
#include <vector>
#include <map>
#include <assert.h>
#include <mutex>
#include <sys/param.h>
#include <queue>
#include <libpmem.h>



// The meta data of basic segment can be persisted.
// The allocators(log allocator and sorted table allocator) only needs to maintain the shared global allocation table
class BaseSegmentMeta
{
public:
    BaseSegmentMeta(char *segment_pool_addr, size_t segment_id)
        : segment_id_(segment_id), start_(segment_pool_addr + segment_id * SEGMENT_SIZE),
          end_(start_ + SEGMENT_SIZE)
    {
        // DEBUG("create segment %lu at %lu(+%lu)",segment_id, (uint64_t)start_,segment_id * SEGMENT_SIZE);
    }

    virtual ~BaseSegmentMeta() {}

    const size_t segment_id_;

protected:
    char *const start_;
    char *const end_;

    // char *AllocSpace(size_t size)
    // {
    //     char *ret = tail_;
    //     if (ret + size <= end_)
    //     {
    //         tail_ += size;
    //         return ret;
    //     }
    //     else
    //     {
    //         return nullptr;
    //     }
    // }

    DISALLOW_COPY_AND_ASSIGN(BaseSegmentMeta);
};
enum SegmentStatus
{
    StatusFree = 0,
    StatusAvailable = 1, // allocated but not used
    StatusUsing = 2,     // writing, other thread cannot write/reopen for delete
    StatusClosed = 3     // closed, read-only
};
enum PBlockType
{
    // TODO: dont use this to denote index and data granularity
    INVALID_NODE = 0,
    INDEX512_TO_ENTRY = 1,
    INDEX512_TO_BLOCK512 = 2,
    INDEX512_TO_BLOCK4K = 3,
    DATABLOCK64 = 4,
    DATABLOCK128 = 5,
    DATABLOCK256 = 6,
    DATABLOCK512 = 7,
    DATABLOCK4K = 8,
    LOG = 9,
    OPEN_FOR_DELETE = 10
};
// The entries in log segment should be self-described (can be recovered if footer is loss).
// This can be realized by specific log entry format. See log_writer.h/cc.

class LogSegment : public BaseSegmentMeta
{
public:
    struct Header
    {
        // uint32_t offset; // only valid when status is closed
        uint32_t segment_status : 2;
        uint32_t segment_block_type : 6;
        uint32_t objects_tail_offset : 24;
        uint32_t magic : 32;
        char reserve[56]; // for cacheline alignment of logging
    };
    

public:
    LogSegment(char *segment_pool_addr, size_t segment_id, bool exist = 0)
        : BaseSegmentMeta(segment_pool_addr, segment_id), tail_(start_ + sizeof(Header))
    {
        // TODO: recover footer by read the persisted footer
        if (exist)
        {
            memcpy(&header_, start_, sizeof(Header));
            header_.segment_status = StatusUsing;
            PersistHeader();
        }
        else
        {
            LOG("create log segment %lu at %lu(+%lu)", segment_id_, (uint64_t)start_, segment_id_ * SEGMENT_SIZE);
            header_.segment_status = StatusUsing;
            header_.segment_block_type = LOG;
            header_.objects_tail_offset = 0;
            PersistHeader();
        }
    };
    void Avail()
    {
        // index is persisted, can be gc
        header_.segment_status = StatusAvailable;
        PersistHeader();
    }
    void Close()
    {
        header_.segment_status = StatusClosed;
        header_.objects_tail_offset = tail_ - start_;
        // TODO: compute checksum
        PersistHeader();
    }

    /**
     * @brief fast-persist log entry
     *
     * @param data
     * @param size
     * @return int return offset, -1 represent overflow
     */
    int Append(const char *data, size_t size)
    {
        LOG("segid=%lu, before append: %lu bytes, tail=%lu,start_=%lu", segment_id_, tail_ - start_, (uint64_t)tail_, (uint64_t)start_ - SEGMENT_SIZE * segment_id_);
        // log data should be fast-persistency
        if (tail_ + size > end_)
            return -1;
        LOG("add log %lu", tail_ - start_ + segment_id_ * SEGMENT_SIZE);
        pmem_memcpy_persist(tail_, data, size);
        tail_ += size;
        LOG("after append: %lu bytes,ret=%lu,ret2=%lu", tail_ - start_, tail_ - start_ - size, tail_ - start_ - size - sizeof(Header));
        return tail_ - start_ - size - sizeof(Header);
    }

    void AlignTailTo64B()
    {
        if ((size_t)tail_ % 64 != 0)
        {
            tail_=(char*)roundup((size_t)tail_,64);
        }
    }

    bool Free()
    {
        LOG("free log segment %lu at %lu(+%lu)", segment_id_, (uint64_t)start_, segment_id_ * SEGMENT_SIZE);
        header_.segment_status = StatusFree;
        header_.objects_tail_offset = 0;
        header_.magic = 0;
        PersistHeader();
        return true;
    }

    Header GetHeader(){
        return header_;
    }

    char* GetStartAddr(){
        return start_;
    }

private:
    Header header_;
    char *tail_;
    inline void PersistHeader()
    {
        pmem_memcpy_persist(start_, &header_, sizeof(Header));
    }
};

/**
 * @brief 512 Bytes header + 1024 Bytes bitmap + 8189 * 512-Byte blocks = 4 MB
 *
 */
static SpinLock write_delete_locks[1024];
class SortedSegment : public BaseSegmentMeta
{
public:
    struct Header // 8 Bytes
    {
        uint16_t segment_status : 2;
        uint64_t segment_block_type : 6;
    };
    const size_t PAGE_SIZE;
    const size_t EXTRA_PAGE_NUM;
    const size_t PAGE_NUM;

private:
    Header header_;
    BitMap bitmap_;
    char *data_;
    bool for_delete_ = false;

public:
    /**
     * @brief Construct a new Sorted Segment object
     *
     * @param segment_pool_addr segment head addr
     * @param segment_id segment index
     * @param type data block type. if exist = true, this will be disabled
     * @param exist if exist, recover the header and bitmap from storage
     */
    SortedSegment(char *segment_pool_addr, size_t segment_id, PBlockType type, int page_size, bool exist = 0)
        : BaseSegmentMeta(segment_pool_addr, segment_id),
          PAGE_SIZE(page_size),
          EXTRA_PAGE_NUM(1 + roundup(SEGMENT_SIZE / PAGE_SIZE / 8, PAGE_SIZE) / PAGE_SIZE),
          PAGE_NUM(SEGMENT_SIZE / PAGE_SIZE - EXTRA_PAGE_NUM),
          bitmap_(PAGE_NUM), // need 1KB-2B for bitmap,use start_ for bitmap_ to align with 64B
          data_(start_ + EXTRA_PAGE_NUM * PAGE_SIZE)
    {
        LOG("new sorted segment, id=%lu,start_addr=%lu", segment_id_, (uint64_t)start_);
        bitmap_.SetPersistAddr(start_ + PAGE_SIZE);
        if (exist)
        {
            bitmap_.Recover();
            memcpy(&header_, start_, sizeof(Header));
            if (type == OPEN_FOR_DELETE)
                for_delete_ = true;
        }
        else
        {
            if (type == INVALID_NODE)
            {
                ERROR_EXIT("error type of index segment");
            }
            LOG("create sorted segment %lu at %lu(+%lu)", segment_id, (uint64_t)start_, segment_id * SEGMENT_SIZE);
            header_.segment_status = StatusUsing;
            header_.segment_block_type = type;
            // bitmap_.PersistToPM();
            PersistBitmapHard();
        }
    }
    ~SortedSegment() {}
    SegmentStatus status() { return (SegmentStatus)header_.segment_status; }
    PBlockType type() { return (PBlockType)header_.segment_block_type; }
    void RecoverHeader() { memcpy(&header_, start_, sizeof(Header)); }
    void SetForDelete(bool value) { for_delete_ = value; }
    /**
     * @brief
     *
     * @return char* page start addr, nullptr denotes no free page.
     */
    char *AllocatePage()
    {
        size_t id = bitmap_.AllocateOne();
        if (id == ERROR_CODE)
        {
            return nullptr;
        }

        return data_ + id * PAGE_SIZE;
    }

    /**
     * @brief allocate consecutive pages
     *
     * @param num
     * @return char* start addr of the allocated pages, nullptr denotes no enough free pages.
     */
    char *BatchAllocatePage(size_t num)
    {
        assert(num <= PAGE_NUM);
        size_t id = bitmap_.AllocateMany(num);
        if (id == ERROR_CODE)
            return nullptr;
        return data_ + id * PAGE_SIZE;
    }

    /**
     * @brief free a page
     *
     * @param id
     * @return true
     * @return false
     */
    bool RecyclePage(size_t id)
    {
        assert(id <= PAGE_NUM);
        LOG("recycle page seg %lu id=%lu", segment_id_, id);
        bool ret = bitmap_.Free(id);
        if (!ret)
        {
            ERROR_EXIT("offset=%lu,seg=%lu,id=%lu", 1536 + SEGMENT_SIZE * segment_id_ + PAGE_SIZE * id, segment_id_, id);
        }
        return ret;
    }
    void Close()
    {
        header_.segment_status = StatusClosed;
        PersistBitmapSoft();
        PersistHeader();
    }
    void Freeze()
    {
        header_.segment_status = StatusAvailable;
        PersistHeader();
        PersistBitmapSoft();
    }
    void Reuse()
    {
        header_.segment_status = StatusUsing;
        PersistHeader();
        bitmap_.Recover();
    }
    bool Full()
    {
        return bitmap_.IsFull();
    }
    void PersistBitmapSoft()
    {
        std::lock_guard<SpinLock> lk(write_delete_locks[segment_id_ % 1024]);
        if (for_delete_)
        {
            bitmap_.PersistToPMOnlyFree();
        }
        else
        {
            bitmap_.PersistToPMOnlyAlloc();
        }
    }
    void PersistBitmapHard()
    {
        bitmap_.PersistToPM();
    }

    inline size_t TrasformOffsetToPageId(size_t pm_offset)
    {
        return ((pm_offset % SEGMENT_SIZE) - EXTRA_PAGE_NUM * PAGE_SIZE) / PAGE_SIZE;
    };

private:
    inline void PersistHeader()
    {
        pmem_memcpy_persist(start_, &header_, sizeof(Header));
    }
};

/**
 * @brief 4096B Header with bitmap + 1023 * 4096-Byte blocks = 4 MB
 *
 */
class SortedSegmentOnSSD
{
    struct Header // 8 Bytes
    {
        uint16_t segment_status : 2;
        uint64_t segment_block_type : 6;
    };
    const size_t PAGE_SIZE;
    const size_t EXTRA_PAGE_NUM;
    const size_t PAGE_NUM;

private:
    Header header_;
    BitMap bitmap_;
    size_t file_id_;
    int fd_;
    char *buf_;

public:
    /**
     * @brief Construct a new Sorted Segment object
     *
     * @param segment_pool_addr pmem addr
     * @param segment_id segment index
     * @param type data block type. if exist = true, this will be disabled
     * @param exist if exist, recover the header and bitmap from storage
     */
    SortedSegmentOnSSD(size_t file_id, int fd, int page_size, bool exist = 0)
        : PAGE_SIZE(page_size),
          EXTRA_PAGE_NUM(1), // use 1 page to store header and bitmap
          PAGE_NUM(SEGMENT_SIZE / PAGE_SIZE - EXTRA_PAGE_NUM),
          bitmap_(PAGE_NUM), // need 1KB-2B for bitmap,use start_ for bitmap_ to align with 64B
          file_id_(file_id),
          fd_(fd),
          buf_(new char[PAGE_SIZE])
    {
        assert(fd_ > 0);
        if (exist)
        {

            LOG("recover bitmap of pindex segment");
            auto ret = pread(fd_, buf_, page_size, 0);
            assert(ret > 0);
            // TODO: check header correctness
            memcpy(&header_, buf_, sizeof(Header));
            bitmap_.RecoverFrom(buf_ + sizeof(Header));
            header_.segment_status = StatusUsing;
            PersistHeader();
        }
        else
        {
            header_.segment_status = StatusUsing;
            header_.segment_block_type = DATABLOCK4K;
        }
    }
    ~SortedSegmentOnSSD()
    {
        PersistHeader();
        delete[] buf_;
    }

    SegmentStatus status() { return (SegmentStatus)header_.segment_status; }
    PBlockType type() { return (PBlockType)header_.segment_block_type; }

    FilePtr AllocatePage()
    {
        size_t id = bitmap_.AllocateOne();
        if (id == ERROR_CODE)
        {
            return FilePtr::InvalidPtr();
        }
        int offset = PAGE_SIZE + id * PAGE_SIZE;
        return FilePtr{fd_, offset};
    }

    FilePtr BatchAllocatePage(size_t num)
    {
        assert(num <= PAGE_NUM);
        size_t id = bitmap_.AllocateMany(num);
        if (id == ERROR_CODE)
            return FilePtr::InvalidPtr();
        int offset = PAGE_SIZE + id * PAGE_SIZE;
        return FilePtr{fd_, offset};
    }

    /**
     * @brief free a page
     *
     * @param id
     * @return true
     * @return false
     */
    bool RecyclePage(size_t id)
    {
        assert(id <= PAGE_NUM);
        bool ret = bitmap_.Free(id);
        return ret;
    }
    void Close()
    {
        header_.segment_status = StatusClosed;
        PersistHeader();
    }
    void Freeze()
    {
        header_.segment_status = StatusAvailable;
        PersistHeader();
    }
    void Reuse()
    {
        header_.segment_status = StatusUsing;
        PersistHeader();
    }
    bool Full()
    {
        return bitmap_.IsFull();
    }
    int get_fd()
    {
        return fd_;
    }

private:
    inline void PersistHeader()
    {
        memcpy(buf_, &header_, sizeof(Header));
        bitmap_.CopyTo(buf_ + sizeof(Header));
        auto ret = pwrite(fd_, buf_, PAGE_SIZE, 0);
        assert(ret > 0);
    }
};