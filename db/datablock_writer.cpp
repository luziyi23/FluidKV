#include "datablock_writer.h"
#include <sys/mman.h>

DataBlockWriterPm::DataBlockWriterPm(SegmentAllocator *allocator) : seg_allocator_(allocator), current_segment_(nullptr)
{
    LOG("DataBlockWriterPm init");
}

DataBlockWriterSsd::DataBlockWriterSsd(SegmentAllocator *allocator) : seg_allocator_(allocator), current_segment_(nullptr)
{
    LOG("DataBlockWriterSsd init");
}

DataBlockWriterPm::~DataBlockWriterPm()
{
    if (current_segment_)
    {
        seg_allocator_->CloseSegment(current_segment_);
    }

    PersistCheckpoint();
}
DataBlockWriterSsd::~DataBlockWriterSsd()
{
    LOG("FlushSSD over");
    PersistCheckpoint();
}

bool DataBlockWriterPm::AddEntry(Slice key, Slice value)
{
    if (key.size() <= 8)
    {
        if (value.size() <= 8)
        {
            auto current_block512 = &blocks_buf_;
            if (!current_block512->valid())
            {
                allocate_block();
            }
            if (current_block512->is_full())
            {
                return false;
            }

            current_block512->add_entry(*reinterpret_cast<const uint64_t *>(key.data()), *reinterpret_cast<const uint64_t *>(value.data()));
            LOG("add entry in datablock: size=%d,%lu:%lu", current_block512->size, key.ToUint64Bswap(), *reinterpret_cast<const uint64_t *>(value.data()));
            return true;
        }
    }
    ERROR_EXIT("not 8+8 byte KV");
    return false;
}

bool DataBlockWriterSsd::AddEntry(Slice key, Slice value)
{
    if (key.size() <= 8)
    {
        if (value.size() <= 8)
        {
            auto current_block = &blocks_buf_;
            if (!current_block->valid())
            {
                allocate_block();
            }
            if (current_block->is_full())
            {
                return false;
            }

            current_block->add_entry(*reinterpret_cast<const uint64_t *>(key.data()), *reinterpret_cast<const uint64_t *>(value.data()));
            LOG("add entry in datablock: size=%d,%lu:%lu", current_block->size, *reinterpret_cast<const uint64_t *>(key.data()), *reinterpret_cast<const uint64_t *>(value.data()));

            return true;
        }
    }
    ERROR_EXIT("not 8+8 byte KV");
    return false;
}

uint64_t DataBlockWriterPm::GetCurrentMinKey()
{
    return blocks_buf_.data_buf.entries[0].key;
}
uint64_t DataBlockWriterPm::GetCurrentMaxKey()
{
    return blocks_buf_.data_buf.entries[blocks_buf_.size].key;
}
uint64_t DataBlockWriterSsd::GetCurrentMinKey()
{
    return blocks_buf_.data_buf.entries[0].key;
}
uint64_t DataBlockWriterSsd::GetCurrentMaxKey()
{
    return blocks_buf_.data_buf.entries[blocks_buf_.size].key;
}

uint64_t DataBlockWriterPm::Flush()
{

    if (blocks_buf_.pm_page_addr != nullptr)
    {
        LOG("flush datablock:size=%d", blocks_buf_.size);
        uint64_t pm_block_addr = (uint64_t)(blocks_buf_.pm_page_addr - seg_allocator_->GetStartAddr());
        if (blocks_buf_.size != 0)
        {
            while (!blocks_buf_.is_full())
            {
                blocks_buf_.add_entry(INVALID_PTR, INVALID_PTR);
            }
            LOG("Flush PM datablock to %lu,offset=%lu,%lu", (uint64_t)blocks_buf_.pm_page_addr, pm_block_addr, sizeof(PDataBlock));
            pmem_memcpy_persist(blocks_buf_.pm_page_addr, &blocks_buf_.data_buf, sizeof(PDataBlock));
        }
        blocks_buf_.clear();
        return pm_block_addr;
    }
    return INVALID_PTR;
}
uint64_t DataBlockWriterSsd::Flush()
{
    if (blocks_buf_.valid())
    {
        FilePtr fptr = blocks_buf_.file_ptr;
        if (blocks_buf_.size != 0)
        {
            int idx = blocks_buf_.size - 1;
            size_t key = blocks_buf_.data_buf.entries[idx].key;
            size_t value = blocks_buf_.data_buf.entries[idx].value;
            while (!blocks_buf_.is_full())
            {
                blocks_buf_.add_entry(key, value);
            }
            auto ret = pwrite(fptr.fd, &blocks_buf_.data_buf, sizeof(PSSDBlock), fptr.offset);
            assert(ret > 0);
        }
        return fptr.data();
    }
    return INVALID_PTR;
}

void DataBlockWriterPm::allocate_block()
{
    assert(blocks_buf_.pm_page_addr == nullptr);
    if (current_segment_ == nullptr)
    {
        current_segment_ = seg_allocator_->AllocSortedSegment(sizeof(PDataBlock), true);
    }
    char *addr = current_segment_->AllocatePage();
    LOG("datablock writer alloc page=%lu from segment %lu", (uint64_t)(addr - seg_allocator_->GetStartAddr()), current_segment_->segment_id_);
    if (addr == nullptr) // if current segment is full, alloc new segment
    {
        used_segments_.push_back(current_segment_);
        // seg_allocator_->CloseSegment(current_segment_);
        current_segment_ = seg_allocator_->AllocSortedSegment(sizeof(PDataBlock), true);
        LOG("retry:datablock writer alloc segment");
        addr = current_segment_->AllocatePage();
        if (addr == nullptr)
        {
            fflush(stdout);
            assert(addr != nullptr);
        }
        LOG("retry:datablock writer alloc page=%lu from segment %lu", (uint64_t)(addr - seg_allocator_->GetStartAddr()), current_segment_->segment_id_);
    }
    blocks_buf_.clear();
    blocks_buf_.set_page(addr);
}
void DataBlockWriterSsd::allocate_block()
{
    // ERROR_EXIT("not implement");
    if (current_segment_ == nullptr)
    {
        current_segment_ = seg_allocator_->AllocSortedSegmentOnSSD(sizeof(PSSDBlock));
    }

    FilePtr fptr = current_segment_->AllocatePage();
    if (!fptr.Valid()) // if current segment is full, alloc new segment
    {
        seg_allocator_->CloseSegment(current_segment_);
        seg_allocator_->AllocSortedSegmentOnSSD(sizeof(PSSDBlock));
        fptr = current_segment_->AllocatePage();
    }
    blocks_buf_.clear();
    blocks_buf_.set_fptr(fptr);
}

int DataBlockWriterPm::PersistCheckpoint()
{
    int size = used_segments_.size();
    if (current_segment_)
    {
        seg_allocator_->CloseSegment(current_segment_);
        current_segment_ = nullptr;
        size++;
    }
    for (auto &seg : used_segments_)
    {
        seg_allocator_->CloseSegment(seg);
    }
    used_segments_.clear();
    return size;
}
int DataBlockWriterSsd::PersistCheckpoint() { return 0; }