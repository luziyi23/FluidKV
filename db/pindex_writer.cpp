#include "pindex_writer.h"

PIndexWriter::PIndexWriter(SegmentAllocator *allocator) : allocator_(allocator)
{
    current_segment_ = allocator_->AllocSortedSegment(sizeof(PIndexBlock));
    LOG("Pindex writer initialized");
}

PIndexWriter::~PIndexWriter()
{
    LOG("destruct PIndexWriter");
    Flush();
    LOG("destruct PIndexWriter: close segment");
    PersistCheckpoint();
    LOG("destruct PIndexWriter over");
}

bool PIndexWriter::AddEntry(Slice key, uint64_t ptr)
{
    // TODO: currently this function only consider ~8-byte key
    //  if there is no allocated indexblock, allocate one
    if (!current_block512_.valid())
    {
        allocate_block512();
    }
    if (current_block512_.is_full())
        return false;
    // add the entry in index block(not persist)

    current_block512_.add_entry(*reinterpret_cast<const uint64_t *>(key.data()), ptr);

    return true;
}

uint64_t PIndexWriter::Flush()
{
    if (current_block512_.pm_page_addr == nullptr)
        return 0;
    uint64_t pm_addr = (uint64_t)(current_block512_.pm_page_addr - allocator_->GetStartAddr());
    if (current_block512_.size != 0)
    {
        auto idx = current_block512_.size - 1;
        size_t key, value;
        key = current_block512_.data_buf.entries[idx].key;
        value = current_block512_.data_buf.entries[idx].leafptr;
        while (!current_block512_.is_full())
        {
            current_block512_.add_entry(key, value);
        }
        LOG("flush index block %lu", current_block512_.pm_page_addr);
        persist_current_block512();
    }
    current_block512_.clear();
    return pm_addr;
}
int PIndexWriter::PersistCheckpoint()
{
    int size = used_segments_.size();
    if (current_segment_)
    {
        allocator_->CloseSegment(current_segment_);
        current_segment_ = nullptr;
        size++;
    }
    for (auto &seg : used_segments_)
    {
        allocator_->CloseSegment(seg);
    }
    used_segments_.clear();
    return size;
}

void PIndexWriter::persist_current_block512()
{
    pmem_memcpy_persist(current_block512_.pm_page_addr, &current_block512_.data_buf, sizeof(PIndexBlock));
}

void PIndexWriter::allocate_block512()
{
    assert(current_block512_.pm_page_addr == nullptr);
    char *addr = current_segment_->AllocatePage();
    if (addr == nullptr) // if current segment is full, alloc new segment
    {
        // allocator_->CloseSegment(current_segment_);
        used_segments_.push_back(current_segment_);
        current_segment_ = allocator_->AllocSortedSegment(sizeof(PIndexBlock));
        addr = current_segment_->AllocatePage();
        assert(addr != nullptr);
    }
    current_block512_.clear();
    current_block512_.set_page(addr);
    LOG("set new page %lu", (uint64_t)addr);
}