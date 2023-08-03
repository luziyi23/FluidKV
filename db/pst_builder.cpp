#include "pst_builder.h"

// TODO： this is only for pm pst. support SSD data_writer_
PSTBuilder::PSTBuilder(SegmentAllocator *segment_allocator, bool use_ssd_for_data) : pindex_writer_(segment_allocator)
{
    if (use_ssd_for_data)
    {
        data_writer_ = new DataBlockWriterSsd(segment_allocator);
    }
    else
    {
        data_writer_ = new DataBlockWriterPm(segment_allocator);
    }
}

PSTBuilder::~PSTBuilder()
{
    PersistCheckpoint();
    if (data_writer_)
    {
        delete (DataBlockWriterPm *)data_writer_;
    }
}

/**
 * @brief
 *
 * @param key
 * @param value
 * @return true ok
 * @return false all datablocks and indexblock is full, need flush PST
 */
bool PSTBuilder::AddEntry(Slice key, Slice value)
{
    if (datablock_metas_.size() >= max_datablock_num)
    {
        return false;
    }
    auto ret = data_writer_->AddEntry(key, value);
    if (!ret)
    {
        uint64_t min_key_in_datablock = data_writer_->GetCurrentMinKey();
        uint64_t datablock_addr = data_writer_->Flush();

        
        meta_.datablock_num_++;
        datablock_metas_.push_back(std::make_pair(min_key_in_datablock, datablock_addr));
        if (datablock_metas_.size() >= max_datablock_num)
        {
            return false;
        }
        auto ret2 = data_writer_->AddEntry(key, value);
        if (!ret2)
        {
            ERROR_EXIT("can't add entry to data block writer");
        }
    }
    if (meta_.min_key_ == MAX_UINT64)
        meta_.min_key_ = key.ToUint64();
    meta_.max_key_ = key.ToUint64();
    meta_.entry_num_++;
    return true;
}

// build a indexblock, then flush all of the datablocks and the indexblock
PSTMeta PSTBuilder::Flush()
{
    // flushed datablocks
    for (auto &datablock : datablock_metas_)
    {
        pindex_writer_.AddEntry(Slice(&datablock.first), datablock.second);
    }
    // maybe there is a current datablock
    uint64_t min_key_in_datablock = data_writer_->GetCurrentMinKey();
    uint64_t datablock_addr = data_writer_->Flush();
    if (datablock_addr != INVALID_PTR)
    {
        pindex_writer_.AddEntry(Slice(&min_key_in_datablock), datablock_addr);
        meta_.datablock_num_++;
    }

    // flush pindex
    meta_.indexblock_ptr_ = pindex_writer_.Flush();

    PSTMeta ret = meta_;
    Clear();
    return ret;
}

void PSTBuilder::Clear()
{
    meta_ =
        {
            .indexblock_ptr_ = 0,
            .max_key_ = 0,
            .min_key_ = MAX_UINT64,
            .entry_num_ = 0,
            .datablock_num_ = 0};
    datablock_metas_.clear();
}

/**
 * @brief need flush before this
 *
 */
void PSTBuilder::PersistCheckpoint()
{
    pindex_writer_.PersistCheckpoint();
    data_writer_->PersistCheckpoint();
}
