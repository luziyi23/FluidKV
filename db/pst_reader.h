/**
 * @file pst_reader.h
 * @author your name (you@domain.com)
 * @brief a wrapper of pindex reader and datablock reader which provides interfaces for reading a certain pst
 * @version 0.1
 * @date 2022-08-31
 *
 * @copyright Copyright (c) 2022
 *
 */
#pragma once
#include "table.h"
#include "pindex_reader.h"
#include "datablock_reader.h"

class PSTReader
{
private:
    PIndexReader pindex_reader_;
    DataBlockReader datablock_reader_;
    std::vector<std::pair<uint64_t, uint64_t>> indexlist_;

public:
    PSTReader(SegmentAllocator *allocator);
    ~PSTReader();

    PSTMeta RecoverPSTMeta(uint64_t pindex_addr);
    bool PointQuery(uint64_t pindex_addr, Slice key, const char *value_out, int *value_size, int datablock_num = PIndexBlock::MAX_ENTRIES);
    class Iterator
    {
    public:
        PSTReader *reader_;
        std::vector<std::pair<uint64_t, uint64_t>> indexes_;
        std::vector<std::pair<uint64_t, uint64_t>> records_;
        int current_datablock_index_ = 0;
        DataBlockMeta current_datablock_meta_;
        int current_record_index_ = 0;
        Iterator(PSTReader *reader, uint64_t pm_offset) : reader_(reader)
        {
            reader_->pindex_reader_.ReadPIndexBlock(pm_offset, indexes_);
            if (!indexes_.empty())
            {
                current_datablock_meta_ = reader_->datablock_reader_.TraverseDataBlock(indexes_[current_datablock_index_].second, &records_);
            }
        };
        ~Iterator()
        {
            std::vector<std::pair<uint64_t, uint64_t>>().swap(indexes_);
            std::vector<std::pair<uint64_t, uint64_t>>().swap(records_);
        };
        bool Next()
        {
            if (current_record_index_ >= records_.size() - 1)
            {
                // read new datablock
                if (current_datablock_index_ >= indexes_.size() - 1)
                    return false;
                current_datablock_index_++;
                size_t pm_offset = indexes_[current_datablock_index_].second;
                current_datablock_meta_ = reader_->datablock_reader_.TraverseDataBlock(pm_offset, &records_);
            }
            current_record_index_++;
            return true;
        };
        uint64_t Key() { return records_[current_record_index_].first; }
        uint64_t Value() { return records_[current_record_index_].second; }
        bool LastOne()
        {
            if (current_datablock_index_ >= indexes_.size() - 1 && current_record_index_ >= records_.size() - 1)
                return true;
            return false;
        }
    };
    Iterator *GetIterator(uint64_t pindex_addr);
};
#define NotOverlappedMark 100000
struct RowIterator
{
public:
    PSTReader *pst_reader_;
    PSTReader::Iterator *pst_iter_ = nullptr;
    std::vector<TaggedPstMeta> &pst_list_;
    int current_pst_idx_ = 0;

    RowIterator(PSTReader *pst_reader, std::vector<TaggedPstMeta> &pst_list) : pst_reader_(pst_reader), pst_list_(pst_list) {}
    ~RowIterator()
    {
        if (pst_iter_)
            delete pst_iter_;
    }
    void MarkPst() { pst_list_[current_pst_idx_].level = NotOverlappedMark; }
    inline TaggedPstMeta GetPst() { return pst_list_[current_pst_idx_]; }

    bool NextPst()
    {
        current_pst_idx_++;
        if (current_pst_idx_ >= pst_list_.size())
            return false;
        return true;
    }
    void ResetPstIter()
    {
        assert(current_pst_idx_ < pst_list_.size());
        pst_iter_ = pst_reader_->GetIterator(GetPst().meta.indexblock_ptr_);
    }

    uint64_t GetCurrentKey()
    {
        if (pst_iter_)
        {
            return pst_iter_->Key();
        }
        return GetPst().meta.min_key_;
    }

    uint64_t GetCurrentValue()
    {
        if (!pst_iter_)
            ResetPstIter();
        return pst_iter_->Value();
    }

    bool NextKey()
    {
        if (current_pst_idx_ >= pst_list_.size())
            return false;
        if (!pst_iter_)
            ResetPstIter();

        if (pst_iter_->Next())
        {
            return true;
        }
        delete pst_iter_;
        pst_iter_ = nullptr;
        return NextPst();
    }
    bool Valid()
    {
        return current_pst_idx_ < pst_list_.size();
    }
};