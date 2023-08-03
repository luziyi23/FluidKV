#pragma once
#include "db_common.h"
/**
 * @brief Contains the addresses of Indexblock and some metadata
 *
 */
struct PSTMeta
{
    uint64_t indexblock_ptr_ = 0;
    uint64_t max_key_ = 0;
    uint64_t min_key_ = MAX_UINT64;
    uint32_t seq_no_ = 0;
    uint16_t entry_num_ = 0;
    uint16_t datablock_num_ = 0;

    static PSTMeta InvalidTable() { return PSTMeta(); }
    bool Valid()
    {
        // DEBUG("indexblock_ptr_=%lu,get_pure_indexblock_ptr=%lu",indexblock_ptr_,get_pure_indexblock_ptr());
        return indexblock_ptr_ != 0;
    }
};

struct TaggedPstMeta
{
    PSTMeta meta;
    // optional information. maybe lost after recovery
    size_t level;
    size_t manifest_position;
    bool Valid()
    {
        return meta.Valid();
    }
};

union TaggedPtr
{
    uint64_t valid : 1;
    uint64_t manifest_idx : 24;
    // 512-byte aligned
    uint64_t indexblock_ptr : 39;
};