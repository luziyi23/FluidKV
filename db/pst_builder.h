/**
 * @file pst_builder.h
 * @author your name (you@domain.com)
 * @brief Persistent Sorted Table(PST) is minimum write granularity for persistently sorted data and consists of an IndexBlock and several Datablocks
 * @version 0.1
 * @date 2022-08-29
 *
 * @copyright Copyright (c) 2022
 *
 */
#pragma once
#include "pindex_writer.h"
#include "datablock_writer.h"
#include "table.h"

class PSTBuilder
{
    static constexpr size_t max_datablock_num = PIndexBlock::MAX_ENTRIES;

private:
    PIndexWriter pindex_writer_;
    DataBlockWriter* data_writer_;
    PSTMeta meta_;
    /**
     * @brief <datablock_min_key,datablock_pm_offset>
     * 
     */
    std::vector<std::pair<uint64_t,uint64_t>> datablock_metas_;

public:
    PSTBuilder(SegmentAllocator *segment_allocator, bool use_ssd_for_data = false);
    ~PSTBuilder();

    bool AddEntry(Slice key, Slice value);
    PSTMeta Flush();
    void Clear();
    void PersistCheckpoint();
};
