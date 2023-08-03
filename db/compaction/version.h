/**
 * @file version.h
 * @author your name (you@domain.com)
 * @brief each version stores a view of the PSTs contained in each layer of the LSM-tree
 * @version 0.1
 * @date 2022-08-31
 *
 * @copyright Copyright (c) 2022
 *
 */
#pragma once
#include "db/table.h"
#include "db/pst_reader.h"
#include "db/pst_builder.h"
#include "db/pst_deleter.h"

#include <algorithm>
#include <queue>
#include <map>
#include <atomic>

struct TreeMeta
{
    bool valid = false;
    uint64_t min_key = MAX_UINT64;
    uint64_t max_key = 0;
    uint64_t size = 0;
};

class Manifest;
class Version
{
private:
    /**
     * @brief level 0 structure
     *
     * | nullptr | nullptr | tree1 | tree2 | tree3 | tree4 | nullptr | nullptr
     *                        ↑                                ↑
     *                        head ->                         tail ->
     *                       oldest tree,compact           newest tree, flush
     */
    std::vector<std::vector<TaggedPstMeta>> level0_table_lists_;
    Index *level0_trees_[MAX_L0_TREE_NUM];
    int l0_tail_ = 0;
    int l0_head_ = 0;
    int l0_read_tail_ = 0;

    TreeMeta level0_tree_meta_[MAX_L0_TREE_NUM];
    // TODO: recover it when recovering db
    int l0_tree_seq_ = 0;
    int l1_seq_ = 0;

    // level1
    std::vector<TaggedPstMeta> level1_tables_;
    Index *level1_tree_;
    std::vector<size_t> level1_free_list_;
    PSTReader pst_reader_;

public:
    Version(SegmentAllocator *seg_allocator);
    ~Version();

    int InsertTableToL0(TaggedPstMeta table, int tree_idx);
    int InsertTableToL1(TaggedPstMeta table);
    bool DeleteTableInL1(PSTMeta table);
    // bool DeleteTable(int idx, int level_id);

    bool Get(Slice key, const char *value_out, int *value_size, PSTReader *pst_reader);
    RowIterator *GetLevel1Iter(Slice key, PSTReader *pst_reader,std::vector<TaggedPstMeta>& table_metas);
    int GetLevelSize(int level)
    {
        if (level == 1)
            return level1_tables_.size() - level1_free_list_.size();
        int count = 0;
        if (level == 0)
        {
            for (int i = 0; i < level0_table_lists_.size(); i++)
            {
                count += level0_table_lists_[i].size();
            }
        }
        return count;
    }

    /**
     * @brief
     *
     * @return int tree idx (determined by l0_tail_)
     */
    bool CheckSpaceForL0Tree();
    int AddLevel0Tree();
    uint32_t GetCurrentL0TreeSeq();
    void SetCurrentL0TreeSeq(uint32_t seq);
    uint32_t GenerateL1Seq();
    bool FreeLevel0Tree();
    void UpdateLevel0ReadTail();
    int GetLevel0TreeNum()
    {
        return (l0_read_tail_ + MAX_L0_TREE_NUM - l0_head_) % MAX_L0_TREE_NUM;
    };
    int PickLevel0Trees(std::vector<std::vector<TaggedPstMeta>> &outputs, std::vector<TreeMeta> &tree_metas, int max_size = MAX_L0_TREE_NUM);
    bool PickOverlappedL1Tables(size_t min, size_t max, std::vector<TaggedPstMeta> &output);

    bool L1TreeConsistencyCheckAndFix(PSTDeleter* pst_deleter,Manifest* manifest);
};
