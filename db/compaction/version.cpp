#include "version.h"
#include "manifest.h"
#include "lib/index_masstree.h"
#ifdef HOT_L1
#include "lib/index_hot.h"
#endif
#include <algorithm>
Version::Version(SegmentAllocator *seg_allocator) : pst_reader_(seg_allocator)
{
    level0_table_lists_.resize(MAX_L0_TREE_NUM);
    for (int i = 0; i < MAX_L0_TREE_NUM; i++)
    {
        level0_trees_[i] = nullptr;
    }
    level1_tables_.reserve(6553600); // reserve for at most 6400M records

#ifdef MASSTREE_L1
    level1_tree_ = new MasstreeIndex();
#endif
#ifdef HOT_L1
    level1_tree_ = new HOTIndex(MAX_PST_L1);
#endif
}

Version::~Version()
{

    if (level1_tree_)
        delete level1_tree_;

    // TODO: maybe save some metadata?
}

std::vector<PSTMeta>::const_iterator FindTableInLevels(const std::vector<PSTMeta> &level,
                                                       const Slice &key)
{
    // OBSOLUTE
    auto cmp = [&](const PSTMeta &f, const Slice &k) -> bool
    {
        return Slice(&f.min_key_).compare(k);
    };
    auto iter = std::lower_bound(level.begin(), level.end(), key, cmp);

    return iter;
}
int Version::InsertTableToL0(TaggedPstMeta tmeta, int tree_idx)
{
    // get position
    PSTMeta table = tmeta.meta;
    int idx = level0_table_lists_[tree_idx].size();
    level0_table_lists_[tree_idx].emplace_back(tmeta);
    // update index
    ValueHelper lh((uint64_t)idx);
    size_t tempk = table.max_key_;
    level0_trees_[tree_idx]->Put(tempk, lh);
    uint64_t maxk = tmeta.meta.max_key_, mink = tmeta.meta.min_key_;
    if (Slice(&maxk).compare(Slice(&level0_tree_meta_[tree_idx].max_key)) > 0)
    {
        level0_tree_meta_[tree_idx].max_key = maxk;
    }
    if (Slice(&mink).compare(Slice(&level0_tree_meta_[tree_idx].min_key)) < 0)
    {
        level0_tree_meta_[tree_idx].min_key = mink;
    }
    return idx;
}
int Version::InsertTableToL1(TaggedPstMeta tmeta)
{
    PSTMeta table = tmeta.meta;

    int idx;
    // find a position
    if (level1_free_list_.empty())
    {
        idx = level1_tables_.size();
        level1_tables_.emplace_back(tmeta);
    }
    else
    {
        idx = level1_free_list_.back();
        level1_free_list_.pop_back();
        level1_tables_[idx] = tmeta;
    }

    assert(level1_tables_.size() > idx);
    // update index
    ValueHelper lh((uint64_t)idx);
    size_t tempk = table.max_key_;
    level1_tree_->Put(tempk, lh);
    // 对重复key要判别，value相同则忽略.value不同要防删,被替换的pst要在levels_[level_id]中删除(放入free list)。
    if (lh.old_val != INVALID_VALUE)
    {
        LOG("replace idx=%lu", lh.old_val);
        level1_free_list_.push_back(lh.old_val);
    }
    return idx;
}

// 删除时比对删除的value是否为table的indexblock_ptr，若不是说明已经被同key的其他pst替代了
bool Version::DeleteTableInL1(PSTMeta table)
{
    // find vector index by searching tree
    uint64_t idx = level1_tree_->Get(table.max_key_);
    // check if table.indexblock_ptr = levels_[level_id][idx].indexblock_ptr, if yes,continue, if no,return
    assert(idx != INVALID_PTR);
    PSTMeta &old = level1_tables_[idx].meta;
    if (table.indexblock_ptr_ != old.indexblock_ptr_)
    {
        LOG("delete meet same key: old=%lu(%lu~%lu),new(%lu~%lu)", idx, __bswap_64(old.min_key_), __bswap_64(old.max_key_), __bswap_64(table.min_key_), __bswap_64(table.max_key_));
        return false;
    }
    // erase table key in the tree
    level1_tree_->Delete(table.max_key_);
    // append vector idx to the freelist
    level1_free_list_.push_back(idx);

    // recycle segment space

    return true;
}

// bool Version::DeleteTable(int idx, int level_id)
// {
//     assert(level_id < max_level_);
//     // TODO: never delete table. when compaction, building new version by copying
//     // find meta in meta array
//     auto table = levels_[level_id][idx];
//     // erase table key in the tree
//     level_trees_[level_id]->Delete(table.meta.max_key_);
//     // append vector idx to the freelist
//     free_lists_[level_id].push(idx);
//     return true;
// }

int FindTableByIndex(size_t key, Index *level_index)
{
    int idx = -1;
#ifdef MASSTREE_L1
    std::vector<uint64_t> temp;
    level_index->Scan(key, 1, temp);
    if (temp.empty())
    {
        return -1;
    }
    idx = temp[0];
#endif
#ifdef HOT_L1
    auto iter = reinterpret_cast<HOTIndex *>(level_index)->Seek(key);
    idx = iter.Value();
    if (idx == INVALID_PTR)
    {
        return -1;
    }
#endif
    return idx;
}
int FindTableByIndex2(size_t key, Index *level_index)
{
    int idx = -1;
#ifdef MASSTREE_L1
    std::vector<uint64_t> temp;
    level_index->Scan(key, 2, temp);
    if (temp.size() != 2)
    {
        return -1;
    }
    idx = temp[1];
#endif
#ifdef HOT_L1
    auto iter = reinterpret_cast<HOTIndex *>(level_index)->Seek(key);
    iter.Next();
    idx = iter.Value();
    if (idx == INVALID_PTR)
    {
        return -1;
    }
#endif
    return idx;
}
inline void ScanIndexForTables(size_t key, Index *level_index, size_t table_num, std::vector<size_t> &output_table_ids)
{
#ifdef MASSTREE_L1
    std::vector<uint64_t> temp;
    level_index->Scan(key, table_num, output_table_ids);
#endif
}

bool Version::Get(Slice key, const char *value_out, int *value_size, PSTReader *pst_reader)
{
    TaggedPstMeta table;
    // DEBUG("read key=%lu,l0head=%d,l0_read_tail=%d",key.ToUint64Bswap(),l0_head_,l0_read_tail_);
    // search level0
    for (int tree_idx = l0_head_; tree_idx != l0_read_tail_; tree_idx = (tree_idx + 1) % MAX_L0_TREE_NUM)
    {
        Index *tree = level0_trees_[tree_idx];
        // DEBUG("1");
        int idx = FindTableByIndex(key.ToUint64(), tree);
        if (idx == -1)
            continue;
        // DEBUG("2");
        table = level0_table_lists_[tree_idx][idx];
        if (!table.Valid())
        {
            continue;
        }
        // DEBUG("3");
        if (table.meta.min_key_ != MAX_UINT64 && __bswap_64(table.meta.min_key_) > key.ToUint64Bswap())
        {
            continue;
        }
        // DEBUG("4");
        bool ret = pst_reader->PointQuery(table.meta.indexblock_ptr_, key, value_out, value_size, table.meta.datablock_num_);
        if (ret)
            return true;
    }
    // searchlevel1
    Index *tree = level1_tree_;
    int idx = FindTableByIndex(key.ToUint64(), tree);

    if (idx == -1)
    {
        DEBUG("5");
        return false;
    }
    table = level1_tables_.at(idx);
    if (!table.Valid())
    {
        DEBUG("6");
        return false;
    }

    if (table.meta.min_key_ != MAX_UINT64 && __bswap_64(table.meta.min_key_) > key.ToUint64Bswap())
    {
        DEBUG("7, key=%lu at %lu (%lu~%lu)", key.ToUint64Bswap(), table.meta.indexblock_ptr_, __bswap_64(table.meta.min_key_), __bswap_64(table.meta.max_key_));
        return false;
    }
    bool ret = pst_reader->PointQuery(table.meta.indexblock_ptr_, key, value_out, value_size, table.meta.datablock_num_);
    return ret;
    // TODO: if l>2 exists, add them
}

RowIterator *Version::GetLevel1Iter(Slice key, PSTReader *pst_reader, std::vector<TaggedPstMeta> &table_metas)
{
    std::vector<size_t> table_ids;
    ScanIndexForTables(key.ToUint64(), level1_tree_, 2, table_ids);
    for (auto id : table_ids)
    {
        table_metas.push_back(level1_tables_[id]);
    }
    return new RowIterator(pst_reader, table_metas);
}

bool Version::CheckSpaceForL0Tree()
{
    return (l0_tail_ + 1) % MAX_L0_TREE_NUM != l0_head_;
}

int Version::AddLevel0Tree()
{
    if ((l0_tail_ + 1) % MAX_L0_TREE_NUM == l0_head_)
        return -1;
    auto ret = l0_tail_;
    l0_tail_ = (l0_tail_ + 1) % MAX_L0_TREE_NUM;
#ifdef MASSTREE_L1
    level0_trees_[ret] = new MasstreeIndex();
#endif
#ifdef HOT_L1
    level0_trees_[ret] = new HOTIndex(MAX_PST_L1);
#endif
    l0_tree_seq_++;
    return ret;
}
uint32_t Version::GetCurrentL0TreeSeq()
{
    return l0_tree_seq_;
}
void Version::SetCurrentL0TreeSeq(uint32_t seq)
{
    l0_tree_seq_ = seq;
}
uint32_t Version::GenerateL1Seq()
{
    return l1_seq_++;
}

bool Version::FreeLevel0Tree()
{

    if (l0_tail_ == l0_head_)
    {
        return false;
    }
    auto idx = l0_head_;
    l0_head_ = (l0_head_ + 1) % MAX_L0_TREE_NUM;
    // printf("before FreeLevel0Tree\n");
    // wait for reader leave the index (just sleep for > 1ms)
    usleep(100);
    delete level0_trees_[idx];
    // printf("after FreeLevel0Tree\n");
    level0_trees_[idx] = nullptr;
    level0_tree_meta_[idx] = TreeMeta();
    level0_table_lists_[idx].clear();
    return true;
}
void Version::UpdateLevel0ReadTail()
{
    l0_read_tail_ = l0_tail_;
}

int Version::PickLevel0Trees(std::vector<std::vector<TaggedPstMeta>> &outputs, std::vector<TreeMeta> &tree_metas, int max_size)
{
    int tail = l0_read_tail_;
    int head = l0_head_;
    DEBUG("pick level0 tree from %d to %d", head, tail);
    int tree_num = (tail + MAX_L0_TREE_NUM - head) % MAX_L0_TREE_NUM;
    outputs.resize(tree_num);
    tree_metas.resize(tree_num);
    for (size_t i = 0; i < tree_num && i < max_size; i++)
    {
        int tree_id = (head + i) % MAX_L0_TREE_NUM;
        Index *tree_index = level0_trees_[tree_id];
        TreeMeta tree_meta = level0_tree_meta_[tree_id];
        tree_metas[tree_num - i - 1] = tree_meta;
        std::vector<uint64_t> value_out;
        tree_index->Scan(0, MAX_INT32, value_out);
        for (auto &idx : value_out)
        {
            // reverse output to prove less row_id -> newest row
            auto pst = level0_table_lists_[tree_id][idx];
            outputs[tree_num - i - 1].emplace_back(pst);
        }
    }
    // for (auto &tree : outputs)
    // {
    //     LOG("tree tables:%lu", tree.size());
    //     RowIterator *row = new RowIterator(&pst_reader_, tree);
    //     size_t cmax = row->GetCurrentKey();
    //     size_t k;
    //     while (row->NextKey())
    //     {
    //         k = row->GetCurrentKey();
    //         printf("cmx:%lu,key=%lu\n",__bswap_64(cmax),__bswap_64(k));
    //         fflush(stdout);
    //         if (__bswap_64(k) < __bswap_64(cmax))
    //             ERROR_EXIT("error sequence");
    //         cmax = k;
    //     }
    //     delete row;
    // }
    return tree_metas.size();
}
bool Version::PickOverlappedL1Tables(size_t min, size_t max, std::vector<TaggedPstMeta> &output)
{
    std::vector<uint64_t> key_out, value_out;
    level1_tree_->ScanByRange(min, max, key_out, value_out);
    size_t last_max = max;
    for (auto &idx : value_out)
    {
        output.emplace_back(level1_tables_[idx]);
        last_max = level1_tables_[idx].meta.max_key_;
    }
    bool over_flag = false;
    while (!over_flag)
    {
        std::vector<uint64_t> value_out2;
        level1_tree_->Scan(last_max, 9, value_out2);
        for (auto &idx : value_out2)
        {
            auto &pst = level1_tables_[idx];
        }
        int i = 0;
        if (value_out2.size() == 0)
            break;
        if (value_out2.size() < 9)
            over_flag = true;
        if (level1_tables_[value_out2[0]].meta.max_key_ == last_max)
        {
            if (value_out2.size() == 1)
                break;
            i = 1;
        }

        for (; i < value_out2.size(); i++)
        {
            size_t idx = value_out2[i];
            auto &pst = level1_tables_[idx];
            if (__bswap_64(pst.meta.min_key_) <= __bswap_64(max))
            {
                LOG("additional table: %lu~%lu", __bswap_64(pst.meta.min_key_), __bswap_64(pst.meta.max_key_));
                output.emplace_back(pst);
                last_max = pst.meta.max_key_;
            }
            else
            {
                over_flag = true;
                break;
            }
        }
    }
    return true;
}

bool Version::L1TreeConsistencyCheckAndFix(PSTDeleter *pst_deleter,Manifest* manifest)
{
    std::vector<uint64_t> key_out, value_out;
    level1_tree_->ScanByRange(0, MAX_UINT64, key_out, value_out);
    TaggedPstMeta last_pst_meta, current_pst_meta;
    uint64_t min_key, max_key;
    DEBUG("L1 tree size=%lu",value_out.size());
    for (auto &idx : value_out)
    {
        current_pst_meta = level1_tables_[idx];
        if (last_pst_meta.Valid())
        {
            if (__bswap_64(last_pst_meta.meta.max_key_) >= __bswap_64(current_pst_meta.meta.min_key_))
            {
                // overlapped! delete the oldest
                if (last_pst_meta.meta.seq_no_ < current_pst_meta.meta.seq_no_)
                {
                    // last is old
                    DeleteTableInL1(last_pst_meta.meta);
                    manifest->DeleteTable(last_pst_meta.manifest_position,1);
                }
                else
                {
                    // current is old
                    DeleteTableInL1(current_pst_meta.meta);
                    manifest->DeleteTable(current_pst_meta.manifest_position,1);
                }
            }
        }
        last_pst_meta = current_pst_meta;
    }
    pst_deleter->PersistCheckpoint();
    return true;
}