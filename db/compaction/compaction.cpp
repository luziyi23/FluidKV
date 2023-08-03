#include "compaction.h"
#include "manifest.h"
#include "version.h"
#include <queue>

struct KeyWithRowId
{
    uint64_t key;
    int row_id;
};

struct UintKeyComparator
{
    bool operator()(const KeyWithRowId l, const KeyWithRowId r) const
    {
        if (unlikely(__bswap_64(l.key) == __bswap_64(r.key)))
        {
            return l.row_id < r.row_id;
        }
        return __bswap_64(l.key) > __bswap_64(r.key);
    }
} cmp;

CompactionJob::CompactionJob(SegmentAllocator *seg_alloc, Version *target_version, Manifest *manifest) : seg_allocater_(seg_alloc), version_(target_version), manifest_(manifest), pst_reader_(seg_allocater_), pst_builder_(seg_allocater_), pst_deleter_(seg_allocater_), output_seq_no_(version_->GenerateL1Seq())
{
}
CompactionJob::~CompactionJob()
{
    outputs_.clear();
    std::vector<TaggedPstMeta>().swap(outputs_);
    for (auto &list : inputs_)
    {
        list.clear();
        std::vector<TaggedPstMeta>().swap(list);
    }
    std::vector<std::vector<TaggedPstMeta>>().swap(inputs_);
}
bool CompactionJob::CheckPmRoomEnough()
{
    // TODO
    return true;
}
size_t CompactionJob::PickCompaction()
{
    // TODO: use some locks or states to avoid picking same pst(level 1) in a compaction and a migration at the same time
    // iterate l0 meta
    std::vector<TreeMeta> tree_metas;
    version_->PickLevel0Trees(inputs_, tree_metas);
    size_t size = inputs_.size();
    if (size == 0)
        return 0;
    LOG("pick %lu level 0 tree", size);
    for (auto &tree_meta : tree_metas)
    {
        if (__bswap_64(min_key_) > __bswap_64(tree_meta.min_key))
        {
            min_key_ = tree_meta.min_key;
        }
        if (__bswap_64(max_key_) < __bswap_64(tree_meta.max_key))
        {
            max_key_ = tree_meta.max_key;
        }
        LOG("min:%lu,max:%lu", __bswap_64(tree_meta.min_key), __bswap_64(tree_meta.max_key));
    }
    LOG("l0 table range=%lu~%lu", __bswap_64(min_key_), __bswap_64(max_key_));
    inputs_.emplace_back(std::vector<TaggedPstMeta>());
    version_->PickOverlappedL1Tables(min_key_, max_key_, inputs_[size]);
    if (inputs_[size].size())
    {
        DEBUG("level1 tables:%lu %lu~%lu", inputs_[size].size(), __bswap_64(inputs_[size][0].meta.min_key_), __bswap_64(inputs_[size][inputs_[size].size() - 1].meta.max_key_));
    }

    return size;
}
bool CompactionJob::RunCompaction()
{
    std::priority_queue<KeyWithRowId, std::vector<KeyWithRowId>, UintKeyComparator> key_heap(cmp);
    std::vector<RowIterator> rows;
    std::vector<PSTReader *> readers;
    size_t marked_output = 0;
    // initialize: init RowIter, add first key to heap, check overlapping for each first key
    for (int i = 0; i < inputs_.size(); i++)
    {
        if (inputs_[i].empty())
            continue;
        auto pr = new PSTReader(seg_allocater_);
        readers.push_back(pr);
        rows.emplace_back(pr, inputs_[i]);
        key_heap.push(KeyWithRowId{rows[i].GetCurrentKey(), i});
    }
    size_t count = 0;
    KeyWithRowId current_max_key = {0, -1};
    // Get key from heap and get the value from row
    while (!key_heap.empty())
    {
        auto topkey = key_heap.top();
        key_heap.pop();
        LOG("now:%lu from row %d, top:%lu from row %d", __bswap_64(topkey.key), topkey.row_id, __bswap_64(key_heap.top().key), key_heap.top().row_id);
        count++;
        if (__bswap_64(topkey.key) < __bswap_64(current_max_key.key))
        {
            ERROR_EXIT("Reverse order found in Compaction %lu(%d)<%lu(%d)", __bswap_64(topkey.key), topkey.row_id, __bswap_64(current_max_key.key), current_max_key.row_id);
        }
        current_max_key = topkey;
        while (!key_heap.empty() && key_heap.top().key == topkey.key)
        {
            DEBUG("重合key %lu from row %d with row %d", __bswap_64(topkey.key), topkey.row_id, key_heap.top().row_id);
            // 如果出现重合key，旧key直接next
            if (rows[topkey.row_id].NextKey())
            {
                key_heap.push(KeyWithRowId{rows[topkey.row_id].GetCurrentKey(), topkey.row_id});
            }
            topkey = key_heap.top();
            key_heap.pop();
        }

        RowIterator &row = rows[topkey.row_id];
        bool is_overlapped = 0;
        if (row.pst_iter_ == nullptr)
        {
            // the key is the first key in a pst, now check if the pst is overlapped with other pst
            uint64_t max = row.GetPst().meta.max_key_;
            for (int i = 0; i < rows.size(); i++)
            {
                if (i == topkey.row_id)
                    continue;
                if (!rows[i].Valid())
                    continue;
                if (__bswap_64(rows[i].GetCurrentKey()) < __bswap_64(max))
                {
                    is_overlapped = 1;
                    break;
                }
            }
            if (!is_overlapped)
            {
                LOG("jump,topkey=%lu,max=%lu, is overlapped=%d", __bswap_64(topkey.key), __bswap_64(max), is_overlapped);
                // not overlapped: directly use the pst as output
                TaggedPstMeta tmeta;
                tmeta.meta = pst_builder_.Flush();
                outputs_.emplace_back(tmeta);
                TaggedPstMeta tmeta2 = row.GetPst();
                row.MarkPst();
                marked_output++;
                outputs_.emplace_back(tmeta2);
                if (row.NextPst())
                    key_heap.push(KeyWithRowId{row.GetCurrentKey(), topkey.row_id});
                continue;
            }
            else
            {
                // overlapped: read the pst, add entry to output pst
                row.ResetPstIter();
                uint64_t key, value;
                key = row.pst_iter_->Key();
                // DEBUG("first key: %lu, topkey:%lu",__bswap_64(key),__bswap_64(topkey.key));
                assert(key == topkey.key);
                value = row.pst_iter_->Value();
                auto success = pst_builder_.AddEntry(Slice(&key), Slice(&value));
                if (!success)
                {
                    auto meta = pst_builder_.Flush();
                    // add meta into manifest
                    TaggedPstMeta tmeta;
                    tmeta.meta = meta;
                    outputs_.emplace_back(tmeta);
                    if (!pst_builder_.AddEntry(Slice(&key), Slice(&value)))
                        ERROR_EXIT("cannot add pst entry in compaction");
                }
            }
        }
        else
        {
            // not the first key in pst : add entry to output pst
            uint64_t key, value;
            key = row.pst_iter_->Key();
            // DEBUG("key: %lu, topkey:%lu",__bswap_64(key),__bswap_64(topkey.key));
            assert(key == topkey.key);
            value = row.pst_iter_->Value();
            auto success = pst_builder_.AddEntry(Slice(&key), Slice(&value));
            if (!success)
            {
                auto meta = pst_builder_.Flush();
                // add meta into manifest
                TaggedPstMeta tmeta;
                tmeta.meta = meta;
                outputs_.emplace_back(tmeta);
                if (!pst_builder_.AddEntry(Slice(&key), Slice(&value)))
                    ERROR_EXIT("cannot add pst entry in compaction");
            }
        }

        if (row.NextKey())
        {
            // DEBUG("next key=%lu,row.pst_iter_=%lu",__bswap_64(row.GetCurrentKey()),(uint64_t)row.pst_iter_);
            key_heap.push(KeyWithRowId{row.GetCurrentKey(), topkey.row_id});
        }
    }
    auto meta = pst_builder_.Flush();
    // add meta into manifest
    TaggedPstMeta tmeta;
    tmeta.meta = meta;
    outputs_.emplace_back(tmeta);
    DEBUG("output=%lu,marked=%lu,rewrite key num=%lu", outputs_.size(), marked_output, count);
    // for(int i=0;i<inputs_.size();i++){
    //     for(auto& pst:inputs_[i]){
    //         DEBUG("input[%d]:level=%lu,min=%lu,max=%lu",i,pst.level,__bswap_64(pst.meta.min_key_), __bswap_64(pst.meta.max_key_));
    //     }
    // }
    pst_builder_.PersistCheckpoint();
    rows.clear();
    for (auto &pr : readers)
    {
        delete pr;
    }
    return true;
}

bool CompactionJob::CleanCompaction()
{

    // 1. add outputs to level 1 index
    for (auto &pst : outputs_)
    {
        pst.meta.seq_no_ = output_seq_no_;
        pst.level = 1;
        pst.manifest_position = manifest_->AddTable(pst.meta, 1);
        version_->InsertTableToL1(pst);
    }
    pst_builder_.PersistCheckpoint();

    // 2. change version in manifest
    manifest_->UpdateL1Version(output_seq_no_);
    int tree_num = inputs_.size() - 1;
    manifest_->UpdateL0Version(manifest_->GetL0Version() + tree_num);

    // 3. delete obsolute PSTs
    // delete inputs[-1](except for .level=1) from level 1 index
    for (auto &pst : inputs_[inputs_.size() - 1])
    {
        // recycle segment space, note that no need to recycle pst whose seq_no = output_seq_no_
        version_->DeleteTableInL1(pst.meta);
        if (pst.level != NotOverlappedMark)
        {
            pst_deleter_.DeletePST(pst.meta);
        }
    }
    pst_deleter_.PersistCheckpoint();
    for (auto &pst : inputs_[inputs_.size() - 1])
    {
        manifest_->DeleteTable(pst.manifest_position, 1);
    }
    // delete level 0 trees;
    for (int i = 0; i < tree_num; i++)
    {
        version_->FreeLevel0Tree();

        for (auto &pst : inputs_[i])
        {
            // DEBUG("delete pst in inputs_[%d]",i);
            // recycle segment space, note that no need to recycle pst whose seq_no = output_seq_no_
            // Better to wait for the reader to finish reading
            if (pst.level != NotOverlappedMark)
            {
                assert(pst.level == 0);
                pst_deleter_.DeletePST(pst.meta);
            }
            manifest_->DeleteTable(pst.manifest_position, 0);
        }
    }
    pst_deleter_.PersistCheckpoint();
    return true;
}
bool CompactionJob::RollbackCompaction()
{
    DEBUG("rollback start!");
    return false;
}