#include "db.h"
#include "db/log_writer.h"
#include "db/log_reader.h"
#include "db/compaction/version.h"
#include <mutex>
#include <algorithm>

/*******************DBClient***********************/
DBClient::DBClient(DB *db, int tid) : db_(db), thread_id_(tid), log_writer_(new LogWriter(db->segment_allocator_, db_->current_memtable_idx_)), log_reader_(new LogReader(db->segment_allocator_)), pst_reader_(new PSTReader(db->segment_allocator_))
{
    current_memtable_idx_ = db_->current_memtable_idx_;
    db_->mem_index_[current_memtable_idx_]->ThreadInit(tid);
    for (auto &num : put_num_in_current_memtable_)
    {
        num = 0;
    }
}
DBClient::~DBClient()
{
    db_->AddTempMemtableSize(current_memtable_idx_, put_num_in_current_memtable_[current_memtable_idx_]);
    delete log_writer_;
    delete log_reader_;
    delete pst_reader_;
    std::lock_guard<SpinLock> lock(db_->client_lock_);
    db_->client_list_[thread_id_] = nullptr;
    DEBUG("close client %d", thread_id_);
}

/**
 * @brief 
 * 
 * @param key 
 * @param value 
 * @param slow add an latency between log writing and index updating to validate crash consistency
 * @return true 
 * @return false 
 */
bool DBClient::Put(const Slice key, const Slice value, bool slow)
{
    bool memtable_idx_changed = StartWrite();
    uint64_t int_key = key.ToUint64();
    // if active log_group is changed, first allocate new segment
    if (unlikely(memtable_idx_changed))
    {
        log_writer_->SwitchToNewSegment(current_memtable_idx_);
    }

    LSN lsn;
#ifdef INDEX_LOG_MEMTABLE
    lsn = db_->GetLSN(int_key);
#endif
#ifdef BUFFER_WAL_MEMTABLE
    lsn = db_->LSN_lock(int_key);
#endif
    uint64_t log_ptr = log_writer_->WriteLogPut(key, value, lsn);
    LOG("put log_ptr = %lu", log_ptr);

    // index updade
#ifdef INDEX_LOG_MEMTABLE
    ValuePtr vp{.detail_ = {.valid = 1,
                            .ptr = log_ptr >> 6,
                            .lsn = lsn.lsn}};
    ValueHelper lh(vp.data_);
    db_->mem_index_[current_memtable_idx_]->PutValidate(int_key, lh);
#endif
#ifdef BUFFER_WAL_MEMTABLE
    ValueHelper lh(value.ToUint64());
    db_->mem_index_[current_memtable_idx_]->Put(int_key, lh);
    db_->LSN_unlock(lsn.epoch);
#endif
    put_num_in_current_memtable_[current_memtable_idx_]++;
    FinishWrite();
    total_writes_.fetch_add(1);
    return true;
}

bool DBClient::Delete(const Slice key)
{
    bool changed = StartWrite();
    uint64_t int_key = key.ToUint64();
    // if active log_group is changed, first allocate new segment
    if (unlikely(changed))
    {
        log_writer_->SwitchToNewSegment(current_memtable_idx_);
    }
    LSN lsn;
#ifdef INDEX_LOG_MEMTABLE
    lsn = db_->GetLSN(int_key);
#endif
#ifdef BUFFER_WAL_MEMTABLE
    lsn = db_->LSN_lock(int_key);
#endif
    uint64_t log_ptr = log_writer_->WriteLogDelete(key, lsn);
    LOG("put log_ptr = %lu", log_ptr);
#ifdef INDEX_LOG_MEMTABLE
    ValuePtr vp{.detail_ = {.valid = 0,
                            .ptr = log_ptr >> 6,
                            .lsn = lsn.lsn}};
    ValueHelper lh(vp.data_);
    db_->mem_index_[current_memtable_idx_]->PutValidate(int_key, lh);
#endif
#ifdef BUFFER_WAL_MEMTABLE
    ValueHelper lh(INVALID_PTR);
    db_->mem_index_[current_memtable_idx_]->Put(int_key, lh);
    db_->LSN_unlock(lsn.epoch);
#endif
    FinishWrite();
    total_writes_.fetch_add(1);
    return true;
}

bool DBClient::Get(const Slice key, Slice &value_out)
{
    total_reads_.fetch_add(1);
    if (GetFromMemtable(key, value_out))
    {
        return true;
    }
    int size;
#ifndef KV_SEPARATE
    return db_->current_version_->Get(key, value_out.data(), &size, pst_reader_);
#else
    ValuePtr vptr;
    bool ret = db_->current_version_->Get(key, (char *)&vptr.data_, &size, pst_reader_);
    if (!ret)
        return false;
    Slice result = log_reader_->ReadLogForValue(key, vptr);
    memcpy((void *)value_out.data(), result.data(), result.size());
    return true;
#endif
}

bool DBClient::GetFromMemtable(const Slice key, Slice &value_out)
{
    LOG("Get %lu(%lu) from memtable", key.ToUint64(), key.ToUint64Bswap());
    ValuePtr vptr;
    // NOTE:不同步到成员变量，成员变量current_memtable_idx只由写操作改变，避免出现bug
    int current_memtable_id = db_->current_memtable_idx_;
    // get from index
    for (int i = 0; i < MAX_MEMTABLE_NUM; i++)
    {
        int memtable_id = (current_memtable_id - i + MAX_MEMTABLE_NUM) % MAX_MEMTABLE_NUM;
        if (db_->memtable_states_[memtable_id].state == MemTableStates::EMPTY)
        {
            break;
        }
        vptr.data_ = db_->mem_index_[memtable_id]->Get(key.ToUint64());

#ifdef INDEX_LOG_MEMTABLE
        if (vptr.data_ == INVALID_PTR) // check tombstone
            continue;
        if (vptr.detail_.valid == 0)
            return false;
        // get from log
        Slice v = log_reader_->ReadLogForValue(key, vptr);
        memcpy((void *)value_out.data(), v.data(), v.size());
#endif
#ifdef BUFFER_WAL_MEMTABLE
        if (vptr.data_ == INVALID_PTR) // check tombstone
            continue;
        memcpy((void *)value_out.data(), &(vptr.data_), 8);
#endif
        return true;
    }
    LOG("memtable over");
    return false;
}

int DBClient::Scan(const Slice start_key, int scan_sz, std::vector<uint64_t> &key_out)
{
    // TODO: Wait for flush/compaction over and no level0_tree
    db_->WaitForFlushAndCompaction();
    ValuePtr vptr;
    std::vector<uint64_t> keys_mem[MAX_MEMTABLE_NUM], values_mem[MAX_MEMTABLE_NUM], keys_level, values_level;
    // get from index
    int cur = db_->current_memtable_idx_;
    for (int i = 0; i < MAX_MEMTABLE_NUM; i++)
    {
        int memidx = (cur + i) % MAX_MEMTABLE_NUM;
        if (db_->mem_index_[memidx] != nullptr)
            db_->mem_index_[memidx]->Scan2(start_key.ToUint64(), scan_sz, keys_mem[i], values_mem[i]);
    }
    std::vector<TaggedPstMeta> table_metas;
    RowIterator *level_row = db_->current_version_->GetLevel1Iter(start_key, pst_reader_, table_metas);
    LOG("level row.valid=%d, current_key=%lu\n", level_row->Valid(), __bswap_64(level_row->GetCurrentKey()));
    // skip keys lower than start_key in the pst
    while (level_row->Valid() && __bswap_64(level_row->GetCurrentKey()) < start_key.ToUint64Bswap())
    {
        // printf("skip %lu , %lu\n", __bswap_64(level_row->GetCurrentKey()),level_row->GetCurrentKey());
        level_row->NextKey();
        // TODO: if return false value, GetLevel1Iter maybe can't get enough KVs from 2 pst
    }

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
    } scancmp;

    // merge iterator
    KeyWithRowId ik;
    std::priority_queue<KeyWithRowId, std::vector<KeyWithRowId>, UintKeyComparator> key_heap(scancmp);
    int mem_idx[MAX_MEMTABLE_NUM];
    for (int i = 0; i < MAX_MEMTABLE_NUM; i++)
    {
        if (!keys_mem[i].empty())
        {
            key_heap.push({keys_mem[i][0], i});
            mem_idx[i] = 0;
        }
    }
    if (level_row->Valid())
    {
        LOG("push %lu\n", __bswap_64(level_row->GetCurrentKey()));
        key_heap.push({level_row->GetCurrentKey(), 99});
    }

    while (scan_sz-- && !key_heap.empty())
    {
        auto topkey = key_heap.top();
        key_heap.pop();
        while (!key_heap.empty() && key_heap.top().key == topkey.key)
        {
            // 如果出现重合key，旧key直接next
            if (topkey.row_id == 99)
            {
                if (level_row->NextKey())
                {
                    key_heap.push(KeyWithRowId{level_row->GetCurrentKey(), 99});
                }
            }
            else if (keys_mem[topkey.row_id].size() > ++mem_idx[topkey.row_id])
            {
                key_heap.push({keys_mem[topkey.row_id][mem_idx[topkey.row_id]], topkey.row_id});
            }
            topkey = key_heap.top();
            key_heap.pop();
        }
        key_out.push_back(topkey.key);
        // read the value of top_key
        // TODO: If kv separate, read log.
        size_t value;
        if (topkey.row_id == 99)
        {
            value = level_row->GetCurrentValue();
            if (level_row->NextKey())
            {
                key_heap.push({level_row->GetCurrentKey(), 99});
            }
        }
        else
        {
            value = values_mem[topkey.row_id][mem_idx[topkey.row_id]];
            mem_idx[topkey.row_id]++;
            if (mem_idx[topkey.row_id] < keys_mem[topkey.row_id].size())
            {
                key_heap.push({keys_mem[topkey.row_id][mem_idx[topkey.row_id]], topkey.row_id});
            }
        }
    }
    delete level_row;
    return true;
}

inline bool DBClient::StartWrite()
{
    //  1 get memtable idx
    int old = current_memtable_idx_;
    current_memtable_idx_ = db_->current_memtable_idx_;
    // 2 modify thread state
    if (old != current_memtable_idx_)
    {
        // db_->mem_index_[current_memtable_idx_]->ThreadInit(thread_id_);
        return true;
    }
    return false;
}

inline void DBClient::FinishWrite()
{

}
