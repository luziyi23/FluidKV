#pragma once

#include <cstdint>
#include <atomic>
#include <string>
#include <array>
#include <memory>
#include <thread>
#include "slice.h"
#include "db_common.h"
#include "db/log_format.h"
#include "lib/ThreadPool/include/threadpool.h"
#include "lib/ThreadPool/include/threadpool_imp.h"

class DBClient;
class SegmentAllocator;
class LogWriter;
class LogReader;
class PSTReader;
class Version;
class Manifest;

struct MemTableStates
{
    enum
    {
        ACTIVE,
        FREEZE,
        EMPTY
    } state = EMPTY;
    bool thread_write_states[MAX_USER_THREAD_NUM];
};

class DB
{
private:
    friend class DBClient;
    // global
    std::string db_path_;

#ifdef INDEX_LOG_MEMTABLE
    static constexpr size_t LSN_MAP_SIZE = 256;
    std::array<std::atomic_uint_fast32_t, LSN_MAP_SIZE> lsn_map_{};
#endif
#ifdef BUFFER_WAL_MEMTABLE
    static constexpr size_t LSN_MAP_SIZE = 4096;
    std::array<std::atomic_uint_fast32_t, LSN_MAP_SIZE> lsn_map_{};
    uint32_t lsn_list_[LSN_MAP_SIZE];
    SpinLock wal_lock_[LSN_MAP_SIZE];
#endif
    SegmentAllocator *segment_allocator_;
    DBClient *client_list_[MAX_USER_THREAD_NUM];
    SpinLock client_lock_;

    // FastWriteStore
    Index *mem_index_[MAX_MEMTABLE_NUM];
    // std::atomic<uint64_t> memtable_size_[MAX_MEMTABLE_NUM];
    MemTableStates memtable_states_[MAX_MEMTABLE_NUM];
    std::atomic_uint64_t temp_memtable_size_[MAX_MEMTABLE_NUM];
    int current_memtable_idx_;

    // control variables
    ThreadPoolImpl *thread_pool_ = nullptr;
    std::thread *bgwork_trigger_ = nullptr;
    bool read_optimized_mode_ = false;
    bool read_only_mode_ = false;
    size_t l0_compaction_tree_num_ = 4;

    std::atomic<bool> is_flushing_ = false;
    std::atomic<bool> is_l0_compacting_ = false;

    int workload_detect_sample_ = 0;

public: // TODO: change to private
    // BufferStore (level 0) + LeveledStore (Level 1 and level 2)
    Version *current_version_;
    Manifest *manifest_;
    bool stop_bgwork_ = false;

public:
    DB(DBConfig cfg = DBConfig());
    ~DB();
    std::unique_ptr<DBClient> GetClient(int tid = -1);
    // static bool initDB();
    // static bool openDB();
private:
    bool RecoverLogAndMemtable();
#ifdef INDEX_LOG_MEMTABLE
    LSN GetLSN(uint64_t i_key);
#endif
#ifdef BUFFER_WAL_MEMTABLE
    LSN LSN_lock(uint64_t i_key);
    void LSN_unlock(size_t epoch);
#endif
    size_t GetMemtableSize(int idx);
    void ClearMemtableSize(int idx);
    void AddTempMemtableSize(int idx, size_t size)
    {
        temp_memtable_size_[idx].fetch_add(size);
    }

public: // TODO: change to private
    bool MayTriggerFlushOrCompaction();
    bool MayTriggerCompaction();
    bool BGFlush();
    bool BGCompaction();
    void WaitForFlushAndCompaction();
    void PrintLogGroup(int id);
    struct TestParams
    {
        DB *db_;
        int log_group_id_;
        std::atomic_bool *is_running_;
        TestParams(DB *db, int id, std::atomic_bool *run) : db_(db), log_group_id_(id), is_running_(run) {}
    };
    static void PrintLogGroupInDB(void *arg);
    struct FlushArgs
    {
        DB *db_;
        FlushArgs(DB *db) : db_(db) {}
    };
    static void TriggerBGFlush(void *arg);
    struct CompactionArgs
    {
        DB *db_;
        CompactionArgs(DB *db) : db_(db) {}
    };

    static void TriggerBGCompaction(void *arg);

    /**
     * @brief force flushing and compaction to remove memtable read latency
     *
     */
    void EnableReadOptimizedMode()
    {
        if (!read_optimized_mode_)
        {
            read_optimized_mode_ = true;
            INFO("read_optimized_mode_ = true");
        }
    }
    void DisableReadOptimizedMode()
    {
        if (read_optimized_mode_)
        {
            read_optimized_mode_ = false;
            INFO("read_optimized_mode_ = false");
        }
    }
    void EnableReadOnlyMode()
    {
        if (!read_only_mode_)
        {
            read_only_mode_ = true;
            INFO("read_only_mode_ = true");
        }
    }
    void DisableReadOnlyMode()
    {
        if (read_only_mode_)
        {
            read_only_mode_ = false;
            INFO("read_only_mode_ = false");
        }
    }
};

class DBClient
{
    friend class DB;

public:
    DBClient(DB *db, int tid);
    ~DBClient();

    bool Put(const Slice key, const Slice value, bool slow = false);
    bool Get(const Slice key, Slice &value_out);
    bool Delete(const Slice key);
    int Scan(const Slice start_key, int scan_sz, std::vector<uint64_t> &key_out);
    const int thread_id_;

private:
    DB *db_;
    LogWriter *log_writer_;
    LogReader *log_reader_;
    int current_memtable_idx_;
    PSTReader *pst_reader_;
    size_t put_num_in_current_memtable_[MAX_MEMTABLE_NUM];
    std::atomic_uint64_t total_writes_ = 0;
    std::atomic_uint64_t total_reads_ = 0;

    bool GetFromMemtable(const Slice key, Slice &value_out);

    /**
     * @brief Update current_memtable_idx_ by db_->current_memtable_idx_
     *
     * @return true means current_memtable_idx_ is changed
     * @return false
     */
    inline bool StartWrite();
    inline void FinishWrite();

    size_t GetMemtablePutCount(int memtable_id)
    {
        return put_num_in_current_memtable_[memtable_id];
    }
    void ClearMemtablePutCount(int memtable_id)
    {
        put_num_in_current_memtable_[memtable_id] = 0;
    }
    float GetWriteRatioAndClear()
    {
        if (total_reads_ + total_writes_ == 0)
        {
            return -1;
        }
        float rwratio = (float)total_writes_ / (total_reads_ + total_writes_);
        total_writes_ = 0;
        total_reads_ = 0;
        return rwratio;
    }
};
