#include "db.h"
#include "allocator/segment_allocator.h"
#include "compaction/version.h"
#include "compaction/manifest.h"
#include "compaction/flush.h"
#include "compaction/compaction.h"
#include "lib/index_masstree.h"
#include "util/stopwatch.hpp"
#ifdef HOT_MEMTABLE
#include "lib/index_hot.h"
#endif
/***********************DB*************************/
void BGWorkTrigger(DB *db)
{
    while (!db->stop_bgwork_)
    {
        bool ret = db->MayTriggerFlushOrCompaction();
        usleep(100000);
    }
    printf("BGWorkTrigger out!\n");
}
DB::DB(DBConfig cfg) : db_path_(cfg.pm_pool_path), segment_allocator_(new SegmentAllocator(db_path_ + "/segments.pool", cfg.pm_pool_size, cfg.ssd_path, cfg.recover))
{
    // TODO: can be config
    current_memtable_idx_ = 0;
#ifdef MASSTREE_MEMTABLE
    mem_index_[current_memtable_idx_] = new MasstreeIndex();
    mem_index_[1] = new MasstreeIndex();
#endif
#ifdef HOT_MEMTABLE
    mem_index_[current_memtable_idx_] = new HOTIndex(MAX_MEMTABLE_ENTRIES * 8);
#endif
    for (int i = 0; i < MAX_USER_THREAD_NUM; i++)
    {
        client_list_[i] = nullptr;
        for (int j = 0; j < MAX_MEMTABLE_NUM; j++)
        {
            memtable_states_[j].thread_write_states[i] = false;
            temp_memtable_size_[j] = 0;
        }
    }
    memtable_states_[current_memtable_idx_].state = MemTableStates::ACTIVE;
    std::string manifest_path = db_path_ + "/manifest";
    size_t mapped_len;
    char *start_addr_ = (char *)pmem_map_file(manifest_path.c_str(), ManifestSize, PMEM_FILE_CREATE, 0666, &mapped_len, nullptr);
    if (mapped_len != ManifestSize)
    {
        ERROR_EXIT("Manifest file mapping error!");
    }
    DEBUG("manifest start = %lu, end = %lu", (uint64_t)start_addr_, (uint64_t)(start_addr_ + mapped_len));
    current_version_ = new Version(segment_allocator_);
    manifest_ = new Manifest(start_addr_, cfg.recover);
    if (cfg.recover)
    {
        stopwatch_t sw;
        printf("start recovering!\n");
        sw.start();
        current_version_ = manifest_->RecoverVersion(current_version_, segment_allocator_);
        printf("manifest recover over! take %f ms\n", sw.elapsed<std::chrono::milliseconds>());
        sw.start();
        bool ret = RecoverLogAndMemtable();
        printf("memtable recover over! take %f ms\n", sw.elapsed<std::chrono::milliseconds>());
        if (!ret)
            ERROR_EXIT("recover error");
    }
    thread_pool_ = new ThreadPoolImpl();
    thread_pool_->SetBackgroundThreads(cfg.background_thread_num);

#ifdef BUFFER_WAL_MEMTABLE
    for (size_t i = 0; i < LSN_MAP_SIZE; i++)
    {
        lsn_list_[i] = 0;
    }
#endif

    bgwork_trigger_ = new std::thread(BGWorkTrigger, this);
}
DB::~DB()
{
    WaitForFlushAndCompaction();
    // segment_allocator_->PrintLogStats();
    printf("closing DB,current idx=%d,L0 version=%u....\n", current_memtable_idx_, manifest_->GetL0Version());
    stop_bgwork_ = true;
    thread_pool_->JoinAllThreads();
    PrintLogGroup(0);
    PrintLogGroup(1);
    if (bgwork_trigger_)
    {
        bgwork_trigger_->join();
        delete bgwork_trigger_;
    }

    delete current_version_;
    delete segment_allocator_;
    delete manifest_;
    delete thread_pool_;
    for (int i = 0; i < MAX_MEMTABLE_NUM; i++)
    {
        if (mem_index_[i] != nullptr)
            delete mem_index_[i];
        DEBUG("g,%lu\n", GetMemtableSize(i));
    }
}

bool DB::RecoverLogAndMemtable()
{
    std::vector<uint64_t> seg_id_list;
    // redo flush log
    manifest_->GetFlushLog(seg_id_list);
    DEBUG("Redo flush log %lu", seg_id_list.size());
    segment_allocator_->RedoFlushLog(seg_id_list);
    // get valid log segments
    DEBUG("Get valid log segments");
    seg_id_list.clear();
    bool ret = segment_allocator_->RecoverLogSegmentAndGetId(seg_id_list);
    LogReader *log_reader = new LogReader(segment_allocator_);
    LogEntry32 logbuffer[SEGMENT_SIZE / 32];
    DEBUG("Log segment list:(size=%lu)", seg_id_list.size());
    printf("\t\t\t\t");
    for (auto &seg_id : seg_id_list)
    {
        printf("%lu,", seg_id);
    }
    printf("\n");
    fflush(stdout);
    for (auto &seg_id : seg_id_list)
    {
        // get segment header
        auto entry_num = log_reader->ReadLogFromSegment(seg_id, logbuffer);
        for (int i = 0; i < entry_num; i++)
        {
            auto &entry = logbuffer[i];
#ifdef INDEX_LOG_MEMTABLE
            ValuePtr vp{.detail_ = {.valid = 1,
                                    .ptr = seg_id * SEGMENT_SIZE + sizeof(LogSegment::Header) + i * sizeof(LogEntry32) >> 6,
                                    .lsn = entry.lsn}};
            ValueHelper lh(vp.data_);
#endif
#ifdef BUFFER_WAL_MEMTABLE
            ValueHelper lh(entry.value_addr);
#endif
            mem_index_[0]->Put(entry.key, lh);
        }
    }

    return ret;
}

std::unique_ptr<DBClient> DB::GetClient(int tid)
{
    std::lock_guard<SpinLock> lock(client_lock_);
    std::unique_ptr<DBClient> c;
    if (tid == -1)
    {
        for (int i = 1; i <= MAX_USER_THREAD_NUM; i++)
        {
            if (client_list_[i] == nullptr)
            {
                c = std::make_unique<DBClient>(this, i);
                client_list_[i] = c.get();
                DEBUG("create client %d", i);
                return c;
            }
        }
        ERROR_EXIT("Not support too much user threads");
        return c;
    }
    c = std::make_unique<DBClient>(this, tid);
    client_list_[tid] = c.get();
    DEBUG("create client %d", tid);
    return c;
}

#ifdef INDEX_LOG_MEMTABLE
LSN DB::GetLSN(uint64_t i_key)
{
    size_t idx = i_key & (LSN_MAP_SIZE - 1);
    size_t id = lsn_map_[idx].fetch_add(1);
    return LSN{idx, id, 0};
}
#endif
#ifdef BUFFER_WAL_MEMTABLE
// LSN DB::LSN_lock(uint64_t i_key)
// {
//     size_t idx = i_key & (LSN_MAP_SIZE - 1);
//     wal_lock_[idx].lock();
//     size_t id = lsn_list_[idx]++;
//     return LSN{idx, id, 0};
// }
// void DB::LSN_unlock(size_t epoch)
// {
//     wal_lock_[epoch].unlock();
// }
LSN DB::LSN_lock(uint64_t i_key)
{
    size_t idx = i_key & (LSN_MAP_SIZE - 1);
    size_t id = lsn_map_[idx].fetch_add(1);
    return LSN{idx, id, 0};
}
void DB::LSN_unlock(size_t epoch)
{
}
#endif

size_t DB::GetMemtableSize(int idx)
{
    size_t num = 0;
    for (int i = 0; i < MAX_USER_THREAD_NUM; i++)
    {
        auto &c = client_list_[i];
        if (c != nullptr)
        {
            num += c->GetMemtablePutCount(idx);
            // printf("get from client %d,num=%lu\n", i, num);
        }
    }
    num += temp_memtable_size_[idx].load();
    return num;
}
void DB::ClearMemtableSize(int idx)
{
    for (int i = 0; i < MAX_USER_THREAD_NUM; i++)
    {
        auto &c = client_list_[i];
        if (c != nullptr)
        {
            c->ClearMemtablePutCount(idx);
        }
    }
    temp_memtable_size_[idx] = 0;
}

bool DB::MayTriggerFlushOrCompaction()
{
    if (++workload_detect_sample_ >= 5)
    {
        if (client_list_[1])
        {
            auto c = client_list_[1];
            auto write_ratio = c->GetWriteRatioAndClear();
            if (write_ratio < 0)
            {
            }
            else if (write_ratio > 0.5)
            {
                DisableReadOnlyMode();
                DisableReadOptimizedMode();
            }
            else if (write_ratio == 0)
            {
                EnableReadOnlyMode();
                EnableReadOptimizedMode();
            }
            else
            {
                DisableReadOnlyMode();
                EnableReadOptimizedMode();
            }
        }
        workload_detect_sample_ = 0;
    }
    // Flush
    size_t memtablesize = GetMemtableSize(current_memtable_idx_);
    size_t flush_threashold = read_only_mode_ ? 1 : MAX_MEMTABLE_ENTRIES;
    if (memtablesize >= flush_threashold)
    {
        // printf("memtablesize[%d]=%lu\n", current_memtable_idx_, memtablesize);
        bool expect = false;
        if (is_flushing_.compare_exchange_weak(expect, true))
        {
            FlushArgs *fa = new FlushArgs(this);
            thread_pool_->Schedule(&DB::TriggerBGFlush, fa, fa, nullptr);
            return true;
        }
    }
    // Compaction
    auto ret = false;
    ret = MayTriggerCompaction();
    return ret;
}

bool DB::MayTriggerCompaction()
{
    size_t compaction_threashold = read_optimized_mode_ ? 1 : l0_compaction_tree_num_;
    // Compaction
    if (current_version_->GetLevel0TreeNum() >= compaction_threashold)
    {
        bool expect = false;
        if (is_l0_compacting_.compare_exchange_weak(expect, true))
        {
            DEBUG("trigger compaction because l0 tree num=%d", current_version_->GetLevel0TreeNum());
            CompactionArgs *ca = new CompactionArgs(this);
            thread_pool_->Schedule(&DB::TriggerBGCompaction, ca, ca, nullptr);
            return true;
        }
    }
    return false;
}

void DB::PrintLogGroup(int id)
{
    DEBUG("current log group = %d, get %d, memtablesize=%lu, level0treenum=%d,table=%d", current_memtable_idx_, id, GetMemtableSize(id), current_version_->GetLevel0TreeNum(), current_version_->GetLevelSize(0));
    // std::vector<uint64_t> list;
    // segment_allocator_->GetElementsFromLogGroup(id, &list);
    // for (auto &seg : list)
    // {
    //     printf("%lu,", seg);
    // }
    // printf("\n");
}

void DB::PrintLogGroupInDB(void *arg)
{
    TestParams tp = *(reinterpret_cast<TestParams *>(arg));
    delete (reinterpret_cast<TestParams *>(arg));
    static_cast<DB *>(tp.db_)->PrintLogGroup(tp.log_group_id_);
    sleep(1);
    static_cast<DB *>(tp.db_)->PrintLogGroup(tp.log_group_id_);
    tp.is_running_->store(false);
}

void DB::TriggerBGFlush(void *arg)
{
    FlushArgs fa = *(reinterpret_cast<FlushArgs *>(arg));
    delete (reinterpret_cast<FlushArgs *>(arg));
    // printf("trigger flush\n");
    static_cast<DB *>(fa.db_)->BGFlush();
}

void DB::TriggerBGCompaction(void *arg)
{
    CompactionArgs ca = *(reinterpret_cast<CompactionArgs *>(arg));
    delete (reinterpret_cast<CompactionArgs *>(arg));
    // printf("trigger bgcompaction\n");
    static_cast<DB *>(ca.db_)->BGCompaction();
}

bool DB::BGFlush()
{
    if (!current_version_->CheckSpaceForL0Tree())
    {
        INFO("flush stall due to full L0");
        is_flushing_ = false;
        return false;
    }
    LOG("start flush active_memtable = %d, memtablesize=(%lu,%lu), level0treenum=%d,table=%d", current_memtable_idx_.load(), memtable_size_[0].load(), memtable_size_[1].load(), current_version_->GetLevel0TreeNum(), current_version_->GetLevelSize(0));
    stopwatch_t sw;
    sw.start();
    // 1. freeze memtable and modify the current_memtable_idx of DB
    int target_memtable_idx = current_memtable_idx_;
    int next_memtable_idx = (target_memtable_idx + 1) % MAX_MEMTABLE_NUM;
/* if only enabling single-thread flush, this memtable state control is not neccessary
    LOG("start flush,target idx=%d,kv=%lu", target_memtable_idx, memtable_size_[target_memtable_idx].load());
    if (memtable_states_[target_memtable_idx].state != MemTableStates::ACTIVE)
    {
        DEBUG("memtable_states_[%d].state =%d, flush failed", target_memtable_idx, memtable_states_[target_memtable_idx].state);
        return false;
    }
    LOG("target memtable is active: ok!");
    memtable_states_[target_memtable_idx].state = MemTableStates::FREEZE;
    if (memtable_states_[next_memtable_idx].state != MemTableStates::EMPTY)
        return false;
*/
#ifdef MASSTREE_MEMTABLE
    mem_index_[next_memtable_idx] = new MasstreeIndex();
#endif
#ifdef HOT_MEMTABLE
    mem_index_[next_memtable_idx] = new HOTIndex(MAX_MEMTABLE_ENTRIES * 8);
#endif
    memtable_states_[next_memtable_idx].state = MemTableStates::ACTIVE;
    current_memtable_idx_ = next_memtable_idx; // change active memtable
    LOG("change memtable. active_memtable = %d, memtablesize=(%lu,%lu), l0treenum=%d,table=%d", current_memtable_idx_.load(), memtable_size_[0].load(), memtable_size_[1].load(), current_version_->GetLevel0TreeNum(), current_version_->GetLevelSize(0));
    // from this time, user thread can write into new memtable and other flush can be triggered on other memtable
    // 2. wait all threads not busy
    DEBUG("step 2");
    // for (int i = 0; i < MAX_USER_THREAD_NUM; i++)
    // {
    //     while (memtable_states_[target_memtable_idx].thread_write_states[i] == true)
    //     {
    //         std::this_thread::yield();
    //     }
    // }
    usleep(100); // just wait for all client put over, instead of checking client state with a shared value
    // 3. core steps
    DEBUG("flush step 3");
    FlushJob *fj = new FlushJob(mem_index_[target_memtable_idx], target_memtable_idx, segment_allocator_, current_version_, manifest_);
    auto ret = fj->run();
    delete fj;
    // 4. change memtable state to EMPTY
    DEBUG("step 4");
    memtable_states_[target_memtable_idx].state = MemTableStates::EMPTY;
    int expect = 0;
    DEBUG("before delete memtable");
#ifdef MASSTREE_MEMTABLE
    delete (MasstreeIndex *)mem_index_[target_memtable_idx];
#endif
    DEBUG("after delete memtable");
#ifdef HOT_MEMTABLE
    delete (HOTIndex *)mem_index_[target_memtable_idx];
#endif
    mem_index_[target_memtable_idx] = nullptr;
    ClearMemtableSize(target_memtable_idx);
    segment_allocator_->ClearLogGroup(target_memtable_idx);

    // MayTriggerFlushOrCompaction(); // to trigger cascade compaction
    is_flushing_ = false;
    auto ms = sw.elapsed<std::chrono::milliseconds>();
    LOG("finish flush active_memtable = %d, memtablesize=(%lu,%lu), level0treenum=%d,table=%d,time=%f ms", current_memtable_idx_, GetMemtableSize(0), GetMemtableSize(1), current_version_->GetLevel0TreeNum(), current_version_->GetLevelSize(0), ms);
    INFO("flush end, time=%f ms", ms);
    return true;
}

bool DB::BGCompaction()
{
    CompactionJob *c = new CompactionJob(segment_allocator_, current_version_, manifest_);
    // 1 PickCompaction (lock, freeze pst range)
    stopwatch_t sw;
    sw.start();
    DEBUG("PickCompaction start");
    auto num = c->PickCompaction();
    auto ms = sw.elapsed<std::chrono::milliseconds>();
    auto total_ms = ms;
    DEBUG("PickCompaction end, time: %f ms", ms);
    if (num == 0)
    {
        is_l0_compacting_ = false;
        return false;
    }
    // 2 Prepare
    auto ret = c->CheckPmRoomEnough();
    if (!ret)
    {
        is_l0_compacting_ = false;
        return false;
    }
    // 3 Merge sorting
    DEBUG("RunCompaction start");
    sw.clear();
    sw.start();
    ret = c->RunCompaction();
    ms = sw.elapsed<std::chrono::milliseconds>();
    DEBUG("RunCompaction end, time: %f ms", ms);
    total_ms += ms;
    // exit(-1);
    if (!ret)
    {
        c->RollbackCompaction();
        is_l0_compacting_ = false;
        return false;
    }
    // 4 delete obsolute psts and change level0 indexes
    DEBUG("CleanCompaction start");
    sw.clear();
    sw.start();
    c->CleanCompaction();
    ms = sw.elapsed<std::chrono::milliseconds>();
    DEBUG("CleanCompaction end, time: %f ms", ms);
    total_ms += ms;
    is_l0_compacting_ = false;
    DEBUG("before compaction end");
    print_dram_consuption();
    delete c;
    print_dram_consuption();
    INFO("comapction end, time=%f ms", total_ms);
    MayTriggerFlushOrCompaction();

    return true;
}

void DB::WaitForFlushAndCompaction()
{
    EnableReadOptimizedMode();
    EnableReadOnlyMode();
    while (is_flushing_.load() || is_l0_compacting_.load())
    {
        sleep(1);
    }
    DisableReadOnlyMode();
}