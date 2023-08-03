/**
 * @file manifest.h
 * @author your name (you@domain.com)
 * @brief a persistent data structure on PM to record the meta data of psts on each level.
 *        Manifest is used to recover Version (including all Index) after crash.
 *
 *      On PM, Manifest occupies a separate address space and allocates this space to metadata for each level.
 *      | SuperMeta | L0Meta:[meta1 meta2 meta3 ...] [padding] | L1Meta:[meta1 meta2 meta3 ...] [padding] | L2Meta:[meta1 meta2 meta3 ...]
 *      | 32B | 32B * MAX_L0_TREE_NUM * MAX_MEMTABLE_ENTRIES/1024 =51200000 | 10 * SIZEOF(L0Meta) |
 *
 *
 * @version 0.1
 * @date 2022-10-26
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "db/table.h"
#include <queue>
#define L0MetaSize 51200000
#define L1MetaSize 128000000
// Each log group can cotain MAX_USER_THREAD_NUM * 8 log segments, which have an 4-byte id
#define OpLogSize (4 * MAX_MEMTABLE_NUM * MAX_USER_THREAD_NUM * 32)
#define ManifestSize (32 + L0MetaSize + L1MetaSize + OpLogSize)

class Version;
struct ManifestSuperMeta
{
    uint32_t l0_min_valid_seq_no = 0;
    uint32_t l1_current_seq_no = 0;
    size_t l0_tail = 0;
    size_t l1_tail = 0;
    struct FlushLog
    {
        uint64_t is_valid : 1;
        uint64_t length : 63;
    } flush_log;
};
class Manifest
{
private:
    const char *start_;
    const char *l0_start_;
    const char *l1_start_;
    const char *flush_log_start_;
    std::queue<int> l0_freelist_;
    std::queue<int> l1_freelist_;
    ManifestSuperMeta *super_;
    const char *end_;

public:
    Manifest(char *pmem_addr, bool recover);
    ~Manifest();
    /**
     * @brief persist new pst in manifest
     *
     * @param meta PST metadata
     * @param level level
     * @return int the id in manifest (for fast delete);
     */
    int AddTable(PSTMeta meta, int level);

    void DeleteTable(int idx, int level);

    void UpdateL0Version(unsigned min_seq_no);

    void UpdateL1Version(unsigned current_seq_no);

    unsigned GetL0Version();

    void L0GC();

    void AddFlushLog(std::vector<uint64_t>& deleted_log_segment_ids);

    void ClearFlushLog();

    bool GetFlushLog(std::vector<uint64_t>& deleted_log_segment_ids);

    Version *RecoverVersion(Version *source,SegmentAllocator* allocator);

private:
    inline const char *GetAddr(int level, int idx);
};
