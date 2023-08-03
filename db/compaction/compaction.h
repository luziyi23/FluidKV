#include "db_common.h"
#include "db/allocator/segment_allocator.h"
#include "db/log_reader.h"
#include "db/pst_builder.h"
#include "db/pst_reader.h"
#include "db/pst_deleter.h"
#include <vector>

class Version;
class Manifest;
class CompactionJob
{
private:
    SegmentAllocator *seg_allocater_;
    Version *version_;
    Manifest *manifest_;
    PSTReader pst_reader_;
    PSTBuilder pst_builder_;
    PSTDeleter pst_deleter_;
    const unsigned output_seq_no_;

    std::vector<std::vector<TaggedPstMeta>> inputs_;
    std::vector<TaggedPstMeta> outputs_;

    // temp variables
    size_t min_key_ = MAX_UINT64;
    size_t max_key_ = 0;

public:
    CompactionJob(SegmentAllocator *seg_alloc, Version *target_version, Manifest *manifest);
    ~CompactionJob();

    bool CheckPmRoomEnough(); // with segment allocator
    /**
     * @brief get all overlapped input psts from version_ to inputs_
     * 
     * @return true 
     * @return false 
     */
    size_t PickCompaction();
    /**
     * @brief merge sorting inputs, writing all output psts to pm 
     *          currently, we persist manifests of outputs, but not persist data (for consistency check when recovery)
     * 
     * @return L0 tree number in the compaction
     */
    bool RunCompaction();
    /**
     * @brief persist data and metadata(manifest) of all output psts, and update L0 and L1 volatile indexes
     * 
     * @return true 
     * @return false 
     */
    bool CleanCompaction();
    bool RollbackCompaction();
};
