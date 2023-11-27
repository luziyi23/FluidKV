#include "db_common.h"
#include "db/allocator/segment_allocator.h"
#include "db/log_reader.h"
#include "db/pst_builder.h"
#include "db/pst_reader.h"
#include "db/pst_deleter.h"
#include <vector>

class Version;
class Manifest;
class ThreadPoolImpl;
class CompactionJob
{
private:
    SegmentAllocator *seg_allocater_;
    Version *version_;
    Manifest *manifest_;
    PSTBuilder pst_builder_;
    PSTDeleter pst_deleter_;
    const unsigned output_seq_no_;

    std::vector<std::vector<TaggedPstMeta>> inputs_;
    std::vector<TaggedPstMeta> outputs_;
	std::vector<TaggedPstMeta> partition_outputs_[RANGE_PARTITION_NUM];

    // temp variables
    size_t min_key_ = MAX_UINT64;
    size_t max_key_ = 0;

	//for sub compaction
	PartitionInfo *partition_info_;
    PSTBuilder* partition_pst_builder_[RANGE_PARTITION_NUM];
	ThreadPoolImpl* compaction_thread_pool_;

public:
    CompactionJob(SegmentAllocator *seg_alloc, Version *target_version, Manifest *manifest,PartitionInfo* partition_info,ThreadPoolImpl* thread_pool);
    ~CompactionJob();

    bool CheckPmRoomEnough(); // with segment allocator
    /**
     * @brief get all overlapped input psts from version_ to inputs_
     * 
     * @return L0 tree number in the compaction
     */
    size_t PickCompaction();
    /**
     * @brief merge sorting inputs, writing all output psts to pm 
     *          currently, we persist manifests of outputs, but not persist data (for consistency check when recovery)
     * 
     */
    bool RunCompaction();
    bool RunSubCompactionParallel();
	void RunSubCompaction(int partition_id);
	/**
     * @brief persist data and metadata(manifest) of all output psts, and update L0 and L1 volatile indexes
     * 
     * @return true 
     * @return false 
     */
    void CleanCompaction();
	void CleanCompactionWhenUsingSubCompaction();
    bool RollbackCompaction();

	static void TriggerSubCompaction(void *arg);
};
