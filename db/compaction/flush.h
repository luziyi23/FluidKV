#include "db_common.h"
#include "db/allocator/segment_allocator.h"
#include "db/log_reader.h"
#include "db/pst_builder.h"
#include "db/pst_reader.h"
#include <vector>

class Version;
class Manifest;
class FlushJob
{
private:
	// input
    Index *memtable_index_;
    int seg_group_id_;
    SegmentAllocator *seg_allocater_;
    Version *version_;
    LogReader log_reader_;
    PSTBuilder pst_builder_;
    PSTReader pst_reader_;
    Manifest *manifest_;
	PartitionInfo *partition_info_;
	// temp
	uint32_t tree_seq_no_;
	std::vector<TaggedPstMeta> output_pst_list_;
	int tree_idx_;

public:
    FlushJob(Index *index, int seg_group_id, SegmentAllocator *seg_alloc, Version *target_version, Manifest *manifest, PartitionInfo* partition_info) : memtable_index_(index), seg_group_id_(seg_group_id), seg_allocater_(seg_alloc), version_(target_version), log_reader_(seg_allocater_), pst_builder_(seg_allocater_),pst_reader_(seg_allocater_), manifest_(manifest),partition_info_(partition_info)
    {	
    }
    ~FlushJob(){};

    //use index to build persistent index blocks
    bool run();
private:
	inline void FlushPST();
};
