#include "compaction.h"
#include "manifest.h"
#include "version.h"
#include "lib/ThreadPool/include/threadpool.h"
#include "lib/ThreadPool/include/threadpool_imp.h"
#include <queue>

size_t total_L1_num = 0;

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

CompactionJob::CompactionJob(SegmentAllocator *seg_alloc, Index *memtable_index, Version *target_version, Manifest *manifest, PartitionInfo *partition_info, ThreadPoolImpl *thread_pool) : seg_allocater_(seg_alloc), memtable_index_(memtable_index), version_(target_version), manifest_(manifest), pst_builder_(seg_allocater_), pst_deleter_(seg_allocater_), output_seq_no_(version_->GenerateL1Seq()), partition_info_(partition_info), compaction_thread_pool_(thread_pool)
{
}
CompactionJob::~CompactionJob()
{
	outputs_.clear();
	std::vector<TaggedPstMeta>().swap(outputs_);
	std::vector<TaggedPstMeta>().swap(input_psts_);
	input_keys_.clear();
	input_values_.clear();
}
bool CompactionJob::CheckPmRoomEnough()
{
	// TODO
	return true;
}
size_t CompactionJob::PickCompaction()
{
	// get memtable data
	memtable_index_->Scan2(0, MAX_INT32, input_keys_, input_values_);
	min_key_ = input_keys_.front();
	max_key_ = input_keys_.back();
	// pick l1 meta
	version_->PickOverlappedL1Tables(min_key_, max_key_, input_psts_);
	if (input_psts_.size())
	{
		DEBUG("level1 tables:%lu %lu~%lu", inputs_[size].size(), __bswap_64(inputs_[size][0].meta.min_key_), __bswap_64(inputs_[size][inputs_[size].size() - 1].meta.max_key_));
	}

	return 2;
}
bool CompactionJob::RunCompaction()
{
	PSTReader reader(seg_allocater_);
	RowIterator row_iter(&reader, input_psts_);
	size_t count = 0;
	KeyWithRowId current_max_key = {0, -1};
	size_t memtable_idx = 0;
	// Get key from heap and get the value from row
	while (memtable_idx < input_keys_.size() || row_iter.Valid())
	{
		count++;
		size_t key, value;
		if (!row_iter.Valid())
		{
			// only memtable
			key = input_keys_[memtable_idx];
			value = input_values_[memtable_idx];
			memtable_idx++;
		}
		else if (memtable_idx >= input_keys_.size())
		{
			// only pst
			key = row_iter.GetCurrentKey();
			value = row_iter.GetCurrentValue();
			row_iter.NextKey();
		}
		else
		{
			// 过滤相同key
			if (__bswap_64(input_keys_[memtable_idx]) < __bswap_64(row_iter.GetCurrentKey()))
			{
				key = input_keys_[memtable_idx];
				value = input_values_[memtable_idx];
				memtable_idx++;
			}
			else if (__bswap_64(input_keys_[memtable_idx]) > __bswap_64(row_iter.GetCurrentKey()))
			{
				key = row_iter.GetCurrentKey();
				value = row_iter.GetCurrentValue();
				row_iter.NextKey();
			}
			else
			{
				key = input_keys_[memtable_idx];
				value = input_values_[memtable_idx];
				memtable_idx++;
				row_iter.NextKey();
			}
		}
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
	auto meta = pst_builder_.Flush();
	// add meta into manifest
	TaggedPstMeta tmeta;
	tmeta.meta = meta;
	outputs_.emplace_back(tmeta);
	DEBUG("output=%lu,marked=%lu,rewrite key num=%lu", outputs_.size(), marked_output, count);
	pst_builder_.PersistCheckpoint();
	return true;
}
struct SubCompactionArgs
{
	CompactionJob *cj_;
	int partition_id_;
	SubCompactionArgs(CompactionJob *cj, int partition_id) : cj_(cj), partition_id_(partition_id) {}
};
bool CompactionJob::RunSubCompactionParallel()
{
	for (int i = 0; i < RANGE_PARTITION_NUM; i++)
	{
		SubCompactionArgs *sca = new SubCompactionArgs(this, i);
		// printf("schedule %d\n",sca->partition_id_);
		compaction_thread_pool_->Schedule(&CompactionJob::TriggerSubCompaction, sca, sca, nullptr);
	}
	compaction_thread_pool_->WaitForJobsAndJoinAllThreads();
	return true;
}

void CompactionJob::RunSubCompaction(int partition_id)
{
	// DEBUG2("sub compaction %d", partition_id);
	PSTBuilder *pst_builder = partition_pst_builder_[partition_id] = new PSTBuilder(seg_allocater_);
	PartitionInfo &partition = partition_info_[partition_id];
	PSTReader reader(seg_allocater_);
	RowIterator row_iter(&reader, input_psts_);
	size_t count = 0;
	KeyWithRowId current_max_key = {0, -1};
	size_t memtable_idx = 0;
	// initialize: init RowIter, add first key to heap, check overlapping for each first key
	while (__bswap_64(input_keys_[memtable_idx]) < __bswap_64(partition.min_key))
	{
		memtable_idx++;
	}
	row_iter.MoveTo(partition.min_key);
	// Get key from heap and get the value from row
	while (memtable_idx < input_keys_.size() || row_iter.Valid())
	{

		size_t key, value;
		if (!row_iter.Valid())
		{
			// only memtable
			key = input_keys_[memtable_idx];
			value = input_values_[memtable_idx];
			memtable_idx++;
		}
		else if (memtable_idx >= input_keys_.size())
		{
			// only pst
			key = row_iter.GetCurrentKey();
			value = row_iter.GetCurrentValue();
			row_iter.NextKey();
		}
		else
		{
			// 过滤相同key
			if (__bswap_64(input_keys_[memtable_idx]) < __bswap_64(row_iter.GetCurrentKey()))
			{
				key = input_keys_[memtable_idx];
				value = input_values_[memtable_idx];
				memtable_idx++;
			}
			else if (__bswap_64(input_keys_[memtable_idx]) > __bswap_64(row_iter.GetCurrentKey()))
			{
				key = row_iter.GetCurrentKey();
				value = row_iter.GetCurrentValue();
				row_iter.NextKey();
			}
			else
			{
				key = input_keys_[memtable_idx];
				value = input_values_[memtable_idx];
				memtable_idx++;
				row_iter.NextKey();
			}
		}
		if (__bswap_64(key) > __bswap_64(partition.max_key))
			break;
		count++;

		auto success = pst_builder->AddEntry(Slice(&key), Slice(&value));
		if (!success)
		{
			auto meta = pst_builder->Flush();
			// add meta into manifest
			TaggedPstMeta tmeta;
			tmeta.meta = meta;
			partition_outputs_[partition_id].emplace_back(tmeta);
			if (!pst_builder->AddEntry(Slice(&key), Slice(&value)))
				ERROR_EXIT("cannot add pst entry in compaction");
		}
	}
	auto meta = pst_builder->Flush();
	// add meta into manifest
	TaggedPstMeta tmeta;
	tmeta.meta = meta;
	partition_outputs_[partition_id].emplace_back(tmeta);
	// partition_pst_builder_[partition_id]->PersistCheckpoint();
}

void CompactionJob::CleanCompaction()
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

	// 3. delete obsolute PSTs
	// delete input_psts from level 1 index
	for (auto &pst : input_psts_)
	{
		version_->DeleteTableInL1(pst.meta);
		pst_deleter_.DeletePST(pst.meta);
	}
	pst_deleter_.PersistCheckpoint();
	for (auto &pst : input_psts_)
	{
		manifest_->DeleteTable(pst.manifest_position, 1);
	}

	total_L1_num = total_L1_num + outputs_.size() - input_psts_.size();
	INFO("L1 add %lu pst, delete %lu pst, total %lu pst", outputs_.size(), input_psts_.size(), total_L1_num);
	manifest_->PrintL1Info();
}
void CompactionJob::CleanCompactionWhenUsingSubCompaction()
{
	// 1. add outputs to level 1 index
	for (int i = 0; i < RANGE_PARTITION_NUM; i++)
	{
		auto outputs = partition_outputs_[i];
		for (auto &pst : outputs)
		{
			pst.meta.seq_no_ = output_seq_no_;
			pst.level = 1;
			pst.manifest_position = manifest_->AddTable(pst.meta, 1);
			version_->InsertTableToL1(pst);
		}
		partition_pst_builder_[i]->PersistCheckpoint();
		delete partition_pst_builder_[i];
	}

	// 2. change version in manifest
	manifest_->UpdateL1Version(output_seq_no_);
	// 3. delete obsolute PSTs
	// delete inputs[-1](except for .level=1) from level 1 index
	for (auto &pst : input_psts_)
	{
		version_->DeleteTableInL1(pst.meta);
		pst_deleter_.DeletePST(pst.meta);
	}
	pst_deleter_.PersistCheckpoint();
	for (auto &pst : input_psts_)
	{
		manifest_->DeleteTable(pst.manifest_position, 1);
	}
	pst_deleter_.PersistCheckpoint();
}
bool CompactionJob::RollbackCompaction()
{
	DEBUG("rollback start!");
	return false;
}
void CompactionJob::TriggerSubCompaction(void *arg)
{
	SubCompactionArgs sca = *(reinterpret_cast<SubCompactionArgs *>(arg));
	// printf("sca.id=%d\n",sca.partition_id_);
	delete (reinterpret_cast<SubCompactionArgs *>(arg));
	// printf("trigger bgcompaction\n");
	static_cast<CompactionJob *>(sca.cj_)->RunSubCompaction(sca.partition_id_);
}