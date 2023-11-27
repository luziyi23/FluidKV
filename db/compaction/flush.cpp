/**
 * @file flush.cpp
 * @author your name (you@domain.com)
 * @brief core steps: 1. iterate all items in the memtable index and copy them into a sorted array
 *        2. get the address of log entries and arrange them to (new vindex? record-grained pindex? pst?)
 *        3. delete index
 *        4. release (or mark with oplog?) the participant log segments for crash-consistentcy
 * @version 0.1
 * @date 2022-09-21
 *
 * @copyright Copyright (c) 2022
 *
 */
#include "flush.h"
#include "manifest.h"
#include "version.h"
#include <queue>

inline void FlushJob::FlushPST()
{
	auto meta = pst_builder_.Flush();
	meta.seq_no_ = tree_seq_no_;
	if (meta.Valid())
	{
		// add meta into manifest
		TaggedPstMeta tmeta;
		tmeta.meta = meta;
		tmeta.level = 0;
		tmeta.manifest_position = manifest_->AddTable(meta, 0);
		version_->InsertTableToL0(tmeta, tree_idx_);
		output_pst_list_.push_back(tmeta);
	}
}

bool FlushJob::run()
{
	// iterate index to get kv list
	LOG("iterate index(scan2)");
	std::vector<uint64_t> keys, values; // TODO: try to avoid memory allocate/free overhead
	memtable_index_->Scan2(0, MAX_INT32, keys, values);
	LOG("scan2 result: size= %lu", keys.size());
	// build psts and version
	LOG("add level0 tree");
	tree_seq_no_ = version_->GetCurrentL0TreeSeq();
	tree_idx_ = version_->AddLevel0Tree();
	while (tree_idx_ == -1)
	{
		INFO("can't addlevel0tree, waiting...");
		usleep(100000);
		tree_idx_ = version_->AddLevel0Tree();
	}
	LOG("tree_idx=%d, read log and build psts", tree_idx_);
	PSTMeta meta;
	ValuePtr vptr;
	Slice key;
	Slice value;
	uint64_t k = 0, v = 0;
	key = Slice(&k);
	value = Slice(&v);
	DEBUG("will flush %lu keys,key = %lu~%lu,", keys.size(), __bswap_64(keys[0]), __bswap_64(keys[keys.size() - 1]));

	int cur_partition = 0;
	Slice partition_max_key = Slice(&(partition_info_[cur_partition].max_key));

	for (size_t i = 0; i < keys.size(); i++)
	{
		k = keys[i];
		// DEBUG("aa:%lu", __bswap_64(k));
#if (defined INDEX_LOG_MEMTABLE) && !(defined KV_SEPARATE)
		vptr.data_ = values[i];
		value = log_reader_.ReadLogForValue(key, vptr);
#else
		v = values[i];
#endif
		while (unlikely(key.compare(partition_max_key) > 0))
		{
			FlushPST();
			cur_partition++;
			if(unlikely(cur_partition >= RANGE_PARTITION_NUM))ERROR_EXIT("key > max_key in the largest partition");
			partition_max_key = Slice(&(partition_info_[cur_partition].max_key));
		}

		bool success = pst_builder_.AddEntry(key, value);
		if (!success)
		{
			FlushPST();
			if (!pst_builder_.AddEntry(key, value))
				ERROR_EXIT("cannot add pst entry in flush");
		}
	}
	FlushPST();

	/** just for validation
	RowIterator *row = new RowIterator(&pst_reader_, output_pst_list_);
	size_t cmax = row->GetCurrentKey();
	size_t kk;
	while (kk = row->NextKey())
	{
		if (__bswap_64(k) < __bswap_64(cmax))
			ERROR_EXIT("error sequence");
		cmax = k;
	}
	delete row;
	**/

	// now the new tree can be read
	version_->UpdateLevel0ReadTail();
	// delete obsolute index and log segments
	std::vector<uint64_t> segment_list;
	seg_allocater_->GetElementsFromLogGroup(seg_group_id_, &segment_list);
	LOG("delete obsolute index and log segments: %lu ,%lu", segment_list.size(), segment_list[0]);
	// printf("flush %d: coressponding segments:",seg_group_id_);
	// for (auto &seg_id : segment_list)
	// {
	//     printf("%lu,",seg_id);
	// }
	// printf("\n");
#ifndef KV_SEPARATE
	manifest_->AddFlushLog(segment_list);
#endif
	for (auto &seg_id : segment_list)
	{
		// TODO: add a new function to free a segment without reopening it
		LOG("ready to delete log segment %lu", seg_id);
		auto log_seg = seg_allocater_->GetLogSegment(seg_id);
#ifdef KV_SEPARATE
		seg_allocater_->CloseSegment(log_seg, true);
#else
		seg_allocater_->FreeSegment(log_seg);
#endif
	}
	LOG("delete over");
#ifndef KV_SEPARATE
	manifest_->ClearFlushLog();
#endif
	return true;
}
