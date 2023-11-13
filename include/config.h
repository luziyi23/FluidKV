#pragma once
#include <string>

#define MAX_MEMTABLE_NUM 2
#define MAX_MEMTABLE_ENTRIES 5000000
#define MAX_L0_TREE_NUM 32 //only a upper limit. trigger is db->l0_compaction_tree_num_
#define MAX_USER_THREAD_NUM 64

#define RANGE_PARTITION_NUM 4


#define MASSTREE_MEMTABLE
#define MASSTREE_L1


// Now defined in CMakeList.txt
// #define INDEX_LOG_MEMTABLE
// #define BUFFER_WAL_MEMTABLE

#ifdef INDEX_LOG_MEMTABLE
#define KV_SEPARATE
#endif

class DBConfig
{
public:
    DBConfig(/* args */){};
    ~DBConfig(){};

    std::string pm_pool_path = "/mnt/pmem/helidb/";
    std::string ssd_path = "/mnt/optane-ssd/helidb/";
    size_t pm_pool_size = 80ul << 30;
    bool recover = false;
};
