#pragma once
#include "db_common.h"

// #define DIRECT_PM_ACCESS
#define ALIGNED_COPY_256

// L0.5 512B indexblock (with bucket mark) -> 64B KV log entry(see log_format.h)
struct PIndexLeafNode512
{
    struct Entry
    {
        uint64_t key;
        ValuePtr vptr;
    };
    Entry entries[32];
};

// Memtable flush = change mem_index into sorted LeafNodes and build inner nodes
struct PIndexBlock4096
{
    static const size_t MAX_ENTRIES = 256;
    struct Entry
    {
        uint64_t key;
        uint64_t leafptr;
    };
    Entry entries[256];
};
// L1 512B indexblock -> 512B datablock
struct PIndexBlock1024
{
    static constexpr uint64_t MAX_ENTRIES = 64;
    // TODO: a header to identify node type
    struct Entry
    {
        uint64_t key;
        uint64_t leafptr;
    };
    Entry entries[64];
};
struct PIndexBlock512
{
    static constexpr uint64_t MAX_ENTRIES = 32;
    // TODO: a header to identify node type
    struct Entry
    {
        uint64_t key;
        uint64_t leafptr;
    };
    Entry entries[32];
};
struct PIndexBlock256
{
    static constexpr uint64_t MAX_ENTRIES = 16;
    // TODO: a header to identify node type
    struct Entry
    {
        uint64_t key;
        uint64_t leafptr;
    };
    Entry entries[16];
};
struct PIndexBlock128
{
    static constexpr uint64_t MAX_ENTRIES = 8;
    // TODO: a header to identify node type
    struct Entry
    {
        uint64_t key;
        uint64_t leafptr;
    };
    Entry entries[8];
};
struct PIndexBlock64
{
    static constexpr uint64_t MAX_ENTRIES = 4;
    // TODO: a header to identify node type
    struct Entry
    {
        uint64_t key;
        uint64_t leafptr;
    };
    Entry entries[4];
};
// TODO: Embed the value length in the valueptr field
struct PDataBlock512Sep
{
    static constexpr uint64_t MAX_ENTRIES = 32;
    struct Entry
    {
        uint64_t key;
        uint64_t valueptr;
    };
    Entry entries[32];
};

// read value as a char[], end by \0
// KV with size over 64B use KVSeparate
// diffrent from log segment, KV in a data block is sorted in data segment
struct PDataBlock1024ForFixed16B
{
    static constexpr int MAX_ENTRIES = 64;
    struct Entry
    {
        uint64_t key;
        uint64_t value;
    };
    Entry entries[64];
};
struct PDataBlock512ForFixed16B
{
    static constexpr int MAX_ENTRIES = 32;
    struct Entry
    {
        uint64_t key;
        uint64_t value;
    };
    Entry entries[32];
};
struct PDataBlock256ForFixed16B
{
    static constexpr int MAX_ENTRIES = 16;
    struct Entry
    {
        uint64_t key;
        uint64_t value;
    };
    Entry entries[16];
};
struct PDataBlock128ForFixed16B
{
    static constexpr int MAX_ENTRIES = 8;
    struct Entry
    {
        uint64_t key;
        uint64_t value;
    };
    Entry entries[8];
};
struct PDataBlock64ForFixed16B
{
    static constexpr int MAX_ENTRIES = 4;
    struct Entry
    {
        uint64_t key;
        uint64_t value;
    };
    Entry entries[4];
};
struct PDataBlock512ForFixed32B
{
    struct Entry
    {
        uint64_t key;
        char value[24];
    };
    Entry entries[16];
};
struct PDataBlock512ForFixed64B
{
    struct Entry
    {
        uint64_t key;
        char value[56];
    };
    Entry entries[8];
};

struct PDataBlock4096ForFixed16B
{
    static const size_t MAX_ENTRIES = 256;
    struct Entry
    {
        uint64_t key;
        uint64_t value;
    };
    Entry entries[256];
};
struct PDataBlockk4096ForFixed32B
{
    struct Entry
    {
        uint64_t key;
        char value[24];
    };
    Entry entries[128];
};
struct PDataBlockk4096ForFixed64B
{
    struct Entry
    {
        uint64_t key;
        char value[56];
    };
    Entry entries[64];
};

// Wrappers
using PIndexBlock = PIndexBlock512;
// using PIndexBlock = PIndexBlock4096;
using PDataBlock = PDataBlock512ForFixed16B;
// using PDataBlock = PDataBlock4096ForFixed16B;
using PSSDBlock = PDataBlock4096ForFixed16B;
struct PIndexBlockWrapper
{

    PIndexBlock data_buf;
    int size = 0;
    char *pm_page_addr = nullptr;

    void clear()
    {
        size = 0;
        pm_page_addr = nullptr;
    }
    bool valid()
    {
        return pm_page_addr != nullptr;
    }
    void set_page(char *addr)
    {
        pm_page_addr = addr;
    }
    void add_entry(uint64_t key, uint64_t ptr)
    {
        data_buf.entries[size].key = key;
        data_buf.entries[size].leafptr = ptr;
        size++;
    }
    bool is_full()
    {
        return size == PIndexBlock::MAX_ENTRIES;
    }
};

struct PDataBlockPmWrapper
{
    PDataBlock data_buf;
    int size = 0;
    char *pm_page_addr = nullptr;

    void clear()
    {
        size = 0;
        pm_page_addr = nullptr;
    }
    bool valid()
    {
        return pm_page_addr != nullptr;
    }
    void set_page(char *addr)
    {
        pm_page_addr = addr;
    }
    void add_entry(uint64_t key, uint64_t ptr)
    {
        data_buf.entries[size].key = key;
        data_buf.entries[size].value = ptr;
        size++;
    }
    bool is_full()
    {
        return size == PDataBlock::MAX_ENTRIES;
    }
};

struct PDataBlockSsdWrapper
{
    PDataBlock4096ForFixed16B data_buf;
    int size = 0;
    FilePtr file_ptr{0,-1};
    // the physical page of this block is marked with fd and offset.
    void clear()
    {
        size = 0;
        file_ptr=FilePtr::InvalidPtr();
    }
    bool valid()
    {
        return file_ptr.Valid();
    }
    void set_fptr(FilePtr fptr)
    {
        file_ptr = fptr;
    }
    void add_entry(uint64_t key, uint64_t value)
    {
        data_buf.entries[size].key = key;
        data_buf.entries[size].value = value;
        size++;
    }
    bool is_full()
    {
        return size == PDataBlock4096ForFixed16B::MAX_ENTRIES;
    }
};