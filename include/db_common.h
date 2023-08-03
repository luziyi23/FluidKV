#pragma once

#include <cstdint>
#include <tuple>
#include <vector>
#include "config.h"
#include "slice.h"
#include "util/debug_helper.h"
#include "util/util.h"
#include "util/lock.h"

#ifndef ERROR_CODE
#define ERROR_CODE (0xffffffffffffffffUL)
#endif
#ifndef INVALID_PTR
#define INVALID_PTR (0xffffffffffffffffUL)
#endif

// key, value in Index
using KeyType = uint64_t;
using ValueType = uint64_t;
static constexpr ValueType INVALID_VALUE = 0;
static constexpr uint64_t SEGMENT_SIZE = 4ul << 20;

#ifndef KV_SEPARATE
union ValuePtr
{
    struct detail
    {
        uint64_t valid : 1;
        uint64_t ptr : 33; // 64B-align => discard the lowest 6bits, 33bits + 6 bits can index 512GB
        uint64_t lsn : 30; // support 1G reports per hash-bucket,  256G records totally
    } detail_;
    uint64_t data_;
};
#else
union ValuePtr
{
    struct detail
    {
        uint64_t valid : 1;
        uint64_t ptr : 34; // 64B-align => discard the lowest 6bits, 34bits + 6 bits can index 1TB
        uint64_t lsn : 29; // support 512B reports per hash-bucket,  2048G records totally
    } detail_;
    uint64_t data_;
};
#endif

struct ValueHelper
{
    ValueType new_val = INVALID_VALUE;
    ValueType old_val = INVALID_VALUE; // in and out for gc put, out for db put
    // char *index_entry = nullptr;
    bool valid = false;

    ValueHelper(ValueType _new_val) : new_val(_new_val) {}
};

class Index
{
public:
    virtual ~Index(){};
    virtual void ThreadInit(int thread_id) = 0;
    virtual ValueType Get(const KeyType key) = 0;
    virtual void Put(const KeyType key, ValueHelper &le_helper) = 0;
    virtual void PutValidate(const KeyType key, ValueHelper &le_helper) = 0;
    virtual void Delete(const KeyType key) = 0;
    virtual void Scan(const KeyType key, int cnt, std::vector<ValueType> &vec)
    {
        ERROR_EXIT("not supported in this class");
    }
    virtual void Scan2(const KeyType key, int cnt, std::vector<uint64_t> &kvec, std::vector<ValueType> &vvec)
    {
        ERROR_EXIT("not supported in this class");
    }
    virtual void ScanByRange(const KeyType start, const KeyType end, std::vector<uint64_t> &kvec, std::vector<ValueType> &vvec)
    {
        ERROR_EXIT("not supported in this class");
    }
};

struct FilePtr
{
#define mask63 ((1UL << 63) - 1)
#define mask32 ((1UL << 32) - 1)
    int fd;
    int offset;

    uint64_t data()
    {
        return (1UL << 63) | ((uint64_t)(fd) << 32) | offset;
    }

    FilePtr(int _fd, int _offset)
    {
        fd = _fd;
        offset = _fd;
    }
    FilePtr(uint64_t data)
    {
        fd = (mask63 & data) >> 32;
        offset = (mask32 & data);
    }
    static FilePtr InvalidPtr()
    {
        return FilePtr{-1, -1};
    }
    bool Valid()
    {
        return fd >= 0 && offset > 0;
    }

    bool operator==(FilePtr b)
    {
        return fd == b.fd && offset == b.offset;
    }
};
