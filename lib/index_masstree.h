#pragma once

#include "util/util.h"
#include "masstree/masstree_wrapper.h"

class MasstreeIndex : public Index
{
public:
    MasstreeIndex() { mt_ = new MasstreeWrapper(); }

    virtual ~MasstreeIndex() override { delete mt_; }

    void MasstreeThreadInit(int thread_id) { MasstreeWrapper::thread_init(thread_id); }

    virtual void ThreadInit(int thread_id) override
    {
        MasstreeThreadInit(thread_id);
    };

    virtual ValueType Get(const KeyType key) override
    {
        ValueType val;
        bool found = mt_->search(key, val);
        if (found)
        {
            return val;
        }
        else
        {
            return INVALID_PTR;
        }
    }

    virtual void Put(const KeyType key, ValueHelper &le_helper)
    {
        mt_->insert(key, le_helper);
    }
    
    virtual void PutValidate(const KeyType key, ValueHelper &le_helper)
    {
        mt_->insert_validate(key, le_helper);
    }

    virtual void Delete(const KeyType key) override
    {
        mt_->remove(key);
    }

    virtual void Scan(const KeyType key, int cnt,
                      std::vector<ValueType> &vec) override
    {
        mt_->scan(key, cnt, vec);
    }
    virtual void Scan2(const KeyType key, int cnt, std::vector<uint64_t> &kvec, std::vector<ValueType> &vvec) override
    {
        mt_->scan(key, cnt, kvec, vvec);
    }
    virtual void ScanByRange(const KeyType start, const KeyType end, std::vector<uint64_t> &kvec, std::vector<ValueType> &vvec) override
    {
        mt_->scan(start, end, kvec, vvec);
    }

private:
    MasstreeWrapper *mt_;

    DISALLOW_COPY_AND_ASSIGN(MasstreeIndex);
};
