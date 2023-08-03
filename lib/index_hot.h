#pragma once

#include "db.h"
#include "hot/rowex/HOTRowex.hpp"
#include "idx/contenthelpers/IdentityKeyExtractor.hpp"

typedef struct IntKeyVal
{
    uint64_t key;
    uint64_t value;
} IntKeyVal;

template <typename ValueType = IntKeyVal *>
class IntKeyExtractor
{
public:
    typedef uint64_t KeyType;

    inline KeyType operator()(ValueType const &value) const
    {
        return value->key;
    }
};
using TrieType = hot::rowex::HOTRowex<IntKeyVal *, IntKeyExtractor>;
class HOTIterator
{
private:
    TrieType::const_iterator iter;

public:
    HOTIterator(TrieType::const_iterator it) : iter(it){};
    bool Next()
    {
        if (iter != iter.end())
        {
            ++iter;
            return true;
        }
        return false;
    };
    ValueType Value()
    {
        if (iter == iter.end())
            return INVALID_PTR;
        return (*iter)->value;
    };
    uint64_t Key()
    {
        if (iter == iter.end())
            return INVALID_PTR;
        return (*iter)->key;
    }
    bool Valid()
    {
        return iter != iter.end();
    }
};

class HOTIndex : public Index
{
    TrieType mTrie;
    IntKeyVal *keys;
    std::atomic_uint32_t idx;
    const size_t max_;

public:
    HOTIndex(size_t max_entries) : idx(0),max_(max_entries > 10000000 ? max_entries : 10000000)
    {
        keys = new IntKeyVal[max_];
    }

    virtual ~HOTIndex() override
    {
        delete[] keys;
    }
    virtual void ThreadInit(int thread_id) override{
    };

    virtual ValueType Get(const KeyType key) override
    {
        auto ret = mTrie.lookup(__bswap_64(key));
        return ret.mIsValid ? ret.mValue->value : INVALID_PTR;
    }

    virtual void Put(const KeyType key, ValueHelper &le_helper) override
    {
        size_t id = idx.fetch_add(1);
        IntKeyVal *kv = &(keys[id]);

        kv->value = le_helper.new_val;
        kv->key = __bswap_64(key);
        
        // IntKeyVal *kv = new IntKeyVal{*(KeyType *)key.data(),le_helper.new_val};
        auto ret = mTrie.upsert(kv);
        if (ret.mIsValid)
        {
            le_helper.valid = true;
            le_helper.old_val = ret.mValue->value;
        }
    }
    virtual void Delete(const KeyType key) override
    {
    }

    virtual void Scan(const KeyType key, int cnt,
                      std::vector<ValueType> &vec) override
    {
        auto iter = Seek(__bswap_64(key));
        while (cnt--)
        {
            if (iter.Valid())
            {
                vec.push_back(iter.Value());
                iter.Next();
            }else{
                break;
            }
        }
    }
    virtual void Scan2(const KeyType key, int cnt, std::vector<uint64_t> &kvec,
                      std::vector<ValueType> &vec) override
    {
        auto iter = Seek(__bswap_64(key));
        while (cnt--)
        {
            if (iter.Valid())
            {
                kvec.push_back(__bswap_64(iter.Key()));
                vec.push_back(iter.Value());
                iter.Next();
            }else{
                break;
            }
        }
    }

    virtual void ScanByRange(const KeyType key, const KeyType end, std::vector<uint64_t> &kvec,
                      std::vector<ValueType> &vec) override
    {
        auto iter = Seek(__bswap_64(key));
        while (iter.Valid())
        {
            if (iter.Key() <= __bswap_64(end))
            {
                kvec.push_back(__bswap_64(iter.Key()));
                vec.push_back(iter.Value());
                iter.Next();
            }else{
                break;
            }
        }
    }

    HOTIterator Seek(const KeyType key)
    {
        return HOTIterator(mTrie.lower_bound(key));
    }
    // virtual void PrefetchEntry(const Shortcut &sc) override {}
};
