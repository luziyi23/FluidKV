#pragma once

#include "db.h"
#include "CCEH/CCEH.h"

class CCEHIndex : public Index
{
public:
    CCEHIndex() { table_ = new CCEH_NAMESPACE::CCEH(128 * 1024); }

    virtual ~CCEHIndex() override { delete table_; }

    virtual void ThreadInit(int thread_id) override {}
    virtual ValueType Get(const KeyType key) override
    {
        return table_->Get(key);
    }

    virtual void Put(const KeyType key, ValueHelper &le_helper) override
    {
        table_->Insert(key, le_helper);
    }

//     virtual void GCMove(const KeyType key, ValueHelper &le_helper) override
//     {
// #ifdef GC_SHORTCUT
//         if (le_helper.shortcut.None() ||
//             !table_->TryGCUpdate(key, le_helper))
//         {
//             table_->Insert(key, le_helper);
//         }
// #else
//         table_->Insert(key, le_helper);
// #endif
//     }

    virtual void Delete(const KeyType key) override
    {
        // TODO
    }

    //   virtual void PrefetchEntry(const Shortcut &sc) override {
    //     CCEH_NAMESPACE::Segment *s = (CCEH_NAMESPACE::Segment *)sc.GetNodeAddr();
    //     __builtin_prefetch(&s->sema);
    //   }

private:
    CCEH_NAMESPACE::CCEH *table_;

    DISALLOW_COPY_AND_ASSIGN(CCEHIndex);
};
