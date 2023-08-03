#include "util/util.h"
#include "db_common.h"
#include <libpmem.h>
#include <sys/param.h>
#include <cstring>
#include <mutex>

// TODO: use atmomic bitmap and optimistic lock
// TODO: persist and recover bitmap
class BitMap
{
public:
    size_t total_bit_num_;
    std::atomic_uint64_t tail_bit_;
    char *bitmap_;
    char *history_bitmap_;
    char *persist_addr_;
    std::vector<size_t> freed_bits_;
    SpinLock lock_;
    bool use_free_list_ = false;
    bool debug_ = false;

public:
    BitMap(size_t total_bits, bool debug = false) : debug_(debug)
    {
        total_bit_num_ = total_bits;

        bitmap_ = new char[roundup(total_bit_num_, 8) / 8];
        memset(bitmap_, 0, roundup(total_bit_num_, 8) / 8);
        history_bitmap_ = new char[roundup(total_bit_num_, 8) / 8];
        memset(history_bitmap_, 0, roundup(total_bit_num_, 8) / 8);
        tail_bit_ = 0;
        freed_bits_.reserve(total_bits);
    }
    ~BitMap()
    {
        std::vector<uint64_t>().swap(freed_bits_);
        delete[] bitmap_;
        delete[] history_bitmap_;
    }
    void SetPersistAddr(char *persist_addr) { 
        persist_addr_ = persist_addr; 
    }
    void Recover()
    {
        freed_bits_.clear();
        memcpy(bitmap_, persist_addr_, roundup(total_bit_num_, 8) / 8);
        memcpy(history_bitmap_, bitmap_, roundup(total_bit_num_, 8) / 8);
        size_t tail = MAX_UINT64;
        bool tail_flag = true;
        for (int i = roundup(total_bit_num_, 8) / 8 - 1; i >= 0; i--)
        {
            if (bitmap_[i] != 0)
            {
                for (int pos = 0; pos < 8; pos++)
                {
                    uint8_t mask = 1 << pos;
                    if ((!(mask & bitmap_[i])) && (i * 8 + pos < total_bit_num_))
                    {
                        freed_bits_.push_back(i * 8 + pos);
                    }
                }

                tail_flag = false;
            }
            else if (tail_flag)
            {
                tail = i;
            }
            else
            {
                for (int pos = 0; pos < 8; pos++)
                {
                    freed_bits_.push_back(i * 8 + pos);
                }
            }
        }
        tail_bit_ = tail * 8;
    }
    void RecoverFrom(char *src)
    {
        char *temp = persist_addr_;
        persist_addr_ = src;
        Recover();
        persist_addr_ = temp;
    }
    inline size_t SizeInByte()
    {
        return roundup(total_bit_num_, 8) / 8;
    }
    void PersistToPM()
    {
        pmem_memcpy_persist(persist_addr_, bitmap_, SizeInByte());
    }
    void PersistToPMOnlyAlloc()
    {
        char *temp_arr = new char[SizeInByte()];
        memcpy(temp_arr, persist_addr_, SizeInByte());
        for (size_t i = 0; i < SizeInByte(); i++)
        {
            // XOR(history,bitmap)=mask
            // OR(mask,pmem)=new_pmem
            temp_arr[i] = (bitmap_[i] ^ history_bitmap_[i]) | temp_arr[i];
        }
        memcpy(bitmap_, temp_arr, SizeInByte());
        pmem_memcpy_persist(persist_addr_, temp_arr, SizeInByte());
        delete[] temp_arr;
    }
    void PersistToPMOnlyFree()
    {
        char *temp_arr = new char[SizeInByte()];
        memcpy(temp_arr, persist_addr_, SizeInByte());
        for (size_t i = 0; i < SizeInByte(); i++)
        {
            // XOR(history,bitmap)=mask
            // AND(~mask,pmem)=new_pmem
            temp_arr[i] = ~(bitmap_[i] ^ history_bitmap_[i]) & temp_arr[i];
        }
        pmem_memcpy_persist(persist_addr_, temp_arr, SizeInByte());
        delete[] temp_arr;
    }
    void CopyTo(char *dst)
    {
        memcpy(dst, bitmap_, SizeInByte());
    }
    bool IsFull()
    {
        if (freed_bits_.size())
        {
            return false;
        }

        if (tail_bit_ < total_bit_num_)
        {
            return false;
        }

        return true;
    }
    size_t AllocateOne()
    {
        size_t position;
        std::lock_guard<SpinLock> lock(lock_);
        while (1)
        {
            // bool find_free_bit;
            if (freed_bits_.size())
            {
                position = freed_bits_.back();
                freed_bits_.pop_back();
                // find_free_bit = true;
                if (position >= total_bit_num_)
                {
                    ERROR_EXIT("freebits inlegal %lu", position);
                }
            }
            else
            {
                position = tail_bit_.fetch_add(1);
                // find_free_bit = false;
            }
            if (position >= total_bit_num_)
            {
                return ERROR_CODE;
            }
            if (allocate(position))
            {
                return position;
            }
        }
    }
    bool AllocatePos(size_t pos)
    {
        std::lock_guard<SpinLock> lock(lock_);
        if (allocate(pos))
        {
            if(tail_bit_<pos){
                tail_bit_.store(pos);
            }
            return true;
        }
        return false;
    }
    size_t AllocateMany(size_t num)
    {
        size_t position;
        std::lock_guard<SpinLock> lock(lock_);
        while (1)
        {

            position = tail_bit_.fetch_add(num);

            if (position >= total_bit_num_)
            {
                return ERROR_CODE;
            }
            if (allocate(position))
            {
                return position;
            }
        }
    }
    bool Free(size_t position)
    {
        assert(position < total_bit_num_);
        std::lock_guard<SpinLock> lock(lock_);
        uint8_t byte = get_byte(position / 8);
        uint8_t bit_mask = 0x1 << (position % 8);
        if (!(byte & bit_mask))
            return false;
        set_byte(position / 8, byte & (~bit_mask));
        freed_bits_.push_back(position);
        return true;
    }
    bool Exist(size_t position)
    {
        std::lock_guard<SpinLock> lock(lock_);
        uint8_t byte = get_byte(position / 8);
        uint8_t bit_mask = 0x1 << (position % 8);
        if (byte & bit_mask)
            return true;
        return false;
    }
    /*not thread safe*/
    bool GetUsedBits(std::vector<uint64_t> &list)
    {
        if (!list.empty())
            return false;
        for (int i = 0; i < roundup(tail_bit_,8)/8; i++)
        {
            if (bitmap_[i] != 0)
            {
                for (int pos = 0; pos < 8; pos++)
                {
                    uint8_t mask = 1 << pos;
                    if ((mask & bitmap_[i]) && (i * 8 + pos < total_bit_num_))
                    {
                        list.push_back(i * 8 + pos);
                    }
                }
            }
        }
        return true;
    }
    size_t GetUsedBitsNum()
    {
        size_t count=0;
        for (int i = 0; i < roundup(tail_bit_,8)/8; i++)
        {
            if (bitmap_[i] != 0)
            {
                for (int pos = 0; pos < 8; pos++)
                {
                    uint8_t mask = 1 << pos;
                    if ((mask & bitmap_[i]) && (i * 8 + pos < total_bit_num_))
                    {
                        count++;
                    }
                }
            }
        }
        return count;
    }

private:
    inline size_t allocate(size_t position)
    {
        assert(position < total_bit_num_);
        uint8_t byte = get_byte(position / 8);
        uint8_t bit_mask = 0x1 << (position % 8);
        if (byte & bit_mask)
            return false;
        set_byte(position / 8, byte | bit_mask);
        return true;
    }
    inline uint8_t get_byte(size_t i)
    {
        assert(i < roundup(total_bit_num_, 8) / 8);
        return bitmap_[i];
    }
    inline void set_byte(size_t i, uint8_t byte)
    {
        assert(i < roundup(total_bit_num_, 8) / 8);
        bitmap_[i] = byte;
    }

    DISALLOW_COPY_AND_ASSIGN(BitMap);
};