#include <vector>
#include <atomic>
#include "lock.h"
#define DEFAULT_SIZE 32

template <typename T>
class AtomicVector
{
private:
    std::vector<T> buf_;
    uint64_t max_;
    std::atomic_uint64_t current_;
    SpinLock enlarge;

public:
    AtomicVector(/* args */) : max_(DEFAULT_SIZE), current_(0)
    {
        buf_.resize(2 * max_);
    }
    ~AtomicVector(){};
    void add(T element)
    {
        auto idx = current_.fetch_add(1);
        if (idx == max_)
        {
            if (enlarge.try_lock())
            {
                max_ *= 2;
                buf_.resize(2 * max_);
                enlarge.unlock();
            }
        }
        buf_[idx] = element;
    }
    T get(int idx)
    {
        return buf_[idx];
    }
    size_t size()
    {
        return current_;
    }
    void clear()
    {
        current_.store(0);
    }

    void get_elements(std::vector<T> *list)
    {
        for (size_t i = 0; i < current_.load(); i++)
        {
            list->emplace_back(buf_[i]);
        }
    }
};
