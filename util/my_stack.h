#include <vector>

template <class T, int capacity = 30>
class stack
{
private:
    std::vector<T> s;

public:
    stack() { s.reserve(capacity); } //调整vector大小，使之可以容纳n个元素，如果当前vector容量小于n，则扩展容量至n

    void push(const T &a) { s.push_back(a); }

    T pop()
    {
        T a = s.back();
        s.pop_back();
        return a;
    }

    bool empty()
    {
        return s.empty();
    }

    void clear() { s.clear(); } // clear()用于清空栈中所有的元素
};
