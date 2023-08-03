#include "slice.h"

/**
 * @brief
 *
 * @tparam T key type:only for int
 * @param arr block addr
 * @param len block size
 * @param key target key
 * @param record_size sizeof(key+value)
 * @return int positive:index, negative: -lower_bound index - 1;
 */
template <typename T>
int binarysearch(char *arr, int len, T key, const int record_size)
{

    int left = 0;        //数组的左侧下标
    int right = len - 1; //数组的右侧下标
    int mid = 0;
    while (left <= right)
    {
        mid = (left + right) / 2;                    //定义中间位的下标
        T mid_key = *(T *)(arr + mid * record_size); //定义中间值的基准值

        if (mid_key == key) //如果基准值正好等于要查找的值，则自动返回要找的位置
        {
            return mid;
        }
        //如果基准值大于要查找的值，表明值在左半边，新的查找范围为中间值-1位，也就是mid-1
        else if (mid_key > key)
        {
            right = mid - 1;
        }
        //如果基准值小于要查找的值，表明值在右半边，新的查找范围为中间数+1位，也就是mid+1；
        else if (mid_key < key)
        {
            left = mid + 1;
        }
    }
    T mid_key = *(T *)(arr + mid * record_size); //定义中间值的基准值
    if (mid_key < key)
        mid++;
    return -mid - 1;
}

int binarysearch(char *arr, int len, Slice key, const int record_size)
{

    int left = 0;        //数组的左侧下标
    int right = len - 1; //数组的右侧下标
    int mid = 0;
    int ret;
    while (left <= right)
    {
        mid = (left + right) / 2;                          //定义中间位的下标
        Slice mid_key = Slice(arr + mid * record_size, 8); //定义中间值的基准值
        ret = mid_key.compare(key);
        if (ret == 0) //如果基准值正好等于要查找的值，则自动返回要找的位置
        {
            return mid;
        }
        //如果基准值大于要查找的值，表明值在左半边，新的查找范围为中间值-1位，也就是mid-1
        else if (ret > 0)
        {
            right = mid - 1;
        }
        //如果基准值小于要查找的值，表明值在右半边，新的查找范围为中间数+1位，也就是mid+1；
        else if (ret < 0)
        {
            left = mid + 1;
        }
    }
    Slice mid_key = Slice(arr + mid * record_size, 8); //定义中间值的基准值
    if (mid_key.compare(key) < 0)
        mid++;
    return -mid - 1;
}
