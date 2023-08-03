#include <cmath>
#include <cstdint>
#include <cstring>
#include <random>
#include <string>

namespace utils
{
    // Starting from 10
    static constexpr uint64_t POWER_TO_G[] = {29, 29, 53, 53, 117, 125, 229, 221,
    469, 517, 589, 861, 1189, 1653, 2333, 3381, 4629, 6565, 9293, 13093, 18509,
    26253, 37117, 52317, 74101, 104581, 147973, 209173, 296029, 418341, 91733,
    836661, 1183221, 1673485, 2366509, 3346853, 4732789, 6693237, 9465541,
    13386341, 18931141, 26772693, 37862197, 53545221, 75724373, 107090317};

    /**
     * @brief Calculates the discrete logarithmic of integer k.
     *
     * This function can be used to shuffle integers in a range without
     * collisions. The implementation is derived from:
     * "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al,
     * SIGMOD 19
     *
     * @tparam POWER_OF_TWO The nterval covered by the function is [1, 2^POWER_OF_TWO].
     * @param k
     * @return uint64_t
     */
    // template<uint32_t POWER_OF_TWO>
    // static uint64_t discrete_log(const uint64_t k) noexcept
    // {
    //     static constexpr uint32_t POWER = POWER_OF_TWO + 2;
    //     static_assert(POWER >=10 && POWER <= 55, "POWER must be in range [10,55].");
    //     static constexpr uint64_t G = POWER_TO_G[POWER-10];
    //     static constexpr uint64_t P = 1ULL << (POWER-2);
    //     static constexpr uint64_t P_MASK = P-1;

    //     //assert(k > 0);
    //     assert(k < (1ULL << (POWER-2)));

    //     uint64_t up = k;
    //     uint64_t x = 0;
    //     uint64_t radix = 1;
    //     uint64_t Gpow = (G-1)/4;
    //     for(uint64_t i=0; up; ++i)
    //     {
    //         if(up & radix)
    //         {
    //             x = x + radix;
    //             up = (up + Gpow + 4 * up * Gpow) & P_MASK;
    //         }
    //         radix = radix << 1;
    //         Gpow = (Gpow + Gpow + 4 * Gpow * Gpow) & P_MASK;
    //     }
    //     return P-x;
    // }

    template<typename T>
    static T fnv1a(const void* data, size_t size)
    {
        static_assert(
            std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value,
            "fnv1a only supports 32 bits and 64 bits variants."
        );

        static constexpr T INIT = std::is_same<T, uint32_t>::value ? 2166136261u : 14695981039346656037ul;
        static constexpr T PRIME = std::is_same<T, uint32_t>::value ? 16777619 : 1099511628211ul;

        auto src = reinterpret_cast<const char*>(data);
        T sum = INIT;
        while (size--)
        {
            sum = (sum ^ *src) * PRIME;
            src++;
        }
        return sum;
    }

    /**
     * @brief Calculate multiplicative hash of integer in the same domain.
     *
     * This was adapted from:
     *
     * ""The Art of Computer Programming, Volume 3, Sorting and Searching",
     * D.E. Knuth, 6.4 p:516"
     *
     * The function should be used as a cheap way to scramble integers in the
     * domain [0,2^T] to integers in the same domain [0,2^T]. If the resulting
     * hash should be in a smaller domain [0,2^m], the result should be right
     * shifted by X bits where X = (32 | 64) - m.
     *
     * @tparam T type of input and output (uint32_t or uint64_t).
     * @param x integer to be hashed.
     * @return T hashed result.
     */
    template<typename T>
    static T multiplicative_hash(T x)
    {
        static_assert(
            std::is_same<T, uint32_t>::value || std::is_same<T, uint64_t>::value,
            "multiplicative hash only supports 32 bits and 64 bits variants."
        );

        static constexpr T A = std::is_same<T, uint32_t>::value ?
            2654435761u
            :
            11400714819323198393ul;
        return A * x;
    }

    /**
     * @brief Verify endianess during runtime.
     *
     * @return true
     * @return false
     */
    static bool is_big_endian(void)
    {
        volatile union {
            uint32_t i;
            char c[4];
        } bint = {0x01020304};

        return bint.c[0] == 1;
    }

    /**
     * @brief Read memory without optimizing out.
     *
     * This function should be called to simulate a dummy use of the variable p.
     * It tells the compiler that there might be side effects and avoids any
     * optimization-out.
     *
     * Source: https://youtu.be/nXaxk27zwlk?t=2550
     *
     * @param addr beginning of memory region.
     * @param size size of memory region.
     */
    static void dummy_use(void* addr, size_t size)
    {
        char* p = static_cast<char*>(p);
        char* end = static_cast<char*>(p) + size;

        if(p >= end)
            return;

        for(; p<end; ++p)
        {
            void* vptr = static_cast<void*>(p);
            asm volatile("" : : "g"(vptr) : "memory");
        }
    }
} // namespace utils

class key_generator_t
{
public:
    /**
     * @brief Construct a new key_generator_t object
     *
     * @param N size of key space.
     * @param size size in Bytes of keys to be generated (excluding prefix).
     * @param prefix prefix to be prepended to every key.
     */
    key_generator_t(size_t N, size_t size, const std::string& prefix = "");

    virtual ~key_generator_t() = default;

    /**
     * @brief Generate next key.
     *
     * Setting 'in_sequence' to true is useful when initially loading the data
     * structure, so no repeated keys are generated and we can guarantee the
     * amount of unique records inserted.
     *
     * Finally, a pointer to buf_ is returned. Since next() overwrites buf_, the
     * pointer returned should not be used across calls to next().
     *
     * @param in_sequence if @true, keys are generated in sequence,
     *                    if @false keys are generated randomly.
     * @return const char* pointer to beginning of key.
     */
    virtual const char* next(bool in_sequence = false) final;

    /**
     * @brief Returns total key size (including prefix).
     *
     * @return size_t
     */
    size_t size() const noexcept { return prefix_.size() + size_; }

    /**
     * @brief Returns size of keyspace in number of elements..
     *
     * @return size_t
     */
    size_t keyspace() const noexcept { return N_; }

    /**
     * @brief Set the seed object.
     *
     * @param seed
     */
    static void set_seed(uint32_t seed)
    {
        seed_ = seed;
        generator_.seed(seed_);
    }

    /**
     * @brief Get the seed object.
     *
     * @return uint32_t
     */
    static uint32_t get_seed() noexcept { return seed_; }

    static constexpr uint32_t KEY_MAX = 128;

    static thread_local uint64_t current_id_;

    const char* hash_id(uint64_t id);

    virtual uint64_t next_id() = 0;

protected:

    /// Engine used for generating random numbers.
    static thread_local std::default_random_engine generator_;

private:
    /// Seed used for generating random numbers.
    static thread_local uint32_t seed_;

    /// Space to materialize the keys (avoid allocation).
    static thread_local char buf_[KEY_MAX];

    /// Size of keyspace to generate keys.
    const size_t N_;

    /// Size in Bytes of keys to be generated (excluding prefix).
    const size_t size_;

    /// Prefix to be preppended to every key.
    const std::string prefix_;

    //uint64_t current_id_ = 0;
};

class uniform_key_generator_t final : public key_generator_t
{
public:
    uniform_key_generator_t(size_t N, size_t size, const std::string& prefix = "")
        : dist_(1, N),
          key_generator_t(N, size, prefix) {}

// protected:
    virtual uint64_t next_id() override
    {
        return dist_(generator_);
    }

private:
    std::uniform_int_distribution<uint64_t> dist_;
};



thread_local std::default_random_engine key_generator_t::generator_;
thread_local uint32_t key_generator_t::seed_;
thread_local char key_generator_t::buf_[KEY_MAX];
thread_local uint64_t key_generator_t::current_id_ = 0;

key_generator_t::key_generator_t(size_t N, size_t size, const std::string& prefix)
    : N_(N),
      size_(size),
      prefix_(prefix)
{
    memset(buf_, 0, KEY_MAX);
    memcpy(buf_, prefix_.c_str(), prefix_.size());
}

const char* key_generator_t::next(bool in_sequence)
{
    uint64_t id = -1;
    if (in_sequence)
    {
      id = ++current_id_;
    }
    else
    {
      //if opt_.bm_mode == mode_t::Operation  
      id = next_id();
    }
    return hash_id(id);
}

const char* key_generator_t::hash_id(uint64_t id)
{
    char* ptr = &buf_[prefix_.size()];

    uint64_t hashed_id = utils::multiplicative_hash<uint64_t>(id);

    if (size_ < sizeof(hashed_id))
    {
        // We want key smaller than 8 Bytes, so discard higher bits.
        auto bits_to_shift = (sizeof(hashed_id) - size_) << 3;

        // Discard high order bits
        if (utils::is_big_endian())
        {
            hashed_id >>= bits_to_shift;
            hashed_id <<= bits_to_shift;
        }
        else
        {
            hashed_id <<= bits_to_shift;
            hashed_id >>= bits_to_shift;
        }

        memcpy(ptr, &hashed_id, size_); // TODO: check if must change to fit endianess
    }
    else
    {
        // TODO: change this, otherwise zeroes act as prefix
        // We want key of at least 8 Bytes, check if we must prepend zeroes
        auto bytes_to_prepend = size_ - sizeof(hashed_id);
        if (bytes_to_prepend > 0)
        {
            memset(ptr, 0, bytes_to_prepend);
            ptr += bytes_to_prepend;
        }
        memcpy(ptr, &hashed_id, sizeof(hashed_id));
    }
    return buf_;
}