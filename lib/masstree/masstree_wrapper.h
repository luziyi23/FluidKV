#pragma once

#include <iostream>
#include <random>
#include <vector>
#include <thread>

#include <pthread.h>

#include "config.h"
#include "compiler.hh"

#include "masstree.hh"
#include "kvthread.hh"
#include "masstree_tcursor.hh"
#include "masstree_insert.hh"
#include "masstree_print.hh"
#include "masstree_remove.hh"
#include "masstree_scan.hh"
#include "string.hh"

#include "db_common.h"

#include <unistd.h>
#include <array>
#include <memory>
#include <string>
#include <stdexcept>
#include <regex>

static void print_dram_consuption()
{
    auto pid = getpid();
    std::array<char, 128> buffer;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(
        popen(("cat /proc/" + std::to_string(pid) + "/status").c_str(), "r"),
        pclose);
    if (!pipe)
    {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr)
    {
        std::string result = buffer.data();
        if (result.find("VmRSS") != std::string::npos)
        {
            // std::cout << result << std::endl;
            std::string mem_ocp = std::regex_replace(
                result, std::regex("[^0-9]*([0-9]+).*"), std::string("$1"));
            DEBUG("DRAM consumption: %.3f GB.", stof(mem_ocp) / 1024 / 1024);
            break;
        }
    }
}

class key_unparse_unsigned
{
public:
    static int unparse_key(Masstree::key<uint64_t> key, char *buf, int buflen)
    {
        return snprintf(buf, buflen, "%" PRIu64, key.ikey());
    }
};

class MasstreeWrapper
{
public:
    static constexpr uint64_t insert_bound = 0xfffff; // 0xffffff;
    struct table_params : public Masstree::nodeparams<15, 15>
    {
        typedef uint64_t value_type;
        typedef Masstree::value_print<value_type> value_print_type;
        typedef threadinfo threadinfo_type;
        typedef key_unparse_unsigned key_unparse_type;
        static constexpr ssize_t print_max_indent_depth = 12;
    };

    typedef Masstree::Str Str;
    typedef Masstree::basic_table<table_params> table_type;
    typedef Masstree::unlocked_tcursor<table_params> unlocked_cursor_type;
    typedef Masstree::tcursor<table_params> cursor_type;
    typedef Masstree::leaf<table_params> leaf_type;
    typedef Masstree::internode<table_params> internode_type;

    typedef typename table_type::node_type node_type;
    typedef typename unlocked_cursor_type::nodeversion_value_type nodeversion_value_type;

    struct Scanner
    {
        const int cnt;
        std::vector<table_params::value_type> &vec;

        Scanner(int cnt, std::vector<table_params::value_type> &v)
            : cnt(cnt), vec(v)
        {
            vec.reserve(cnt);
        }

        template <typename SS, typename K>
        void visit_leaf(const SS &, const K &, threadinfo &) {}

        bool visit_value(Str key, table_params::value_type val, threadinfo &)
        {
            // TODO: skip the val which points to hybrid index block
            vec.push_back(val);
            if (vec.size() == cnt)
            {
                return false;
            }
            return true;
        }
    };
    struct Scanner2
    {
        const int cnt;
        std::vector<table_params::value_type> &vec;
        std::vector<uint64_t> &k_vec;

        Scanner2(int cnt, std::vector<uint64_t> &k, std::vector<table_params::value_type> &v)
            : cnt(cnt), vec(v), k_vec(k)
        {
            vec.reserve(cnt);
            k_vec.reserve(cnt);
        }

        template <typename SS, typename K>
        void visit_leaf(const SS &, const K &, threadinfo &) {}

        bool visit_value(Str key, table_params::value_type val, threadinfo &)
        {
            // TODO: skip the val which points to hybrid index block
            uint64_t kint = *(uint64_t *)key.data();
            vec.push_back(val);
            k_vec.emplace_back(kint);
            if (vec.size() == cnt)
            {
                return false;
            }
            return true;
        }
    };

    struct Scanner3
    {
        Str end;
        std::vector<table_params::value_type> &vec;
        std::vector<uint64_t> &k_vec;

        Scanner3(Str end_key, std::vector<uint64_t> &k, std::vector<table_params::value_type> &v)
            : end(end_key), vec(v), k_vec(k)
        {
        }

        template <typename SS, typename K>
        void visit_leaf(const SS &, const K &, threadinfo &) {}

        bool visit_value(Str key, table_params::value_type val, threadinfo &)
        {
            // TODO: skip the val which points to hybrid index block
            uint64_t kint = *(uint64_t *)key.data();
            if (key > end)
            {
                return false;
            }
            vec.push_back(val);
            k_vec.emplace_back(kint);
            return true;
        }
    };

    // static thread_local typename table_params::threadinfo_type *ti;
    static thread_local int thread_id;
    typename table_params::threadinfo_type *tis[65];

    MasstreeWrapper()
    {
        for (int i = 0; i < 65; i++)
        {
            tis[i] = nullptr;
        }
        this->table_init();
    }

    ~MasstreeWrapper()
    {
        // printf("before free ti\n");
        // print_dram_consuption();
        for (int i = 0; i < 65; i++)
        {
            if (tis[i] != nullptr)
            {
                threadinfo::free_ti(tis[i]);
            }
        }
        // print_dram_consuption();
        // printf("after free ti\n");
    }

    void table_init()
    {

        if (tis[0] == nullptr)
        {
            tis[0] = threadinfo::make(threadinfo::TI_MAIN, -1);
        }
        table_params::threadinfo_type *ti = tis[0];
        table_.initialize(*ti);
        key_gen_ = 0;
    }

    void keygen_reset()
    {
        key_gen_ = 0;
    }

    static void thread_init(int tid)
    {
        assert(tid > 0 && tid <= 64);
        thread_id = tid;
    }

    inline table_params::threadinfo_type *get_ti()
    {
        assert(thread_id >= 0 && thread_id <= 64);
        if (unlikely(tis[thread_id] == nullptr))
        {
            tis[thread_id] = threadinfo::make(threadinfo::TI_PROCESS, thread_id);
        }
        return tis[thread_id];
    }

    void insert(uint64_t int_key, ValueHelper &le_helper)
    {
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
        cursor_type lp(table_, key);
        table_params::threadinfo_type *ti = get_ti();
        bool found = lp.find_insert(*ti);
        if (found)
        {
            le_helper.old_val = lp.value();
        }
        lp.value() = le_helper.new_val;
        fence();
        lp.finish(1, *ti);
    }

    void insert_validate(uint64_t int_key, ValueHelper &le_helper)
    {
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
        cursor_type lp(table_, key);
        table_params::threadinfo_type *ti = get_ti();
        bool found = lp.find_insert(*ti);
        if (unlikely(found))
        {
            // le_helper.old_val = lp.value();
            uint64_t new_lsn = ((ValuePtr*)(&le_helper.new_val))->detail_.lsn;
            uint64_t old_lsn = ((ValuePtr*)(&lp.value()))->detail_.lsn;
            if (new_lsn >= old_lsn)
                lp.value() = le_helper.new_val;
        }
        else
        {
            lp.value() = le_helper.new_val;
        }
        fence();
        lp.finish(1, *ti);
    }

    bool search(uint64_t int_key, uint64_t &value)
    {
        table_params::threadinfo_type *ti = get_ti();
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
        bool found = table_.get(key, value, *ti);
        return found;
    }

    void scan(uint64_t int_key, int cnt, std::vector<uint64_t> &vec)
    {
        table_params::threadinfo_type *ti = get_ti();
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
        Scanner scanner(cnt, vec);
        table_.scan(key, true, scanner, *ti);
    }

    void scan(uint64_t int_key, int cnt, std::vector<uint64_t> &kvec, std::vector<uint64_t> &vvec)
    {
        table_params::threadinfo_type *ti = get_ti();
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
        Scanner2 scanner(cnt, kvec, vvec);
        table_.scan(key, true, scanner, *ti);
    }

    void scan(uint64_t start, uint64_t end, std::vector<uint64_t> &kvec, std::vector<uint64_t> &vvec)
    {
        table_params::threadinfo_type *ti = get_ti();
        uint64_t start_buf, end_buf;
        Str start_str = make_key(start, start_buf);
        Str end_str = make_key(end, end_buf);
        Scanner3 scanner(end_str, kvec, vvec);
        table_.scan(start_str, true, scanner, *ti);
    }

    bool remove(uint64_t int_key)
    {
        table_params::threadinfo_type *ti = get_ti();
        uint64_t key_buf;
        Str key = make_key(int_key, key_buf);
        cursor_type lp(table_, key);
        bool found = lp.find_locked(*ti);
        lp.finish(-1, *ti);
        return true;
    }

private:
    table_type table_;
    uint64_t key_gen_;
    static bool stopping;
    static uint32_t printing;

    static inline Str make_key(uint64_t int_key, uint64_t &key_buf)
    {
        // key_buf = __builtin_bswap64(int_key);
        key_buf = int_key;
        return Str((const char *)&key_buf, sizeof(key_buf));
    }
};
