#include <cstdio>
#include <iostream>
#include <unistd.h>
#include <string>
#include <sstream>
#include <array>
#include <regex>
#include <filesystem>
#include <gflags/gflags.h>

#include "db.h"
#include "util/stopwatch.hpp"
#include "util/kgen.h"

DEFINE_uint64(num, 200000000, "Total number of data");
DEFINE_uint64(num_ops, 100000000, "Number of operations for each benchmark");
DEFINE_string(benchmarks, "read", "write: random update, read: random get");
DEFINE_uint64(threads, 1, "Number of user threads during loading and benchmarking");
DEFINE_uint64(value_size, 8, "value size, only available with KV separation enabled");
DEFINE_string(pool_path, "/mnt/pmem/fluidkv", "Directory of target pmem");
DEFINE_uint64(pool_size_GB, 40, "Total size of pmem pool");
DEFINE_bool(recover, false, "Recover an existing db instead of recreating a new one");
DEFINE_bool(skip_load, false, "Not load data");

void print_dram_consuption()
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
            printf("DRAM consumption: %.3f GB.\n", stof(mem_ocp) / 1024 / 1024);
            break;
        }
    }
}

char value[1024] = "valuexxxxxx";

void put_thread(DB *db, size_t start, size_t count)
{
    size_t keybuf;
    Slice k(&keybuf);
    Slice v;
#ifdef KV_SEPARATE
    v = Slice(value, FLAGS_value_size);
#else
    v = Slice(value, 8);
#endif
    std::unique_ptr<DBClient>
        c = db->GetClient();
    for (size_t i = start; i < start + count; i++)
    {
        size_t key = utils::multiplicative_hash<uint64_t>(i + 1);
        keybuf = __builtin_bswap64(key);
        auto success = c->Put(k, v);

        if (!success)
        {
            ERROR_EXIT("insert after flush error");
        }
        if ((i != start) && ((i - start) % 5000000 == 0))
        {
            printf("thread %d, %lu operations finished\n", c->thread_id_, i - start);
        }
    }
    c.reset();
}
void get_thread(DB *db, size_t start, size_t count)
{
    size_t keybuf;
    Slice k(&keybuf);
    std::unique_ptr<DBClient> c = db->GetClient();
    char vbuf[1024];
    Slice valueout(vbuf);
    for (size_t i = start; i < start + count; i++)
    {
        size_t key = utils::multiplicative_hash<uint64_t>(i + 1);
        keybuf = __builtin_bswap64(key);
        auto success = c->Get(k, valueout);

        if (!success)
        {
            ERROR_EXIT("read error, %lu", i - start);
        }
        if ((i != start) && ((i - start) % 5000000 == 0))
        {
            printf("thread %d, %lu operations finished\n", c->thread_id_, i - start);
        }
    }
    c.reset();
}

int main(int argc, char **argv)
{
    google::SetUsageMessage("FluidKV benchmarks");
    google::ParseCommandLineFlags(&argc, &argv, true);
#ifdef BUFFER_WAL_MEMTABLE
    std::cout << "KV separation is disabled" << std::endl;
#else
#ifdef KV_SEPARATE
    std::cout << "KV separation is enabled" << std::endl;
#endif
#endif
    if (FLAGS_num == 0)
    {
        std::fprintf(stderr, "Invalid flag 'num=%lu'\n", FLAGS_num);
        std::exit(1);
    }
    if (FLAGS_num_ops == 0 || FLAGS_num_ops > FLAGS_num)
    {
        std::fprintf(stderr, "Invalid flag 'num_ops=%lu'\n", FLAGS_num_ops);
        std::exit(1);
    }

    std::stringstream benchmark_stream(FLAGS_benchmarks);
    std::string name;
    std::vector<int> benchmarks;
    while (std::getline(benchmark_stream, name, ','))
    {
        if (name == "write")
        {
            benchmarks.push_back(0);
        }
        else if (name == "read")
        {
            benchmarks.push_back(1);
        }
        else if (!name.empty())
        { // No error message for empty name
            fprintf(stderr, "unknown benchmark '%s'\n", name.c_str());
            std::exit(1);
        }
    }
    if (!std::filesystem::exists(FLAGS_pool_path))
    {
        fprintf(stderr, "pool path '%s' not exists\n", FLAGS_pool_path.c_str());
        std::exit(1);
    }
    if (FLAGS_recover && !FLAGS_skip_load)
    {
        printf("[Warning] Recover is enabled but skip_load is disabled. This will result in full dataset updating\n");
    }
    if (!FLAGS_recover && FLAGS_skip_load)
    {
        printf("[Warning] Recover is disabled but skip_load is enabled. This will result in all reads failing\n");
    }
    // init
    stopwatch_t sw;
    DBConfig cfg;
    cfg.pm_pool_path = FLAGS_pool_path;
    cfg.pm_pool_size = FLAGS_pool_size_GB << 30ul;
    cfg.recover = FLAGS_recover;
    // if (!FLAGS_recover)
    // {
    //     auto ok = std::filesystem::remove(FLAGS_pool_path+"/*");
    //     // ok = std::filesystem::create_directory(FLAGS_pool_path);
    // }

    sw.start();
    DB *db = new DB(cfg);
    auto elapsed = sw.elapsed<std::chrono::milliseconds>();
    std::cout << "initialize or recover time:" << elapsed << "ms" << std::endl;
    uniform_key_generator_t keygen(FLAGS_num, 8);

    std::vector<std::thread> tlist;

    // load
    if (!FLAGS_skip_load)
    {
        sw.start();
        for (int i = 0; i < FLAGS_threads; i++)
        {
            tlist.emplace_back(std::thread(put_thread, db, FLAGS_num / FLAGS_threads * i, FLAGS_num / FLAGS_threads));
        }
        for (auto &th : tlist)
        {
            th.join();
        }
        auto us = sw.elapsed<std::chrono::microseconds>();
        std::cout << "***************\ncount=" << FLAGS_num << "thpt=" << FLAGS_num / us << "MOPS, total time:" << us / 1000000 << "s\n*****************" << std::endl;
        tlist.clear();
    }
    print_dram_consuption();
    printf("wait for unfinished flush and compaction....\n");
    db->EnableReadOnlyMode();
    db->EnableReadOptimizedMode();
    db->WaitForFlushAndCompaction();
    print_dram_consuption();
    // run benckmark
    for (auto &bench : benchmarks)
    {
        std::cout << "run benchmark " << (bench ? "read" : "write") << std::endl;

        sw.start();
        for (int i = 0; i < FLAGS_threads; i++)
        {
            if (bench)
            {
                tlist.emplace_back(std::thread(get_thread, db, FLAGS_num_ops / FLAGS_threads * i, FLAGS_num_ops / FLAGS_threads));
            }
            else
            {
                tlist.emplace_back(std::thread(put_thread, db, FLAGS_num_ops / FLAGS_threads * i, FLAGS_num_ops / FLAGS_threads));
            }
        }
        for (auto &th : tlist)
        {
            th.join();
        }
        auto us = sw.elapsed<std::chrono::microseconds>();
        std::cout << "********************\ncount=" << FLAGS_num_ops << " thpt=" << FLAGS_num_ops / us << "MOPS, total time:" << us / 1000000 << "s\n********************" << std::endl;
        tlist.clear();
        db->WaitForFlushAndCompaction();
    }
    delete db;
    return 0;
}