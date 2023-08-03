#pragma once
#include <cstdint>

/**
 * @brief 64bit only-read log sequence number
 *
 */
struct LSN
{
    uint64_t epoch : 16;
    uint64_t lsn : 30;
    uint64_t padding : 18;
};
/**
 * @brief log entry with fixed key size
 * 32Bytes or 64Bytes
 *
 */
enum OpCode : uint16_t
{
    NOP,
    INSERT,
    DELETE
};
enum LogType : uint16_t
{
    ERROR,
    LogEntryFixK32,
    LogEntryFixK64,
    LogEntryFixK128,
    LogEntryFixK256,
    LogEntryVarK64,
    LogEntryVarK128,
    LogEntryVarK256
};

// TODO: 32Byte entry make 2x space consuption for 16Byte KV. Try to make LogEntry in 20Bytes so each cacheline contain 3 entry, 4/3x space.
// TODO: or make pure log entry only with kv-size(vsize=0 denotes delete) for L1 or L2 with larger capacity.
struct LogEntry32
{

    /* data */
    uint32_t valid : 1;
    uint32_t lsn : 31;

    uint16_t key_sz = 0;
    uint16_t value_sz = 0; // 0~16:32Bytes log entry, 17~48:64Bytes log entry, >48:var entry
    uint64_t key = 0;
    union
    {
        uint64_t value_addr = 0;
        char value[16]; // may expanded with ~32 bytes suffix of value, depended on value_sz
    };
};
// static constexpr size_t size = sizeof(LogEntry32);
struct LogEntry64
{
    /* data */

    uint32_t valid : 1;
    uint32_t lsn : 31;
    uint16_t key_sz = 0;
    uint16_t value_sz = 0;
    uint64_t key = 0;
    char value[48]; // upto 32 bytes value or value_ptr
};
// static constexpr size_t size = sizeof(LogEntry64);
struct LogEntryVar64
{
    /* data */

    uint32_t valid : 1;
    uint32_t lsn : 31;
    uint16_t key_sz;
    uint16_t value_sz;
    uint64_t key;
    char value[];
};

// static constexpr size_t size=sizeof(LogEntryVar64);

// struct LogEntryVar128
// {
//     /* data */
//     uint64_t lsn;
//     uint16_t opcode;
//     uint16_t key_sz;
//     uint16_t value_sz;
//     uint16_t reserved = 0;
//     char data[112];
// };