# FluidKV
A multi-stage key-value store for high performance and memory efficiency on persistent memory

## Directory contents
* `include/db.h`: FluidKV interface
* `include/config.h`: static (macro defination) and dynamic (parameters) configurations.
* `db/`: implementation of FluidKV
* `db/allocator/`: the coarse-grained PM allocator
* `db/blocks/`: structures of index block and data block
* `db/compaction/`: implementation of flush and compaction
* `lib/`: code we modified from external libraries, mainly including [Masstree](https://github.com/kohler/masstree-beta) and [RocksDB](https://github.com/facebook/rocksdb) thread pool.
* `util/`: some utilities

## Terminology Correspondence between our paper and codes
| In the paper               | In the code                                |
| -------------------------- | ------------------------------------------ |
| Chunk                      | Segment                                    |
| Logical sorted table (LST) | Persistent sorted table (PST)              |
| FastStore                  | Memtable                                   |
| Manifest                   | Version (volatile) + Manifest (persistent) |
| BufferStore                | Level 0 (L0)                               |
| Buffer-tree                | Level 0 tree                               |
| StableStore                | Level 1 (L1)                               |

## Building

Dependencies:
- CMake
- [gflags](https://github.com/gflags/gflags) 
- [PMDK](https://github.com/pmem/pmdk) (libpmem) 

FluidKV is based on persistent memory. So a configured PM path is necessary. 
If you have a configured PM path, skip this step.
```shell
# set Optane DCPMM to AppDirect mode
$ sudo ipmctl create -f -goal persistentmemorytype=appdirect

# configure PM device to fsdax mode
$ sudo ndctl create-namespace -m fsdax

# create and mount a file system with DAX
$ sudo mkfs.ext4 -f /dev/pmem0
$ sudo mount -o dax /dev/pmem0 /mnt/pmem
```

Build FluidKV with CMake:
```shell
$ cmake -B build # use -DKV_SEPARATION=ON to enable key-value separation for supporting variable-sized value
$ cmake --build build -j${n_proc}
```

A static library (./build/libfluidkv.a) and a benchmarking tool (./build/benchmarks/benchmark) will be generated.

## Running benchmark

```
./build/benchmarks/benchmark
```
Parameters:
```
-benchmarks (write: random update, read: random get) type: string
      default: "read"
-num (total number of data) type: uint64 default: 200000000
-num_ops (number of operations for each benchmark) type: uint64
      default: 100000000
-pool_path (directory of target pmem) type: string
      default: "/mnt/pmem/fluidkv"
-pool_size_GB (total size of pmem pool) type: uint64 default: 40
-recover (recover an existing db instead of recreating a new one)
      type: bool default: false
-skip_load (skip the load data step) type: bool default: false
-threads (number of user threads during loading and benchmarking)
      type: uint64 default: 1
-value_size (value size, only available with KV separation enabled) 
      type: uint64 default: 8
```

## For comparisons with baselines
We did macro-benchmarks and comparison experiments with [PKBench](https://github.com/luziyi23/PKBench) which is our modified version of [PiBench](https://github.com/sfu-dis/pibench) for PM-based key-value stores. Please see PKBench repository for more in-depth benchmarking.
