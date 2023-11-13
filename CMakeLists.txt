cmake_minimum_required(VERSION 3.14)
project(fluidkv)
# SET(CMAKE_C_COMPILER "/usr/local/bin/gcc")
# SET(CMAKE_CXX_COMPILER "/usr/local/bin/g++")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

option(KV_SEPARATION "Enabling KV seaparation to support variable-sized value but decreasing read performance slightly" OFF)


# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fstack-protector-all -fsanitize=address -mssse3 -mavx -mavx2 -mbmi2 -mlzcnt -mbmi -Wno-narrowing")
# set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -g -fstack-protector-all -fsanitize=address -mssse3 -mavx -mavx2 -mbmi2 -mlzcnt -mbmi -Wno-narrowing")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g  -mssse3 -mavx -mavx2 -mbmi2 -mlzcnt -mbmi -Wno-narrowing")
# set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -g -mssse3 -mavx -mavx2 -mbmi2 -mlzcnt -mbmi -Wno-narrowing")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -march=native -mssse3 -mavx -mavx2 -mbmi2 -mlzcnt -mbmi -Wno-narrowing -Wno-error=register")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -march=native -mssse3 -mavx -mavx2 -mbmi2 -mlzcnt -mbmi -Wno-narrowing -Wno-error=register")


# third-party: thread pool from rocksdb
add_library(ThreadPool SHARED IMPORTED)
set_target_properties(ThreadPool 
PROPERTIES IMPORTED_LOCATION
${PROJECT_SOURCE_DIR}/lib/ThreadPool/libthreadpool.so)
# third-party: masstree
aux_source_directory(${PROJECT_SOURCE_DIR}/lib/masstree MASSTREE_SOURCE_FILES)
add_library(masstree ${MASSTREE_SOURCE_FILES})
target_compile_options(masstree PRIVATE
          $<$<COMPILE_LANGUAGE:CXX>:-include ${PROJECT_SOURCE_DIR}/lib/masstree/config.h>)

# FluidKV files
aux_source_directory(${PROJECT_SOURCE_DIR}/db DB_FILES)
aux_source_directory(${PROJECT_SOURCE_DIR}/db/compaction DB_FILES)
include_directories(${PROJECT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/include)



add_library(fluidkv ${DB_FILES})
target_link_libraries(fluidkv pmem pthread masstree ThreadPool)
if(KV_SEPARATION)
    message(STATUS "KV_SEPARATION is ON")
    add_definitions(-DINDEX_LOG_MEMTABLE)
else()
    message(STATUS "KV_SEPARATION is OFF")
    add_definitions(-DBUFFER_WAL_MEMTABLE)
endif()


# third-party: Some alternative volatile index types are now deprecated
# aux_source_directory(${PROJECT_SOURCE_DIR}/lib/BwTree BWTREE_SOURCE_FILES)
# add_library(bwtree ${BWTREE_SOURCE_FILES})
# target_link_libraries(bwtree atomic pthread)
# include_directories(${PROJECT_SOURCE_DIR}/lib/HOT)
# add_subdirectory(lib/HOT)
# add_subdirectory(lib/TLBtree-master/Concurrent)

add_subdirectory(benchmarks)
add_subdirectory(test)
