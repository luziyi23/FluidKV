project(hot)


set(TC_MALLOC_NAMES tcmalloc tcmalloc_minimal libtcmalloc_minimal.so.4.3.0 )
# SET(TBB_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/tbb")
# include(${TBB_ROOT}/cmake/TBBBuild.cmake)
# tbb_build(TBB_ROOT ${TBB_ROOT} CONFIG_DIR TBB_DIR MAKE_ARGS)
# find_package(TBB REQUIRED)

find_library(TC_MALLOC_LIBRARY
	NAMES ${TC_MALLOC_NAMES}
	PATHS /usr/lib/x86_64-linux-gnu /usr/lib64 /lib /usr/lib /usr/local/lib /opt/local/lib
	NO_DEFAULT_PATH
)

find_library(JemallocLib jemalloc)
find_library(TbbLib tbb)
find_package(Threads)

aux_source_directory(${PROJECT_SOURCE_DIR}/idx/contenthelpers contenthelpers-HEADERS)
add_library(content-helpers-lib INTERFACE)
target_sources(content-helpers-lib INTERFACE ${contenthelpers-HEADERS})
target_include_directories(content-helpers-lib INTERFACE "${PROJECT_SOURCE_DIR}/idx/contenthelpers")



aux_source_directory(${PROJECT_SOURCE_DIR}/hot/commons hot-commons-lib-HEADERS)
add_library(hot-commons-lib INTERFACE)
target_sources(hot-commons-lib INTERFACE ${hot-commons-lib-HEADERS})
target_include_directories(hot-commons-lib INTERFACE "${PROJECT_SOURCE_DIR}/hot/commons")
target_link_libraries(hot-commons-lib INTERFACE content-helpers-lib)


aux_source_directory(${PROJECT_SOURCE_DIR}/hot/rowex hot-rowex-lib-HEADERS)
add_library(hot-rowex-lib INTERFACE)
target_sources(hot-rowex-lib INTERFACE ${hot-rowex-lib-HEADERS})
target_include_directories(hot-rowex-lib INTERFACE "${PROJECT_SOURCE_DIR}/hot/rowex")
target_link_libraries(hot-rowex-lib INTERFACE content-helpers-lib hot-commons-lib pthread ${TC_MALLOC_LIBRARY} ${Threads} ${TbbLib})


