add_library(wotree "wotree256.cc")

add_library(tlbtree "tlbtree_impl.cc")

set(TLBtree
    wotree
    tlbtree
    CACHE STRING "TLBTREE" FORCE
)