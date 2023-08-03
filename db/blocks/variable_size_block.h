#pragma once
#include "db_common.h"
#include "util/aligned_buffer.h"

struct PDataBlock512VarWrapper
{
    static constexpr uint64_t MAX_SIZE = 512;
    char buf_[512];
    char *const start_;
    char *current_;
    PDataBlock512VarWrapper() : start_(buf_), current_(start_){};
};
