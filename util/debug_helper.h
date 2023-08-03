#pragma once

#include <cstdio>
#include <cstdlib>

// #define LOGGING
// #define DEBUGGING
#define SHOWINFO

#ifdef SHOWINFO
#define INFO(fmt, ...) fprintf(stdout, "\033[1;32m[INFO]\033[0m" fmt "\n", ##__VA_ARGS__)
#else
#define INFO(fmt, ...)
#endif

#ifdef LOGGING
#define LOG(fmt, ...)                                                          \
  fprintf(stderr, "\033[1;32mLOG(%s:%d %s): \033[0m" fmt "\n", __FILE__,     \
          __LINE__, __func__, ##__VA_ARGS__)
#else
#define LOG(fmt, ...)
#endif

#ifdef DEBUGGING
#define DEBUG(fmt, ...)                                                      \
    fprintf(stderr, "\033[1;33mDEBUG(%s:%d %s): \033[0m" fmt "\n", __FILE__, \
            __LINE__, __func__, ##__VA_ARGS__)
#else
#define DEBUG(fmt, ...)
#endif

#define DEBUG2(fmt, ...)                                                      \
    fprintf(stderr, "\033[1;33mDEBUG2(%s:%d %s): \033[0m" fmt "\n", __FILE__, \
            __LINE__, __func__, ##__VA_ARGS__)

#define ERROR_EXIT(fmt, ...)                                                     \
    do                                                                           \
    {                                                                            \
        fprintf(stderr, "\033[1;31mError(%s:%d %s): \033[0m" fmt "\n", __FILE__, \
                __LINE__, __func__, ##__VA_ARGS__);                              \
        abort();                                                                 \
    } while (0)
