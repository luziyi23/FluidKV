#include "masstree_wrapper.h"

// thread_local typename MasstreeWrapper::table_params::threadinfo_type* MasstreeWrapper::ti = nullptr;
thread_local int MasstreeWrapper::thread_id = 0;
bool MasstreeWrapper::stopping = false;
uint32_t MasstreeWrapper::printing = 0;
kvtimestamp_t initial_timestamp;

volatile mrcu_epoch_type active_epoch = 1;
volatile uint64_t globalepoch = 1;
volatile bool recovering = false;
