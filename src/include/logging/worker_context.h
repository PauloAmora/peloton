//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// worker_context.h
//
// Identification: src/backend/logging/worker_context.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <thread>
#include <list>
#include <stack>
#include <map>

#include "libcuckoo/cuckoohash_map.hh"
#include "concurrency/transaction_context.h"
#include "concurrency/epoch_manager.h"
#include "logging/log_buffer.h"
#include "logging/log_record.h"
#include "logging/log_buffer_pool.h"
#include "logging/log_manager.h"
#include "common/internal_types.h"
#include "type/serializer.h"
#include "container/lock_free_queue.h"
#include "common/logger.h"
#include "common/timer.h"

#define INVALID_TXN_WORKER_ID std::numeric_limits<size_t >::max()

namespace peloton {
  namespace concurrency {
    extern thread_local size_t tl_txn_worker_id;
  }

namespace logging {

  
  // the worker context is constructed when registering the worker to the logger.
  struct WorkerContext {

    WorkerContext(oid_t id)
      : per_epoch_buffer_ptrs(40960),
        buffer_pool(id), 
        output_buffer(),
        current_commit_eid(MAX_EID),
        persist_eid(0),
        reported_eid(INVALID_EID),
        current_cid(INVALID_CID), 
        worker_id(id),
        transaction_worker_id(INVALID_TXN_WORKER_ID),
        per_epoch_dependencies(40960) {
      LOG_TRACE("Create worker %d", (int) worker_id);
     // PL_ASSERT(concurrency::tl_txn_worker_id != INVALID_TXN_WORKER_ID);
      transaction_worker_id = 0;
    }

    ~WorkerContext() {
      LOG_TRACE("Destroy worker %d", (int) worker_id);
    }


    // Every epoch has a buffer stack
    // TODO: Remove this, workers should push the buffer to the logger. -- Jiexi
    std::vector<std::stack<std::unique_ptr<LogBuffer>>> per_epoch_buffer_ptrs;

    // each worker thread has a buffer pool. each buffer pool contains 16 log buffers.
    LogBufferPool buffer_pool;
    // serialize each tuple to string.
    CopySerializeOutput output_buffer;

    // current epoch id
    size_t current_commit_eid;
    // persisted epoch id
    // TODO: Move this to logger -- Jiexi
    size_t persist_eid;
    // reported epoch id
    size_t reported_eid;

    // current transaction id
    cid_t current_cid;

    // worker thread id
    oid_t worker_id;
    // transaction worker id from the epoch manager's point of view
    size_t transaction_worker_id;

    /* Statistics */

    // XXX: Simulation of early lock release
    uint64_t cur_txn_start_time;
    std::map<size_t, std::vector<uint64_t>> pending_txn_timers;

    // Note: Only used by dep log manager
    // Per epoch dependency graph
    // TODO: Remove this, workers should push the dependencies along with the buffer to the logger. -- Jiexi
    std::vector<std::unordered_set<size_t>> per_epoch_dependencies;
  };

}
}
