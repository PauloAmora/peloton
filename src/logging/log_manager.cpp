//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// log_manager.cpp
//
// Identification: src/backend/logging/loggers/log_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "logging/durability_factory.h"
#include "logging/log_manager.h"
#include "storage/tile_group_header.h"

namespace peloton {
namespace logging {

  thread_local WorkerContext* tl_worker_ctx = nullptr;

  void LogManager::StartTxn(concurrency::TransactionContext *txn) {
    PL_ASSERT(tl_worker_ctx);
    size_t txn_eid = txn->GetEpochId();

    PL_ASSERT(tl_worker_ctx->current_commit_eid == MAX_EID || tl_worker_ctx->current_commit_eid <= txn_eid);

    // Handle the epoch id
    if (tl_worker_ctx->current_commit_eid == INVALID_EID
      || tl_worker_ctx->current_commit_eid != txn_eid) {
      // if this is a new epoch, then write to a new buffer
      tl_worker_ctx->current_commit_eid = txn_eid;
      RegisterNewBufferToEpoch(std::move(tl_worker_ctx->buffer_pool.GetBuffer(txn_eid)));
    }

    // Handle the commit id
    cid_t txn_cid = txn->GetCommitId();
    tl_worker_ctx->current_cid = txn_cid;
  }

  void LogManager::FinishPendingTxn() {
    PL_ASSERT(tl_worker_ctx);
  }

  void LogManager::MarkTupleCommitEpochId(storage::TileGroupHeader *tg_header UNUSED_ATTRIBUTE, oid_t tuple_slot UNUSED_ATTRIBUTE) {
    if (DurabilityFactory::GetLoggingType() == LOGGING_TYPE_INVALID) {
      return;
    }

    PL_ASSERT(tl_worker_ctx != nullptr);
    PL_ASSERT(tl_worker_ctx->current_commit_eid != MAX_EID && tl_worker_ctx->current_commit_eid != INVALID_EID);
//    tg_header->SetLoggingCommitEpochId(tuple_slot, tl_worker_ctx->current_commit_eid);
  }

}
}
