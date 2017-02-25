//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_executor.h
//
// Identification: src/include/executor/hash_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <unordered_map>
#include <unordered_set>

#include "type/types.h"
#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"
#include "common/container_tuple.h"
#include "storage/data_table.h"

namespace peloton {
namespace executor {


class PopulateIndexExecutor : public AbstractExecutor {
 public:
  PopulateIndexExecutor(const PopulateIndexExecutor &) = delete;
  PopulateIndexExecutor &operator=(const PopulateIndexExecutor &) = delete;
  PopulateIndexExecutor(const PopulateIndexExecutor &&) = delete;
  PopulateIndexExecutor &operator=(const PopulateIndexExecutor &&) = delete;

  explicit PopulateIndexExecutor(const planner::AbstractPlan *node,
                        ExecutorContext *executor_context);
  inline storage::DataTable *GetTable() const { return target_table_; }

 protected:
  bool DInit();

  bool DExecute();

 private:
  /** @brief Input tiles from child node */
  std::vector<std::unique_ptr<LogicalTile>> child_tiles_;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief Pointer to table to scan from. */
  storage::DataTable *target_table_ = nullptr;
  std::vector<oid_t> column_ids_;
  bool done_ = false;
};

} /* namespace executor */
} /* namespace peloton */