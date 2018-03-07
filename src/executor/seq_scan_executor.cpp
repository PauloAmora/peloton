//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_executor.cpp
//
// Identification: src/executor/seq_scan_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/seq_scan_executor.h"

#include "common/internal_types.h"
#include "type/value_factory.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "executor/executor_context.h"
#include "expression/abstract_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/comparison_expression.h"
#include "common/container_tuple.h"
#include "planner/create_plan.h"
#include "storage/data_table.h"
#include "storage/tile_group_header.h"
#include "storage/tile.h"
#include "concurrency/transaction_manager_factory.h"
#include "common/logger.h"
#include "storage/zone_map_manager.h"
#include "expression/expression_util.h"
#include "eviction/evicter.h"

namespace peloton {
namespace executor {

int SeqScanExecutor::hot_access;

/**
 * @brief Constructor for seqscan executor.
 * @param node Seqscan node corresponding to this executor.
 */
SeqScanExecutor::SeqScanExecutor(const planner::AbstractPlan *node,
                                 ExecutorContext *executor_context)
    : AbstractScanExecutor(node, executor_context) {}

/**
 * @brief Let base class DInit() first, then do mine.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DInit() {
  auto status = AbstractScanExecutor::DInit();

  if (!status) return false;

  // Grab data from plan node.
  const planner::SeqScanPlan &node = GetPlanNode<planner::SeqScanPlan>();

  target_table_ = node.GetTable();

  current_tile_group_offset_ = START_OID;

  old_predicate_ = predicate_;

  if (target_table_ != nullptr) {
    table_tile_group_count_ = target_table_->GetTileGroupCount();

    if (column_ids_.empty()) {
      column_ids_.resize(target_table_->GetSchema()->GetColumnCount());
      std::iota(column_ids_.begin(), column_ids_.end(), 0);
    }
  }

  return true;
}

/**
 * @brief Creates logical tile from tile group and applies scan predicate.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DExecute() {
  // Scanning over a logical tile.
  if (children_.size() == 1 &&
      // There will be a child node on the create index scenario,
      // but we don't want to use this execution flow
      !(GetRawNode()->GetChildren().size() > 0 &&
        GetRawNode()->GetChildren()[0].get()->GetPlanNodeType() ==
            PlanNodeType::CREATE &&
        ((planner::CreatePlan *)GetRawNode()->GetChildren()[0].get())
                ->GetCreateType() == CreateType::INDEX)) {
    // FIXME Check all requirements for children_.size() == 0 case.
    LOG_TRACE("Seq Scan executor :: 1 child ");

    PL_ASSERT(target_table_ == nullptr);
    PL_ASSERT(column_ids_.size() == 0);

    while (children_[0]->Execute()) {
      std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());

      if (predicate_ != nullptr) {
        // Invalidate tuples that don't satisfy the predicate.
        for (oid_t tuple_id : *tile) {
          ContainerTuple<LogicalTile> tuple(tile.get(), tuple_id);
          auto eval = predicate_->Evaluate(&tuple, nullptr, executor_context_);
          if (eval.IsFalse()) {
            // if (predicate_->Evaluate(&tuple, nullptr, executor_context_)
            //        .IsFalse()) {
            tile->RemoveVisibility(tuple_id);
          }
        }
      }

      if (0 == tile->GetTupleCount()) {  // Avoid returning empty tiles
        continue;
      }

      /* Hopefully we needn't do projections here */
      SetOutput(tile.release());
      return true;
    }
    return false;
  }
  // Scanning a table
  else if (children_.size() == 0 ||
           // If we are creating an index, there will be a child
           (children_.size() == 1 &&
            // This check is only needed to pass seq_scan_test
            // unless it is possible to add a executor child
            // without a corresponding plan.
            GetRawNode()->GetChildren().size() > 0 &&
            // Check if the plan is what we actually expect.
            GetRawNode()->GetChildren()[0].get()->GetPlanNodeType() ==
                PlanNodeType::CREATE &&
            // If it is, confirm it is for indexes
            ((planner::CreatePlan *)GetRawNode()->GetChildren()[0].get())
                    ->GetCreateType() == CreateType::INDEX)) {
    LOG_TRACE("Seq Scan executor :: 0 child ");

    PL_ASSERT(target_table_ != nullptr);
    PL_ASSERT(column_ids_.size() > 0);
    if (children_.size() > 0 && !index_done_) {
      children_[0]->Execute();
      // This stops continuous executions due to
      // a parent and avoids multiple creations
      // of the same index.
      index_done_ = true;
    }
    
    concurrency::TransactionManager &transaction_manager =
           concurrency::TransactionManagerFactory::GetInstance();

       bool acquire_owner = GetPlanNode<planner::AbstractScan>().IsForUpdate();
       auto current_txn = executor_context_->GetTransaction();

       // Retrieve next tile group.
       while (current_tile_group_offset_ < table_tile_group_count_) {
         auto tile_group =
             target_table_->GetTileGroup(current_tile_group_offset_++);
         auto tile_group_header = tile_group->GetHeader();

         oid_t active_tuple_count = tile_group->GetNextTupleSlot();

         // Construct position list by looping through tile group
         // and applying the predicate.
         std::vector<oid_t> position_list;
         for (oid_t tuple_id = 0; tuple_id < active_tuple_count; tuple_id++) {
           ItemPointer location(tile_group->GetTileGroupId(), tuple_id);

           auto visibility = transaction_manager.IsVisible(
               current_txn, tile_group_header, tuple_id);

           // check transaction visibility
           if (visibility == VisibilityType::OK) {
             // if the tuple is visible, then perform predicate evaluation.
             if (predicate_ == nullptr) {
               position_list.push_back(tuple_id);
               auto res = transaction_manager.PerformRead(current_txn, location,
                                                          acquire_owner);
               if (!res) {
                 transaction_manager.SetTransactionResult(current_txn,
                                                          ResultType::FAILURE);
                 return res;
               }
             } else {
               ContainerTuple<storage::TileGroup> tuple(tile_group.get(),
                                                        tuple_id);
               LOG_TRACE("Evaluate predicate for a tuple");
               auto eval =
                   predicate_->Evaluate(&tuple, nullptr, executor_context_);
               LOG_TRACE("Evaluation result: %s", eval.GetInfo().c_str());
               if (eval.IsTrue()) {
                 position_list.push_back(tuple_id);
                 auto res = transaction_manager.PerformRead(current_txn, location,
                                                            acquire_owner);
                 if (!res) {
                   transaction_manager.SetTransactionResult(current_txn,
                                                            ResultType::FAILURE);
                   return res;
                 } else {
                   LOG_TRACE("Sequential Scan Predicate Satisfied");
                 }
               }
             }
           }
         }

         // Don't return empty tiles
         if (position_list.size() == 0) {
           continue;
         }

         // Construct logical tile.
         std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
         logical_tile->AddColumns(tile_group, column_ids_);
         logical_tile->AddPositionList(std::move(position_list));
         query_answered_ = true;
         LOG_TRACE("Information %s", logical_tile->GetInfo().c_str());
//                   LOG_DEBUG("Query answered from hot data");
         SeqScanExecutor::hot_access++;
         SetOutput(logical_tile.release());
         return true;
       }
     }
  if(!query_answered_ && predicate_ != nullptr){
      auto t =  executor_context_->GetParamValues();
     // LOG_DEBUG("%d", t);
      auto map = target_table_->GetFilterMap().find(0)->second;
      auto status = map->Contain(t[0].GetAs<int>());
      if(status == cuckoofilter::Status::NotFound)
      {
         LOG_DEBUG("Skipped tile group");
         return false;
      } else {
          auto zm_manager = storage::ZoneMapManager::GetInstance();
          //zm_manager->CandidateTileGroups()
          std::vector<storage::PredicateInfo> parsed_predicates;
          size_t num_preds = 0;
          storage::PredicateInfo* predicate_array = nullptr;


          //auto expr_type = predicate_->GetExpressionType();

            // The right child should be a constant.
            auto right_child = predicate_->GetModifiableChild(1);

            if (right_child->GetExpressionType() == ExpressionType::VALUE_PARAMETER) {
//              auto right_exp = (const expression::ParameterValueExpression
//                                    *)(predicate_->GetModifiableChild(1));
              auto predicate_val = t[0];
              // Get the column id for this predicate
//              auto left_exp =
//                  (const expression::TupleValueExpression *)(predicate_->GetModifiableChild(
//                      0));
//              int col_id = left_exp->GetColumnId();

              auto comparison_operator = (int)predicate_->GetExpressionType();
              storage::PredicateInfo p_info;

              p_info.col_id = 0;
              p_info.comparison_operator = comparison_operator;
              p_info.predicate_value = predicate_val;

              parsed_predicates.push_back(p_info);




          num_preds = parsed_predicates.size();
          predicate_array = new storage::PredicateInfo[num_preds];
          for (size_t i = 0; i < num_preds; i++) {
            predicate_array[i].col_id = parsed_predicates[i].col_id;
            predicate_array[i].comparison_operator =
                parsed_predicates[i].comparison_operator;
            predicate_array[i].predicate_value =
                parsed_predicates[i].predicate_value;
          }
          auto evicter = new eviction::Evicter();
          column_ids_.insert(column_ids_.begin(),0);
          auto cold_table = evicter->GetColdData(target_table_->GetOid(),zm_manager->CandidateTileGroups(predicate_array, num_preds, target_table_),column_ids_);
          auto cold_tg =
              cold_table.GetTileGroup(0);
          std::vector<oid_t> cold_col_ids;
          std::vector<oid_t> pred_col_ids;
          pred_col_ids.push_back(0);
          for(uint o = 1; o < column_ids_.size(); o++)
              cold_col_ids.push_back(o);
          pred_col_ids.insert(std::end(pred_col_ids), std::begin(cold_col_ids), std::end(cold_col_ids));

          oid_t cold_active_tuple_count = cold_tg->GetNextTupleSlot();

          // Construct position list by looping through tile group
          // and applying the predicate.
          std::vector<oid_t> cold_position_list;
          for (oid_t tuple_id = 0; tuple_id < cold_active_tuple_count; tuple_id++) {
            ItemPointer location(cold_tg->GetTileGroupId(), tuple_id);

                ContainerTuple<storage::TileGroup> tuple(cold_tg.get(),
                                                         tuple_id);
                LOG_TRACE("Evaluate predicate for a tuple");
                auto eval =
                    predicate_->Evaluate(&tuple, nullptr, executor_context_);
                LOG_TRACE("Evaluation result: %s", eval.GetInfo().c_str());
                if (eval.IsTrue()) {
                  cold_position_list.push_back(tuple_id);
                  //auto res = transaction_manager.PerformRead(current_txn, location,
                 //                                            acquire_owner);
                 // if (!res) {
                  //  transaction_manager.SetTransactionResult(current_txn,
                    //                                         ResultType::FAILURE);
                   // return res;
                  }
                }




          // Don't return empty tiles
          if (cold_position_list.size() == 0) {
            return false;
          }


          // Construct logical tile.
          std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
          logical_tile->AddColumns(cold_tg, cold_col_ids);
          logical_tile->AddPositionList(std::move(cold_position_list));
          //query_answered_ = true;
         // LOG_DEBUG("Information %s", logical_tile->GetInfo().c_str());
//          LOG_DEBUG("Query answered from cold data");
          SetOutput(logical_tile.release());
          return false;

          }
      }
  }
          //LOG_DEBUG("Query NOT answered ");
  return false;
}

// Update Predicate expression
// this is used in the NLJoin executor
void SeqScanExecutor::UpdatePredicate(const std::vector<oid_t> &column_ids,
                                      const std::vector<type::Value> &values) {
  std::vector<oid_t> predicate_column_ids;

  PL_ASSERT(column_ids.size() <= column_ids_.size());

  // columns_ids is the column id
  // in the join executor, should
  // convert to the column id in the
  // seq scan executor
  for (auto column_id : column_ids) {
    predicate_column_ids.push_back(column_ids_[column_id]);
  }

  expression::AbstractExpression *new_predicate =
      values.size() != 0 ? ColumnsValuesToExpr(predicate_column_ids, values, 0)
                         : nullptr;

  // combine with original predicate
  if (old_predicate_ != nullptr) {
    expression::AbstractExpression *lexpr = new_predicate,
                                   *rexpr = old_predicate_->Copy();

    new_predicate = new expression::ConjunctionExpression(
        ExpressionType::CONJUNCTION_AND, lexpr, rexpr);
  }

  // Currently a hack that prevent memory leak
  // we should eventually make prediate_ a unique_ptr
  new_predicate_.reset(new_predicate);
  predicate_ = new_predicate;
}

// Transfer a list of equality predicate
// to a expression tree
expression::AbstractExpression *SeqScanExecutor::ColumnsValuesToExpr(
    const std::vector<oid_t> &predicate_column_ids,
    const std::vector<type::Value> &values, size_t idx) {
  if (idx + 1 == predicate_column_ids.size())
    return ColumnValueToCmpExpr(predicate_column_ids[idx], values[idx]);

  // recursively build the expression tree
  expression::AbstractExpression *lexpr = ColumnValueToCmpExpr(
                                     predicate_column_ids[idx], values[idx]),
                                 *rexpr = ColumnsValuesToExpr(
                                     predicate_column_ids, values, idx + 1);

  expression::AbstractExpression *root_expr =
      new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_AND,
                                            lexpr, rexpr);

  root_expr->DeduceExpressionType();
  return root_expr;
}

expression::AbstractExpression *SeqScanExecutor::ColumnValueToCmpExpr(
    const oid_t column_id, const type::Value &value) {
  expression::AbstractExpression *lexpr =
      new expression::TupleValueExpression("");
  reinterpret_cast<expression::TupleValueExpression *>(lexpr)->SetValueType(
      target_table_->GetSchema()->GetColumn(column_id).GetType());
  reinterpret_cast<expression::TupleValueExpression *>(lexpr)
      ->SetValueIdx(column_id);

  expression::AbstractExpression *rexpr =
      new expression::ConstantValueExpression(value);

  expression::AbstractExpression *root_expr =
      new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, lexpr,
                                           rexpr);

  root_expr->DeduceExpressionType();
  return root_expr;
}
}  // namespace executor
}  // namespace peloton
