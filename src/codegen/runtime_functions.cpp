//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// runtime_functions.cpp
//
// Identification: src/codegen/runtime_functions.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/runtime_functions.h"

#include <nmmintrin.h>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/data_table.h"
#include "storage/tile_group.h"
#include "storage/tile.h"
#include "storage/zone_map.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"

namespace peloton {
namespace codegen {

#define CRC32(op, crc, type, buf, len)                   \
  do {                                                   \
    for (; (len) >= sizeof(type);                        \
         (len) -= sizeof(type), (buf) += sizeof(type)) { \
      (crc) = op((crc), *(type *)buf);                   \
    }                                                    \
  } while (0)

//===----------------------------------------------------------------------===//
// Calculate the CRC64 checksum over the given buffer of the provided length
// using the provided CRC as the initial/running CRC value
//===----------------------------------------------------------------------===//
uint64_t RuntimeFunctions::HashCrc64(const char *buf, uint64_t length,
                                     uint64_t crc) {
  // If the string is empty, return the CRC calculated so far
  if (length == 0) {
    return crc;
  }

#if defined(__x86_64__) || defined(_M_X64)
  // Try to each up as many 8-byte values as possible
  CRC32(_mm_crc32_u64, crc, uint64_t, buf, length);
#endif
  // Now we perform CRC in 4-byte, then 2-byte chunks.  Finally, we process
  // what remains in byte chunks.
  CRC32(_mm_crc32_u32, crc, uint32_t, buf, length);
  CRC32(_mm_crc32_u16, crc, uint16_t, buf, length);
  CRC32(_mm_crc32_u8, crc, uint8_t, buf, length);
  // Done
  return crc;
}

//===----------------------------------------------------------------------===//
// Get the tile group with the given index from the table.
//
// TODO: DataTable::GetTileGroup() returns a std::shared_ptr<> that we strip
//       off. This means we could be touching free'd data. This must be fixed.
//===----------------------------------------------------------------------===//
storage::TileGroup *RuntimeFunctions::GetTileGroup(storage::DataTable *table,
                                                   uint64_t tile_group_index) {
  LOG_DEBUG("Called");
  auto tile_group = table->GetTileGroup(tile_group_index);
  return tile_group.get();
}

storage::ZoneMap *RuntimeFunctions::GetZoneMap(storage::TileGroup *tile_group) {
  return tile_group->GetZoneMap();
}

int32_t RuntimeFunctions::GetMinValuefromZoneMap(storage::ZoneMap *zone_map, uint32_t col_num) {
  return zone_map->GetMinValue(col_num);
}

int32_t RuntimeFunctions::GetMaxValuefromZoneMap(storage::ZoneMap *zone_map, uint32_t col_num) {
  return zone_map->GetMaxValue(col_num);
}

void RuntimeFunctions::PrintPredicate(const expression::AbstractExpression *expr, storage::PredicateInfo *predicate_array) {
  LOG_DEBUG("Called");
  // std::string predicate_str;
  // predicate_str = expr->GetInfo();
  // size_t num_preds = expr->GetNumberofParsedPredicates();
  // LOG_DEBUG("Predicate is [%s] : ", predicate_str.c_str());
  // LOG_DEBUG("Number of Parsed Predicates is [%lu]", num_preds);
  const std::vector<std::unique_ptr<const expression::AbstractExpression>> *parsed_predicates;
  parsed_predicates = expr->GetParsedPredicates();
  size_t num_preds = parsed_predicates->size();
  size_t i;
  for (i = 0; i < num_preds; i++) {
      auto temp_predicate = ((*parsed_predicates)[i]).get();
      std::string predicate_str;
      predicate_str = expr->GetInfo();
      LOG_DEBUG("Predicate is [%s] : ", predicate_str.c_str());
      auto right_exp = (const expression::ConstantValueExpression *)(temp_predicate->GetChild(1));
      auto predicate_val = right_exp->GetValue();
      // Get the column id for this predicate
      auto left_exp = (const expression::TupleValueExpression *)(temp_predicate->GetChild(0));
      int col_id = left_exp->GetColumnId();
      // Set the values in the struct
      predicate_array[i].col_id = col_id;
      predicate_array[i].comparison_operator = (int)temp_predicate->GetExpressionType();
      predicate_array[i].predicate_value = predicate_val;

  }
  LOG_DEBUG("Number of Parsed Predicates is [%lu]", num_preds);
}

//===----------------------------------------------------------------------===//
// For every column in the tile group, fill out the layout information for the
// column in the provided 'infos' array.  Specifically, we need a pointer to
// where the first value of the column can be found, and the amount of bytes
// to skip over to find successive values of the column.
//===----------------------------------------------------------------------===//
void RuntimeFunctions::GetTileGroupLayout(const storage::TileGroup *tile_group,
                                          ColumnLayoutInfo *infos,
                                          uint32_t num_cols) {
  for (uint32_t col_idx = 0; col_idx < num_cols; col_idx++) {
    // Map the current column to a tile and a column offset in the tile
    oid_t tile_offset, tile_column_offset;
    tile_group->LocateTileAndColumn(col_idx, tile_offset, tile_column_offset);

    // Now grab the column information
    auto *tile = tile_group->GetTile(tile_offset);
    auto *tile_schema = tile->GetSchema();
    infos[col_idx].column =
        tile->GetTupleLocation(0) + tile_schema->GetOffset(tile_column_offset);
    infos[col_idx].stride = tile_schema->GetLength();
    infos[col_idx].is_columnar = tile_schema->GetColumnCount() == 1;
    LOG_DEBUG("Col [%u] start: %p, stride: %u, columnar: %s", col_idx,
              infos[col_idx].column, infos[col_idx].stride,
              infos[col_idx].is_columnar ? "true" : "false");
  }
}

void RuntimeFunctions::ThrowDivideByZeroException() {
  throw DivideByZeroException("ERROR: division by zero");
}

void RuntimeFunctions::ThrowOverflowException() {
  throw std::overflow_error("ERROR: overflow");
}

}  // namespace codegen
}  // namespace peloton