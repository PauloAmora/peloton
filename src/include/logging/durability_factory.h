//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// durability_factory.h
//
// Identification: src/backend/logging/durability_factory.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "logging/dummy_log_manager.h"
#include "logging/phylog_checkpoint_manager.h"
#include "logging/physical_checkpoint_manager.h"
#include "logging/dummy_checkpoint_manager.h"
#include "logging/reordered_physical_log_manager.h"
#include "logging/reordered_phylog_log_manager.h"

namespace peloton {
namespace logging {

class DurabilityFactory {
 public:

  static LogManager& GetLoggerInstance() {
    switch (logging_type_) {
      case LOGGING_TYPE_REORDERED_PHYSICAL:
        return ReorderedPhysicalLogManager::GetInstance();
      case LOGGING_TYPE_REORDERED_PHYLOG:
        return ReorderedPhyLogLogManager::GetInstance();
      default:
        return DummyLogManager::GetInstance();
    }
  }

  static CheckpointManager &GetCheckpointerInstance() {
    switch (checkpoint_type_) {
      case CHECKPOINT_TYPE_PHYLOG:
        return PhyLogCheckpointManager::GetInstance();
      case CHECKPOINT_TYPE_PHYSICAL:
        return PhysicalCheckpointManager::GetInstance();
      default:
      return DummyCheckpointManager::GetInstance();
    }
  }

  static void Configure(LoggingType logging_type, CheckpointType checkpoint_type) {
    logging_type_ = logging_type;
    checkpoint_type_ = checkpoint_type;
  }

  inline static LoggingType GetLoggingType() { return logging_type_; }

  inline static CheckpointType GetCheckpointType() { return checkpoint_type_; }


private:
  static LoggingType logging_type_;
  static CheckpointType checkpoint_type_;

};
} // namespace gc
} // namespace peloton
