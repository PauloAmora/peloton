//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// dummy_log_manager.h
//
// Identification: src/backend/logging/loggers/dummy_log_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "logging/log_manager.h"
#include "common/macros.h"

namespace peloton {
namespace logging {

class DummyLogManager : public LogManager {
  DummyLogManager() {}

public:
  static DummyLogManager &GetInstance() {
    static DummyLogManager log_manager;
    return log_manager;
  }
  virtual ~DummyLogManager() {}


  virtual void SetDirectories(std::string logging_dir UNUSED_ATTRIBUTE) final {}

  virtual const std::string GetDirectories() final { return dirs_; }

  virtual void DoRecovery() final {};

  virtual void StartLoggers() final {};
  virtual void StopLoggers() final {};

private:
  std::string dirs_;

};

}
}
