/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

/// \file iceberg/metrics_config.h
/// \brief Metrics configuration for Iceberg tables

#include <string>
#include <unordered_map>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/formattable.h"
#include "iceberg/util/macros.h"

namespace iceberg {

class ICEBERG_EXPORT MetricsMode : public util::Formattable {
 public:
  enum class Kind : uint8_t {
    kNone,
    kCounts,
    kTruncate,
    kFull,
  };

  static Result<std::shared_ptr<MetricsMode>> FromString(const std::string& mode);

  /// \brief Return the kind of this metrics mode.
  virtual Kind kind() const = 0;

  std::string ToString() const override = 0;
};

class ICEBERG_EXPORT NoneMetricsMode : public MetricsMode {
 public:
  Kind kind() const override { return MetricsMode::Kind::kNone; }

  std::string ToString() const override { return "none"; }

  static std::shared_ptr<MetricsMode> Get() {
    static std::shared_ptr<MetricsMode> instance = std::make_shared<NoneMetricsMode>();
    return instance;
  }
};

class ICEBERG_EXPORT CountsMetricsMode : public MetricsMode {
 public:
  Kind kind() const override { return MetricsMode::Kind::kCounts; }

  std::string ToString() const override { return "counts"; }

  static std::shared_ptr<MetricsMode> Get() {
    static std::shared_ptr<MetricsMode> instance = std::make_shared<CountsMetricsMode>();
    return instance;
  }
};

class ICEBERG_EXPORT TruncateMetricsMode : public MetricsMode {
 public:
  explicit TruncateMetricsMode(int32_t length) : length_(length) {}

  Kind kind() const override { return MetricsMode::Kind::kTruncate; }

  std::string ToString() const override {
    return "truncate(" + std::to_string(length_) + ")";
  }

  static Result<std::shared_ptr<MetricsMode>> Get(int32_t length) {
    ICEBERG_PRECHECK(length > 0, "Truncate length should be positive.");
    return std::make_shared<TruncateMetricsMode>(length);
  }

 private:
  int32_t length_;
};

class ICEBERG_EXPORT FullMetricsMode : public MetricsMode {
 public:
  Kind kind() const override { return MetricsMode::Kind::kFull; }

  std::string ToString() const override { return "full"; }

  static std::shared_ptr<MetricsMode> Get() {
    static std::shared_ptr<MetricsMode> instance = std::make_shared<FullMetricsMode>();
    return instance;
  }
};

/// \brief Configuration utilities for table metrics
class ICEBERG_EXPORT MetricsConfig {
 public:
  /// \brief Create a default metrics config
  static std::shared_ptr<MetricsConfig> Default();

  /// \brief Create a metrics config for position delete
  static std::shared_ptr<MetricsConfig> ForPositionDelete();

  /// \brief Create a metrics config from table properties
  static Result<std::shared_ptr<MetricsConfig>> ForTable(const TableProperties& props,
                                                         const Schema& schema,
                                                         const SortOrder& sort_order);

  /// \brief Verify that all referenced columns are valid
  /// \param updates The updates to verify
  /// \param schema The schema to verify against
  /// \return OK if all referenced columns are valid
  static Status VerifyReferencedColumns(
      const std::unordered_map<std::string, std::string>& updates, const Schema& schema);

  /// \brief Get the metrics mode for a specific column
  /// \param column_name The name of the column
  /// \return The metrics mode for the column
  std::shared_ptr<MetricsMode> ColumnMode(const std::string& column_name) const;

  MetricsConfig(
      std::unordered_map<std::string, std::shared_ptr<MetricsMode>> column_modes,
      std::shared_ptr<MetricsMode> default_mode);

 private:
  std::unordered_map<std::string, std::shared_ptr<MetricsMode>> column_modes_;
  std::shared_ptr<MetricsMode> default_mode_;
};

}  // namespace iceberg
