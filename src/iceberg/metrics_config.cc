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

#include "iceberg/metrics_config.h"

#include <regex>
#include <string>
#include <unordered_map>

#include "iceberg/metadata_columns.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_properties.h"

namespace iceberg {

namespace {

const std::shared_ptr<MetricsMode> kDefaultMetricsMode =
    TruncateMetricsMode::Get(16).value();

std::set<int32_t> LimitFieldIds(const Schema& schema, int32_t limit) {
  std::set<int32_t> id_set;

  // This is a simplified version - actual implementation would need to traverse the
  // schema similar to the Java implementation using TypeUtil.visit
  // auto visitor = [&id_set, limit](const Types::NestedField& field) {
  //   if (id_set.size() >= static_cast<size_t>(limit)) {
  //     return;
  //   }
  //   // Add field ID if it's a primitive type
  //   if (field.type().is_primitive()) {
  //     id_set.insert(field.field_id());
  //   }
  // };

  // schema.visit(visitor);

  return id_set;
}

std::shared_ptr<MetricsMode> GetSortedColumnDefaultMode(
    std::shared_ptr<MetricsMode> default_mode) {
  if (default_mode->kind() == MetricsMode::Kind::kNone ||
      default_mode->kind() == MetricsMode::Kind::kCounts) {
    return kDefaultMetricsMode;
  } else {
    return std::move(default_mode);
  }
}

int32_t GetMaxInferredColumnDefaults(const TableProperties& properties) {
  int32_t max_inferred_default_columns =
      properties.Get(TableProperties::kMetricsMaxInferredColumnDefaults);
  if (max_inferred_default_columns < 0) {
    // fallback to default
    return TableProperties::kMetricsMaxInferredColumnDefaults.value();
  }
  return max_inferred_default_columns;
}

Result<std::shared_ptr<MetricsMode>> ParseMode(const std::string& mode,
                                               std::shared_ptr<MetricsMode> fallback) {
  auto metrics_mode = MetricsMode::FromString(mode);
  if (!metrics_mode.has_value()) {
    return std::move(fallback);
  }
  return std::move(metrics_mode.value());
}

}  // namespace

Result<std::shared_ptr<MetricsMode>> MetricsMode::FromString(const std::string& mode) {
  if (StringUtils::EqualsIgnoreCase(mode, "none")) {
    return NoneMetricsMode::Get();
  } else if (StringUtils::EqualsIgnoreCase(mode, "counts")) {
    return CountsMetricsMode::Get();
  } else if (StringUtils::EqualsIgnoreCase(mode, "full")) {
    return FullMetricsMode::Get();
  }

  // Check for truncate pattern using regex
  std::regex truncate_regex(R"(truncate:(\d+))");
  std::smatch match;
  if (std::regex_match(mode, match, truncate_regex)) {
    try {
      int32_t length = std::stoi(match[1].str());
      return TruncateMetricsMode::Get(length);
    } catch (const std::exception& e) {
      return InvalidArgument("Invalid truncate mode: {}", mode);
    }
  }
  return InvalidArgument("Invalid metrics mode: {}", mode);
}

std::shared_ptr<MetricsConfig> MetricsConfig::Default() {
  static auto default_config = std::make_shared<MetricsConfig>(
      std::unordered_map<std::string, std::shared_ptr<MetricsMode>>{},
      kDefaultMetricsMode);
  return default_config;
}

std::shared_ptr<MetricsConfig> MetricsConfig::ForPositionDelete() {
  static auto position_delete_config = std::make_shared<MetricsConfig>(
      std::unordered_map<std::string, std::shared_ptr<MetricsMode>>{
          {std::string(MetadataColumns::kDeleteFilePath.name()), FullMetricsMode::Get()},
          {std::string(MetadataColumns::kDeleteFilePos.name()), FullMetricsMode::Get()}},
      kDefaultMetricsMode);
  return position_delete_config;
}

Result<std::shared_ptr<MetricsConfig>> MetricsConfig::ForTable(
    const TableProperties& props, const Schema& schema, const SortOrder& sort_order) {
  int32_t max_inferred_default_columns = GetMaxInferredColumnDefaults(props);
  std::unordered_map<std::string, std::shared_ptr<MetricsMode>> column_modes;

  std::shared_ptr<MetricsMode> default_mode = kDefaultMetricsMode;
  if (props.configs().contains(TableProperties::kDefaultWriteMetricsMode.key())) {
    std::string configured_default = props.Get(TableProperties::kDefaultWriteMetricsMode);
    ICEBERG_ASSIGN_OR_RAISE(default_mode,
                            ParseMode(configured_default, kDefaultMetricsMode));
  } else {
    // TODO(zhuo.wang)
    default_mode = NoneMetricsMode::Get();
  }

  // First set sorted column with sorted column default (can be overridden by user)
  std::shared_ptr<MetricsMode> sorted_col_default_mode =
      GetSortedColumnDefaultMode(default_mode);
  auto sorted_columns = SortOrder::OrderPreservingSortedColumns(schema, sort_order);
  for (const auto& sc : sorted_columns) {
    column_modes[sc] = sorted_col_default_mode;
  }

  // Handle user overrides of defaults
  for (const auto& prop : props.configs()) {
    if (prop.first.starts_with(TableProperties::kMetricModeColumnConfPrefix.size())) {
      std::string column_alias =
          prop.first.substr(TableProperties::kMetricModeColumnConfPrefix.size());
      ICEBERG_ASSIGN_OR_RAISE(auto mode, ParseMode(prop.second, default_mode));
      column_modes[column_alias] = mode;
    }
  }

  return std::make_shared<MetricsConfig>(column_modes, std::move(default_mode));
}

MetricsConfig::MetricsConfig(
    std::unordered_map<std::string, std::shared_ptr<MetricsMode>> column_modes,
    std::shared_ptr<MetricsMode> default_mode)
    : column_modes_(std::move(column_modes)), default_mode_(std::move(default_mode)) {}

std::shared_ptr<MetricsMode> MetricsConfig::ColumnMode(
    const std::string& column_name) const {
  auto it = column_modes_.find(column_name);
  if (it != column_modes_.end()) {
    return it->second;
  }
  return default_mode_;
}

Status MetricsConfig::VerifyReferencedColumns(
    const std::unordered_map<std::string, std::string>& updates, const Schema& schema) {
  for (const auto& [key, value] : updates) {
    if (!key.starts_with(TableProperties::kMetricModeColumnConfPrefix)) {
      continue;
    }
    auto field_name =
        std::string_view(key).substr(TableProperties::kMetricModeColumnConfPrefix.size());
    ICEBERG_ASSIGN_OR_RAISE(auto field, schema.FindFieldByName(field_name));
    if (!field.has_value()) {
      return ValidationFailed(
          "Invalid metrics config, could not find column {} from table prop {} in "
          "schema {}",
          field_name, key, schema.ToString());
    }
  }
  return {};
}

}  // namespace iceberg
