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

#include "iceberg/parquet/parquet_metrics.h"

#include <cmath>
#include <functional>
#include <limits>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <vector>

#include <parquet/column_reader.h>
#include <parquet/schema.h>
#include <parquet/statistics.h>
#include <parquet/types.h>

#include "iceberg/expression/literal.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/truncate_util.h"
#include "iceberg/util/visit_type.h"

namespace iceberg::parquet {

namespace {

/// \brief Get the truncate length from a MetricsMode.
/// \return 0 for None/Counts modes, the truncate length for Truncate mode,
///         or INT_MAX for Full mode.
int32_t TruncateLength(const MetricsMode& mode) {
  switch (mode.kind) {
    case MetricsMode::Kind::kNone:
    case MetricsMode::Kind::kCounts:
      return 0;
    case MetricsMode::Kind::kTruncate:
      return std::get<int32_t>(mode.length);
    case MetricsMode::Kind::kFull:
      return std::numeric_limits<int32_t>::max();
  }
  return 0;
}

/// \brief Convert a Parquet primitive value to an Iceberg Literal.
///
/// This function converts values extracted from Parquet statistics to Iceberg Literal
/// values based on the Iceberg type.
///
/// \param iceberg_type The target Iceberg primitive type.
/// \param parquet_type The source Parquet physical type.
/// \param data Pointer to the raw data bytes.
/// \param length Length of the data in bytes.
/// \return The converted Literal value, or nullopt if conversion fails.
std::optional<Literal> ConvertParquetValue(const PrimitiveType& iceberg_type,
                                           ::parquet::Type::type parquet_type,
                                           const uint8_t* data, int32_t length) {
  switch (iceberg_type.type_id()) {
    case TypeId::kBoolean: {
      if (parquet_type != ::parquet::Type::BOOLEAN || length < 1) {
        return std::nullopt;
      }
      return Literal::Boolean(*reinterpret_cast<const bool*>(data));
    }
    case TypeId::kInt: {
      if (parquet_type != ::parquet::Type::INT32 ||
          length < static_cast<int32_t>(sizeof(int32_t))) {
        return std::nullopt;
      }
      return Literal::Int(*reinterpret_cast<const int32_t*>(data));
    }
    case TypeId::kLong: {
      if (parquet_type == ::parquet::Type::INT64 &&
          length >= static_cast<int32_t>(sizeof(int64_t))) {
        return Literal::Long(*reinterpret_cast<const int64_t*>(data));
      }
      // Promotion from int32 to int64
      if (parquet_type == ::parquet::Type::INT32 &&
          length >= static_cast<int32_t>(sizeof(int32_t))) {
        return Literal::Long(
            static_cast<int64_t>(*reinterpret_cast<const int32_t*>(data)));
      }
      return std::nullopt;
    }
    case TypeId::kFloat: {
      if (parquet_type != ::parquet::Type::FLOAT ||
          length < static_cast<int32_t>(sizeof(float))) {
        return std::nullopt;
      }
      return Literal::Float(*reinterpret_cast<const float*>(data));
    }
    case TypeId::kDouble: {
      if (parquet_type == ::parquet::Type::DOUBLE &&
          length >= static_cast<int32_t>(sizeof(double))) {
        return Literal::Double(*reinterpret_cast<const double*>(data));
      }
      // Promotion from float to double
      if (parquet_type == ::parquet::Type::FLOAT &&
          length >= static_cast<int32_t>(sizeof(float))) {
        return Literal::Double(
            static_cast<double>(*reinterpret_cast<const float*>(data)));
      }
      return std::nullopt;
    }
    case TypeId::kDate: {
      if (parquet_type != ::parquet::Type::INT32 ||
          length < static_cast<int32_t>(sizeof(int32_t))) {
        return std::nullopt;
      }
      return Literal::Date(*reinterpret_cast<const int32_t*>(data));
    }
    case TypeId::kTime: {
      if (parquet_type != ::parquet::Type::INT64 ||
          length < static_cast<int32_t>(sizeof(int64_t))) {
        return std::nullopt;
      }
      return Literal::Time(*reinterpret_cast<const int64_t*>(data));
    }
    case TypeId::kTimestamp: {
      if (parquet_type != ::parquet::Type::INT64 ||
          length < static_cast<int32_t>(sizeof(int64_t))) {
        return std::nullopt;
      }
      return Literal::Timestamp(*reinterpret_cast<const int64_t*>(data));
    }
    case TypeId::kTimestampTz: {
      if (parquet_type != ::parquet::Type::INT64 ||
          length < static_cast<int32_t>(sizeof(int64_t))) {
        return std::nullopt;
      }
      return Literal::TimestampTz(*reinterpret_cast<const int64_t*>(data));
    }
    case TypeId::kString: {
      if (parquet_type != ::parquet::Type::BYTE_ARRAY) {
        return std::nullopt;
      }
      return Literal::String(std::string(reinterpret_cast<const char*>(data), length));
    }
    case TypeId::kBinary: {
      if (parquet_type != ::parquet::Type::BYTE_ARRAY) {
        return std::nullopt;
      }
      return Literal::Binary(std::vector<uint8_t>(data, data + length));
    }
    case TypeId::kFixed: {
      if (parquet_type != ::parquet::Type::FIXED_LEN_BYTE_ARRAY) {
        return std::nullopt;
      }
      return Literal::Fixed(std::vector<uint8_t>(data, data + length));
    }
    case TypeId::kDecimal: {
      const auto& decimal_type = internal::checked_cast<const DecimalType&>(iceberg_type);
      if (parquet_type == ::parquet::Type::FIXED_LEN_BYTE_ARRAY ||
          parquet_type == ::parquet::Type::BYTE_ARRAY) {
        // Decimal stored as big-endian bytes
        int128_t value = 0;
        bool negative = (length > 0 && (data[0] & 0x80) != 0);
        for (int32_t i = 0; i < length; ++i) {
          value = (value << 8) | data[i];
        }
        // Sign extend if negative
        if (negative) {
          for (int32_t i = length; i < 16; ++i) {
            value |= (static_cast<int128_t>(0xFF) << (i * 8));
          }
        }
        return Literal::Decimal(value, decimal_type.precision(), decimal_type.scale());
      }
      if (parquet_type == ::parquet::Type::INT32 &&
          length >= static_cast<int32_t>(sizeof(int32_t))) {
        int32_t int_val = *reinterpret_cast<const int32_t*>(data);
        return Literal::Decimal(static_cast<int128_t>(int_val), decimal_type.precision(),
                                decimal_type.scale());
      }
      if (parquet_type == ::parquet::Type::INT64 &&
          length >= static_cast<int32_t>(sizeof(int64_t))) {
        int64_t long_val = *reinterpret_cast<const int64_t*>(data);
        return Literal::Decimal(static_cast<int128_t>(long_val), decimal_type.precision(),
                                decimal_type.scale());
      }
      return std::nullopt;
    }
    case TypeId::kUuid: {
      if (parquet_type != ::parquet::Type::FIXED_LEN_BYTE_ARRAY || length != 16) {
        return std::nullopt;
      }
      auto uuid_result = Uuid::FromBytes(std::span<const uint8_t>(data, 16));
      if (!uuid_result.has_value()) {
        return std::nullopt;
      }
      return Literal::UUID(uuid_result.value());
    }
    default:
      return std::nullopt;
  }
}

/// \brief Get the Iceberg field ID from a Parquet column descriptor.
/// \return The field ID, or nullopt if no field ID is set.
std::optional<int32_t> GetFieldId(const ::parquet::ColumnDescriptor* column) {
  const auto& node = column->schema_node();
  if (node == nullptr || !node->is_primitive()) {
    return std::nullopt;
  }
  if (node->field_id() < 0) {
    return std::nullopt;
  }
  return node->field_id();
}

/// \brief Find the column index for a field in the Parquet schema.
std::optional<int> FindColumnIndex(const ::parquet::SchemaDescriptor& parquet_schema,
                                   int32_t field_id) {
  auto columns = std::views::iota(0, parquet_schema.num_columns());
  auto it = std::ranges::find_if(columns, [&](int i) {
    auto column_field_id = GetFieldId(parquet_schema.Column(i));
    return column_field_id.has_value() && column_field_id.value() == field_id;
  });
  return it != columns.end() ? std::optional(*it) : std::nullopt;
}

/// \brief Collect column sizes from Parquet file metadata.
Status CollectColumnSizes(const ::parquet::SchemaDescriptor& parquet_schema,
                          const Schema& schema, const MetricsConfig& metrics_config,
                          const ::parquet::FileMetaData& metadata, Metrics& metrics) {
  // Compute row count from all row groups
  int64_t row_count = 0;
  for (int rg = 0; rg < metadata.num_row_groups(); ++rg) {
    auto row_group = metadata.RowGroup(rg);
    row_count += row_group->num_rows();
    for (int col = 0; col < row_group->num_columns(); ++col) {
      auto column_chunk = row_group->ColumnChunk(col);
      auto column_desc = parquet_schema.Column(col);

      auto field_id_opt = GetFieldId(column_desc);
      if (!field_id_opt.has_value()) {
        continue;
      }

      int32_t field_id = field_id_opt.value();

      ICEBERG_ASSIGN_OR_RAISE(auto field_opt, schema.GetFieldById(field_id));
      if (!field_opt.has_value()) {
        continue;
      }
      const auto& field = field_opt->get();
      MetricsMode mode = metrics_config.ColumnMode(field.name());

      if (mode.kind != MetricsMode::Kind::kNone) {
        metrics.column_sizes[field_id] =
            metrics.column_sizes[field_id] + column_chunk->total_compressed_size();
      }
    }
  }
  metrics.row_count = row_count;
  return {};
}

/// \brief Collect counts (value count and null count) from footer statistics.
///
/// \param field_id The Iceberg field ID.
/// \param parquet_schema The Parquet schema.
/// \param metadata The Parquet file metadata.
/// \return A pair of (value_count, null_count), or nullopt if stats are not available.
std::optional<std::pair<int64_t, int64_t>> CollectCounts(
    int32_t field_id, const ::parquet::SchemaDescriptor& parquet_schema,
    const ::parquet::FileMetaData& metadata) {
  auto column_idx = FindColumnIndex(parquet_schema, field_id);
  if (!column_idx.has_value()) {
    return std::nullopt;
  }

  int64_t value_count = 0;
  int64_t null_count = 0;

  for (int rg = 0; rg < metadata.num_row_groups(); ++rg) {
    auto row_group = metadata.RowGroup(rg);
    auto column_chunk = row_group->ColumnChunk(column_idx.value());

    if (!column_chunk->is_stats_set()) {
      return std::nullopt;
    }

    auto stats = column_chunk->statistics();
    if (stats == nullptr || !stats->HasNullCount()) {
      return std::nullopt;
    }

    null_count += stats->null_count();
    value_count += column_chunk->num_values();
  }

  return std::make_pair(value_count, null_count);
}

/// \brief Collect bounds (lower and upper) from footer statistics.
std::optional<std::pair<Literal, Literal>> CollectBoundsTyped(
    int32_t field_id, const PrimitiveType& iceberg_type,
    const ::parquet::SchemaDescriptor& parquet_schema,
    const ::parquet::FileMetaData& metadata) {
  auto column_idx = FindColumnIndex(parquet_schema, field_id);
  if (!column_idx.has_value()) {
    return std::nullopt;
  }

  auto column_desc = parquet_schema.Column(column_idx.value());
  ::parquet::Type::type parquet_type = column_desc->physical_type();

  std::optional<Literal> lower_bound;
  std::optional<Literal> upper_bound;

  for (int rg = 0; rg < metadata.num_row_groups(); ++rg) {
    auto row_group = metadata.RowGroup(rg);
    auto column_chunk = row_group->ColumnChunk(column_idx.value());

    if (!column_chunk->is_stats_set()) {
      return std::nullopt;
    }

    auto stats = column_chunk->statistics();
    if (stats == nullptr || !stats->HasMinMax()) {
      // Skip row groups without min/max stats
      continue;
    }

    // Get min value
    auto min_bytes = stats->EncodeMin();
    auto converted_min = ConvertParquetValue(
        iceberg_type, parquet_type, reinterpret_cast<const uint8_t*>(min_bytes.data()),
        static_cast<int32_t>(min_bytes.size()));
    if (converted_min.has_value()) {
      if (converted_min.value().IsNaN()) {
        return std::nullopt;  // NaN values invalidate statistics
      }
      if (!lower_bound.has_value() || converted_min.value() < lower_bound.value()) {
        lower_bound = converted_min;
      }
    }

    // Get max value
    auto max_bytes = stats->EncodeMax();
    auto converted_max = ConvertParquetValue(
        iceberg_type, parquet_type, reinterpret_cast<const uint8_t*>(max_bytes.data()),
        static_cast<int32_t>(max_bytes.size()));
    if (converted_max.has_value()) {
      if (converted_max.value().IsNaN()) {
        return std::nullopt;  // NaN values invalidate statistics
      }
      if (!upper_bound.has_value() || converted_max.value() > upper_bound.value()) {
        upper_bound = converted_max;
      }
    }
  }

  if (lower_bound.has_value() && upper_bound.has_value()) {
    return std::make_pair(lower_bound.value(), upper_bound.value());
  }
  return std::nullopt;
}

/// \brief Truncate the lower bound of a string or binary value.
std::optional<Literal> TruncateLowerBound(const PrimitiveType& type, const Literal& value,
                                          int32_t length) {
  if (value.IsNull()) {
    return std::nullopt;
  }

  switch (type.type_id()) {
    case TypeId::kString: {
      const auto& str = std::get<std::string>(value.value());
      // Truncate to at most 'length' UTF-8 code points
      std::string truncated = TruncateUtils::TruncateUTF8(str, length);
      return Literal::String(std::move(truncated));
    }
    case TypeId::kBinary: {
      const auto& data = std::get<std::vector<uint8_t>>(value.value());
      if (static_cast<int32_t>(data.size()) <= length) {
        return value;
      }
      return Literal::Binary(std::vector<uint8_t>(data.begin(), data.begin() + length));
    }
    default:
      // Other types don't need truncation
      return value;
  }
}

/// \brief Truncate the upper bound of a string or binary value.
std::optional<Literal> TruncateUpperBound(const PrimitiveType& type, const Literal& value,
                                          int32_t length) {
  if (value.IsNull()) {
    return std::nullopt;
  }

  switch (type.type_id()) {
    case TypeId::kString: {
      const auto& str = std::get<std::string>(value.value());
      if (str.size() <= static_cast<size_t>(length)) {
        return value;
      }

      // Truncate and try to increment the last character
      std::string truncated = TruncateUtils::TruncateUTF8(str, length);

      // If we truncated something, we need to increment the last byte to maintain
      // the upper bound property. Find the last byte that can be incremented (< 0xFF)
      for (auto it = truncated.rbegin(); it != truncated.rend(); ++it) {
        auto byte = static_cast<uint8_t>(*it);
        if (byte < 0xFF) {
          *it = static_cast<char>(byte + 1);
          // Remove any bytes after this one
          truncated.resize(truncated.size() - std::distance(truncated.rbegin(), it));
          return Literal::String(std::move(truncated));
        }
      }
      // All bytes are 0xFF, cannot create a valid upper bound
      return std::nullopt;
    }
    case TypeId::kBinary: {
      const auto& data = std::get<std::vector<uint8_t>>(value.value());
      if (static_cast<int32_t>(data.size()) <= length) {
        return value;
      }

      std::vector<uint8_t> truncated(data.begin(), data.begin() + length);
      // Increment the last byte that can be incremented
      for (auto it = truncated.rbegin(); it != truncated.rend(); ++it) {
        if (*it < 0xFF) {
          ++(*it);
          truncated.resize(truncated.size() - std::distance(truncated.rbegin(), it));
          return Literal::Binary(std::move(truncated));
        }
      }
      // All bytes are 0xFF, cannot create a valid upper bound
      return std::nullopt;
    }
    default:
      return value;
  }
}

/// \brief Process pre-computed field metrics, applying truncation if needed.
/// \param field_id The field ID to look up.
/// \param field_metrics The map of pre-computed field metrics.
/// \param primitive_type The primitive type for truncation.
/// \param truncate_length The truncation length (0 means no bounds).
/// \return Processed FieldMetrics with truncated bounds if applicable, or nullopt if not
/// found.
std::optional<FieldMetrics> MetricsFromFieldMetrics(
    int32_t field_id, const std::unordered_map<int32_t, FieldMetrics>& field_metrics,
    const PrimitiveType& primitive_type, int32_t truncate_length) {
  auto it = field_metrics.find(field_id);
  if (it == field_metrics.end()) {
    return std::nullopt;
  }

  const auto& fm = it->second;
  FieldMetrics result;
  result.field_id = fm.field_id;
  result.value_count = fm.value_count;
  result.null_value_count = fm.null_value_count;
  result.nan_value_count = fm.nan_value_count;

  if (truncate_length > 0) {
    if (fm.lower_bound.has_value()) {
      result.lower_bound =
          TruncateLowerBound(primitive_type, fm.lower_bound.value(), truncate_length);
    }
    if (fm.upper_bound.has_value()) {
      result.upper_bound =
          TruncateUpperBound(primitive_type, fm.upper_bound.value(), truncate_length);
    }
  }

  return result;
}

/// \brief Collect metrics for a single primitive field from footer statistics.
std::optional<FieldMetrics> MetricsFromFooter(
    int32_t field_id, const PrimitiveType& iceberg_type,
    const ::parquet::SchemaDescriptor& parquet_schema,
    const ::parquet::FileMetaData& metadata, int32_t truncate_length) {
  // Skip INT96 (legacy timestamp) as it has no meaningful statistics
  auto column_idx = FindColumnIndex(parquet_schema, field_id);
  if (!column_idx.has_value()) {
    return std::nullopt;
  }

  auto column_desc = parquet_schema.Column(column_idx.value());
  if (column_desc->physical_type() == ::parquet::Type::INT96) {
    return std::nullopt;
  }

  // Collect counts
  auto counts = CollectCounts(field_id, parquet_schema, metadata);
  if (!counts.has_value()) {
    return std::nullopt;
  }

  FieldMetrics metrics;
  metrics.field_id = field_id;
  metrics.value_count = counts->first;
  metrics.null_value_count = counts->second;

  // Collect bounds if truncate_length > 0
  if (truncate_length > 0) {
    auto bounds = CollectBoundsTyped(field_id, iceberg_type, parquet_schema, metadata);
    if (bounds.has_value()) {
      // Truncate bounds if necessary
      metrics.lower_bound =
          TruncateLowerBound(iceberg_type, bounds->first, truncate_length);
      metrics.upper_bound =
          TruncateUpperBound(iceberg_type, bounds->second, truncate_length);
    }
  }

  return metrics;
}

/// \brief Visitor for collecting metrics from all primitive fields in a schema.
class CollectMetricsVisitor {
 public:
  CollectMetricsVisitor(const ::parquet::SchemaDescriptor& parquet_schema,
                        const MetricsConfig& metrics_config,
                        const ::parquet::FileMetaData& metadata,
                        const std::unordered_map<int32_t, FieldMetrics>& field_metrics,
                        Metrics& metrics)
      : parquet_schema_(parquet_schema),
        metrics_config_(metrics_config),
        metadata_(metadata),
        field_metrics_(field_metrics),
        metrics_(metrics) {}

  Status VisitStruct(const StructType& type, const std::string& prefix) {
    for (const auto& field : type.fields()) {
      std::string full_name = prefix.empty() ? std::string(field.name())
                                             : prefix + "." + std::string(field.name());
      ICEBERG_RETURN_UNEXPECTED(VisitField(field, full_name));
    }
    return {};
  }

  Status VisitList(const ListType& /*type*/, const std::string& /*prefix*/) { return {}; }

  Status VisitMap(const MapType& /*type*/, const std::string& /*prefix*/) { return {}; }

  Status VisitPrimitive(const PrimitiveType& /*type*/, const std::string& /*prefix*/) {
    return {};
  }

 private:
  Status VisitField(const SchemaField& field, const std::string& full_name) {
    if (field.type()->is_primitive()) {
      ProcessPrimitiveField(field, full_name);
    } else if (field.type()->is_nested()) {
      return VisitTypeCategory(*field.type(), this, full_name);
    }
    return {};
  }

  void ProcessPrimitiveField(const SchemaField& field, const std::string& full_name) {
    int32_t field_id = field.field_id();
    MetricsMode mode = metrics_config_.ColumnMode(full_name);

    if (mode.kind == MetricsMode::Kind::kNone) {
      return;
    }

    int32_t truncate_length = TruncateLength(mode);
    const auto& primitive_type =
        internal::checked_cast<const PrimitiveType&>(*field.type());

    // Check if we have pre-computed field metrics
    auto field_metrics = MetricsFromFieldMetrics(field_id, field_metrics_, primitive_type,
                                                 truncate_length);
    if (field_metrics.has_value()) {
      ApplyFieldMetrics(field_id, field_metrics.value());
      return;
    }

    // Fall back to footer statistics
    auto footer_metrics = MetricsFromFooter(field_id, primitive_type, parquet_schema_,
                                            metadata_, truncate_length);
    if (footer_metrics.has_value()) {
      ApplyFieldMetrics(field_id, footer_metrics.value());
    }
  }

  void ApplyFieldMetrics(int32_t field_id, const FieldMetrics& fm) {
    if (fm.value_count >= 0) {
      metrics_.value_counts[field_id] = fm.value_count;
    }
    if (fm.null_value_count >= 0) {
      metrics_.null_value_counts[field_id] = fm.null_value_count;
    }
    if (fm.nan_value_count >= 0) {
      metrics_.nan_value_counts[field_id] = fm.nan_value_count;
    }
    if (fm.lower_bound.has_value()) {
      metrics_.lower_bounds.emplace(field_id, std::move(fm.lower_bound.value()));
    }
    if (fm.upper_bound.has_value()) {
      metrics_.upper_bounds.emplace(field_id, std::move(fm.upper_bound.value()));
    }
  }

  const ::parquet::SchemaDescriptor& parquet_schema_;
  const MetricsConfig& metrics_config_;
  const ::parquet::FileMetaData& metadata_;
  const std::unordered_map<int32_t, FieldMetrics>& field_metrics_;
  Metrics& metrics_;
};

}  // namespace

Result<Metrics> ParquetMetrics::ComputeMetrics(
    const Schema& schema, const ::parquet::SchemaDescriptor& parquet_schema,
    const MetricsConfig& metrics_config, const ::parquet::FileMetaData& metadata,
    const std::unordered_map<int32_t, FieldMetrics>& field_metrics) {
  Metrics metrics;

  // Collect row count and column sizes
  ICEBERG_RETURN_UNEXPECTED(
      CollectColumnSizes(parquet_schema, schema, metrics_config, metadata, metrics));

  // Collect metrics for all primitive fields
  CollectMetricsVisitor visitor(parquet_schema, metrics_config, metadata, field_metrics,
                                metrics);
  ICEBERG_RETURN_UNEXPECTED(visitor.VisitStruct(schema, ""));

  return metrics;
}

}  // namespace iceberg::parquet
