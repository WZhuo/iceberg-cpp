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

#include "iceberg/table_scan.h"

#include <cstring>
#include <ranges>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/residual_evaluator.h"
#include "iceberg/file_reader.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_group.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util_internal.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

namespace {

const std::vector<std::string> kScanColumns = {
    "snapshot_id",         "file_path",          "file_ordinal",  "file_format",
    "block_size_in_bytes", "file_size_in_bytes", "record_count",  "partition",
    "key_metadata",        "split_offsets",      "sort_order_id",
};

const std::vector<std::string> kStatsColumns = {
    "value_counts", "null_value_counts", "nan_value_counts",
    "lower_bounds", "upper_bounds",      "column_sizes",
};

const std::vector<std::string> kScanColumnsWithStats = [] {
  auto cols = kScanColumns;
  cols.insert(cols.end(), kStatsColumns.begin(), kStatsColumns.end());
  return cols;
}();

/// \brief Private data structure to hold the Reader and error state
struct ReaderStreamPrivateData {
  std::unique_ptr<Reader> reader;
  std::string last_error;

  explicit ReaderStreamPrivateData(std::unique_ptr<Reader> reader_ptr)
      : reader(std::move(reader_ptr)) {}

  ~ReaderStreamPrivateData() {
    if (reader) {
      std::ignore = reader->Close();
    }
  }
};

/// \brief Callback to get the stream schema
static int GetSchema(struct ArrowArrayStream* stream, struct ArrowSchema* out) {
  if (!stream || !stream->private_data) {
    return EINVAL;
  }
  auto* private_data = static_cast<ReaderStreamPrivateData*>(stream->private_data);
  // Get schema from reader
  auto schema_result = private_data->reader->Schema();
  if (!schema_result.has_value()) {
    private_data->last_error = schema_result.error().message;
    std::memset(out, 0, sizeof(ArrowSchema));
    return EIO;
  }

  *out = std::move(schema_result.value());
  return 0;
}

/// \brief Callback to get the next array from the stream
static int GetNext(struct ArrowArrayStream* stream, struct ArrowArray* out) {
  if (!stream || !stream->private_data) {
    return EINVAL;
  }

  auto* private_data = static_cast<ReaderStreamPrivateData*>(stream->private_data);

  auto next_result = private_data->reader->Next();
  if (!next_result.has_value()) {
    private_data->last_error = next_result.error().message;
    std::memset(out, 0, sizeof(ArrowArray));
    return EIO;
  }

  auto& optional_array = next_result.value();
  if (optional_array.has_value()) {
    *out = std::move(optional_array.value());
  } else {
    // End of stream - set release to nullptr to signal end
    std::memset(out, 0, sizeof(ArrowArray));
    out->release = nullptr;
  }

  return 0;
}

/// \brief Callback to get the last error message
static const char* GetLastError(struct ArrowArrayStream* stream) {
  if (!stream || !stream->private_data) {
    return nullptr;
  }

  auto* private_data = static_cast<ReaderStreamPrivateData*>(stream->private_data);
  return private_data->last_error.empty() ? nullptr : private_data->last_error.c_str();
}

/// \brief Callback to release the stream resources
static void Release(struct ArrowArrayStream* stream) {
  if (!stream || !stream->private_data) {
    return;
  }

  delete static_cast<ReaderStreamPrivateData*>(stream->private_data);
  stream->private_data = nullptr;
  stream->release = nullptr;
}

Result<ArrowArrayStream> MakeArrowArrayStream(std::unique_ptr<Reader> reader) {
  if (!reader) {
    return InvalidArgument("Reader cannot be null");
  }

  auto private_data = std::make_unique<ReaderStreamPrivateData>(std::move(reader));

  ArrowArrayStream stream{.get_schema = GetSchema,
                          .get_next = GetNext,
                          .get_last_error = GetLastError,
                          .release = Release,
                          .private_data = private_data.release()};

  return stream;
}

}  // namespace

namespace internal {

Status TableScanContext::Validate() const {
  ICEBERG_CHECK(columns_to_keep_stats.empty() || return_column_stats,
                "Cannot select columns to keep stats when column stats are not returned");
  ICEBERG_CHECK(projected_schema == nullptr || selected_columns.empty(),
                "Cannot set projection schema and selected columns at the same time");
  ICEBERG_CHECK(!snapshot_id.has_value() ||
                    (!from_snapshot_id.has_value() && !to_snapshot_id.has_value()),
                "Cannot mix snapshot scan and incremental scan");
  ICEBERG_CHECK(!min_rows_requested.has_value() || min_rows_requested.value() >= 0,
                "Min rows requested cannot be negative");
  return {};
}

bool TableScanContext::IsScanCurrentLineage() const {
  return !from_snapshot_id.has_value() && !to_snapshot_id.has_value();
}

Result<int64_t> TableScanContext::ToSnapshotIdInclusive(
    const TableMetadata& metadata) const {
  // Get the branch's current snapshot ID if branch is set
  std::shared_ptr<Snapshot> branch_snapshot;
  if (!branch.empty()) {
    auto iter = metadata.refs.find(branch);
    ICEBERG_CHECK(iter != metadata.refs.end() && iter->second != nullptr,
                  "Cannot find branch: {}", branch);
    ICEBERG_ASSIGN_OR_RAISE(branch_snapshot,
                            metadata.SnapshotById(iter->second->snapshot_id));
  }

  if (to_snapshot_id.has_value()) {
    int64_t to_snapshot_id_value = to_snapshot_id.value();

    if (branch_snapshot != nullptr) {
      // Validate `to_snapshot_id` is on the current branch
      ICEBERG_ASSIGN_OR_RAISE(
          bool is_ancestor,
          SnapshotUtil::IsAncestorOf(metadata, branch_snapshot->snapshot_id,
                                     to_snapshot_id_value));
      ICEBERG_CHECK(is_ancestor,
                    "End snapshot is not a valid snapshot on the current branch: {}",
                    branch);
    }

    return to_snapshot_id_value;
  }

  // If to_snapshot_id is not set, use branch's current snapshot if branch is set
  if (branch_snapshot != nullptr) {
    return branch_snapshot->snapshot_id;
  }

  // Get current snapshot from table's current snapshot
  std::shared_ptr<Snapshot> current_snapshot;
  ICEBERG_ASSIGN_OR_RAISE(current_snapshot, metadata.Snapshot());
  ICEBERG_CHECK(current_snapshot != nullptr,
                "End snapshot is not set and table has no current snapshot");
  return current_snapshot->snapshot_id;
}

Result<std::optional<int64_t>> TableScanContext::FromSnapshotIdExclusive(
    const TableMetadata& metadata, int64_t to_snapshot_id_inclusive) const {
  if (!from_snapshot_id.has_value()) {
    return std::nullopt;
  }

  int64_t from_snapshot_id_value = from_snapshot_id.value();

  // Validate `from_snapshot_id` is an ancestor of `to_snapshot_id_inclusive`
  if (from_snapshot_id_inclusive) {
    ICEBERG_ASSIGN_OR_RAISE(bool is_ancestor,
                            SnapshotUtil::IsAncestorOf(metadata, to_snapshot_id_inclusive,
                                                       from_snapshot_id_value));
    ICEBERG_CHECK(
        is_ancestor,
        "Starting snapshot (inclusive) {} is not an ancestor of end snapshot {}",
        from_snapshot_id_value, to_snapshot_id_inclusive);

    // For inclusive behavior, return the parent snapshot ID (can be nullopt)
    ICEBERG_ASSIGN_OR_RAISE(auto from_snapshot,
                            metadata.SnapshotById(from_snapshot_id_value));
    return from_snapshot->parent_snapshot_id;
  }

  // Validate there is an ancestor of `to_snapshot_id_inclusive` where parent is
  // `from_snapshot_id`
  ICEBERG_ASSIGN_OR_RAISE(bool is_parent_ancestor, SnapshotUtil::IsParentAncestorOf(
                                                       metadata, to_snapshot_id_inclusive,
                                                       from_snapshot_id_value));
  ICEBERG_CHECK(
      is_parent_ancestor,
      "Starting snapshot (exclusive) {} is not a parent ancestor of end snapshot {}",
      from_snapshot_id_value, to_snapshot_id_inclusive);

  return from_snapshot_id_value;
}

}  // namespace internal

ScanTask::~ScanTask() = default;

// FileScanTask implementation

FileScanTask::FileScanTask(std::shared_ptr<DataFile> data_file,
                           std::vector<std::shared_ptr<DataFile>> delete_files,
                           std::shared_ptr<Expression> residual_filter)
    : data_file_(std::move(data_file)),
      delete_files_(std::move(delete_files)),
      residual_filter_(std::move(residual_filter)) {
  ICEBERG_DCHECK(data_file_ != nullptr, "Data file cannot be null for FileScanTask");
}

int64_t FileScanTask::size_bytes() const { return data_file_->file_size_in_bytes; }

int32_t FileScanTask::files_count() const { return 1; }

int64_t FileScanTask::estimated_row_count() const { return data_file_->record_count; }

Result<ArrowArrayStream> FileScanTask::ToArrow(
    const std::shared_ptr<FileIO>& io, std::shared_ptr<Schema> projected_schema) const {
  if (!delete_files_.empty()) {
    return NotSupported("Reading data files with delete files is not yet supported.");
  }

  const ReaderOptions options{.path = data_file_->file_path,
                              .length = data_file_->file_size_in_bytes,
                              .io = io,
                              .projection = std::move(projected_schema),
                              .filter = residual_filter_};

  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          ReaderFactoryRegistry::Open(data_file_->file_format, options));

  return MakeArrowArrayStream(std::move(reader));
}

// ChangelogScanTask implementation

int64_t ChangelogScanTask::size_bytes() const {
  int64_t total_size = data_file_->file_size_in_bytes;
  for (const auto& delete_file : delete_files_) {
    total_size +=
        (delete_file->IsDeletionVector() ? delete_file->content_size_in_bytes.value_or(0)
                                         : delete_file->file_size_in_bytes);
  }
  return total_size;
}

int32_t ChangelogScanTask::files_count() const { return 1 + delete_files_.size(); }

int64_t ChangelogScanTask::estimated_row_count() const {
  return data_file_->record_count;
}

// AddedRowsScanTask implementation

// DeletedDataFileScanTask implementation

Result<std::unique_ptr<TableScanBuilder>> TableScanBuilder::Make(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<FileIO> io) {
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  return std::unique_ptr<TableScanBuilder>(
      new TableScanBuilder(std::move(metadata), std::move(io)));
}

TableScanBuilder::TableScanBuilder(std::shared_ptr<TableMetadata> table_metadata,
                                   std::shared_ptr<FileIO> file_io)
    : metadata_(std::move(table_metadata)), io_(std::move(file_io)) {}

TableScanBuilder& TableScanBuilder::Option(std::string key, std::string value) {
  context_.options[std::move(key)] = std::move(value);
  return *this;
}

TableScanBuilder& TableScanBuilder::Project(std::shared_ptr<Schema> schema) {
  context_.projected_schema = std::move(schema);
  return *this;
}

TableScanBuilder& TableScanBuilder::CaseSensitive(bool case_sensitive) {
  context_.case_sensitive = case_sensitive;
  return *this;
}

TableScanBuilder& TableScanBuilder::IncludeColumnStats() {
  context_.return_column_stats = true;
  return *this;
}

TableScanBuilder& TableScanBuilder::IncludeColumnStats(
    const std::vector<std::string>& requested_columns) {
  context_.return_column_stats = true;
  context_.columns_to_keep_stats.clear();
  context_.columns_to_keep_stats.reserve(requested_columns.size());

  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto schema_ref, ResolveSnapshotSchema());
  const auto& schema = schema_ref.get();
  for (const auto& column_name : requested_columns) {
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto field, schema->FindFieldByName(column_name));
    if (field.has_value()) {
      context_.columns_to_keep_stats.insert(field.value().get().field_id());
    }
  }

  return *this;
}

TableScanBuilder& TableScanBuilder::Select(const std::vector<std::string>& column_names) {
  context_.selected_columns = column_names;
  return *this;
}

TableScanBuilder& TableScanBuilder::Filter(std::shared_ptr<Expression> filter) {
  context_.filter = std::move(filter);
  return *this;
}

TableScanBuilder& TableScanBuilder::IgnoreResiduals() {
  context_.ignore_residuals = true;
  return *this;
}

TableScanBuilder& TableScanBuilder::MinRowsRequested(int64_t num_rows) {
  context_.min_rows_requested = num_rows;
  return *this;
}

TableScanBuilder& TableScanBuilder::UseSnapshot(int64_t snapshot_id) {
  ICEBERG_BUILDER_CHECK(!context_.snapshot_id.has_value(),
                        "Cannot override snapshot, already set snapshot id={}",
                        context_.snapshot_id.value());
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(std::ignore, metadata_->SnapshotById(snapshot_id));
  context_.snapshot_id = snapshot_id;
  return *this;
}

TableScanBuilder& TableScanBuilder::UseRef(const std::string& ref) {
  if (ref == SnapshotRef::kMainBranch) {
    snapshot_schema_ = nullptr;
    context_.snapshot_id.reset();
    return *this;
  }

  ICEBERG_BUILDER_CHECK(!context_.snapshot_id.has_value(),
                        "Cannot override ref, already set snapshot id={}",
                        context_.snapshot_id.value());
  auto iter = metadata_->refs.find(ref);
  ICEBERG_BUILDER_CHECK(iter != metadata_->refs.end(), "Cannot find ref {}", ref);
  ICEBERG_BUILDER_CHECK(iter->second != nullptr, "Ref {} is null", ref);
  int32_t snapshot_id = iter->second->snapshot_id;
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(std::ignore, metadata_->SnapshotById(snapshot_id));
  context_.snapshot_id = snapshot_id;

  return *this;
}

TableScanBuilder& TableScanBuilder::AsOfTime(int64_t timestamp_millis) {
  auto time_point_ms = TimePointMsFromUnixMs(timestamp_millis);
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(
      auto snapshot_id, SnapshotUtil::SnapshotIdAsOfTime(*metadata_, time_point_ms));
  return UseSnapshot(snapshot_id);
}

Result<std::reference_wrapper<const std::shared_ptr<Schema>>>
TableScanBuilder::ResolveSnapshotSchema() {
  if (snapshot_schema_ == nullptr) {
    if (context_.snapshot_id.has_value()) {
      ICEBERG_ASSIGN_OR_RAISE(auto snapshot,
                              metadata_->SnapshotById(*context_.snapshot_id));
      int32_t schema_id = snapshot->schema_id.value_or(Schema::kInitialSchemaId);
      ICEBERG_ASSIGN_OR_RAISE(snapshot_schema_, metadata_->SchemaById(schema_id));
    } else {
      ICEBERG_ASSIGN_OR_RAISE(snapshot_schema_, metadata_->Schema());
    }
  }
  ICEBERG_CHECK(snapshot_schema_ != nullptr, "Snapshot schema is null");
  return snapshot_schema_;
}

Result<std::unique_ptr<DataTableScan>> TableScanBuilder::Build() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  ICEBERG_RETURN_UNEXPECTED(context_.Validate());

  ICEBERG_ASSIGN_OR_RAISE(auto schema, ResolveSnapshotSchema());
  return DataTableScan::Make(metadata_, schema.get(), io_, std::move(context_));
}

TableScan::TableScan(std::shared_ptr<TableMetadata> metadata,
                     std::shared_ptr<Schema> schema, std::shared_ptr<FileIO> file_io,
                     internal::TableScanContext context)
    : metadata_(std::move(metadata)),
      schema_(std::move(schema)),
      io_(std::move(file_io)),
      context_(std::move(context)) {}

TableScan::~TableScan() = default;

const std::shared_ptr<TableMetadata>& TableScan::metadata() const { return metadata_; }

Result<std::shared_ptr<Snapshot>> TableScan::snapshot() const {
  auto snapshot_id = context_.snapshot_id ? context_.snapshot_id.value()
                                          : metadata_->current_snapshot_id;
  if (snapshot_id == kInvalidSnapshotId) {
    return std::shared_ptr<Snapshot>{nullptr};
  }
  return metadata_->SnapshotById(snapshot_id);
}

Result<std::shared_ptr<Schema>> TableScan::schema() const {
  return ResolveProjectedSchema();
}

const internal::TableScanContext& TableScan::context() const { return context_; }

const std::shared_ptr<FileIO>& TableScan::io() const { return io_; }

const std::shared_ptr<Expression>& TableScan::filter() const {
  const static std::shared_ptr<Expression> true_expr = True::Instance();
  if (!context_.filter) {
    return true_expr;
  }
  return context_.filter;
}

bool TableScan::is_case_sensitive() const { return context_.case_sensitive; }

Result<std::reference_wrapper<const std::shared_ptr<Schema>>>
TableScan::ResolveProjectedSchema() const {
  if (projected_schema_ != nullptr) {
    return projected_schema_;
  }

  if (!context_.selected_columns.empty()) {
    /// TODO(gangwu): port Java BaseScan.lazyColumnProjection to collect field ids
    /// from selected column names and bound references in the filter, and then create
    /// projected schema based on the collected field ids.
    return NotImplemented(
        "Selecting columns by name to create projected schema is not yet implemented");
  } else if (context_.projected_schema != nullptr) {
    projected_schema_ = context_.projected_schema;
  } else {
    projected_schema_ = schema_;
  }

  return projected_schema_;
}

const std::vector<std::string>& TableScan::ScanColumns() const {
  return context_.return_column_stats ? kScanColumnsWithStats : kScanColumns;
}

Result<std::unique_ptr<DataTableScan>> DataTableScan::Make(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileIO> io, internal::TableScanContext context) {
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(schema != nullptr, "Schema cannot be null");
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  return std::unique_ptr<DataTableScan>(new DataTableScan(
      std::move(metadata), std::move(schema), std::move(io), std::move(context)));
}

Result<std::vector<std::shared_ptr<FileScanTask>>> DataTableScan::PlanFiles() const {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot, this->snapshot());
  if (!snapshot) {
    return std::vector<std::shared_ptr<FileScanTask>>{};
  }

  TableMetadataCache metadata_cache(metadata_.get());
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, metadata_cache.GetPartitionSpecsById());

  SnapshotCache snapshot_cache(snapshot.get());
  ICEBERG_ASSIGN_OR_RAISE(auto data_manifests, snapshot_cache.DataManifests(io_));
  ICEBERG_ASSIGN_OR_RAISE(auto delete_manifests, snapshot_cache.DeleteManifests(io_));

  ICEBERG_ASSIGN_OR_RAISE(
      auto manifest_group,
      ManifestGroup::Make(io_, schema_, specs_by_id,
                          {data_manifests.begin(), data_manifests.end()},
                          {delete_manifests.begin(), delete_manifests.end()}));
  manifest_group->CaseSensitive(context_.case_sensitive)
      .Select(ScanColumns())
      .FilterData(filter())
      .IgnoreDeleted()
      .ColumnsToKeepStats(context_.columns_to_keep_stats);
  if (context_.ignore_residuals) {
    manifest_group->IgnoreResiduals();
  }
  return manifest_group->PlanFiles();
}

// BaseIncrementalScanBuilder implementation

BaseIncrementalScanBuilder& BaseIncrementalScanBuilder::FromSnapshot(
    int64_t from_snapshot_id, bool inclusive) {
  if (inclusive) {
    // Inclusive: check snapshot exists
    ICEBERG_BUILDER_CHECK(metadata_->SnapshotById(from_snapshot_id).has_value(),
                          "Cannot find the starting snapshot: {}", from_snapshot_id);
    context_.from_snapshot_id = from_snapshot_id;
    context_.from_snapshot_id_inclusive = true;
  } else {
    // Exclusive: do not check existence (could be expired)
    context_.from_snapshot_id = from_snapshot_id;
    context_.from_snapshot_id_inclusive = false;
  }
  return *this;
}

BaseIncrementalScanBuilder& BaseIncrementalScanBuilder::FromSnapshot(
    const std::string& ref, bool inclusive) {
  auto iter = metadata_->refs.find(ref);
  ICEBERG_BUILDER_CHECK(iter != metadata_->refs.end() && iter->second != nullptr,
                        "Cannot find ref: {}", ref);
  ICEBERG_BUILDER_CHECK(iter->second->type() == SnapshotRefType::kTag,
                        "Ref {} is not a tag", ref);
  return FromSnapshot(iter->second->snapshot_id, inclusive);
}

BaseIncrementalScanBuilder& BaseIncrementalScanBuilder::ToSnapshot(
    int64_t to_snapshot_id) {
  ICEBERG_BUILDER_CHECK(metadata_->SnapshotById(to_snapshot_id).has_value(),
                        "Cannot find the end snapshot: {}", to_snapshot_id);
  context_.to_snapshot_id = to_snapshot_id;
  return *this;
}

BaseIncrementalScanBuilder& BaseIncrementalScanBuilder::ToSnapshot(
    const std::string& ref) {
  auto iter = metadata_->refs.find(ref);
  ICEBERG_BUILDER_CHECK(iter != metadata_->refs.end() && iter->second != nullptr,
                        "Cannot find ref: {}", ref);
  ICEBERG_BUILDER_CHECK(iter->second->type() == SnapshotRefType::kTag,
                        "Ref {} is not a tag", ref);
  return ToSnapshot(iter->second->snapshot_id);
}

BaseIncrementalScanBuilder& BaseIncrementalScanBuilder::UseBranch(
    const std::string& branch) {
  auto iter = metadata_->refs.find(branch);
  ICEBERG_BUILDER_CHECK(iter != metadata_->refs.end() && iter->second != nullptr,
                        "Cannot find ref: {}", branch);
  ICEBERG_BUILDER_CHECK(iter->second->type() == SnapshotRefType::kBranch,
                        "Ref {} is not a branch", branch);
  context_.branch = branch;
  return *this;
}

template <typename ScanType>
Result<std::unique_ptr<IncrementalScanBuilder<ScanType>>>
IncrementalScanBuilder<ScanType>::Make(std::shared_ptr<TableMetadata> metadata,
                                       std::shared_ptr<FileIO> io) {
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  return std::unique_ptr<IncrementalScanBuilder<ScanType>>(
      new IncrementalScanBuilder<ScanType>(std::move(metadata), std::move(io)));
}

template <typename ScanType>
Result<std::unique_ptr<ScanType>> IncrementalScanBuilder<ScanType>::Build() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  ICEBERG_RETURN_UNEXPECTED(context_.Validate());
  ICEBERG_ASSIGN_OR_RAISE(auto schema, ResolveSnapshotSchema());

  return ScanType::Make(metadata_, schema.get(), io_, context_);
}

template class IncrementalScanBuilder<IncrementalAppendScan>;
template class IncrementalScanBuilder<IncrementalChangelogScan>;

template <typename ScanTaskType>
Result<std::vector<std::shared_ptr<ScanTaskType>>>
IncrementalScan<ScanTaskType>::PlanFiles() const {
  if (context_.IsScanCurrentLineage()) {
    ICEBERG_ASSIGN_OR_RAISE(auto current_snapshot, metadata_->Snapshot());
    if (current_snapshot == nullptr) {
      return std::vector<std::shared_ptr<ScanTaskType>>{};
    }
  }

  ICEBERG_ASSIGN_OR_RAISE(int64_t to_snapshot_id_inclusive,
                          context_.ToSnapshotIdInclusive(*metadata_));
  ICEBERG_ASSIGN_OR_RAISE(
      std::optional<int64_t> from_snapshot_id_exclusive,
      context_.FromSnapshotIdExclusive(*metadata_, to_snapshot_id_inclusive));

  return PlanFiles(from_snapshot_id_exclusive, to_snapshot_id_inclusive);
}

template class IncrementalScan<FileScanTask>;
template class IncrementalScan<ChangelogScanTask>;

// IncrementalAppendScan implementation

Result<std::unique_ptr<IncrementalAppendScan>> IncrementalAppendScan::Make(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileIO> io, internal::TableScanContext context) {
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(schema != nullptr, "Schema cannot be null");
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  return std::unique_ptr<IncrementalAppendScan>(new IncrementalAppendScan(
      std::move(metadata), std::move(schema), std::move(io), std::move(context)));
}

Result<std::vector<std::shared_ptr<FileScanTask>>> IncrementalAppendScan::PlanFiles(
    std::optional<int64_t> from_snapshot_id_exclusive,
    int64_t to_snapshot_id_inclusive) const {
  ICEBERG_ASSIGN_OR_RAISE(
      auto ancestors_snapshots,
      SnapshotUtil::AncestorsBetween(*metadata_, to_snapshot_id_inclusive,
                                     from_snapshot_id_exclusive));

  std::vector<std::shared_ptr<Snapshot>> append_snapshots;
  std::ranges::copy_if(ancestors_snapshots, std::back_inserter(append_snapshots),
                       [](const auto& snapshot) {
                         return snapshot != nullptr &&
                                snapshot->Operation().has_value() &&
                                snapshot->Operation().value() == DataOperation::kAppend;
                       });
  if (append_snapshots.empty()) {
    return std::vector<std::shared_ptr<FileScanTask>>{};
  }

  std::unordered_set<int64_t> snapshot_ids;
  std::ranges::transform(append_snapshots,
                         std::inserter(snapshot_ids, snapshot_ids.end()),
                         [](const auto& snapshot) { return snapshot->snapshot_id; });

  std::vector<ManifestFile> data_manifests;
  for (const auto& snapshot : append_snapshots) {
    SnapshotCache snapshot_cache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, snapshot_cache.DataManifests(io_));
    std::ranges::copy_if(manifests, std::back_inserter(data_manifests),
                         [&snapshot_ids](const ManifestFile& manifest) {
                           return snapshot_ids.contains(manifest.added_snapshot_id);
                         });
  }
  if (data_manifests.empty()) {
    return std::vector<std::shared_ptr<FileScanTask>>{};
  }

  TableMetadataCache metadata_cache(metadata_.get());
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, metadata_cache.GetPartitionSpecsById());

  ICEBERG_ASSIGN_OR_RAISE(
      auto manifest_group,
      ManifestGroup::Make(io_, schema_, specs_by_id, std::move(data_manifests), {}));

  manifest_group->CaseSensitive(context_.case_sensitive)
      .Select(ScanColumns())
      .FilterData(filter())
      .FilterManifestEntries([&snapshot_ids](const ManifestEntry& entry) {
        return entry.snapshot_id.has_value() &&
               snapshot_ids.contains(entry.snapshot_id.value()) &&
               entry.status == ManifestStatus::kAdded;
      })
      .IgnoreDeleted()
      .ColumnsToKeepStats(context_.columns_to_keep_stats);

  if (context_.ignore_residuals) {
    manifest_group->IgnoreResiduals();
  }

  return manifest_group->PlanFiles();
}

// IncrementalChangelogScan implementation

Result<std::unique_ptr<IncrementalChangelogScan>> IncrementalChangelogScan::Make(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileIO> io, internal::TableScanContext context) {
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(schema != nullptr, "Schema cannot be null");
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  return std::unique_ptr<IncrementalChangelogScan>(new IncrementalChangelogScan(
      std::move(metadata), std::move(schema), std::move(io), std::move(context)));
}

Result<std::vector<std::shared_ptr<ChangelogScanTask>>>
IncrementalChangelogScan::PlanFiles(std::optional<int64_t> from_snapshot_id_exclusive,
                                    int64_t to_snapshot_id_inclusive) const {
  ICEBERG_ASSIGN_OR_RAISE(
      auto ancestors_snapshots,
      SnapshotUtil::AncestorsBetween(*metadata_, to_snapshot_id_inclusive,
                                     from_snapshot_id_exclusive));

  std::vector<std::pair<std::shared_ptr<Snapshot>, std::unique_ptr<SnapshotCache>>>
      changelog_snapshots;

  for (const auto& snapshot : std::ranges::reverse_view(ancestors_snapshots)) {
    auto operation = snapshot->Operation();
    if (!operation.has_value() || operation.value() != DataOperation::kReplace) {
      auto snapshot_cache = std::make_unique<SnapshotCache>(snapshot.get());
      ICEBERG_ASSIGN_OR_RAISE(auto delete_manifests,
                              snapshot_cache->DeleteManifests(io_));
      if (!delete_manifests.empty()) {
        return NotSupported(
            "Delete files are currently not supported in changelog scans");
      }
      changelog_snapshots.emplace_back(snapshot, std::move(snapshot_cache));
    }
  }
  if (changelog_snapshots.empty()) {
    return std::vector<std::shared_ptr<ChangelogScanTask>>{};
  }

  std::unordered_set<int64_t> snapshot_ids;
  std::unordered_map<int64_t, int32_t> snapshot_ordinals;
  int32_t ordinal = 0;
  for (const auto& snapshot : changelog_snapshots) {
    snapshot_ids.insert(snapshot.first->snapshot_id);
    snapshot_ordinals[snapshot.first->snapshot_id] = ordinal++;
  }

  std::vector<ManifestFile> data_manifests;
  for (const auto& snapshot : changelog_snapshots) {
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, snapshot.second->DataManifests(io_));
    std::ranges::copy_if(manifests, std::back_inserter(data_manifests),
                         [&snapshot_ids](const ManifestFile& manifest) {
                           return snapshot_ids.contains(manifest.added_snapshot_id);
                         });
  }
  if (data_manifests.empty()) {
    return std::vector<std::shared_ptr<ChangelogScanTask>>{};
  }

  TableMetadataCache metadata_cache(metadata_.get());
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, metadata_cache.GetPartitionSpecsById());

  ICEBERG_ASSIGN_OR_RAISE(
      auto manifest_group,
      ManifestGroup::Make(io_, schema_, specs_by_id, std::move(data_manifests), {}));

  manifest_group->CaseSensitive(context_.case_sensitive)
      .Select(ScanColumns())
      .FilterData(filter())
      .FilterManifestEntries([&snapshot_ids](const ManifestEntry& entry) {
        return entry.snapshot_id.has_value() &&
               snapshot_ids.contains(entry.snapshot_id.value());
      })
      .IgnoreExisting()
      .ColumnsToKeepStats(context_.columns_to_keep_stats);

  if (context_.ignore_residuals) {
    manifest_group->IgnoreResiduals();
  }

  auto create_tasks_func =
      [&snapshot_ordinals](
          std::vector<ManifestEntry>&& entries,
          const TaskContext& ctx) -> Result<std::vector<std::shared_ptr<ScanTask>>> {
    std::vector<std::shared_ptr<ScanTask>> tasks;
    tasks.reserve(entries.size());

    for (auto& entry : entries) {
      if (!entry.snapshot_id.has_value() || entry.data_file == nullptr) {
        continue;
      }

      int64_t commit_snapshot_id = entry.snapshot_id.value();
      auto ordinal_it = snapshot_ordinals.find(commit_snapshot_id);
      if (ordinal_it == snapshot_ordinals.end()) {
        continue;
      }
      int32_t change_ordinal = ordinal_it->second;

      ICEBERG_ASSIGN_OR_RAISE(auto residual,
                              ctx.residuals->ResidualFor(entry.data_file->partition));

      switch (entry.status) {
        case ManifestStatus::kAdded:
          tasks.push_back(std::make_shared<AddedRowsScanTask>(
              change_ordinal, commit_snapshot_id, std::move(entry.data_file),
              std::vector<std::shared_ptr<DataFile>>{}, std::move(residual)));
          break;
        case ManifestStatus::kDeleted:
          tasks.push_back(std::make_shared<DeletedDataFileScanTask>(
              change_ordinal, commit_snapshot_id, std::move(entry.data_file),
              std::vector<std::shared_ptr<DataFile>>{}, std::move(residual)));
          break;
        case ManifestStatus::kExisting:
          break;
      }
    }

    return tasks;
  };
  ICEBERG_ASSIGN_OR_RAISE(auto tasks, manifest_group->Plan(create_tasks_func));
  return tasks | std::views::transform([](const auto& task) {
           return std::static_pointer_cast<ChangelogScanTask>(task);
         }) |
         std::ranges::to<std::vector>();
}

}  // namespace iceberg
