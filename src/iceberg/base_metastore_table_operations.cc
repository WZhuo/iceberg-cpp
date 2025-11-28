// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "iceberg/base_metastore_table_operations.h"

#include <format>
#include <memory>
#include <string>

#include "iceberg/catalog_util.h"
#include "iceberg/result.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/location_util.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::shared_ptr<TableMetadata>> BaseMetastoreTableOperations::Current() {
  if (should_refresh_.load()) {
    return Refresh();
  }
  return current_metadata_;
}

std::string BaseMetastoreTableOperations::CurrentMetadataLocation() const {
  return current_metadata_location_;
}

int BaseMetastoreTableOperations::CurrentVersion() const { return version_; }

Result<std::shared_ptr<TableMetadata>> BaseMetastoreTableOperations::Refresh() {
  bool current_metadata_was_available = (current_metadata_ != nullptr);
  auto status = DoRefresh();
  if (!status.has_value()) {
    should_refresh_ = true;

    current_metadata_ = nullptr;
    current_metadata_location_ = "";
    version_ = -1;
    return std::unexpected<Error>(status.error());
  }
  return Current();
}

Status BaseMetastoreTableOperations::Commit(const TableMetadata* base,
                                            const TableMetadata* metadata) {
  if (metadata == nullptr) {
    // Nothing to commit
    return {};
  }

  // If the metadata is already out of date, reject it
  auto current_metadata = Current().value_or(nullptr);
  if (base != nullptr) {
    if (current_metadata == nullptr || *base != *current_metadata) {
      return CommitFailed("Cannot commit: stale table metadata");
    } else if (*base == *metadata) {
      // Nothing to commit
      return {};
    }
  } else if (current_metadata != nullptr) {
    return AlreadyExists("Table already exists: {}", table_name());
  }

  ICEBERG_RETURN_UNEXPECTED(DoCommit(base, metadata));

  CatalogUtil::DeleteRemovedMetadataFiles(io(), base, metadata);
  RequestRefresh();
  return {};
}

std::string BaseMetastoreTableOperations::WriteNewMetadataIfRequired(
    bool new_table, const TableMetadata& metadata) {
  return new_table && !metadata.location.empty()
             ? metadata.location
             : WriteNewMetadata(metadata, CurrentVersion() + 1);
}

std::string BaseMetastoreTableOperations::WriteNewMetadata(const TableMetadata& metadata,
                                                           int new_version) {
  std::string new_location = NewTableMetadataFilePath(metadata, new_version);
  TableMetadataUtil::Write(*io(), new_location, metadata);
  return new_location;
}

Status BaseMetastoreTableOperations::RefreshFromMetadataLocation(
    const std::string& new_location) {
  return RefreshFromMetadataLocation(
      new_location,
      [&](const std::string& location) -> Result<std::unique_ptr<TableMetadata>> {
        return TableMetadataUtil::Read(*io(), location);
      });
}

Status BaseMetastoreTableOperations::RefreshFromMetadataLocation(
    const std::string& new_location,
    const std::function<Result<std::unique_ptr<TableMetadata>>(const std::string&)>&
        metadata_loader) {
  if (current_metadata_location_ != new_location) {
    auto result = metadata_loader(new_location);
    if (result.has_value()) {
      std::shared_ptr<TableMetadata> new_metadata = std::move(result.value());

      if (current_metadata_ &&
          current_metadata_->table_uuid != new_metadata->table_uuid) {
        return InvalidArgument("Table UUID does not match: current={} != refreshed={}",
                               current_metadata_->table_uuid, new_metadata->table_uuid);
      }

      current_metadata_ = new_metadata;
      current_metadata_location_ = new_location;
      version_ = ParseVersion(new_location);
    }
  }
  should_refresh_.store(false);
  return {};
}

std::string BaseMetastoreTableOperations::MetadataFileLocation(
    const TableMetadata* metadata, const std::string& filename) const {
  // if the metadata has a write.metadata.location property, use that
  auto it = metadata->properties.find("write.metadata.location");
  if (it != metadata->properties.end()) {
    return std::format("{}/{}", LocationUtil::StripTrailingSlash(it->second), filename);
  } else {
    return std::format("{}/{}/{}", metadata->location, kMetadataFolderName, filename);
  }
}

std::string BaseMetastoreTableOperations::MetadataFileLocation(
    const std::string& filename) {
  return MetadataFileLocation(Current().value_or(nullptr).get(), filename);
}

std::shared_ptr<LocationProvider> BaseMetastoreTableOperations::GetLocationProvider() {
  // TODO(zhuo.wang) impl location provider
  return nullptr;
}

Status BaseMetastoreTableOperations::DoRefresh() {
  return NotImplemented("Not implemented: DoRefresh");
}

Status BaseMetastoreTableOperations::DoCommit(const TableMetadata* /*base*/,
                                              const TableMetadata* /*metadata*/) {
  return NotImplemented("Not implemented: DoCommit");
}

void BaseMetastoreTableOperations::RequestRefresh() { should_refresh_.store(true); }

void BaseMetastoreTableOperations::DisableRefresh() { should_refresh_.store(false); }

std::string BaseMetastoreTableOperations::NewTableMetadataFilePath(
    const TableMetadata& meta, int new_version) {
  std::string codec_name = "none";
  auto it = meta.properties.find("metadata.compression");
  if (it != meta.properties.end()) {
    codec_name = it->second;
  }

  // TODO(zhuo.wang) Impl TableMetadataParser
  std::string file_extension = "";  // TableMetadataParser::GetFileExtension(codec_name);

  // In a real implementation, you'd generate a UUID
  std::string uuid = "temp-uuid";

  return MetadataFileLocation(
      &meta, std::format("{:05d}-{}.{}", new_version, uuid, file_extension));
}

int BaseMetastoreTableOperations::ParseVersion(const std::string& metadata_location) {
  size_t version_start = metadata_location.find_last_of('/') + 1;
  size_t version_end = metadata_location.find('-', version_start);

  if (version_end == std::string::npos) {
    // found filesystem table's metadata
    return -1;
  }

  try {
    return std::stoi(
        metadata_location.substr(version_start, version_end - version_start));
  } catch (const std::exception& e) {
    // Unable to parse version from metadata location
    return -1;
  }
}

}  // namespace iceberg