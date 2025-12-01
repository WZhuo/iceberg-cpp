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

#include "iceberg/catalog_util.h"

#include "iceberg/file_io.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/util/location_util.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

static constexpr std::string_view kMetadataFolderName = "metadata";

Result<std::string> CatalogUtil::WriteNewMetadataFile(std::shared_ptr<FileIO> io,
                                                      const TableMetadata* base,
                                                      const TableMetadata* metadata) {
  ICEBERG_CHECK(metadata != nullptr, "The metadata is nullptr.");

  int version = -1;
  if (base != nullptr && !base->location.empty()) {
    // parse current version from location
    version = ParseVersionFromLocation(base->location);
  }

  std::string new_location = metadata->location;
  if (new_location.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(
        new_location, CatalogUtil::NewTableMetadataFilePath(*metadata, version + 1));
  }
  ICEBERG_RETURN_UNEXPECTED(TableMetadataUtil::Write(*io, new_location, *metadata));
  return new_location;
}

void CatalogUtil::DeleteRemovedMetadataFiles(std::shared_ptr<FileIO> io,
                                             const TableMetadata* base,
                                             const TableMetadata* metadata) {
  if (!base) {
    return;
  }

  auto properties = TableProperties::FromMap(metadata->properties);
  bool delete_after_commit =
      properties->Get(TableProperties::kMetadataDeleteAfterCommitEnabled);
  if (delete_after_commit) {
    // Create a set of previous metadata files from base
    std::unordered_set<MetadataLogEntry, MetadataLogEntry::Hasher>
        removed_previous_metadata_files(base->metadata_log.begin(),
                                        base->metadata_log.end());

    // TableMetadata#addPreviousFile builds up the metadata log and uses
    // TableProperties.METADATA_PREVIOUS_VERSIONS_MAX to determine how many files should
    // stay in the log, thus we don't include metadata.previousFiles() for deletion -
    // everything else can be removed
    for (const auto& entry : metadata->metadata_log) {
      removed_previous_metadata_files.erase(entry);
    }

    // TODO(zhuo.wang) bulk delete files

    for (const auto& entry : removed_previous_metadata_files) {
      auto status = io->DeleteFile(entry.metadata_file);
      // TODO(zhuo.wang) log error
    }
  }
}

int CatalogUtil::ParseVersionFromLocation(const std::string& metadata_location) {
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

Result<std::string> CatalogUtil::NewTableMetadataFilePath(const TableMetadata& meta,
                                                          int new_version) {
  std::string codec_name = "none";
  auto it = meta.properties.find("metadata.compression");
  if (it != meta.properties.end()) {
    codec_name = it->second;
  }
  ICEBERG_ASSIGN_OR_RAISE(std::string file_extension,
                          TableMetadataUtil::CodecNameToFileExtension(codec_name));

  std::string uuid = Uuid::GenerateV7().ToString();
  std::string filename = std::format("{:05d}-{}.{}", new_version, uuid, file_extension);

  it = meta.properties.find("write.metadata.location");
  if (it != meta.properties.end()) {
    return std::format("{}/{}", LocationUtil::StripTrailingSlash(it->second), filename);
  } else {
    return std::format("{}/{}/{}", meta.location, kMetadataFolderName, filename);
  }
}

}  // namespace iceberg
