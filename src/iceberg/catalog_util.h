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

#include "iceberg/file_io.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"

namespace iceberg {

/// \brief A Catalog API for table create, drop, and load operations.
///
/// Note that these functions are named after the corresponding operationId
/// specified by the Iceberg Rest Catalog API.
class ICEBERG_EXPORT CatalogUtil {
 public:
  static void DeleteRemovedMetadataFiles(std::shared_ptr<FileIO> io,
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
};

}  // namespace iceberg
