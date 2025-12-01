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

#include <string>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief CatalogUtil provides utility methods for interacting with catalogs.
///
/// This class contains static utility methods for common catalog operations
/// such as parsing metadata versions, generating metadata file paths,
/// writing metadata files, and cleaning up obsolete metadata files.
class ICEBERG_EXPORT CatalogUtil {
 public:
  /// \brief Write a new metadata file to storage.
  ///
  /// Serializes the table metadata to JSON and writes it to a new metadata
  /// file. If no location is specified in the metadata, generates a new
  /// file path based on the version number.
  ///
  /// \param io The FileIO instance for writing files
  /// \param base The base metadata (can be null for new tables)
  /// \param metadata The metadata to write
  /// \return The location of the written metadata file, or an error if writing fails
  static Result<std::string> WriteNewMetadataFile(std::shared_ptr<FileIO> io,
                                                  const TableMetadata* base,
                                                  const TableMetadata* metadata);

  /// \brief Delete removed metadata files based on retention policy.
  ///
  /// Removes obsolete metadata files that are no longer referenced in the
  /// current metadata log, based on the metadata.delete-after-commit.enabled
  /// property.
  ///
  /// \param io The FileIO instance for deleting files
  /// \param base The previous metadata version
  /// \param metadata The current metadata containing the updated log
  static void DeleteRemovedMetadataFiles(std::shared_ptr<FileIO> io,
                                         const TableMetadata* base,
                                         const TableMetadata* metadata);

 private:
  /// \brief Parse the version number from a metadata file location.
  ///
  /// Extracts the version number from a metadata file path which follows
  /// the format: vvvvv-uuid.metadata.json where vvvvv is the zero-padded
  /// version number.
  ///
  /// \param metadata_location The metadata file location string
  /// \return The parsed version number, or -1 if parsing fails or the
  ///         location doesn't contain a version
  static int ParseVersionFromLocation(const std::string& metadata_location);

  /// \brief Generate a new metadata file path for a table.
  ///
  /// Creates a new metadata file path with the appropriate naming convention
  /// including version number, UUID, and file extension based on compression
  /// settings.
  ///
  /// \param meta The table metadata containing properties and location info
  /// \param new_version The version number for the new metadata file
  /// \return The generated metadata file path, or an error if generation fails
  static Result<std::string> NewTableMetadataFilePath(const TableMetadata& meta,
                                                      int new_version);
};

}  // namespace iceberg
