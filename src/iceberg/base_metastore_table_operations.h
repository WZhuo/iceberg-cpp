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

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>

// #include "iceberg/base_metastore_operations.h"
#include "iceberg/location_provider.h"
#include "iceberg/result.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_operations.h"

namespace iceberg {

static constexpr const char* kTableTypeProp = "table_type";
static constexpr const char* kIcebergTableTypeValue = "iceberg";
static constexpr const char* kMetadataLocationProp = "metadata_location";
static constexpr const char* kPreviousMetadataLocationProp = "previous_metadata_location";

class ICEBERG_EXPORT BaseMetastoreTableOperations : public TableOperations {
 public:
  ~BaseMetastoreTableOperations() = default;

  /// \brief Get current table metadata
  /// \return Current table metadata
  Result<std::shared_ptr<TableMetadata>> Current() override;

  /// \brief Get current metadata file location
  /// \return Metadata file location
  std::string CurrentMetadataLocation() const;

  /// \brief Get current version number
  /// \return Version number
  int CurrentVersion() const;

  /// \brief Refresh table metadata
  /// \return Updated table metadata
  Result<std::shared_ptr<TableMetadata>> Refresh() override;

  /// \brief Commit changes to table metadata
  /// \param base Base metadata
  /// \param metadata New metadata
  /// \return Status of the operation
  Status Commit(const TableMetadata* base, const TableMetadata* metadata) override;

  /// \brief Write new metadata if required
  /// \param new_table Whether this is a new table
  /// \param metadata Table metadata
  /// \return Path to metadata file
  std::string WriteNewMetadataIfRequired(bool new_table, const TableMetadata& metadata);

  /// \brief Write new metadata
  /// \param metadata Table metadata
  /// \param new_version New version number
  /// \return Path to metadata file
  std::string WriteNewMetadata(const TableMetadata& metadata, int new_version);

  /// \brief Refresh from metadata location
  /// \param new_location New metadata location
  Status RefreshFromMetadataLocation(const std::string& new_location);

  /// \brief Refresh from metadata location with retry logic
  /// \param new_location New metadata location
  /// \param metadata_loader Function to load metadata
  Status RefreshFromMetadataLocation(
      const std::string& new_location,
      const std::function<Result<std::unique_ptr<TableMetadata>>(const std::string&)>&
          metadata_loader);

  /// \brief Get metadata file location
  /// \param metadata Table metadata
  /// \param filename Metadata filename
  /// \return Full path to metadata file
  std::string MetadataFileLocation(const TableMetadata* metadata,
                                   const std::string& filename) const;

  /// \brief Get metadata file location for current metadata
  /// \param filename Metadata filename
  /// \return Full path to metadata file
  std::string MetadataFileLocation(const std::string& filename) override;

  /// \brief Get location provider
  /// \return Location provider instance
  std::shared_ptr<LocationProvider> GetLocationProvider() override;

 protected:
  /// \brief Get table name for logging purposes
  /// \return Full table name
  virtual std::string table_name() const = 0;

  /// \brief Perform actual refresh operation
  virtual Status DoRefresh();

  /// \brief Perform actual commit operation
  /// \param base Base metadata
  /// \param metadata New metadata
  virtual Status DoCommit(const TableMetadata* base, const TableMetadata* metadata);

  /// \brief Request metadata refresh
  void RequestRefresh();

  /// \brief Disable automatic refresh
  void DisableRefresh();

 private:
  std::shared_ptr<TableMetadata> current_metadata_;
  std::string current_metadata_location_;
  std::atomic<bool> should_refresh_{true};
  int version_{-1};

  static constexpr std::string_view kMetadataFolderName = "metadata";

  /// \brief Generate new table metadata file path
  /// \param meta Table metadata
  /// \param new_version New version number
  /// \return Path to new metadata file
  std::string NewTableMetadataFilePath(const TableMetadata& meta, int new_version);

  /// \brief Parse version from metadata location
  /// \param metadata_location Metadata file location
  /// \return Version number or -1 if not parseable
  static int ParseVersion(const std::string& metadata_location);
};

}  // namespace iceberg
