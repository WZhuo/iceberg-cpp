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

#include <memory>
#include <string>
#include <functional>

#include "iceberg/result.h"
#include "iceberg/table_metadata.h"
#include "iceberg/location_provider.h"

namespace iceberg {

class FileIO;
class EncryptionManager;
class TableOperations;

/// \brief SPI interface to abstract table metadata access and updates.
class ICEBERG_EXPORT TableOperations {
 public:
  virtual ~TableOperations() = default;

  /// \brief Return the currently loaded table metadata, without checking for updates.
  /// \return Table metadata
  virtual Result<std::shared_ptr<TableMetadata>> Current() = 0;

  /// \brief Return the current table metadata after checking for updates.
  /// \return Table metadata
  virtual Result<std::shared_ptr<TableMetadata>> Refresh() = 0;

  /// \brief Replace the base table metadata with a new version.
  ///
  /// This method should implement and document atomicity guarantees.
  ///
  /// Implementations must check that the base metadata is current to avoid overwriting updates.
  /// Once the atomic commit operation succeeds, implementations must not perform any operations that
  /// may fail because failure in this method cannot be distinguished from commit failure.
  ///
  /// Implementations must return a CommitStateUnknownException in cases where it cannot be
  /// determined if the commit succeeded or failed. For example if a network partition causes the
  /// confirmation of the commit to be lost, the implementation should return a
  /// CommitStateUnknownException. This is important because downstream users of this API need to
  /// know whether they can clean up the commit or not, if the state is unknown then it is not safe
  /// to remove any files. All other exceptions will be treated as if the commit has failed.
  ///
  /// \param base Table metadata on which changes were based
  /// \param metadata New table metadata with updates
  /// \return Status of the operation
  virtual Status Commit(const TableMetadata* base, const TableMetadata* metadata) = 0;

  /// \brief Returns a FileIO to read and write table data and metadata files.
  /// \return FileIO instance
  virtual std::shared_ptr<FileIO> io() = 0;

  /// \brief Given the name of a metadata file, obtain the full path of that file using an appropriate base
  /// location of the implementation's choosing.
  ///
  /// The file may not exist yet, in which case the path should be returned as if it were to be
  /// created by e.g. FileIO::new_output_file().
  ///
  /// \param fileName Name of the metadata file
  /// \return Full path to the metadata file
  virtual std::string MetadataFileLocation(const std::string& fileName) = 0;

  /// \brief Returns a LocationProvider that supplies locations for new data files.
  /// \return A location provider configured for the current table state
  virtual std::shared_ptr<LocationProvider> GetLocationProvider() = 0;
};

} // namespace iceberg