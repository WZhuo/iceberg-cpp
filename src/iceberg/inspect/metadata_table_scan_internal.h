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

/// \file iceberg/inspect/metadata_table_scan_internal.h
/// Internal helpers backing metadata-table row production.

#include <memory>
#include <optional>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/file_reader.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief A `Reader` that serves pre-materialized Arrow batches from memory.
///
/// Metadata tables read directly from in-memory `TableMetadata`/snapshot
/// structures, so their rows are built eagerly and handed to this reader. This
/// keeps the metadata-table row path on the same `Reader` contract used by the
/// data path, so callers can consume batches the same way.
class ICEBERG_EXPORT InMemoryBatchReader : public Reader {
 public:
  /// \brief Create an in-memory reader.
  ///
  /// \param schema The Iceberg schema describing every batch.
  /// \param batches Owned Arrow batches to emit in order. Ownership of each
  ///   batch transfers to the reader.
  InMemoryBatchReader(std::shared_ptr<iceberg::Schema> schema,
                      std::vector<ArrowArray> batches);

  ~InMemoryBatchReader() override;

  Status Open(const ReaderOptions& options) override;

  Status Close() override;

  Result<std::optional<ArrowArray>> Next() override;

  Result<ArrowSchema> Schema() override;

  Result<std::unordered_map<std::string, std::string>> Metadata() override;

 private:
  std::shared_ptr<iceberg::Schema> schema_;
  std::vector<ArrowArray> batches_;
  size_t next_index_ = 0;
};

}  // namespace iceberg
