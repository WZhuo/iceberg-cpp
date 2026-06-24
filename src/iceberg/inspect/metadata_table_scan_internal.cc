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

#include "iceberg/inspect/metadata_table_scan_internal.h"

#include <utility>

#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"

namespace iceberg {

InMemoryBatchReader::InMemoryBatchReader(std::shared_ptr<iceberg::Schema> schema,
                                         std::vector<ArrowArray> batches)
    : schema_(std::move(schema)), batches_(std::move(batches)) {}

InMemoryBatchReader::~InMemoryBatchReader() { std::ignore = Close(); }

Status InMemoryBatchReader::Open(const ReaderOptions& options) { return {}; }

Status InMemoryBatchReader::Close() {
  for (size_t i = next_index_; i < batches_.size(); ++i) {
    if (batches_[i].release != nullptr) {
      batches_[i].release(&batches_[i]);
    }
  }
  batches_.clear();
  next_index_ = 0;
  return {};
}

Result<std::optional<ArrowArray>> InMemoryBatchReader::Next() {
  if (next_index_ >= batches_.size()) {
    return std::nullopt;
  }
  ArrowArray array = batches_[next_index_];
  batches_[next_index_].release = nullptr;
  ++next_index_;
  return array;
}

Result<ArrowSchema> InMemoryBatchReader::Schema() {
  ArrowSchema arrow_schema;
  ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*schema_, &arrow_schema));
  return arrow_schema;
}

Result<std::unordered_map<std::string, std::string>> InMemoryBatchReader::Metadata() {
  return std::unordered_map<std::string, std::string>{};
}

}  // namespace iceberg
