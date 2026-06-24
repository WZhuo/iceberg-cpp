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

#include "iceberg/inspect/snapshots_table.h"

#include <memory>
#include <utility>
#include <vector>

#include "iceberg/file_reader.h"
#include "iceberg/inspect/metadata_table_scan_internal.h"
#include "iceberg/inspect/row_builder_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {
namespace {

std::shared_ptr<Schema> MakeSnapshotsTableSchema() {
  return std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "committed_at", timestamp_tz()),
      SchemaField::MakeRequired(2, "snapshot_id", int64()),
      SchemaField::MakeOptional(3, "parent_id", int64()),
      SchemaField::MakeOptional(4, "operation", string()),
      SchemaField::MakeOptional(5, "manifest_list", string()),
      SchemaField::MakeOptional(6, "summary",
                                std::make_shared<iceberg::MapType>(
                                    SchemaField::MakeRequired(7, "key", string()),
                                    SchemaField::MakeRequired(8, "value", string())))});
}

TableIdentifier MakeSnapshotsTableName(const TableIdentifier& source_name) {
  return TableIdentifier{.ns = source_name.ns, .name = source_name.name + ".snapshots"};
}

}  // namespace

SnapshotsTable::SnapshotsTable(std::shared_ptr<Table> table)
    : MetadataTable(table, MakeSnapshotsTableName(table->name()),
                    MakeSnapshotsTableSchema()) {}

SnapshotsTable::~SnapshotsTable() = default;

Result<std::unique_ptr<SnapshotsTable>> SnapshotsTable::Make(
    std::shared_ptr<Table> table) {
  if (table == nullptr) [[unlikely]] {
    return InvalidArgument("Table cannot be null");
  }
  return std::unique_ptr<SnapshotsTable>(new SnapshotsTable(std::move(table)));
}

Result<std::unique_ptr<Reader>> SnapshotsTable::Scan() const {
  // Column order matches MakeSnapshotsTableSchema():
  //   0: committed_at, 1: snapshot_id, 2: parent_id, 3: operation,
  //   4: manifest_list, 5: summary
  ICEBERG_ASSIGN_OR_RAISE(auto builder, ArrowRowBuilder::Make(*schema()));

  const auto& snapshots = source_table()->snapshots();
  for (const auto& snapshot : snapshots) {
    // committed_at: timestamp stored in microseconds.
    const int64_t committed_at_us = UnixMsFromTimePointMs(snapshot->timestamp_ms) * 1000;
    ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(0), committed_at_us));

    // snapshot_id
    ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(1), snapshot->snapshot_id));

    // parent_id (optional)
    if (snapshot->parent_snapshot_id.has_value()) {
      ICEBERG_RETURN_UNEXPECTED(
          AppendInt(builder.column(2), snapshot->parent_snapshot_id.value()));
    } else {
      ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(2)));
    }

    // operation (optional)
    if (auto operation = snapshot->Operation(); operation.has_value()) {
      ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(3), operation.value()));
    } else {
      ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(3)));
    }

    // manifest_list (optional)
    if (snapshot->manifest_list.empty()) {
      ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(4)));
    } else {
      ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(4), snapshot->manifest_list));
    }

    // summary (map<string, string>)
    ICEBERG_RETURN_UNEXPECTED(AppendStringMap(builder.column(5), snapshot->summary));

    ICEBERG_RETURN_UNEXPECTED(builder.FinishRow());
  }

  std::vector<ArrowArray> batches;
  if (!snapshots.empty()) {
    ICEBERG_ASSIGN_OR_RAISE(auto batch, std::move(builder).Finish());
    batches.push_back(batch);
  }

  return std::make_unique<InMemoryBatchReader>(schema(), std::move(batches));
}

}  // namespace iceberg
