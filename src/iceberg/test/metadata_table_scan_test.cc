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

#include <memory>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/file_reader.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/type.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {
namespace {

std::shared_ptr<Snapshot> MakeSnapshot(int64_t snapshot_id,
                                       std::optional<int64_t> parent_id,
                                       int64_t sequence_number, int64_t timestamp_unix_ms,
                                       std::string operation, std::string manifest_list) {
  return std::make_shared<Snapshot>(Snapshot{
      .snapshot_id = snapshot_id,
      .parent_snapshot_id = parent_id,
      .sequence_number = sequence_number,
      .timestamp_ms = TimePointMsFromUnixMs(timestamp_unix_ms),
      .manifest_list = std::move(manifest_list),
      .summary = {{SnapshotSummaryFields::kOperation, operation},
                  {SnapshotSummaryFields::kAddedDataFiles, "1"}},
      .schema_id = 1,
  });
}

}  // namespace

class SnapshotsTableScanTest : public ::testing::Test {
 protected:
  std::shared_ptr<Table> MakeTable(std::vector<std::shared_ptr<Snapshot>> snapshots,
                                   int64_t current_snapshot_id) {
    auto io = std::make_shared<MockFileIO>();
    auto catalog = std::make_shared<MockCatalog>();

    auto schema = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                                 SchemaField::MakeOptional(2, "name", string())},
        1);
    auto metadata = std::make_shared<TableMetadata>(
        TableMetadata{.format_version = 2,
                      .schemas = {schema},
                      .current_schema_id = 1,
                      .current_snapshot_id = current_snapshot_id,
                      .snapshots = std::move(snapshots)});

    TableIdentifier ident{.ns = Namespace{.levels = {"db"}}, .name = "source_table"};
    auto table_result =
        Table::Make(ident, metadata, "s3://bucket/meta.json", io, catalog);
    EXPECT_THAT(table_result, IsOk());
    return *table_result;
  }
};

TEST_F(SnapshotsTableScanTest, EmitsRowsMatchingSnapshots) {
  auto table = MakeTable(
      {MakeSnapshot(1, std::nullopt, 1, 1000, DataOperation::kAppend, "s3://snap-1.avro"),
       MakeSnapshot(2, 1, 2, 2000, DataOperation::kDelete, "s3://snap-2.avro")},
      /*current_snapshot_id=*/2);

  auto metadata_table_result =
      MetadataTable::Make(table, MetadataTable::Kind::kSnapshots);
  ASSERT_THAT(metadata_table_result, IsOk());
  auto& metadata_table = *metadata_table_result;

  auto reader_result = metadata_table->Scan();
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(*reader_result);
  ASSERT_THAT(reader->Open(ReaderOptions{}), IsOk());

  auto schema_result = reader->Schema();
  ASSERT_THAT(schema_result, IsOk());
  ArrowSchema c_schema = std::move(*schema_result);
  auto arrow_schema = ::arrow::ImportSchema(&c_schema).ValueOrDie();

  auto next_result = reader->Next();
  ASSERT_THAT(next_result, IsOk());
  ASSERT_TRUE(next_result->has_value());
  ArrowArray c_array = **next_result;
  auto batch = ::arrow::ImportRecordBatch(&c_array, arrow_schema).ValueOrDie();

  ASSERT_EQ(batch->num_rows(), 2);
  ASSERT_EQ(batch->num_columns(), 6);

  // committed_at (timestamp, microseconds)
  auto committed_at = std::static_pointer_cast<::arrow::TimestampArray>(batch->column(0));
  EXPECT_EQ(committed_at->Value(0), 1000 * 1000);
  EXPECT_EQ(committed_at->Value(1), 2000 * 1000);

  // snapshot_id
  auto snapshot_id = std::static_pointer_cast<::arrow::Int64Array>(batch->column(1));
  EXPECT_EQ(snapshot_id->Value(0), 1);
  EXPECT_EQ(snapshot_id->Value(1), 2);

  // parent_id (optional)
  auto parent_id = std::static_pointer_cast<::arrow::Int64Array>(batch->column(2));
  EXPECT_TRUE(parent_id->IsNull(0));
  EXPECT_EQ(parent_id->Value(1), 1);

  // operation
  auto operation = std::static_pointer_cast<::arrow::StringArray>(batch->column(3));
  EXPECT_EQ(operation->GetString(0), DataOperation::kAppend);
  EXPECT_EQ(operation->GetString(1), DataOperation::kDelete);

  // manifest_list
  auto manifest_list = std::static_pointer_cast<::arrow::StringArray>(batch->column(4));
  EXPECT_EQ(manifest_list->GetString(0), "s3://snap-1.avro");
  EXPECT_EQ(manifest_list->GetString(1), "s3://snap-2.avro");

  // summary (map<string, string>) has two entries per row.
  auto summary = std::static_pointer_cast<::arrow::MapArray>(batch->column(5));
  EXPECT_EQ(summary->value_length(0), 2);
  EXPECT_EQ(summary->value_length(1), 2);

  // The stream is exhausted after the single batch.
  auto exhausted = reader->Next();
  ASSERT_THAT(exhausted, IsOk());
  EXPECT_FALSE(exhausted->has_value());

  ASSERT_THAT(reader->Close(), IsOk());
}

TEST_F(SnapshotsTableScanTest, EmptySnapshotsYieldsNoBatches) {
  auto table = MakeTable({}, /*current_snapshot_id=*/-1);

  auto metadata_table_result =
      MetadataTable::Make(table, MetadataTable::Kind::kSnapshots);
  ASSERT_THAT(metadata_table_result, IsOk());

  auto reader_result = (*metadata_table_result)->Scan();
  ASSERT_THAT(reader_result, IsOk());
  auto reader = std::move(*reader_result);
  ASSERT_THAT(reader->Open(ReaderOptions{}), IsOk());

  // Schema is still available even when there are no rows.
  auto schema_result = reader->Schema();
  ASSERT_THAT(schema_result, IsOk());
  ArrowSchema c_schema = std::move(*schema_result);
  c_schema.release(&c_schema);

  auto next_result = reader->Next();
  ASSERT_THAT(next_result, IsOk());
  EXPECT_FALSE(next_result->has_value());

  ASSERT_THAT(reader->Close(), IsOk());
}

TEST_F(SnapshotsTableScanTest, HistoryScanIsNotImplemented) {
  auto table = MakeTable({MakeSnapshot(1, std::nullopt, 1, 1000, DataOperation::kAppend,
                                       "s3://snap-1.avro")},
                         /*current_snapshot_id=*/1);

  auto history_result = MetadataTable::Make(table, MetadataTable::Kind::kHistory);
  ASSERT_THAT(history_result, IsOk());

  auto reader_result = (*history_result)->Scan();
  EXPECT_THAT(reader_result, IsError(ErrorKind::kNotImplemented));
}

}  // namespace iceberg
