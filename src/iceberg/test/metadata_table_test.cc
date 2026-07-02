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

#include "iceberg/inspect/metadata_table.h"

#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/type.h"

namespace iceberg {
namespace {

std::shared_ptr<Schema> MakeSnapshotsSchema() {
  return std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "committed_at", timestamp_tz()),
      SchemaField::MakeRequired(2, "snapshot_id", int64()),
      SchemaField::MakeOptional(3, "parent_id", int64()),
      SchemaField::MakeOptional(4, "operation", string()),
      SchemaField::MakeOptional(5, "manifest_list", string()),
      SchemaField::MakeOptional(
          6, "summary",
          std::make_shared<MapType>(SchemaField::MakeRequired(7, "key", string()),
                                    SchemaField::MakeRequired(8, "value", string())))});
}

std::shared_ptr<Schema> MakeHistorySchema() {
  return std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "made_current_at", timestamp_tz()),
      SchemaField::MakeRequired(2, "snapshot_id", int64()),
      SchemaField::MakeOptional(3, "parent_id", int64()),
      SchemaField::MakeRequired(4, "is_current_ancestor", boolean())});
}

}  // namespace

class MetadataTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    io_ = std::make_shared<MockFileIO>();
    catalog_ = std::make_shared<MockCatalog>();

    auto schema = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                                 SchemaField::MakeOptional(2, "name", string())},
        1);
    metadata_ = std::make_shared<TableMetadata>(
        TableMetadata{.format_version = 2, .schemas = {schema}, .current_schema_id = 1});

    TableIdentifier source_ident{.ns = Namespace{.levels = {"db"}},
                                 .name = "source_table"};
    auto source_table_result =
        Table::Make(source_ident, metadata_, "s3://bucket/meta.json", io_, catalog_);
    EXPECT_THAT(source_table_result, IsOk());
    source_table_ = *source_table_result;

    auto snapshots_table_result =
        MetadataTable::Make(source_table_, MetadataTable::Kind::kSnapshots);
    EXPECT_THAT(snapshots_table_result, IsOk());
    snapshots_table_ = std::move(*snapshots_table_result);
  }

  std::shared_ptr<MockFileIO> io_;
  std::shared_ptr<MockCatalog> catalog_;
  std::shared_ptr<TableMetadata> metadata_;
  std::shared_ptr<Table> source_table_;
  std::unique_ptr<MetadataTable> snapshots_table_;
};

TEST_F(MetadataTableTest, Constructor) {
  EXPECT_EQ(snapshots_table_->kind(), MetadataTable::Kind::kSnapshots);
  EXPECT_EQ(snapshots_table_->source_table(), source_table_);
  EXPECT_EQ(snapshots_table_->name().name, "source_table.snapshots");
  EXPECT_EQ(snapshots_table_->name().ns.levels, (std::vector<std::string>{"db"}));
  EXPECT_NE(snapshots_table_->schema(), nullptr);
}

TEST_F(MetadataTableTest, SnapshotsSchemaMatchesIcebergSchema) {
  EXPECT_TRUE(*snapshots_table_->schema() == *MakeSnapshotsSchema());
}

TEST_F(MetadataTableTest, HistorySchemaMatchesIcebergSchema) {
  auto history_table_result =
      MetadataTable::Make(source_table_, MetadataTable::Kind::kHistory);
  ASSERT_THAT(history_table_result, IsOk());

  EXPECT_TRUE(*(*history_table_result)->schema() == *MakeHistorySchema());
}

TEST_F(MetadataTableTest, FactoryRejectsNullSourceTable) {
  auto result = MetadataTable::Make(nullptr, MetadataTable::Kind::kSnapshots);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Table cannot be null"));
}

TEST_F(MetadataTableTest, AllKindEnumValues) {
  // Verify all 16 Kind enum values are present and distinct.
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kEntries), 0);
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kFiles), 1);
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kDataFiles), 2);
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kDeleteFiles), 3);
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kHistory), 4);
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kMetadataLogEntries), 5);
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kSnapshots), 6);
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kRefs), 7);
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kManifests), 8);
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kPartitions), 9);
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kAllDataFiles), 10);
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kAllDeleteFiles), 11);
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kAllFiles), 12);
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kAllManifests), 13);
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kAllEntries), 14);
  EXPECT_EQ(static_cast<int>(MetadataTable::Kind::kPositionDeletes), 15);
}

TEST_F(MetadataTableTest, UnimplementedKindsReturnNotSupported) {
  // All new (not-yet-implemented) kinds should return NotSupported from the factory.
  std::vector<MetadataTable::Kind> unimplemented = {
      MetadataTable::Kind::kEntries,
      MetadataTable::Kind::kFiles,
      MetadataTable::Kind::kDataFiles,
      MetadataTable::Kind::kDeleteFiles,
      MetadataTable::Kind::kMetadataLogEntries,
      MetadataTable::Kind::kRefs,
      MetadataTable::Kind::kManifests,
      MetadataTable::Kind::kPartitions,
      MetadataTable::Kind::kAllDataFiles,
      MetadataTable::Kind::kAllDeleteFiles,
      MetadataTable::Kind::kAllFiles,
      MetadataTable::Kind::kAllManifests,
      MetadataTable::Kind::kAllEntries,
      MetadataTable::Kind::kPositionDeletes,
  };

  for (auto kind : unimplemented) {
    auto result = MetadataTable::Make(source_table_, kind);
    EXPECT_THAT(result, IsError(ErrorKind::kNotSupported))
        << "Kind " << static_cast<int>(kind) << " should return NotSupported";
  }
}

TEST_F(MetadataTableTest, DefaultSupportsTimeTravelReturnsFalse) {
  // The base class default implementation should return false.
  EXPECT_FALSE(snapshots_table_->supports_time_travel());
}

TEST_F(MetadataTableTest, DefaultScanReturnsNotSupported) {
  // The base class default Scan() should return NotSupported.
  auto result = snapshots_table_->Scan(std::nullopt);
  EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
}

}  // namespace iceberg
