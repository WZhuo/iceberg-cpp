# Iceberg C++ Inspect Feature — Implementation Plan

> **Status:** Draft
> **Date:** 2026-07-02
> **Reference:** Java Iceberg implementation at `/Users/wz/Code/iceberg`
> **Target directory:** `src/iceberg/inspect/`

---

## 1. Overview

### 1.1 What Is "Inspect"?

In Apache Iceberg, the **inspect** feature exposes table metadata as queryable virtual tables — called **Metadata Tables**. Users (and tools) can inspect a table’s history, snapshots, file manifests, partition statistics, references, and more through a consistent read-only table interface.

In the Java implementation, there is no class named "InspectTable." Instead, the feature is implemented through:

- **`MetadataTableType`** — an enum of 16 metadata table types
- **`MetadataTableUtils`** — a factory that creates concrete metadata table instances
- **`BaseMetadataTable`** — an abstract base class (extends `BaseReadOnlyTable`, which implements `Table`)
- **Concrete table classes** — one per metadata table type (e.g., `SnapshotsTable`, `HistoryTable`, `FilesTable`, etc.)

The Python Iceberg client exposes this as `table.inspect.snapshots()`, `table.inspect.files()`, etc., but the underlying mechanism is the same metadata table abstraction.

### 1.2 Current State (iceberg-cpp)

The C++ codebase currently has a minimal stub implementation:

| File | Purpose |
|------|---------|
| `inspect/metadata_table.h/.cc` | Base class with `Kind` enum (2 values: `kSnapshots`, `kHistory`), factory, name/schema/source accessors |
| `inspect/snapshots_table.h/.cc` | Schema-only: defines 6-column schema; **no data scanning** |
| `inspect/history_table.h/.cc` | Schema-only: defines 4-column schema; **no data scanning** |

**What's missing:**
- Only 2 of 16 metadata table types are defined
- Existing stubs have no row-scanning/data-iteration capability
- No catalog integration (metadata table name resolution)
- No tests beyond basic constructor/schema checks

### 1.3 Target State

Implement all 16 metadata table types consistent with the Java reference, with:
- Complete schema definitions matching Java exactly
- Data scanning that produces rows from the underlying table metadata and manifest files
- Integration with the existing `Table`, `Snapshot`, `ManifestReader`, and catalog infrastructure
- Comprehensive tests

---

## 2. Architecture Design

### 2.1 Class Hierarchy

Following the Java design, but adapted to C++ idioms:

```
MetadataTable (abstract base)
  ├── Kind enum: all 16 types
  ├── name(), schema(), source_table(), kind()
  ├── supports_time_travel() → bool [NEW]
  ├── Scan() → Result<vector<unique_ptr<StructLike>>> [NEW]
  │
  ├── SnapshotsTable          (static data: table.snapshots())
  ├── HistoryTable            (static data: table.history() + ancestor IDs)
  ├── RefsTable               (static data: table.refs())
  ├── MetadataLogEntriesTable (static data: metadata_log + previous files)
  ├── ManifestsTable          (static-like: snapshot's manifest list, time-travel-aware)
  ├── AllManifestsTable       (static-like: all snapshots' manifest lists)
  ├── PartitionsTable         (aggregated from manifests, time-travel-aware)
  │
  ├── BaseFilesTable (abstract)    [NEW — shared by 6 file-level tables]
  │   ├── FilesTable               (data+delete, current snapshot)
  │   ├── DataFilesTable           (data only, current snapshot)
  │   ├── DeleteFilesTable         (delete only, current snapshot)
  │   ├── AllFilesTable            (data+delete, all snapshots)
  │   ├── AllDataFilesTable        (data only, all snapshots)
  │   └── AllDeleteFilesTable      (delete only, all snapshots)
  │
  ├── BaseEntriesTable (abstract)  [NEW — shared by 2 entry-level tables]
  │   ├── ManifestEntriesTable     (current snapshot entries)
  │   └── AllEntriesTable          (all snapshots entries)
  │
  └── PositionDeletesTable    (position delete file content)
```

**Key design decisions:**

1. **`MetadataTable` does NOT extend `Table`.** In Java, metadata tables are full `Table` implementations so they work in Spark SQL. In C++, the `Table` class has many mutation methods (`NewAppend()`, `NewDelete()`, etc.) that don't apply. Keeping `MetadataTable` separate avoids the `BaseReadOnlyTable` layer and keeps the API clean. If full `Table` integration is needed later, a wrapper/adapter can be added.

2. **`Scan()` returns `Result<std::vector<std::unique_ptr<StructLike>>>`.** This is a batch-oriented API that reads all rows at once. A future iteration could add lazy/streaming iteration, but batch is simpler and matches the current `ManifestReader::Entries()` pattern.

3. **Base classes for shared scanning logic.** `BaseFilesTable` and `BaseEntriesTable` encapsulate manifest reading, projection, and filtering shared by the 6 file-level and 2 entry-level tables respectively.

### 2.2 Scanning Patterns

There are three distinct patterns for producing rows:

#### Pattern A: Static In-Memory Scan
Used by: `SnapshotsTable`, `HistoryTable`, `RefsTable`, `MetadataLogEntriesTable`, `ManifestsTable`, `AllManifestsTable`

These tables iterate over in-memory data structures (`table.snapshots()`, `table.history()`, `table.metadata()->refs`, `table.metadata()->metadata_log`) or read manifest lists and produce rows directly. No manifest entry reading is needed.

```cpp
Result<std::vector<std::unique_ptr<StructLike>>> Scan() override {
  std::vector<std::unique_ptr<StructLike>> rows;
  for (const auto& snapshot : source_table()->snapshots()) {
    rows.push_back(MakeSnapshotRow(*snapshot));
  }
  return rows;
}
```

#### Pattern B: Manifest Entry Scan
Used by: `FilesTable`, `DataFilesTable`, `DeleteFilesTable`, `ManifestEntriesTable`

These tables read manifest entries from the current snapshot's manifests, producing one row per `DataFile` or `ManifestEntry`. They support column projection for performance.

```cpp
Result<std::vector<std::unique_ptr<StructLike>>> Scan() override {
  auto snapshot = /* resolve current snapshot */;
  auto manifests = snapshot->DataManifests(io);  // or AllManifests, DeleteManifests
  std::vector<std::unique_ptr<StructLike>> rows;
  for (const auto& manifest : manifests) {
    auto reader = ManifestReader::Make(manifest, io, schema, spec);
    for (const auto& entry : reader->Entries()) {
      rows.push_back(MakeRow(entry));
    }
  }
  return rows;
}
```

#### Pattern C: All-* Manifest Entry Scan (Cross-Snapshot)
Used by: `AllFilesTable`, `AllDataFilesTable`, `AllDeleteFilesTable`, `AllEntriesTable`, `AllManifestsTable`

These iterate over ALL snapshots, deduplicate manifests (by path), and read entries from each unique manifest.

#### Pattern D: Aggregated Partition Scan
Used by: `PartitionsTable`

This reads manifest entries, groups by partition key, and aggregates counts/sizes per partition.

### 2.3 Time Travel Support

Following Java, these metadata tables support snapshot selection (time travel):
- `ManifestsTable`, `FilesTable`, `DataFilesTable`, `DeleteFilesTable`
- `ManifestEntriesTable`, `PartitionsTable`, `PositionDeletesTable`

Time travel is exposed via an optional `SnapshotSelection` parameter to `Scan()`:

```cpp
struct SnapshotSelection {
  std::optional<int64_t> snapshot_id;
  std::optional<TimePointMs> as_of_timestamp;
  std::optional<std::string> ref_name;  // branch or tag name
};

Result<std::vector<std::unique_ptr<StructLike>>> Scan(
    std::optional<SnapshotSelection> snapshot_selection = std::nullopt);
```

### 2.4 Naming Convention

Following Java's `MetadataTableUtils.metadataTableName()`:
- Catalog tables (no `/` in name): `source_table.<type>` (e.g., `prod.db.orders.history`)
- Path-based tables (Hadoop): `path#type` (e.g., `/warehouse/db/table#history`)
- Type names are always **lowercase** (e.g., `history`, `all_data_files`)

The C++ naming is already consistent for existing tables (e.g., `source_table.snapshots`).

---

## 3. Detailed Schema Specifications

### 3.1 SnapshotsTable

| # | Field Name | Type | Required | Source |
|---|-----------|------|----------|--------|
| 1 | committed_at | timestamptz | ✓ | `snapshot.timestamp_ms` |
| 2 | snapshot_id | long | ✓ | `snapshot.snapshot_id` |
| 3 | parent_id | long | | `snapshot.parent_snapshot_id` |
| 4 | operation | string | | `snapshot.summary["operation"]` |
| 5 | manifest_list | string | | `snapshot.manifest_list` |
| 6 | summary | map<string,string> | | `snapshot.summary` |

### 3.2 HistoryTable

| # | Field Name | Type | Required | Source |
|---|-----------|------|----------|--------|
| 1 | made_current_at | timestamptz | ✓ | `entry.timestamp_ms` |
| 2 | snapshot_id | long | ✓ | `entry.snapshot_id` |
| 3 | parent_id | long | | resolved via `SnapshotUtil.currentAncestorIds()` |
| 4 | is_current_ancestor | boolean | ✓ | computed from ancestor set |

### 3.3 ManifestsTable

| # | Field Name | Type | Required | Source |
|---|-----------|------|----------|--------|
| 1 | content | int | ✓ | `manifest.content` |
| 2 | path | string | ✓ | `manifest.manifest_path` |
| 3 | length | long | ✓ | `manifest.manifest_length` |
| 4 | partition_spec_id | int | ✓ | `manifest.partition_spec_id` |
| 5 | added_snapshot_id | long | ✓ | `manifest.added_snapshot_id` |
| 6 | added_data_files_count | int | | `manifest.added_files_count` (for data) |
| 7 | existing_data_files_count | int | | `manifest.existing_files_count` (for data) |
| 8 | deleted_data_files_count | int | | `manifest.deleted_files_count` (for data) |
| 9 | added_delete_files_count | int | | `manifest.added_files_count` (for deletes) |
| 10 | existing_delete_files_count | int | | `manifest.existing_files_count` (for deletes) |
| 11 | deleted_delete_files_count | int | | `manifest.deleted_files_count` (for deletes) |
| 12 | partition_summaries | list<struct> | | `manifest.partitions` |

### 3.4 AllManifestsTable

Same as `ManifestsTable` plus:

| # | Field Name | Type | Required | Source |
|---|-----------|------|----------|--------|
| 13 | reference_snapshot_id | long | ✓ | snapshot that references this manifest |
| 14 | key_metadata | binary | | `manifest.key_metadata` |

### 3.5 FilesTable / DataFilesTable / DeleteFilesTable

These share the same schema as `DataFile` fields. Each row represents a data file in the current snapshot.

Following `DataFile::Type(partition_type)` in Java, the schema includes:
- `content` (int), `file_path` (string), `file_format` (string)
- `spec_id` (int), `partition` (struct from partition spec)
- `record_count` (long), `file_size_in_bytes` (long)
- `column_sizes`, `value_counts`, `null_value_counts`, `nan_value_counts` (map<int,long>)
- `lower_bounds`, `upper_bounds` (map<int,binary>)
- `key_metadata` (binary), `split_offsets` (list<long>)
- `equality_ids` (list<int>), `sort_order_id` (int)
- `first_row_id` (long), `referenced_data_file` (string)
- `content_offset` (long), `content_size_in_bytes` (long)
- `readable_metrics` (struct) — computed virtual column

The difference between the three tables:
- `FilesTable` → all manifests (data + delete)
- `DataFilesTable` → data manifests only (content == 0)
- `DeleteFilesTable` → delete manifests only (content != 0)

### 3.6 AllFilesTable / AllDataFilesTable / AllDeleteFilesTable

Same schema as the file tables above, but rows cover **all snapshots**. Files may appear multiple times if they exist in multiple snapshots.

### 3.7 ManifestEntriesTable / AllEntriesTable

Schema from `ManifestEntry::GetSchema(partition_type)`:

| # | Field Name | Type | Required | Source |
|---|-----------|------|----------|--------|
| 1 | status | int | ✓ | `entry.status` |
| 2 | snapshot_id | long | | `entry.snapshot_id` |
| 3 | sequence_number | long | | `entry.sequence_number` |
| 4 | file_sequence_number | long | | `entry.file_sequence_number` |
| 5 | data_file | struct | ✓ | `entry.data_file` (nested DataFile struct) |

`readable_metrics` virtual column is also supported.

### 3.8 PartitionsTable

| # | Field Name | Type | Required | Source |
|---|-----------|------|----------|--------|
| 1 | partition | struct | ✓ | partition key (from spec) |
| 2 | spec_id | int | ✓ | partition spec ID |
| 3 | record_count | long | ✓ | sum of live entry record counts |
| 4 | file_count | int | ✓ | count of live files |
| 5 | total_data_file_size_in_bytes | long | ✓ | sum of file sizes |
| 6 | position_delete_record_count | long | | sum of position delete records |
| 7 | position_delete_file_count | int | | count of position delete files |
| 8 | equality_delete_record_count | long | | sum of equality delete records |
| 9 | equality_delete_file_count | int | | count of equality delete files |
| 10 | last_updated_at | timestamptz | | max timestamp of snapshot with changes |
| 11 | last_updated_snapshot_id | long | | snapshot ID of last update |

### 3.9 RefsTable

| # | Field Name | Type | Required | Source |
|---|-----------|------|----------|--------|
| 1 | name | string | ✓ | ref name from `table.refs()` |
| 2 | type | string | ✓ | "branch" or "tag" |
| 3 | snapshot_id | long | ✓ | `ref.snapshot_id` |
| 4 | max_reference_age_in_ms | long | | `ref.max_ref_age_ms` |
| 5 | min_snapshots_to_keep | int | | branch: `min_snapshots_to_keep` |
| 6 | max_snapshot_age_in_ms | long | | branch: `max_snapshot_age_ms` |

### 3.10 MetadataLogEntriesTable

| # | Field Name | Type | Required | Source |
|---|-----------|------|----------|--------|
| 1 | timestamp | timestamptz | ✓ | `entry.timestamp_ms` |
| 2 | file | string | ✓ | `entry.metadata_file` |
| 3 | latest_snapshot_id | long | | resolved from metadata file |
| 4 | latest_schema_id | int | | resolved from metadata file |
| 5 | latest_sequence_number | long | | resolved from metadata file |

### 3.11 PositionDeletesTable

Schema is per-spec: `delete_file_path` (string), `delete_file_pos` (long), `delete_file_row` (struct of table schema), `partition` (struct), `spec_id` (int). For v3+: also `content_offset` (long) and `content_size_in_bytes` (long).

---

## 4. Implementation Phases

### Phase 1: Foundation — Base Class & Kind Enum Expansion

**Goal:** Expand the metadata table framework without implementing scanning yet.

**Files to modify:**
- `src/iceberg/inspect/metadata_table.h` — expand `Kind` enum to 16 values, add `supports_time_travel()`, add `Scan()` and `SnapshotSelection`
- `src/iceberg/inspect/metadata_table.cc` — update factory switch to handle new kinds (return `NotSupported` for now)

**New `Kind` enum:**
```cpp
enum class Kind {
  kEntries,
  kFiles,
  kDataFiles,
  kDeleteFiles,
  kHistory,
  kMetadataLogEntries,
  kSnapshots,
  kRefs,
  kManifests,
  kPartitions,
  kAllDataFiles,
  kAllDeleteFiles,
  kAllFiles,
  kAllManifests,
  kAllEntries,
  kPositionDeletes,
};
```

**Build system changes:**
- Update `src/iceberg/CMakeLists.txt`: no new source files yet
- Update `src/iceberg/inspect/meson.build`: add any new headers

**Estimated new files:** 0 (modifications only)
**Estimated LOC:** ~50 lines changed

---

### Phase 2: Static Metadata Tables (No Manifest Reading)

**Goal:** Implement the 6 tables that produce rows from in-memory data or manifest list files.

#### 2a. SnapshotsTable — Add Scanning

**Files to modify:**
- `src/iceberg/inspect/snapshots_table.h` — add `Scan()` declaration
- `src/iceberg/inspect/snapshots_table.cc` — implement `Scan()` iterating `table.snapshots()`

**Implementation:** Iterate `source_table()->snapshots()`, create one `StructLike` row per snapshot. Extract `operation` from `summary["operation"]`.

#### 2b. HistoryTable — Add Scanning

**Files to modify:**
- `src/iceberg/inspect/history_table.h` — add `Scan()` declaration
- `src/iceberg/inspect/history_table.cc` — implement `Scan()` iterating `table.history()`

**Implementation:** Iterate `source_table()->history()`, resolve parent snapshot IDs and ancestor status. Ancestor resolution walks the snapshot tree from the current snapshot to build a set of ancestor IDs.

#### 2c. RefsTable (NEW)

**New files:**
- `src/iceberg/inspect/refs_table.h`
- `src/iceberg/inspect/refs_table.cc`

**Schema:** 6 columns (name, type, snapshot_id, max_reference_age_in_ms, min_snapshots_to_keep, max_snapshot_age_in_ms)
**Scan:** Iterate `table.metadata()->refs`, extract retention policy fields.

#### 2d. MetadataLogEntriesTable (NEW)

**New files:**
- `src/iceberg/inspect/metadata_log_entries_table.h`
- `src/iceberg/inspect/metadata_log_entries_table.cc`

**Schema:** 5 columns (timestamp, file, latest_snapshot_id, latest_schema_id, latest_sequence_number)
**Scan:** Iterate `table.metadata()->metadata_log`, optionally resolve metadata file contents for `latest_*` fields. The `latest_*` fields require reading each metadata file, which is expensive — these can be left as `nullopt` initially and resolved lazily.

#### 2e. ManifestsTable (NEW)

**New files:**
- `src/iceberg/inspect/manifests_table.h`
- `src/iceberg/inspect/manifests_table.cc`

**Schema:** 12 columns (see section 3.3)
**Scan (time-travel-aware):**
1. Resolve target snapshot (current or from `SnapshotSelection`)
2. Load manifest list via `ManifestListReader`
3. Produce one row per `ManifestFile`
4. Split counts between data/delete based on `manifest.content`

#### 2f. AllManifestsTable (NEW)

**New files:**
- `src/iceberg/inspect/all_manifests_table.h`
- `src/iceberg/inspect/all_manifests_table.cc`

**Schema:** Same as ManifestsTable + `reference_snapshot_id` + `key_metadata`
**Scan:** Iterate all snapshots, load each manifest list, deduplicate by `manifest_path`, produce rows.

**Build system changes:**
- Add new `.cc` files to `ICEBERG_SOURCES` in `src/iceberg/CMakeLists.txt`
- Update `src/iceberg/inspect/meson.build`

**Estimated new files:** 8 (4 headers + 4 sources)
**Estimated LOC:** ~600 lines

---

### Phase 3: Manifest-Based Tables (Current Snapshot)

**Goal:** Implement tables that read manifest entries from the current snapshot.

#### 3a. BaseFilesTable (NEW — abstract)

**New files:**
- `src/iceberg/inspect/base_files_table.h`
- `src/iceberg/inspect/base_files_table.cc`

**Purpose:** Shared implementation for the 6 file-level tables.

**Key methods:**
- `MakeSchema(partition_type)` — builds the DataFile schema
- `ShouldInclude(ManifestEntry)` — filter by content type (data/delete/both)
- `MakeRow(DataFile, partition_type)` — converts DataFile to StructLike row
- `ManifestsForScan(Snapshot)` — returns which manifests to scan (data/delete/all)
- `Scan()` — orchestrates: resolve snapshot → get manifests → read entries → produce rows

The `readable_metrics` virtual column converts lower/upper bound byte arrays to human-readable strings using partition transforms. This is complex and can be deferred to a later phase.

#### 3b. FilesTable / DataFilesTable / DeleteFilesTable (NEW)

**New files:**
- `src/iceberg/inspect/files_table.h` + `.cc`
- `src/iceberg/inspect/data_files_table.h` + `.cc`
- `src/iceberg/inspect/delete_files_table.h` + `.cc`

Each is a thin subclass of `BaseFilesTable` with:
- A static `Make()` factory
- Override to configure which manifest content types to include
- Appropriate table name suffix (`.files`, `.data_files`, `.delete_files`)

#### 3c. BaseEntriesTable (NEW — abstract)

**New files:**
- `src/iceberg/inspect/base_entries_table.h`
- `src/iceberg/inspect/base_entries_table.cc`

**Purpose:** Shared implementation for the 2 entry-level tables.

**Key methods:**
- `MakeSchema(partition_type)` — builds the ManifestEntry schema with nested DataFile
- `MakeRow(ManifestEntry, partition_type)` — converts ManifestEntry to StructLike row
- Content evaluation: filter entries by `ManifestContent` (data vs delete)

#### 3d. ManifestEntriesTable (NEW)

**New files:**
- `src/iceberg/inspect/manifest_entries_table.h`
- `src/iceberg/inspect/manifest_entries_table.cc`

Thin subclass of `BaseEntriesTable`. Scans manifest entries from the current snapshot.

**Build system changes:**
- Add 10 new `.cc` files to `ICEBERG_SOURCES`

**Estimated new files:** 10 (5 headers + 5 sources)
**Estimated LOC:** ~1200 lines

---

### Phase 4: All-* Tables (Cross-Snapshot)

**Goal:** Implement tables that scan across all snapshots.

#### 4a. AllFilesTable / AllDataFilesTable / AllDeleteFilesTable (NEW)

**New files:**
- `src/iceberg/inspect/all_files_table.h` + `.cc`
- `src/iceberg/inspect/all_data_files_table.h` + `.cc`
- `src/iceberg/inspect/all_delete_files_table.h` + `.cc`

These extend `BaseFilesTable` but override manifest collection to include manifests from ALL snapshots, deduplicated by path. The deduplication is important because the same manifest can be referenced by multiple snapshots.

#### 4b. AllEntriesTable (NEW)

**New files:**
- `src/iceberg/inspect/all_entries_table.h`
- `src/iceberg/inspect/all_entries_table.cc`

Extends `BaseEntriesTable`, iterates all snapshots, collects unique manifests, reads entries from each.

**Build system changes:**
- Add 8 new `.cc` files to `ICEBERG_SOURCES`

**Estimated new files:** 8 (4 headers + 4 sources)
**Estimated LOC:** ~400 lines (thin overrides of base classes)

---

### Phase 5: PartitionsTable & PositionDeletesTable

#### 5a. PartitionsTable (NEW)

**New files:**
- `src/iceberg/inspect/partitions_table.h`
- `src/iceberg/inspect/partitions_table.cc`

**This is the most complex table.** Implementation approach:

1. Get manifests for target snapshot
2. Read ALL live manifest entries
3. Group entries by partition key (using `StructLikeMap` or `std::unordered_map` with a custom hash)
4. For each partition group, aggregate:
   - Record count, file count, total file size
   - Per-content-type counts (data, position deletes, equality deletes)
   - Last update timestamp and snapshot ID
5. Produce one row per partition group

**Considerations:**
- Memory: for large tables, this could consume significant memory. Consider streaming/parallel aggregation in a future enhancement.
- Partition equality: depends on the partition spec — two rows are in the same partition if their partition values match according to the spec's transform functions.

#### 5b. PositionDeletesTable (NEW)

**New files:**
- `src/iceberg/inspect/position_deletes_table.h`
- `src/iceberg/inspect/position_deletes_table.cc`

**Implementation:**
1. Resolve target snapshot
2. Get delete manifests
3. Read position delete entries (content == kPositionDeletes)
4. Read the actual position delete files (Puffin format for DVs, or data files for legacy position deletes)
5. Produce one row per deleted row position

This requires the `iceberg_data` library (which includes `deletes/` and `puffin/` modules). The `PositionDeletesTable` should be compiled as part of `iceberg_data` or conditionally included.

**Build system changes:**
- Add 4 new `.cc` files to `ICEBERG_SOURCES`

**Estimated new files:** 4 (2 headers + 2 sources)
**Estimated LOC:** ~500 lines

---

### Phase 6: Catalog Integration

**Goal:** Enable transparent metadata table name resolution in catalogs.

#### 6a. Metadata Table Name Detection

Add to `MetadataTable`:
```cpp
/// Returns true if the given table name suffix matches a metadata table type.
static std::optional<Kind> KindFromName(std::string_view table_name);
```

This parses the last segment of a dotted name and matches it against known metadata table type suffixes.

#### 6b. Catalog Integration

Modify catalog implementations (`RestCatalog`, `InMemoryCatalog`, `SessionCatalog`) to:
1. When `LoadTable()` is called, check if the name refers to a metadata table
2. If so, load the base table first, then create the appropriate metadata table
3. The metadata table is returned as a read-only table-like object

This mirrors `BaseMetastoreCatalog.loadMetadataTable()` in Java.

**Build system changes:** Minimal

**Estimated LOC:** ~150 lines

---

### Phase 7: Testing

#### 7a. Existing Test Updates

- Expand `metadata_table_test.cc` to test all 16 `Kind` values, factory dispatch, time travel support flags, and table naming conventions.

#### 7b. New Test Files

| Test File | Covers |
|-----------|--------|
| `snapshots_table_test.cc` | Schema + scan with mock snapshots |
| `history_table_test.cc` | Schema + scan with mock history entries |
| `refs_table_test.cc` | Schema + scan with mock refs |
| `metadata_log_entries_table_test.cc` | Schema + scan with mock log entries |
| `manifests_table_test.cc` | Schema + scan with mock manifests |
| `all_manifests_table_test.cc` | Schema + cross-snapshot dedup |
| `files_table_test.cc` | Schema + scan with real manifest entries |
| `partitions_table_test.cc` | Partition aggregation logic |
| `base_files_table_test.cc` | Shared file-level logic |
| `base_entries_table_test.cc` | Shared entry-level logic |

**Test patterns:**
- Tests use `MockFileIO`, `MockCatalog`, and `TableMetadata` with hand-crafted data
- Tests verify schema correctness (column names, types, required/optional)
- Tests verify scan output matches expected rows
- Tests verify error handling (null table, missing snapshot, etc.)
- Use `IsOk()`, `IsError()`, `HasErrorMessage()` matchers from `test/matchers.h`

#### 7c. Integration Tests

Create integration tests that:
1. Create a table with multiple snapshots
2. Verify each metadata table type produces correct results
3. Test time travel snapshot selection
4. Test edge cases: empty table, single snapshot, deleted files

**Estimated LOC:** ~1500 lines (test code)

---

## 5. Build System Changes Summary

### `src/iceberg/CMakeLists.txt`

Add to `ICEBERG_SOURCES`:
```
inspect/all_data_files_table.cc
inspect/all_delete_files_table.cc
inspect/all_entries_table.cc
inspect/all_files_table.cc
inspect/all_manifests_table.cc
inspect/base_entries_table.cc
inspect/base_files_table.cc
inspect/data_files_table.cc
inspect/delete_files_table.cc
inspect/files_table.cc
inspect/manifest_entries_table.cc
inspect/manifests_table.cc
inspect/metadata_log_entries_table.cc
inspect/partitions_table.cc
inspect/position_deletes_table.cc
inspect/refs_table.cc
```

### `src/iceberg/inspect/meson.build`

Update `install_headers()` to include all new header files.

### `src/iceberg/inspect/CMakeLists.txt`

Update `iceberg_install_all_headers(iceberg/inspect)` — no changes needed if `iceberg_install_all_headers` globs the directory.

### `src/iceberg/test/CMakeLists.txt`

Add new test targets. Tests that need `USE_BUNDLE` or `USE_DATA` should be linked accordingly. Most inspect tests only need `iceberg_static`.

---

## 6. File Manifest (Complete)

### New Files (26 files)

```
src/iceberg/inspect/
├── all_data_files_table.h
├── all_data_files_table.cc
├── all_delete_files_table.h
├── all_delete_files_table.cc
├── all_entries_table.h
├── all_entries_table.cc
├── all_files_table.h
├── all_files_table.cc
├── all_manifests_table.h
├── all_manifests_table.cc
├── base_entries_table.h
├── base_entries_table.cc
├── base_files_table.h
├── base_files_table.cc
├── data_files_table.h
├── data_files_table.cc
├── delete_files_table.h
├── delete_files_table.cc
├── files_table.h
├── files_table.cc
├── manifest_entries_table.h
├── manifest_entries_table.cc
├── manifests_table.h
├── manifests_table.cc
├── metadata_log_entries_table.h
├── metadata_log_entries_table.cc
├── partitions_table.h
├── partitions_table.cc
├── position_deletes_table.h
├── position_deletes_table.cc
├── refs_table.h
└── refs_table.cc
```

### Modified Files (8 files)

```
src/iceberg/inspect/metadata_table.h      — expand Kind, add Scan/SnapshotSelection
src/iceberg/inspect/metadata_table.cc     — update factory dispatch
src/iceberg/inspect/snapshots_table.h     — add Scan()
src/iceberg/inspect/snapshots_table.cc    — implement Scan()
src/iceberg/inspect/history_table.h       — add Scan()
src/iceberg/inspect/history_table.cc      — implement Scan()
src/iceberg/inspect/CMakeLists.txt        — (may not need change)
src/iceberg/inspect/meson.build           — add new headers
src/iceberg/CMakeLists.txt                — add new source files
src/iceberg/test/CMakeLists.txt           — add new test targets
```

---

## 7. Implementation Order & Dependencies

```
Phase 1: Foundation
  └─ No dependencies (Kind enum expansion)

Phase 2a-b: SnapshotsTable + HistoryTable scanning
  └─ Depends on Phase 1

Phase 2c-f: Refs, MetadataLogEntries, Manifests, AllManifests
  └─ Depends on Phase 1

Phase 3: BaseFilesTable + BaseEntriesTable + concrete file/entry tables
  └─ Depends on Phase 1
  └─ Uses ManifestReader, ManifestListReader (already exist)

Phase 4: All-* tables
  └─ Depends on Phase 3 (extends base classes)

Phase 5: PartitionsTable + PositionDeletesTable
  └─ PartitionsTable depends on Phase 3 (manifest reading)
  └─ PositionDeletesTable depends on iceberg_data library

Phase 6: Catalog Integration
  └─ Depends on Phase 1-5

Phase 7: Testing
  └─ Each phase has its own tests, written alongside implementation
```

## 8. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| `PositionDeletesTable` requires Puffin format support | Could delay phase 5 | Defer to later iteration; it's a niche feature |
| `readable_metrics` virtual column is complex | Increases implementation time | Defer; document as TODO |
| `PartitionsTable` aggregation can be memory-intensive | Performance for large tables | Document limitation; add streaming in future |
| `StructLike` serialization for file/entry tables is verbose | Lots of boilerplate code | Create helper templates/macros for row construction |
| Catalog integration may conflict with existing catalog implementations | Breaking changes | Discuss with team; feature-flag the integration |

---

## 9. Open Questions

1. **Should `MetadataTable` extend `Table`?** Currently it doesn't. If full `Table` integration is needed (e.g., for a SQL frontend), we should add `BaseReadOnlyTable` as in Java. This can be done as a follow-up.

2. **Lazy vs. eager scanning?** `Scan()` currently returns a full vector. For very large tables, a lazy iterator (`Result<std::unique_ptr<CloseableIterable<StructLike>>>`) would be more memory-efficient. This matches Java's `CloseableIterable` pattern.

3. **Should `readable_metrics` be implemented in Phase 3?** It's a significant amount of work (converting binary bounds to human-readable strings via partition transforms). I recommend deferring.

4. **`PositionDeletesTable` in core or data library?** It requires puffin/deletes support from `iceberg_data`. Might need to be in a separate translation unit or conditionally compiled.

5. **Metadata file content resolution in `MetadataLogEntriesTable`:** Reading historical metadata files to extract `latest_snapshot_id` is expensive. Java does this lazily. Should we do the same?

---

## 10. References

- Java Metadata Tables: `/Users/wz/Code/iceberg/core/src/main/java/org/apache/iceberg/`
  - `MetadataTableType.java` — enum of all 16 types
  - `MetadataTableUtils.java` — factory
  - `BaseMetadataTable.java` — base class
  - `BaseReadOnlyTable.java` — mutation rejection
  - `BaseFilesTable.java` — file-level scanning
  - `BaseEntriesTable.java` — entry-level scanning
  - `HistoryTable.java`, `SnapshotsTable.java`, `ManifestsTable.java`, etc.
- Iceberg Spec: https://iceberg.apache.org/spec/
- C++ Codebase: `src/iceberg/inspect/`, `src/iceberg/manifest/`, `src/iceberg/snapshot.h`, `src/iceberg/table_metadata.h`
