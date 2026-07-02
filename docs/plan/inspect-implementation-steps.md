# Iceberg C++ Inspect Feature — 分步实现方案

> 每个 Step 作为一个独立 PR 提交，可单独 review 和合并。
> 参考实现：Java Iceberg `/Users/wz/Code/iceberg/core/src/main/java/org/apache/iceberg/`
> 目标路径：`src/iceberg/inspect/`

---

## 行构建基础设施：ArrowRowBuilder

所有 metadata table 的 `Scan()` 使用 **`ArrowRowBuilder`**（`src/iceberg/arrow_row_builder_internal.h`）将内存中的数据构造为 Arrow 列式数组。该工具封装了 nanoarrow 的生命周期：

```
ArrowRowBuilder::Make(schema)
  → builder.column(0), builder.column(1), ...
  → AppendInt(col, val)  / AppendString(col, val)  / AppendNull(col)  / ...
  → builder.FinishRow()
  → std::move(builder).Finish()  →  ArrowArray (struct array)
```

**已有的 append helpers**（`arrow_row_builder_internal.h` + `.cc`）：

| 函数 | 用途 |
|------|------|
| `AppendNull(ArrowArray*)` | 写入 null |
| `AppendBoolean(ArrowArray*, bool)` | 写入 boolean |
| `AppendInt(ArrowArray*, int64_t)` | 写入 int32 / int64 / timestamp |
| `AppendString(ArrowArray*, string_view)` | 写入 string |
| `AppendStringMap(ArrowArray*, const unordered_map<string,string>&)` | 写入 map<string,string> |

> **按需添加 helpers：** 以上 5 个 helpers 足以覆盖 `SnapshotsTable`、`HistoryTable`、`RefsTable`、`MetadataLogEntriesTable` 等纯内存数据表的 `Scan()` 实现。当后续 Step（如 `BaseFilesTable`）需要写入 `map<int32,int64>`、`list<int64>`、`binary` 等复杂类型时，由该 Step 的 PR 负责在 `arrow_row_builder_internal.h` + `.cc` 中新增对应的 helper 函数（如 `AppendIntMap`、`AppendListInt64`、`AppendBinary` 等），遵循已有的 nanoarrow `ArrowArrayAppend*` + `ArrowArrayFinishElement` 模式。不提前预加用不到的 helpers。

**嵌套 struct 写入手动模式：** 对于 struct 列（如 DataFile 的 `partition` 列），通过 `builder.column(idx)->children[...]` 递归访问子列：
```cpp
// partition 列本身是 struct，其 children 是 partition 的各个字段
ArrowArray* partition_col = builder.column(partition_idx);
ArrowArray* part_field_0 = partition_col->children[0];  // 如 "year"
ArrowArray* part_field_1 = partition_col->children[1];  // 如 "month"
// ... append values to children ...
ArrowArrayFinishElement(partition_col);  // 完成这一个 struct 元素
```

---

## 依赖关系图

```
Step 0: Framework Expansion
  ├── Step 1: SnapshotsTable + HistoryTable Scanning
  ├── Step 2: RefsTable + MetadataLogEntriesTable          ─┐
  ├── Step 3: ManifestsTable + AllManifestsTable            │ 彼此独立，
  ├── Step 4: BaseFilesTable (abstract base, no concrete)   │ 可并行开发
  ├── Step 5: BaseEntriesTable + ManifestEntriesTable       │
  └── Step 6: PartitionsTable                              ─┘
          │
          ├── Step 7: FilesTable + DataFilesTable + DeleteFilesTable  (depends on Step 4)
          ├── Step 8: All*Files Tables  (depends on Step 4)
          └── Step 9: AllEntriesTable  (depends on Step 5)
                  │
                  └── Step 10: Catalog Integration
```

**关键约束：**
- Step 0 必须最先完成
- Step 1-6 之间互不依赖，可并行开发/提交
- Step 7, 8 依赖 Step 4（`BaseFilesTable`）
- Step 9 依赖 Step 5（`BaseEntriesTable`）
- Step 10 依赖 Step 7, 8, 9（所有 table 类型就绪后集成）

---

## Step 0: Framework Expansion — 框架扩展

> **依赖：** 无
> **目标：** 扩展 `MetadataTable` 基础框架支持全部 16 种类型，定义 `Scan()` 接口
> **原则：** 纯框架变更，所有新 Kind 在 factory 中返回 `NotSupported`；不预加 ArrowRowBuilder helpers

### 修改文件

| 文件 | 变更内容 |
|------|---------|
| `src/iceberg/inspect/metadata_table.h` | Kind 枚举扩展到 16 个值；新增 `SnapshotSelection` 结构体；新增虚函数 `supports_time_travel()`、`Scan()` |
| `src/iceberg/inspect/metadata_table.cc` | Factory switch 覆盖全部 16 个 Kind（新类型返回 `NotSupported`） |
| `src/iceberg/test/metadata_table_test.cc` | 扩展测试：验证所有 16 个 Kind 值、factory 行为、未实现类型返回错误 |

### Kind 枚举定义

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

### 新增类型

```cpp
/// \brief Parameters for snapshot selection (time travel).
struct SnapshotSelection {
  std::optional<int64_t> snapshot_id;
  std::optional<TimePointMs> as_of_timestamp;
  std::optional<std::string> ref_name;
};
```

### `Scan()` 签名

```cpp
/// \brief Scan the metadata table and return all rows as an Arrow struct array.
///
/// The returned ArrowArray is a struct array where each element is one row.
/// The caller takes ownership and must call ArrowArrayRelease when done.
virtual Result<ArrowArray> Scan(
    std::optional<SnapshotSelection> snapshot_selection = std::nullopt);
```

### 测试要点

- 16 个 Kind 枚举值正确
- `Make(table, Kind::kSnapshots)` → IsOk
- `Make(table, Kind::kFiles)` → IsError(NotSupported)
- `Make(nullptr, *)` → IsError(InvalidArgument)
- 默认 `supports_time_travel()` 返回 false
- 默认 `Scan()` 返回 NotSupported

### 预估
- 修改文件：3 个
- 新增行数：~80 行
- 新增测试行数：~60 行
- PR 规模：Small

---

## Step 1: SnapshotsTable + HistoryTable Scanning

> **依赖：** Step 0
> **目标：** 为已有的两个 stub 实现 `Scan()` 数据读取逻辑
> **数据源：** `table.snapshots()` / `table.history()`（纯内存数据，不读文件）

### 修改文件

| 文件 | 变更内容 |
|------|---------|
| `src/iceberg/inspect/snapshots_table.h` | 声明 `Scan()` override |
| `src/iceberg/inspect/snapshots_table.cc` | 实现 `Scan()`：遍历 `source_table()->snapshots()`，逐行写入 `ArrowRowBuilder` |
| `src/iceberg/inspect/history_table.h` | 声明 `Scan()` override |
| `src/iceberg/inspect/history_table.cc` | 实现 `Scan()`：遍历 `source_table()->history()`，计算 `parent_id` 和 `is_current_ancestor` |

### 关键实现：SnapshotsTable::Scan()

```cpp
Result<ArrowArray> SnapshotsTable::Scan(std::optional<SnapshotSelection>) {
  ICEBERG_ASSIGN_OR_RAISE(auto builder, ArrowRowBuilder::Make(*schema()));

  for (const auto& snapshot : source_table()->snapshots()) {
    // column 0: committed_at (timestamptz → int64 micros)
    ICEBERG_RETURN_UNEXPECTED(
        AppendInt(builder.column(0), snapshot->timestamp_ms.count()));

    // column 1: snapshot_id (long)
    ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(1), snapshot->snapshot_id));

    // column 2: parent_id (long, optional)
    if (snapshot->parent_snapshot_id.has_value()) {
      ICEBERG_RETURN_UNEXPECTED(
          AppendInt(builder.column(2), *snapshot->parent_snapshot_id));
    } else {
      ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(2)));
    }

    // column 3: operation (string, optional)
    auto op = snapshot->Operation();
    if (op.has_value()) {
      ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(3), *op));
    } else {
      ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(3)));
    }

    // column 4: manifest_list (string, optional)
    ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(4), snapshot->manifest_list));

    // column 5: summary (map<string,string>)
    ICEBERG_RETURN_UNEXPECTED(AppendStringMap(builder.column(5), snapshot->summary));

    ICEBERG_RETURN_UNEXPECTED(builder.FinishRow());
  }

  return std::move(builder).Finish();
}
```

### 关键实现：HistoryTable::Scan()

额外逻辑：
- `parent_id`：通过 `SnapshotById(entry.snapshot_id)` 查找 parent
- `is_current_ancestor`：从 `current_snapshot()` 沿 parent chain 向上遍历，构建 `unordered_set<int64_t>` ancestor 集合，然后对每个 entry 检查其 `snapshot_id` 是否在集合中

### 测试要点

- 测试中使用 `FinishAndImport()` 模式：`std::move(builder).Finish()` → ArrowArray → `::arrow::ImportRecordBatch()` → 断言行数、列值
- `Scan()` 返回正确的行数
- 每列的值与源数据一致
- 空 snapshot 列表返回 0 行
- `is_current_ancestor` 计算正确（多 snapshot + branching 场景）
- 可选列为 null 时正确处理

### 预估
- 修改文件：4 个
- 修改行数：~180 行
- 新增测试行数：~200 行（`snapshots_scan_test.cc`、`history_scan_test.cc` 或扩展现有 test）
- PR 规模：Medium

---

## Step 2: RefsTable + MetadataLogEntriesTable

> **依赖：** Step 0
> **目标：** 新增两个静态元数据表，数据来源为 `TableMetadata` 内存字段
> **数据源：** `table.metadata()->refs` / `table.metadata()->metadata_log`

### 新增文件

| 文件 | 内容 |
|------|------|
| `inspect/refs_table.h` | `RefsTable` 类声明 |
| `inspect/refs_table.cc` | Schema 定义（6 列）+ `Make()` + `Scan()` |
| `inspect/metadata_log_entries_table.h` | `MetadataLogEntriesTable` 类声明 |
| `inspect/metadata_log_entries_table.cc` | Schema 定义（5 列）+ `Make()` + `Scan()` |

### 修改文件

| 文件 | 变更内容 |
|------|---------|
| `src/iceberg/inspect/metadata_table.cc` | Factory 中添加 `kRefs` 和 `kMetadataLogEntries` 分发 |
| `src/iceberg/type_fwd.h` | 添加 `RefsTable`、`MetadataLogEntriesTable` 前向声明 |
| `src/iceberg/CMakeLists.txt` | `ICEBERG_SOURCES` 添加 2 个 `.cc` 文件 |
| `src/iceberg/inspect/meson.build` | 添加 2 个 `.h` 文件 |

### Schema 定义

**RefsTable:**
| # | 列名 | 类型 | 来源 |
|---|------|------|------|
| 1 | name | string (required) | ref map key |
| 2 | type | string (required) | `ToString(ref.type())` → "branch"/"tag" |
| 3 | snapshot_id | long (required) | `ref.snapshot_id` |
| 4 | max_reference_age_in_ms | long (optional) | `ref.max_ref_age_ms()`; null if none |
| 5 | min_snapshots_to_keep | int (optional) | branch: `branch.min_snapshots_to_keep`; tag: null |
| 6 | max_snapshot_age_in_ms | long (optional) | branch: `branch.max_snapshot_age_ms`; tag: null |

**MetadataLogEntriesTable:**
| # | 列名 | 类型 | 来源 |
|---|------|------|------|
| 1 | timestamp | timestamptz (required) | `entry.timestamp_ms` |
| 2 | file | string (required) | `entry.metadata_file` |
| 3 | latest_snapshot_id | long (optional) | 暂不解析，始终 null |
| 4 | latest_schema_id | int (optional) | 暂不解析，始终 null |
| 5 | latest_sequence_number | long (optional) | 暂不解析，始终 null |

> **注意：** `latest_*` 字段需要读取每个历史 metadata 文件才能获取，Java 中也标记为可选且昂贵。第一版输出 null，后续可优化。

### Scan 实现模式（RefsTable 示例）

```cpp
Result<ArrowArray> RefsTable::Scan(std::optional<SnapshotSelection>) {
  ICEBERG_ASSIGN_OR_RAISE(auto builder, ArrowRowBuilder::Make(*schema()));

  for (const auto& [name, ref] : source_table()->metadata()->refs) {
    ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(0), name));          // name
    ICEBERG_RETURN_UNEXPECTED(
        AppendString(builder.column(1), ToString(ref->type())));               // type
    ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(2), ref->snapshot_id)); // snapshot_id

    auto max_ref_age = ref->max_ref_age_ms();
    if (max_ref_age.has_value()) {
      ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(3), *max_ref_age));
    } else {
      ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(3)));
    }

    // column 4/5: branch-specific retention fields
    if (auto* branch = std::get_if<SnapshotRef::Branch>(&ref->retention)) {
      if (branch->min_snapshots_to_keep.has_value()) {
        ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(4),
                                            *branch->min_snapshots_to_keep));
      } else {
        ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(4)));
      }
      if (branch->max_snapshot_age_ms.has_value()) {
        ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(5),
                                            *branch->max_snapshot_age_ms));
      } else {
        ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(5)));
      }
    } else {
      ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(4)));
      ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(5)));
    }

    ICEBERG_RETURN_UNEXPECTED(builder.FinishRow());
  }

  return std::move(builder).Finish();
}
```

### 测试要点

- Schema 校验：列名、类型、required/optional 正确
- Scan 返回正确的行数和内容
- 空 refs / 空 metadata_log 返回 0 行
- Branch vs Tag 的 retention 字段差异（tag 的 min_snapshots_to_keep 为 null）
- `FinishAndImport()` 后验证 Arrow RecordBatch

### 预估
- 新增文件：4 个
- 修改文件：4 个
- 新增代码行数：~250 行
- 新增测试行数：~200 行
- PR 规模：Medium

---

## Step 3: ManifestsTable + AllManifestsTable

> **依赖：** Step 0
> **目标：** 新增 manifest 列表级别的元数据表
> **关键依赖：** `ManifestListReader`（已存在）、`SnapshotCache`（已存在）

### 新增文件

| 文件 | 内容 |
|------|------|
| `inspect/manifests_table.h` | `ManifestsTable` 声明（time-travel 支持） |
| `inspect/manifests_table.cc` | Schema（12 列）+ `Make()` + `Scan()` |
| `inspect/all_manifests_table.h` | `AllManifestsTable` 声明 |
| `inspect/all_manifests_table.cc` | Schema（14 列）+ `Make()` + `Scan()` |

### 修改文件

| 文件 | 变更内容 |
|------|---------|
| `src/iceberg/inspect/metadata_table.cc` | Factory 添加 `kManifests`、`kAllManifests` |
| `src/iceberg/type_fwd.h` | 前向声明 |
| `src/iceberg/CMakeLists.txt` | 添加 2 个 `.cc` |
| `src/iceberg/inspect/meson.build` | 添加 2 个 `.h` |

### Schema（12 列）

```
content (int), path (string), length (long), partition_spec_id (int),
added_snapshot_id (long),
added_data_files_count (int?), existing_data_files_count (int?),
deleted_data_files_count (int?),
added_delete_files_count (int?), existing_delete_files_count (int?),
deleted_delete_files_count (int?),
partition_summaries (list<struct>)
```

data/delete 计数拆分逻辑：
```cpp
bool is_data = manifest.content == ManifestContent::kData;
// added_data_files_count
if (is_data && manifest.added_files_count.has_value())
  AppendInt(col, *manifest.added_files_count);
else if (is_data)
  AppendNull(col);
else
  AppendInt(col, 0);
// added_delete_files_count: 对称
```

### ManifestsTable::Scan() 流程

```
1. 解析 snapshot（支持 SnapshotSelection），默认 current_snapshot()
2. ManifestListReader::Make(snapshot.manifest_list, io) → Files()
3. 对每个 ManifestFile，使用 ArrowRowBuilder 构造一行
4. partition_summaries 列：遍历 manifest.partitions，每个 PartitionFieldSummary
   写入为嵌套 struct（contains_null, contains_nan?, lower_bound?, upper_bound?）
```

### AllManifestsTable::Scan() 流程

```
1. 遍历所有 snapshots
2. 对每个 snapshot，读取其 manifest list
3. 按 manifest_path 去重
4. 额外字段 reference_snapshot_id = 引用此 manifest 的 snapshot.snapshot_id
5. 额外字段 key_metadata = manifest.key_metadata (binary)
```

### 测试要点

- 当前 snapshot 的 manifest 列表正确
- Time travel：指定 `snapshot_id` 读取历史 snapshot 的 manifest
- AllManifestsTable 跨 snapshot 去重正确
- 空 snapshot 返回 0 行
- data/delete 计数拆分逻辑
- partition_summaries 嵌套结构正确

### 预估
- 新增文件：4 个
- 修改文件：4 个
- 新增代码行数：~400 行
- 新增测试行数：~300 行
- PR 规模：Medium

---

## Step 4: BaseFilesTable — 文件级别抽象基类

> **依赖：** Step 0
> **目标：** 创建 `BaseFilesTable` 抽象基类，封装 manifest entry 读取和 DataFile → ArrowArray 行写入逻辑
> **注意：** 此步骤仅创建抽象基类 + 单元测试，不创建任何具体表

### 新增文件

| 文件 | 内容 |
|------|------|
| `inspect/base_files_table.h` | `BaseFilesTable` 抽象类声明 |
| `inspect/base_files_table.cc` | Schema 构建、manifest 收集、`ShouldInclude()`、`AppendFileRow()` 等共享逻辑 |

### 修改文件

| 文件 | 变更内容 |
|------|---------|
| `src/iceberg/arrow_row_builder_internal.h` | 新增 `AppendDouble`、`AppendBinary`、`AppendListInt32`、`AppendListInt64`、`AppendIntMap`、`AppendBinaryMap` helpers |
| `src/iceberg/arrow_row_builder.cc` | 实现上述新增 helpers |
| `src/iceberg/type_fwd.h` | 前向声明 |
| `src/iceberg/CMakeLists.txt` | 添加 `base_files_table.cc` |
| `src/iceberg/inspect/meson.build` | 添加 `base_files_table.h` |

### 类设计

```cpp
/// \brief Abstract base for file-level metadata tables.
///
/// Subclasses control:
///   - which manifests to scan: data, delete, or both
///   - whether to scan all snapshots or just the current snapshot
class ICEBERG_EXPORT BaseFilesTable : public MetadataTable {
 public:
  /// \brief Whether to include a file entry in the results.
  virtual bool ShouldInclude(const ManifestEntry& entry) const = 0;

  /// \brief Collect manifests for the scan.
  virtual Result<std::vector<ManifestFile>> CollectManifests(
      std::shared_ptr<FileIO> io,
      std::optional<SnapshotSelection> snapshot_selection) = 0;

  Result<ArrowArray> Scan(
      std::optional<SnapshotSelection> snapshot_selection) override;

 protected:
  BaseFilesTable(std::shared_ptr<Table> source_table,
                 TableIdentifier identifier,
                 std::shared_ptr<Schema> schema);

  /// \brief Build the schema for a file table given a partition type.
  static std::shared_ptr<Schema> MakeSchema(
      std::shared_ptr<StructType> partition_type);

  /// \brief Append one DataFile row to an ArrowRowBuilder.
  static Status AppendFileRow(ArrowRowBuilder& builder,
                              const DataFile& data_file,
                              std::shared_ptr<StructType> partition_type);
};
```

### Scan 流程

```
Scan(snapshot_selection)
  ├── 1. manifests = CollectManifests(io, snapshot_selection)
  ├── 2. builder = ArrowRowBuilder::Make(*schema())
  ├── 3. For each manifest:
  │     ├── reader = ManifestReader::Make(manifest, io, ...)
  │     ├── for each entry in reader->Entries() where ShouldInclude(entry):
  │     │     AppendFileRow(builder, *entry.data_file, partition_type)
  │     │     builder.FinishRow()
  └── 4. return std::move(builder).Finish()
```

### AppendFileRow 核心逻辑

`AppendFileRow` 将 `DataFile` 的全部字段按 schema 顺序写入 builder。以 `DataFile` 的静态 `SchemaField` 定义（`kContent`, `kFilePath`, `kFileFormat`, …）为参考：

```
列 0: content (int)          → AppendInt(col, data_file.content)
列 1: file_path (string)     → AppendString(col, data_file.file_path)
列 2: file_format (string)   → ToString(data_file.file_format)
列 3: spec_id (int?)         → AppendInt or AppendNull
列 4: partition (struct)     → 对 partition 的每个 field 递归写入 children
列 5: record_count (long)    → AppendInt(col, data_file.record_count)
...
列 N: column_sizes (map<int32,int64>?) → AppendIntMap or AppendNull
列 N+1: value_counts (map<int32,int64>?) → ...
...
列 M: lower_bounds (map<int32,binary>?)  → AppendBinaryMap or AppendNull
列 M+1: upper_bounds (map<int32,binary>?) → ...
...
列 P: split_offsets (list<int64>)  → AppendListInt64 or AppendNull
列 P+1: equality_ids (list<int32>) → AppendListInt32 or AppendNull
...
```

**本 Step 需新增的 ArrowRowBuilder helpers：**

`AppendFileRow` 写入 `DataFile` 的 `column_sizes`、`value_counts`、`null_value_counts`、`nan_value_counts`（均为 `map<int32,int64>`）、`lower_bounds`、`upper_bounds`（`map<int32,binary>`）、`split_offsets`（`list<int64>`）、`equality_ids`（`list<int32>`）等字段时，需要 `arrow_row_builder_internal.h` 中尚不存在的 helpers。本 Step 的 PR 负责新增：

```cpp
Status AppendDouble(ArrowArray* array, double value);
Status AppendBinary(ArrowArray* array, std::span<const uint8_t> value);
Status AppendListInt32(ArrowArray* array, const std::vector<int32_t>& values);
Status AppendListInt64(ArrowArray* array, const std::vector<int64_t>& values);
Status AppendIntMap(ArrowArray* array, const std::map<int32_t, int64_t>& entries);
Status AppendBinaryMap(ArrowArray* array,
                       const std::map<int32_t, std::vector<uint8_t>>& entries);
```

每个 helper 遵循已有模式：操作 nanoarrow `ArrowArray*`，通过 `ArrowArrayAppend*` 和 `ArrowArrayFinishElement` 完成。例如：
```cpp
Status AppendListInt64(ArrowArray* array, const std::vector<int64_t>& values) {
  auto* elem_array = array->children[0];
  for (const auto& v : values) {
    ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayAppendInt(elem_array, v));
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(array));
  return {};
}
```

**partition struct 写入示例：**
```cpp
// partition 列是 struct 类型，cols[partition_idx] 指向 struct array
// struct array 的 children 对应 partition spec 的各字段
Status AppendPartitionStruct(ArrowArray* partition_col,
                             const PartitionValues& partition,
                             const StructType& partition_type) {
  for (size_t i = 0; i < partition_type.fields().size(); i++) {
    auto* field_col = partition_col->children[i];
    auto val = partition.ValueAt(i);
    // 根据 partition_type.fields()[i].type() 分发到 AppendInt/String/Null/...
  }
  ICEBERG_NANOARROW_RETURN_UNEXPECTED(ArrowArrayFinishElement(partition_col));
  return {};
}
```

### 测试要点

- Schema 结构与 Java `DataFile.getType(partition_type)` 一致
- `AppendFileRow()` 正确写入所有 DataFile 字段（可构造 mock DataFile，通过 `FinishAndImport` 验证）
- `ShouldInclude()` 子类化可用性：创建匿名子类验证虚函数分发
- 多个 DataFile 行连续写入后 batch 行数正确

### 预估
- 新增文件：2 个
- 修改文件：5 个（含 `arrow_row_builder_internal.h` + `.cc`）
- 新增代码行数：~500 行（AppendFileRow ~20 字段 + 6 个新 helpers）
- 新增测试行数：~250 行（含新 helpers 的单元测试）
- PR 规模：Large

---

## Step 5: BaseEntriesTable + ManifestEntriesTable

> **依赖：** Step 0
> **目标：** 创建 entry 级别抽象基类 + 当前 snapshot 的 manifest entries 表
> **与 Step 4 的关系：** 独立，Entry 表输出 `ManifestEntry` 行（含嵌套 `DataFile` struct），File 表输出平铺的 `DataFile` 行

### 新增文件

| 文件 | 内容 |
|------|------|
| `inspect/base_entries_table.h` | `BaseEntriesTable` 抽象类声明 |
| `inspect/base_entries_table.cc` | Entry schema 构建、manifest 收集、`AppendEntryRow()` 等 |
| `inspect/manifest_entries_table.h` | `ManifestEntriesTable` 声明 |
| `inspect/manifest_entries_table.cc` | 具体实现：当前 snapshot 的 entries |

### 修改文件

| 文件 | 变更内容 |
|------|---------|
| `src/iceberg/inspect/metadata_table.cc` | Factory 添加 `kEntries` |
| `src/iceberg/type_fwd.h` | 前向声明 |
| `src/iceberg/CMakeLists.txt` | 添加 3 个 `.cc` |
| `src/iceberg/inspect/meson.build` | 添加 3 个 `.h` |

### 类设计

```cpp
class ICEBERG_EXPORT BaseEntriesTable : public MetadataTable {
 public:
  Result<ArrowArray> Scan(
      std::optional<SnapshotSelection> snapshot_selection) override;

  bool supports_time_travel() const noexcept override { return true; }

 protected:
  BaseEntriesTable(std::shared_ptr<Table> source_table,
                   TableIdentifier identifier,
                   std::shared_ptr<Schema> schema);

  /// Build the entry schema: status, snapshot_id?, sequence_number?,
  /// file_sequence_number?, data_file (nested struct).
  static std::shared_ptr<Schema> MakeSchema(
      std::shared_ptr<StructType> partition_type);

  /// Append one ManifestEntry row to the builder.
  static Status AppendEntryRow(ArrowRowBuilder& builder,
                               const ManifestEntry& entry,
                               std::shared_ptr<StructType> partition_type);
};
```

### Schema（ManifestEntry 行）

```
status (int), snapshot_id (long?), sequence_number (long?),
file_sequence_number (long?), data_file (struct)
```

`data_file` 是嵌套 struct，其字段定义与 `BaseFilesTable::AppendFileRow` 输出的 DataFile 结构一致。`AppendEntryRow` 内部复用 `AppendFileRow` 相同的 partition struct 写入逻辑。

### 关键差异：Entries vs Files

| 维度 | EntriesTable | FilesTable |
|------|-------------|------------|
| 输出粒度 | 每行 = 1 个 manifest entry | 每行 = 1 个 data file |
| 包含字段 | status + snapshot_id + seq_num + nested data_file | data_file 所有字段（平铺） |
| status 过滤 | 包含 ADDED/EXISTING/DELETED | 通常只包含 ADDED/EXISTING |
| 用途 | 查看 manifest 变更历史 | 查看当前活跃文件列表 |

### 测试要点

- Entry schema 正确（含嵌套 data_file struct）
- `AppendEntryRow()` 正确写入 ManifestEntry → ArrowArray
- Time travel：指定 snapshot_id 读取历史 entry
- status 字段值映射：0=EXISTING, 1=ADDED, 2=DELETED

### 预估
- 新增文件：4 个
- 修改文件：4 个
- 新增代码行数：~350 行
- 新增测试行数：~200 行
- PR 规模：Medium

---

## Step 6: PartitionsTable

> **依赖：** Step 0
> **目标：** 实现分区级别聚合统计表
> **关键依赖：** `ManifestReader`（已存在）、partition key grouping

### 新增文件

| 文件 | 内容 |
|------|------|
| `inspect/partitions_table.h` | `PartitionsTable` 声明（time-travel 支持） |
| `inspect/partitions_table.cc` | Schema（11 列）+ partition 聚合逻辑 |

### 修改文件

| 文件 | 变更内容 |
|------|---------|
| `src/iceberg/inspect/metadata_table.cc` | Factory 添加 `kPartitions` |
| `src/iceberg/type_fwd.h` | 前向声明 |
| `src/iceberg/CMakeLists.txt` | 添加 `partitions_table.cc` |
| `src/iceberg/inspect/meson.build` | 添加 `partitions_table.h` |

### 聚合算法

```
1. 获取目标 snapshot（支持 time travel）
2. 读取所有 data manifests 的 live entries
3. 按 partition key 分组
   - partition key = entry.data_file.partition (PartitionValues)
   - 使用 std::unordered_map<PartitionValues, AggregateResult, PartitionValuesHash>
4. 对每个 partition group 聚合：
   - record_count: sum(entry.data_file.record_count)
   - file_count: count(*)
   - total_data_file_size_in_bytes: sum(entry.data_file.file_size_in_bytes)
   - 按 data_file.content 分别统计 position/equality delete 数量和记录数
   - last_updated_at: max(snapshot.timestamp_ms)
   - last_updated_snapshot_id: max(snapshot.snapshot_id)
5. 用 ArrowRowBuilder 输出每个 partition 一行
```

### Schema（11 列）

```
partition (struct), spec_id (int), record_count (long), file_count (int),
total_data_file_size_in_bytes (long),
position_delete_record_count (long), position_delete_file_count (int),
equality_delete_record_count (long), equality_delete_file_count (int),
last_updated_at (timestamptz), last_updated_snapshot_id (long)
```

### 测试要点

- 不同 partition 的数据正确分组
- 聚合数值计算正确
- 空 snapshot 返回 0 行
- 只有 data files 没有 delete files 时，delete 计数字段为 0
- Time travel 支持

### 预估
- 新增文件：2 个
- 修改文件：4 个
- 新增代码行数：~350 行
- 新增测试行数：~250 行
- PR 规模：Medium

---

## Step 7: FilesTable + DataFilesTable + DeleteFilesTable

> **依赖：** Step 4（`BaseFilesTable`）
> **目标：** 实现当前 snapshot 的文件级别元数据表

### 新增文件

| 文件 | 内容 |
|------|------|
| `inspect/files_table.h` | `FilesTable` — data + delete manifests |
| `inspect/files_table.cc` | 实现：扫描所有 manifests |
| `inspect/data_files_table.h` | `DataFilesTable` — data manifests only |
| `inspect/data_files_table.cc` | 实现：过滤 content == kData |
| `inspect/delete_files_table.h` | `DeleteFilesTable` — delete manifests only |
| `inspect/delete_files_table.cc` | 实现：过滤 content != kData |

### 修改文件

| 文件 | 变更内容 |
|------|---------|
| `src/iceberg/inspect/metadata_table.cc` | Factory 添加 `kFiles`、`kDataFiles`、`kDeleteFiles` |
| `src/iceberg/type_fwd.h` | 前向声明 |
| `src/iceberg/CMakeLists.txt` | 添加 3 个 `.cc` |
| `src/iceberg/inspect/meson.build` | 添加 3 个 `.h` |

### 三个表的关系

```
BaseFilesTable
  ├── FilesTable        ── ShouldInclude() = true
  │     manifests = AllManifests(current_snapshot)
  │
  ├── DataFilesTable    ── ShouldInclude() = content == kData
  │     manifests = DataManifests(current_snapshot)
  │
  └── DeleteFilesTable  ── ShouldInclude() = content != kData
        manifests = DeleteManifests(current_snapshot)
```

### 实现要点

每个具体表只有 ~30 行代码（thin wrapper）：

```cpp
// data_files_table.cc 示例
class DataFilesTable : public BaseFilesTable {
 public:
  static Result<std::unique_ptr<DataFilesTable>> Make(std::shared_ptr<Table> table);

  Kind kind() const noexcept override { return Kind::kDataFiles; }
  bool supports_time_travel() const noexcept override { return true; }

 protected:
  bool ShouldInclude(const ManifestEntry& entry) const override {
    return entry.data_file->content == DataFile::Content::kData;
  }

  Result<std::vector<ManifestFile>> CollectManifests(
      std::shared_ptr<FileIO> io,
      std::optional<SnapshotSelection> snapshot_selection) override {
    // Resolve snapshot → SnapshotCache::DataManifests(io)
  }
};
```

### 测试要点

- 三个表返回不同的文件子集（data vs delete）
- `FilesTable` 返回 data + delete 文件的并集
- `DataFilesTable` 只返回 data files（content == 0）
- `DeleteFilesTable` 只返回 position 和 equality delete files
- Time travel 正确
- `readable_metrics` 列暂不实现（标记为 TODO）

### 预估
- 新增文件：6 个
- 修改文件：4 个
- 新增代码行数：~300 行（thin wrappers）
- 新增测试行数：~300 行
- PR 规模：Medium

---

## Step 8: All*Files Tables

> **依赖：** Step 4（`BaseFilesTable`）
> **目标：** 实现跨所有 snapshot 的文件级别元数据表
> **注意：** 不依赖 Step 7

### 新增文件

| 文件 | 内容 |
|------|------|
| `inspect/all_files_table.h` | `AllFilesTable` — 全 snapshot，所有 manifest |
| `inspect/all_files_table.cc` | 实现：遍历所有 snapshot 收集 manifests，去重 |
| `inspect/all_data_files_table.h` | `AllDataFilesTable` — 全 snapshot，只 data manifests |
| `inspect/all_data_files_table.cc` | 实现 |
| `inspect/all_delete_files_table.h` | `AllDeleteFilesTable` — 全 snapshot，只 delete manifests |
| `inspect/all_delete_files_table.cc` | 实现 |

### 修改文件

| 文件 | 变更内容 |
|------|---------|
| `src/iceberg/inspect/metadata_table.cc` | Factory 添加 `kAllFiles`、`kAllDataFiles`、`kAllDeleteFiles` |
| `src/iceberg/type_fwd.h` | 前向声明 |
| `src/iceberg/CMakeLists.txt` | 添加 3 个 `.cc` |
| `src/iceberg/inspect/meson.build` | 添加 3 个 `.h` |

### 与 Step 7 的关键差异

| 维度 | FilesTable (Step 7) | AllFilesTable (Step 8) |
|------|---------------------|------------------------|
| Snapshot 范围 | 当前 snapshot | 所有 snapshot |
| 去重 | 不需要（单个 snapshot） | 按 manifest_path 去重 |
| Time travel | 支持 | 不支持（`supports_time_travel() = false`） |
| 可能重复 | 无 | 有（同一文件可能在不同 snapshot 中多次出现） |

### CollectManifests 实现

```cpp
Result<std::vector<ManifestFile>> AllDataFilesTable::CollectManifests(
    std::shared_ptr<FileIO> io,
    std::optional<SnapshotSelection> /* unused */) {
  std::unordered_set<std::string> seen;
  std::vector<ManifestFile> manifests;
  for (const auto& snapshot : source_table()->snapshots()) {
    SnapshotCache cache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto files, cache.DataManifests(io));
    for (const auto& m : files) {
      if (seen.insert(m.manifest_path).second) {
        manifests.push_back(m);
      }
    }
  }
  return manifests;
}
```

### 测试要点

- 跨 snapshot 去重正确
- 历史文件出现在结果中
- 与 `FilesTable`（Step 7）结果对比：对于只有 1 个 snapshot 的表，两者结果相同
- 多个 snapshot 后结果包含所有历史文件

### 预估
- 新增文件：6 个
- 修改文件：4 个
- 新增代码行数：~250 行
- 新增测试行数：~250 行
- PR 规模：Medium

---

## Step 9: AllEntriesTable

> **依赖：** Step 5（`BaseEntriesTable`）
> **目标：** 实现跨所有 snapshot 的 entry 级别元数据表

### 新增文件

| 文件 | 内容 |
|------|------|
| `inspect/all_entries_table.h` | `AllEntriesTable` 声明 |
| `inspect/all_entries_table.cc` | 实现：遍历所有 snapshot，收集 manifests，读 entries |

### 修改文件

| 文件 | 变更内容 |
|------|---------|
| `src/iceberg/inspect/metadata_table.cc` | Factory 添加 `kAllEntries` |
| `src/iceberg/type_fwd.h` | 前向声明 |
| `src/iceberg/CMakeLists.txt` | 添加 `all_entries_table.cc` |
| `src/iceberg/inspect/meson.build` | 添加 `all_entries_table.h` |

### 实现要点

与 `ManifestEntriesTable`（Step 5）的区别仅在于 manifest 收集范围：
- `ManifestEntriesTable` → 当前 snapshot 的 manifests
- `AllEntriesTable` → 所有 snapshot 的 manifests（去重）

其余逻辑（entry 过滤、行写入）继承自 `BaseEntriesTable`。

### 预估
- 新增文件：2 个
- 修改文件：4 个
- 新增代码行数：~100 行
- 新增测试行数：~150 行
- PR 规模：Small

---

## Step 10: Catalog Integration（可选）

> **依赖：** Step 7, 8, 9（所有 table 类型就绪）
> **目标：** 在 Catalog 中自动解析 metadata table 名称，透明返回 metadata table

### 修改文件

| 文件 | 变更内容 |
|------|---------|
| `src/iceberg/catalog.h` | `Catalog` 基类添加 `LoadMetadataTable()` 虚方法 |
| `src/iceberg/inspect/metadata_table.h` | 添加 `KindFromName(string_view) → optional<Kind>` 静态方法 |
| `src/iceberg/catalog/session_catalog.cc` | `LoadTable()` 中检测 metadata table 名称后缀 |
| `src/iceberg/catalog/memory/in_memory_catalog.cc` | 同上 |

### 命名解析逻辑

```cpp
// metadata_table.h
static std::optional<Kind> MetadataTable::KindFromName(std::string_view table_name) {
  // 从右向左匹配已知 metadata table 后缀
  // "orders.history" → Kind::kHistory
  // "orders.data_files" → Kind::kDataFiles
  // "orders" → nullopt
}
```

### 预估
- 修改文件：4 个
- 新增代码行数：~120 行
- 新增测试行数：~100 行
- PR 规模：Small

---

## Post-MVP（后续步骤）

| 事项 | 说明 |
|------|------|
| **PositionDeletesTable** | 需要 `iceberg_data` 库（Puffin/DV 支持），复杂度高，建议单独 PR |
| **readable_metrics 虚拟列** | binary bounds → 可读字符串，需要 partition transform 集成 |
| **Lazy/Streaming Scan** | `Scan()` → 返回逐行迭代器替代全量 ArrowArray，处理大表 |
| **Column Projection** | `Scan(projected_columns)` 支持只读部分列以提升性能 |
| **MetadataLogEntriesTable latest_* 字段** | 读取历史 metadata 文件以填充 snapshot/schema/sequence 信息 |

---

## 汇总

### 文件清单（全部 10 个 Step 完成后）

**新增文件（~32 个）：**
```
src/iceberg/inspect/
├── refs_table.h                           # Step 2
├── refs_table.cc                          # Step 2
├── metadata_log_entries_table.h           # Step 2
├── metadata_log_entries_table.cc          # Step 2
├── manifests_table.h                      # Step 3
├── manifests_table.cc                     # Step 3
├── all_manifests_table.h                  # Step 3
├── all_manifests_table.cc                 # Step 3
├── base_files_table.h                     # Step 4
├── base_files_table.cc                    # Step 4
├── base_entries_table.h                   # Step 5
├── base_entries_table.cc                  # Step 5
├── manifest_entries_table.h              # Step 5
├── manifest_entries_table.cc             # Step 5
├── partitions_table.h                     # Step 6
├── partitions_table.cc                    # Step 6
├── files_table.h                          # Step 7
├── files_table.cc                         # Step 7
├── data_files_table.h                     # Step 7
├── data_files_table.cc                    # Step 7
├── delete_files_table.h                   # Step 7
├── delete_files_table.cc                  # Step 7
├── all_files_table.h                      # Step 8
├── all_files_table.cc                     # Step 8
├── all_data_files_table.h                 # Step 8
├── all_data_files_table.cc                # Step 8
├── all_delete_files_table.h               # Step 8
├── all_delete_files_table.cc              # Step 8
├── all_entries_table.h                    # Step 9
└── all_entries_table.cc                   # Step 9
```

**修改文件（每个 Step 的增量）：**
```
src/iceberg/inspect/metadata_table.h   — Kind, SnapshotSelection, Scan() (Step 0)
src/iceberg/inspect/metadata_table.cc  — Factory dispatch (Step 0 + 各 Step 新增)
src/iceberg/inspect/snapshots_table.h  — Scan() (Step 1)
src/iceberg/inspect/snapshots_table.cc — Scan() impl (Step 1)
src/iceberg/inspect/history_table.h    — Scan() (Step 1)
src/iceberg/inspect/history_table.cc   — Scan() impl (Step 1)
src/iceberg/arrow_row_builder_internal.h — new helpers (Step 4)
src/iceberg/arrow_row_builder.cc        — new helpers impl (Step 4)
src/iceberg/type_fwd.h                 — forward decls (multiple steps)
src/iceberg/CMakeLists.txt             — new .cc files (multiple steps)
src/iceberg/inspect/meson.build        — new .h files (multiple steps)
src/iceberg/test/CMakeLists.txt        — new test targets (multiple steps)
src/iceberg/catalog.h                  — LoadMetadataTable (Step 10)
src/iceberg/catalog/session_catalog.cc — metadata table resolution (Step 10)
```

### 各 Step 规模汇总

| Step | PR 规模 | 新增文件 | 新增代码 | 测试代码 | 依赖 |
|------|---------|---------|---------|---------|------|
| 0: Framework | Small | 0 | ~80 | ~60 | — |
| 1: Snapshots + History | Medium | 0 | ~180 | ~200 | Step 0 |
| 2: Refs + MetadataLog | Medium | 4 | ~250 | ~200 | Step 0 |
| 3: Manifests + AllManifests | Medium | 4 | ~400 | ~300 | Step 0 |
| 4: BaseFilesTable | Large | 2 | ~500 | ~250 | Step 0 |
| 5: BaseEntries + Entries | Medium | 4 | ~350 | ~200 | Step 0 |
| 6: PartitionsTable | Medium | 2 | ~350 | ~250 | Step 0 |
| 7: Files/Data/Delete | Medium | 6 | ~300 | ~300 | Step 4 |
| 8: All*Files | Medium | 6 | ~250 | ~250 | Step 4 |
| 9: AllEntriesTable | Small | 2 | ~100 | ~150 | Step 5 |
| 10: Catalog Integration | Small | 0 | ~120 | ~100 | Step 7,8,9 |
| **总计** | — | **~30** | **~2880** | **~2260** | — |

### 并行开发策略

```
Week 1-2:  Step 0 (必须先完成)
Week 2-4:  Step 1, 2, 3, 4, 5, 6  ← 6 人可并行
Week 4-5:  Step 7, 8 (depends on Step 4)
           Step 9 (depends on Step 5)
Week 5-6:  Step 10 (depends on 7, 8, 9)
```

Step 1-6 之间完全没有依赖关系，可以由不同开发者并行推进。Step 7 和 Step 8 都只依赖 Step 4，在 Step 4 完成后也可以并行。Step 9 依赖 Step 5，相对独立。
