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

/// \file iceberg/table_identifier.h
/// A TableIdentifier is a unique identifier for a table

#include <format>
#include <sstream>
#include <string>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief A namespace in a catalog.
struct ICEBERG_EXPORT Namespace {
  std::vector<std::string> levels;

  bool operator==(const Namespace& other) const { return levels == other.levels; }

  std::string ToString() const {
    std::ostringstream oss;
    for (size_t i = 0; i < levels.size(); ++i) {
      if (i) oss << '.';
      oss << levels[i];
    }
    return oss.str();
  }
};

/// \brief Identifies a table in iceberg catalog.
struct ICEBERG_EXPORT TableIdentifier {
  Namespace ns;
  std::string name;

  bool operator==(const TableIdentifier& other) const {
    return ns == other.ns && name == other.name;
  }

  /// \brief Validates the TableIdentifier.
  Status Validate() const {
    if (name.empty()) {
      return Invalid("Invalid table identifier: missing table name");
    }
    return {};
  }

  std::string ToString() const { return ns.ToString() + '.' + name; }
};

}  // namespace iceberg

namespace std {

template <>
struct formatter<iceberg::Namespace> : std::formatter<std::string> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  auto format(const iceberg::Namespace& ns, format_context& ctx) const {
    return std::formatter<std::string>::format(ns.ToString(), ctx);
  }
};

template <>
struct formatter<iceberg::TableIdentifier> : std::formatter<std::string> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  auto format(const iceberg::TableIdentifier& id, format_context& ctx) const {
    return std::formatter<std::string>::format(id.ToString(), ctx);
  }
};
}  // namespace std
