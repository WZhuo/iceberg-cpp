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

#include "iceberg/util/location_util.h"

#include <gtest/gtest.h>

namespace iceberg {

TEST(LocationUtilTest, StripTrailingSlash) {
  // Test normal paths with trailing slashes
  ASSERT_EQ(LocationUtil::StripTrailingSlash("/path/to/dir/"), "/path/to/dir");
  ASSERT_EQ(LocationUtil::StripTrailingSlash("/path/to/dir//"), "/path/to/dir");
  ASSERT_EQ(LocationUtil::StripTrailingSlash("/path/to/dir///"), "/path/to/dir");

  // Test paths without trailing slashes
  ASSERT_EQ(LocationUtil::StripTrailingSlash("/path/to/dir"), "/path/to/dir");
  ASSERT_EQ(LocationUtil::StripTrailingSlash("/path/to/file.txt"), "/path/to/file.txt");

  // Test root path
  ASSERT_EQ(LocationUtil::StripTrailingSlash("/"), "");
  ASSERT_EQ(LocationUtil::StripTrailingSlash("//"), "");

  // Test empty string
  ASSERT_EQ(LocationUtil::StripTrailingSlash(""), "");

  // Test URLs with protocols
  ASSERT_EQ(LocationUtil::StripTrailingSlash("http://example.com/"),
            "http://example.com");
  ASSERT_EQ(LocationUtil::StripTrailingSlash("https://example.com/path/"),
            "https://example.com/path");

  // Test that protocol endings are preserved
  ASSERT_EQ(LocationUtil::StripTrailingSlash("http://"), "http://");
  ASSERT_EQ(LocationUtil::StripTrailingSlash("https://"), "https://");
  ASSERT_EQ(LocationUtil::StripTrailingSlash("s3://"), "s3://");

  // Test paths with protocol-like substrings in the middle
  ASSERT_EQ(LocationUtil::StripTrailingSlash("/path/http://test/"), "/path/http://test");
  ASSERT_EQ(LocationUtil::StripTrailingSlash("/path/https://test/"),
            "/path/https://test");

  // Test multiple slashes not at the end
  ASSERT_EQ(LocationUtil::StripTrailingSlash("/path//to/dir/"), "/path//to/dir");
  ASSERT_EQ(LocationUtil::StripTrailingSlash("/path///to/dir/"), "/path///to/dir");
}

}  // namespace iceberg
