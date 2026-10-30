<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Vendored googleapis protos

The `google/` tree is a copy of files from
[googleapis/googleapis](https://github.com/googleapis/googleapis) at commit
`dff1cd0c7ff8c26ff4c9f7a87cbe175fb5124d4e`. Do not edit the files. They keep their
original Apache-2.0 headers.

The tree has two root protos and all the googleapis files that they import:

- `google/bigtable/v2/bigtable.proto` (cargo feature `bigtable`)
- `google/cloud/bigquery/storage/v1/storage.proto` (cargo feature `bigquery-storage`)

The `google/protobuf/*.proto` imports come from the include directory of `protoc`.
[`build.rs`](../build.rs) generates the clients of the enabled features only.

To update the tree, copy the root protos and their imports from a newer commit, then
change the commit in this file.
