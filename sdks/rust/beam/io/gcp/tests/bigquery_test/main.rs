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

//! Tests for Google Cloud BigQuery: the cross-language transforms, table specs, and the
//! native Storage Read and Write connectors (`storage_*`).

mod builders;
mod fixtures;
mod pipeline;
mod read;
mod storage_read;
mod storage_read_failures;
mod storage_read_options;
mod storage_read_restriction;
mod storage_schema;
mod storage_write;
mod storage_write_append;
mod storage_write_buffered;
mod storage_write_encode;
mod storage_write_options;
mod storage_write_projection;
mod storage_write_schema;
mod table;
mod write;
