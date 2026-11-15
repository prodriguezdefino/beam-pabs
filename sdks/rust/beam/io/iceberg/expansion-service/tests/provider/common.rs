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

//! Shared helpers for the Iceberg expansion provider tests.

use std::collections::BTreeMap;

use beam_io_iceberg_expansion::IcebergWriteConfig;

pub(crate) const CATALOG_URI: &str = "http://127.0.0.1:8181";

pub(crate) fn catalog_props(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

pub(crate) fn base_config(table: &str) -> IcebergWriteConfig {
    IcebergWriteConfig {
        table: table.to_string(),
        catalog_properties: Some(catalog_props(&[("uri", CATALOG_URI)])),
        ..Default::default()
    }
}
