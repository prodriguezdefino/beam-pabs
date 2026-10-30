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

//! Server-side `RowFilter` of a read configuration: the encoded `row_filter` and the native
//! value bitmask options.

use beam::io::gcp::bigtable::RowFilterExt as _;
use beam::io::gcp::bigtable::proto::RowFilter;
use beam::io::gcp::bigtable::read::ReadOptions;
use bytes::Bytes;

/// The value bitmask options of a read configuration.
pub(super) struct BitmaskConfig {
    pub(super) mask: Option<Bytes>,
    pub(super) family: Option<String>,
    pub(super) qualifier: Option<Bytes>,
}

/// Sets the server-side filter of `options`. With both filters, the read applies `row_filter`
/// and then the bitmask filter.
pub(super) fn with_filters(
    options: ReadOptions,
    row_filter: Option<Bytes>,
    bitmask: BitmaskConfig,
) -> Result<ReadOptions, String> {
    let options = match row_filter.filter(|f| !f.is_empty()) {
        Some(bytes) => options.with_row_filter_bytes(bytes.to_vec()),
        None => options,
    };
    let Some(bitmask) = bitmask_filter(bitmask)? else {
        return Ok(options);
    };
    let filter = match options.decoded_filter()? {
        Some(row_filter) => RowFilter::chain([row_filter, bitmask]),
        None => bitmask,
    };
    Ok(options.with_row_filter(&filter))
}

fn bitmask_filter(config: BitmaskConfig) -> Result<Option<RowFilter>, String> {
    match config {
        BitmaskConfig {
            mask: None,
            family: None,
            qualifier: None,
        } => Ok(None),
        BitmaskConfig {
            mask: Some(mask),
            family: None,
            qualifier: None,
        } => Ok(Some(RowFilter::value_bitmask(mask))),
        BitmaskConfig {
            mask: Some(mask),
            family: Some(family),
            qualifier: Some(qualifier),
        } => Ok(Some(RowFilter::column_value_bitmask(
            &family, qualifier, mask,
        ))),
        BitmaskConfig { mask: None, .. } => {
            Err("value_bitmask_family and value_bitmask_qualifier need value_bitmask".to_string())
        }
        BitmaskConfig { mask: Some(_), .. } => {
            Err("Set both value_bitmask_family and value_bitmask_qualifier, or neither".to_string())
        }
    }
}
