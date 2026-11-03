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

//! Parquet writer settings from the properties of a table, with the Iceberg defaults.

use std::collections::HashMap;
use std::str::FromStr;

use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;

const COMPRESSION_CODEC: &str = "write.parquet.compression-codec";
const COMPRESSION_LEVEL: &str = "write.parquet.compression-level";
const ROW_GROUP_BYTES: &str = "write.parquet.row-group-size-bytes";
const PAGE_BYTES: &str = "write.parquet.page-size-bytes";
const PAGE_ROWS: &str = "write.parquet.page-row-limit";
const DICTIONARY_BYTES: &str = "write.parquet.dict-size-bytes";
const TARGET_FILE_BYTES: &str = "write.target-file-size-bytes";

const DEFAULT_ROW_GROUP_BYTES: usize = 128 * 1024 * 1024;
const DEFAULT_PAGE_BYTES: usize = 1024 * 1024;
const DEFAULT_PAGE_ROWS: usize = 20_000;
const DEFAULT_DICTIONARY_BYTES: usize = 2 * 1024 * 1024;
pub const DEFAULT_TARGET_FILE_BYTES: usize = 512 * 1024 * 1024;

fn number<T: FromStr>(
    properties: &HashMap<String, String>,
    key: &str,
    default: T,
) -> beam::Result<T> {
    properties
        .get(key)
        .map(|value| {
            value.parse().map_err(|_| {
                beam::Error::msg(format!(
                    "The table property {key} = '{value}' is not a number"
                ))
            })
        })
        .unwrap_or(Ok(default))
}

/// The Parquet writer settings of a table.
///
/// # Errors
///
/// Fails if a property is not a number, or if the codec or its level is not supported.
pub fn writer_properties(properties: &HashMap<String, String>) -> beam::Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(compression(properties)?)
        .set_max_row_group_bytes(Some(number(
            properties,
            ROW_GROUP_BYTES,
            DEFAULT_ROW_GROUP_BYTES,
        )?))
        .set_data_page_size_limit(number(properties, PAGE_BYTES, DEFAULT_PAGE_BYTES)?)
        .set_data_page_row_count_limit(number(properties, PAGE_ROWS, DEFAULT_PAGE_ROWS)?)
        .set_dictionary_page_size_limit(number(
            properties,
            DICTIONARY_BYTES,
            DEFAULT_DICTIONARY_BYTES,
        )?)
        .build())
}

/// The size at which the writer starts a new data file.
///
/// # Errors
///
/// Fails if the property is not a number.
pub fn target_file_size(properties: &HashMap<String, String>) -> beam::Result<usize> {
    number(properties, TARGET_FILE_BYTES, DEFAULT_TARGET_FILE_BYTES)
}

fn compression(properties: &HashMap<String, String>) -> beam::Result<Compression> {
    let codec = properties
        .get(COMPRESSION_CODEC)
        .map_or("zstd", String::as_str)
        .to_ascii_lowercase();
    let level = properties
        .get(COMPRESSION_LEVEL)
        .map(|value| {
            value.parse::<i32>().map_err(|_| {
                beam::Error::msg(format!(
                    "The table property {COMPRESSION_LEVEL} = '{value}' is not a number"
                ))
            })
        })
        .transpose()?;
    let bad_level = |e: parquet::errors::ParquetError| {
        beam::Error::msg(format!("The table property {COMPRESSION_LEVEL}: {e}"))
    };
    match (codec.as_str(), level) {
        ("zstd", None) => Ok(Compression::ZSTD(ZstdLevel::default())),
        ("zstd", Some(level)) => Ok(Compression::ZSTD(
            ZstdLevel::try_new(level).map_err(bad_level)?,
        )),
        ("gzip", None) => Ok(Compression::GZIP(GzipLevel::default())),
        ("gzip", Some(level)) => {
            let level = u32::try_from(level).map_err(beam::Error::msg)?;
            Ok(Compression::GZIP(
                GzipLevel::try_new(level).map_err(bad_level)?,
            ))
        }
        ("snappy", _) => Ok(Compression::SNAPPY),
        ("lz4", _) => Ok(Compression::LZ4),
        ("uncompressed" | "none", _) => Ok(Compression::UNCOMPRESSED),
        (other, _) => Err(beam::Error::msg(format!(
            "The table property {COMPRESSION_CODEC} = '{other}' is not a codec that the write \
             supports: zstd, gzip, snappy, lz4 or uncompressed"
        ))),
    }
}
