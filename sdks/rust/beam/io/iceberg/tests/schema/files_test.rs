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

//! Parquet writer settings from table properties, and the hash directories of object storage.

use std::collections::HashMap;

use beam_io_iceberg::location::hash_dirs;
use beam_io_iceberg::parquet::{target_file_size, writer_properties};
use parquet::basic::{Compression, ZstdLevel};
use parquet::schema::types::ColumnPath;

fn properties(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

#[test]
fn a_table_with_no_properties_gets_the_iceberg_defaults() {
    let props = writer_properties(&HashMap::new()).expect("the defaults are valid");
    let column = ColumnPath::from("any");
    assert_eq!(
        props.compression(&column),
        Compression::ZSTD(ZstdLevel::default())
    );
    assert_eq!(props.max_row_group_bytes(), Some(128 * 1024 * 1024));
    assert_eq!(props.data_page_size_limit(), 1024 * 1024);
    assert_eq!(props.data_page_row_count_limit(), 20_000);
    assert_eq!(props.dictionary_page_size_limit(), 2 * 1024 * 1024);
    assert_eq!(
        target_file_size(&HashMap::new()).expect("a size"),
        512 * 1024 * 1024
    );
}

#[test]
fn the_properties_set_the_codec_level_and_sizes() {
    let props = writer_properties(&properties(&[
        ("write.parquet.compression-codec", "ZSTD"),
        ("write.parquet.compression-level", "9"),
        ("write.parquet.page-size-bytes", "4096"),
    ]))
    .expect("the properties are valid");
    let column = ColumnPath::from("any");
    assert_eq!(
        props.compression(&column),
        Compression::ZSTD(ZstdLevel::try_new(9).expect("a level"))
    );
    assert_eq!(props.data_page_size_limit(), 4096);
}

#[test]
fn an_unknown_codec_or_a_bad_number_names_its_property() {
    for (key, value) in [
        ("write.parquet.compression-codec", "brotli"),
        ("write.parquet.page-size-bytes", "big"),
        ("write.parquet.compression-level", "x"),
    ] {
        let error = writer_properties(&properties(&[(key, value)])).expect_err("not valid");
        assert!(error.to_string().contains(key), "{error}");
    }
    target_file_size(&properties(&[("write.target-file-size-bytes", "-1")]))
        .expect_err("not a size");
}

#[test]
fn hash_directories_are_20_bits_in_dirs_of_4_4_4_and_8() {
    let dirs = hash_dirs("data-file-00001.parquet");
    let parts: Vec<&str> = dirs.split('/').collect();
    assert_eq!(
        parts.iter().map(|part| part.len()).collect::<Vec<_>>(),
        vec![4, 4, 4, 8]
    );
    assert!(dirs.chars().all(|c| matches!(c, '0' | '1' | '/')));
    assert_eq!(dirs, hash_dirs("data-file-00001.parquet"));
    assert_ne!(dirs, hash_dirs("data-file-00002.parquet"));
}
