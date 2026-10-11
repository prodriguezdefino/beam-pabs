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

//! Corrupt, truncated and missing files.

use std::fs;

use parquet_io::parquetio::{ParquetIoError, ParquetSink, schema_of};

use crate::common::{Simple, TempDir, read_with_reader, simples, write_file};

enum SchemaErr {
    /// A Parquet error whose message contains the given text.
    Parquet(&'static str),
    NotFound,
}

#[test]
fn damaged_and_missing_files_are_rejected() {
    let dir = TempDir::new("corrupt");
    let valid = dir.file("valid.parquet");
    write_file(&ParquetSink::<Simple>::new(), &valid, &simples());
    let bytes = fs::read(&valid).unwrap();
    // Drop the trailing "PAR1" magic and part of the footer length.
    let truncated = bytes[..bytes.len() - 6].to_vec();
    // Point the footer length past the start of the file.
    let mut bad_footer_len = bytes.clone();
    let len_at = bytes.len() - 8;
    bad_footer_len[len_at..len_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());

    let footer_err = Some("Failed to read Parquet footer of '");
    let cases = [
        (
            "truncated",
            Some(truncated),
            SchemaErr::Parquet("Corrupt footer"),
            footer_err,
        ),
        (
            "bad_footer_len",
            Some(bad_footer_len),
            SchemaErr::Parquet(""),
            footer_err,
        ),
        (
            "not_parquet",
            Some(b"this is plainly not a parquet file".to_vec()),
            SchemaErr::Parquet(""),
            None,
        ),
        ("missing", None, SchemaErr::NotFound, None),
    ];
    for (name, contents, schema_err, reader_err) in cases {
        let path = dir.file(&format!("{name}.parquet"));
        if let Some(contents) = contents {
            fs::write(&path, contents).unwrap();
        }
        match (schema_of(&path), schema_err) {
            (Err(ParquetIoError::Parquet(err)), SchemaErr::Parquet(text)) => {
                assert!(err.to_string().contains(text), "{name}: {err}");
            }
            (Err(ParquetIoError::Io(err)), SchemaErr::NotFound) => {
                assert_eq!(err.kind(), std::io::ErrorKind::NotFound, "{name}");
            }
            (other, _) => panic!("{name}: unexpected schema_of result {other:?}"),
        }
        if let Some(prefix) = reader_err {
            let err = read_with_reader::<Simple>(&path).unwrap_err();
            assert!(err.starts_with(prefix), "{name}: {err}");
        }
    }
}
