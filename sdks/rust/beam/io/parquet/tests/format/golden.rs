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

//! Reading a file written with the low-level parquet API.

use std::fs;
use std::sync::Arc;

use arrow_io::schema::{date_type, micros_instant_type, millis_instant_type};
use beam::schema::{BeamRow, FieldType, Schema};
use chrono::{DateTime, NaiveDate, Utc};
use parquet_io::parquet::basic::{Compression as Codec, Encoding};
use parquet_io::parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet_io::parquet::file::properties::WriterProperties;
use parquet_io::parquet::file::reader::FileReader;
use parquet_io::parquet::file::writer::SerializedFileWriter;
use parquet_io::parquet::schema::parser::parse_message_type;
use parquet_io::parquetio::schema_of;

use crate::common::{TempDir, open, read_with_reader};

const GOLDEN_MESSAGE: &str = "
message golden {
  REQUIRED INT64 id;
  OPTIONAL BYTE_ARRAY name (STRING);
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 at (TIMESTAMP(MICROS,true));
  OPTIONAL INT64 at_ms (TIMESTAMP(MILLIS,true));
  REQUIRED INT32 small (INTEGER(8,true));
  OPTIONAL group tags (LIST) {
    REPEATED group list {
      OPTIONAL BYTE_ARRAY element (STRING);
    }
  }
}
";

/// Writes four rows column by column, with dictionary encoding enabled and no
/// compression, and no Arrow schema hint in the footer.
fn write_golden(path: &str) {
    let schema = Arc::new(parse_message_type(GOLDEN_MESSAGE).unwrap());
    let props = Arc::new(
        WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_compression(Codec::UNCOMPRESSED)
            .build(),
    );
    let file = fs::File::create(path).unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();
    let mut rg = writer.next_row_group().unwrap();
    let s = |v: &str| ByteArray::from(v);

    let mut index = 0;
    while let Some(mut col) = rg.next_column().unwrap() {
        match index {
            0 => {
                col.typed::<Int64Type>()
                    .write_batch(&[1, 2, 3, 4], None, None)
                    .unwrap();
            }
            1 => {
                // "alpha" repeats, so the dictionary has two entries for 3 values.
                col.typed::<ByteArrayType>()
                    .write_batch(
                        &[s("alpha"), s("alpha"), s("beta")],
                        Some(&[1, 0, 1, 1]),
                        None,
                    )
                    .unwrap();
            }
            2 => {
                col.typed::<Int32Type>()
                    .write_batch(&[19_723, -1, 0, 20_000], None, None)
                    .unwrap();
            }
            3 => {
                col.typed::<Int64Type>()
                    .write_batch(&[1_700_000_000_123_456, -1, 0, 1], None, None)
                    .unwrap();
            }
            4 => {
                col.typed::<Int64Type>()
                    .write_batch(&[1_700_000_000_123, -1, 5], Some(&[1, 0, 1, 1]), None)
                    .unwrap();
            }
            5 => {
                col.typed::<Int32Type>()
                    .write_batch(&[-5, 127, 0, -128], None, None)
                    .unwrap();
            }
            6 => {
                // ["a", "b"], null, [], [null, "c"]
                col.typed::<ByteArrayType>()
                    .write_batch(
                        &[s("a"), s("b"), s("c")],
                        Some(&[3, 3, 0, 1, 2, 3]),
                        Some(&[0, 1, 0, 0, 0, 1]),
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        col.close().unwrap();
        index += 1;
    }
    assert_eq!(index, 7);
    rg.close().unwrap();
    writer.close().unwrap();
}

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Golden {
    id: i64,
    name: Option<String>,
    day: NaiveDate,
    at: DateTime<Utc>,
    at_ms: Option<DateTime<Utc>>,
    small: i8,
    tags: Option<Vec<Option<String>>>,
}

#[test]
fn golden_fixture_is_really_dictionary_encoded_without_arrow_hint() {
    let dir = TempDir::new("golden_meta");
    let path = dir.file("golden.parquet");
    write_golden(&path);
    let reader = open(&path);
    let meta = reader.metadata();
    assert!(meta.file_metadata().key_value_metadata().is_none());
    let name = meta.row_group(0).column(1);
    assert!(name.dictionary_page_offset().is_some());
    assert!(
        name.encodings().any(|e| e == Encoding::RLE_DICTIONARY),
        "{:?}",
        name.encodings().collect::<Vec<_>>()
    );
}

#[test]
fn reads_low_level_fixture_exactly() {
    let dir = TempDir::new("golden");
    let path = dir.file("golden.parquet");
    write_golden(&path);
    let ts = |s: i64, n: u32| DateTime::from_timestamp(s, n).unwrap();
    let day = |y: i32, m: u32, d: u32| NaiveDate::from_ymd_opt(y, m, d).unwrap();
    let tag = |v: &str| Some(v.to_string());
    assert_eq!(
        read_with_reader::<Golden>(&path).unwrap(),
        vec![
            Golden {
                id: 1,
                name: Some("alpha".into()),
                day: day(2024, 1, 1),
                at: ts(1_700_000_000, 123_456_000),
                at_ms: Some(ts(1_700_000_000, 123_000_000)),
                small: -5,
                tags: Some(vec![tag("a"), tag("b")]),
            },
            Golden {
                id: 2,
                name: None,
                day: day(1969, 12, 31),
                at: ts(-1, 999_999_000),
                at_ms: None,
                small: 127,
                tags: None,
            },
            Golden {
                id: 3,
                name: Some("alpha".into()),
                day: day(1970, 1, 1),
                at: ts(0, 0),
                at_ms: Some(ts(-1, 999_000_000)),
                small: 0,
                tags: Some(vec![]),
            },
            Golden {
                id: 4,
                name: Some("beta".into()),
                day: day(2024, 10, 4),
                at: ts(0, 1_000),
                at_ms: Some(ts(0, 5_000_000)),
                small: -128,
                tags: Some(vec![None, tag("c")]),
            },
        ]
    );
}

#[test]
fn schema_of_low_level_fixture_maps_logical_types() {
    let dir = TempDir::new("golden_schema");
    let path = dir.file("golden.parquet");
    write_golden(&path);
    assert_eq!(
        schema_of(&path).unwrap(),
        Schema::builder()
            .field("id", FieldType::int64())
            .nullable_field("name", FieldType::string())
            .field("day", date_type())
            .field("at", micros_instant_type())
            .nullable_field("at_ms", millis_instant_type())
            .field("small", FieldType::byte())
            .nullable_field(
                "tags",
                FieldType::array(FieldType::string().with_nullable(true))
            )
            .build()
    );
}
