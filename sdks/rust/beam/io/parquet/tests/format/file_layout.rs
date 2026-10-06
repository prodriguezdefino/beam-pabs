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

//! What the sink writes: file schema, metadata, codecs, raw values, buffering.

use std::collections::BTreeMap;

use beam::schema::BeamRow;
use chrono::{DateTime, NaiveDate, Utc};
use file::sink::FileSink;
use parquet_io::parquet::basic::Compression as Codec;
use parquet_io::parquet::file::reader::FileReader;
use parquet_io::parquet::record::Field;
use parquet_io::parquet::schema::printer::print_schema;
use parquet_io::parquetio::{Compression, ParquetSink, ZstdLevel};

use crate::common::{Simple, TempDir, column_codecs, open, simples, write_file};

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Address {
    city: String,
    zip: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Event {
    id: i64,
    small: i8,
    medium: i16,
    user: String,
    score: Option<f64>,
    ratio: f32,
    active: bool,
    tags: Vec<String>,
    attrs: BTreeMap<String, i64>,
    home: Address,
    previous: Option<Address>,
    day: NaiveDate,
    at: DateTime<Utc>,
    #[beam(bytes)]
    blob: Vec<u8>,
}

/// The Parquet file schema, as printed by the parquet crate. Guards physical
/// and logical types (`INTEGER(8,true)`, `DATE`, `TIMESTAMP(MICROS,true)`,
/// `STRING`), repetition, and the standard 3-level LIST / MAP layouts.
const EVENT_PARQUET_SCHEMA: &str = "\
message arrow_schema {
  REQUIRED INT64 id;
  REQUIRED INT32 small (INTEGER(8,true));
  REQUIRED INT32 medium (INTEGER(16,true));
  REQUIRED BYTE_ARRAY user (STRING);
  OPTIONAL DOUBLE score;
  REQUIRED FLOAT ratio;
  REQUIRED BOOLEAN active;
  REQUIRED group tags (LIST) {
    REPEATED group list {
      REQUIRED BYTE_ARRAY item (STRING);
    }
  }
  REQUIRED group attrs (MAP) {
    REPEATED group entries {
      REQUIRED BYTE_ARRAY key (STRING);
      REQUIRED INT64 value;
    }
  }
  REQUIRED group home {
    REQUIRED BYTE_ARRAY city (STRING);
    OPTIONAL INT32 zip;
  }
  OPTIONAL group previous {
    REQUIRED BYTE_ARRAY city (STRING);
    OPTIONAL INT32 zip;
  }
  REQUIRED INT32 day (DATE);
  REQUIRED INT64 at (TIMESTAMP(MICROS,true));
  REQUIRED BYTE_ARRAY blob;
}
";

#[test]
fn file_schema_and_metadata_are_exact() {
    let dir = TempDir::new("schema");
    let path = dir.file("events.parquet");
    write_file::<Event>(&ParquetSink::new(), &path, &[]);
    let reader = open(&path);
    let meta = reader.metadata().file_metadata();

    let mut printed = Vec::new();
    print_schema(&mut printed, meta.schema());
    assert_eq!(String::from_utf8(printed).unwrap(), EVENT_PARQUET_SCHEMA);

    // Only the Arrow schema hint is added as key-value metadata.
    let keys: Vec<&str> = meta
        .key_value_metadata()
        .unwrap()
        .iter()
        .map(|kv| kv.key.as_str())
        .collect();
    assert_eq!(keys, ["ARROW:schema"]);
    assert_eq!(meta.num_rows(), 0);
    assert_eq!(reader.metadata().num_row_groups(), 0);
}

#[test]
fn raw_column_values_are_micros_and_days() {
    let dir = TempDir::new("raw");
    let path = dir.file("simple.parquet");
    write_file(&ParquetSink::<Simple>::new(), &path, &simples());

    // Decoded by the parquet crate's own record API, not by arrow_io.
    let rows: Vec<Vec<(String, Field)>> = open(&path)
        .get_row_iter(None)
        .unwrap()
        .map(|row| {
            row.unwrap()
                .get_column_iter()
                .map(|(name, field)| (name.clone(), field.clone()))
                .collect()
        })
        .collect();
    let col = |name: &str, field: Field| (name.to_string(), field);
    assert_eq!(
        rows,
        vec![
            vec![
                col("id", Field::Long(1)),
                col("name", Field::Str("ab".into())),
                col("score", Field::Null),
                col("day", Field::Date(19_723)),
                col("at", Field::TimestampMicros(1_700_000_000_123_456)),
            ],
            vec![
                col("id", Field::Long(-2)),
                col("name", Field::Str(String::new())),
                col("score", Field::Double(1.5)),
                col("day", Field::Date(-1)),
                col("at", Field::TimestampMicros(-1)),
            ],
        ]
    );
}

#[test]
fn every_column_chunk_uses_the_configured_codec() {
    let dir = TempDir::new("codec");
    for (compression, expected) in [
        (None, Codec::SNAPPY),
        (Some(Compression::UNCOMPRESSED), Codec::UNCOMPRESSED),
        (Some(Compression::SNAPPY), Codec::SNAPPY),
        (
            Some(Compression::ZSTD(ZstdLevel::try_new(3).unwrap())),
            Codec::ZSTD(ZstdLevel::try_new(3).unwrap()),
        ),
    ] {
        let path = dir.file(&format!("{expected:?}.parquet"));
        let sink = match compression {
            None => ParquetSink::<Simple>::new(),
            Some(c) => ParquetSink::<Simple>::new().with_compression(c),
        };
        write_file(&sink, &path, &simples());
        let codecs = column_codecs(&path);
        assert_eq!(codecs.len(), 5);
        // The file footer records the codec but not the ZSTD level; compare kinds.
        for codec in codecs {
            assert_eq!(
                std::mem::discriminant(&codec),
                std::mem::discriminant(&expected),
                "{compression:?}"
            );
        }
    }
}

#[test]
fn buffered_bytes_grows_per_batch_and_resets_after_row_group_flush() {
    let sink = ParquetSink::<Simple>::new()
        .with_batch_size(10)
        .with_row_group_size(50);
    let mut writer = sink.open(Box::new(std::io::sink())).unwrap();
    let item = simples()[0].clone();
    assert_eq!(writer.buffered_bytes(), 0);

    let mut after_batches = Vec::new();
    for i in 1..=60 {
        writer.write(&item).unwrap();
        if i == 5 {
            // Rows waiting for a batch are not encoded yet.
            assert_eq!(writer.buffered_bytes(), 0);
        }
        if i % 10 == 0 {
            after_batches.push(writer.buffered_bytes());
        }
    }
    let (in_group, rest) = after_batches.split_at(4);
    assert!(
        in_group.windows(2).all(|w| w[0] < w[1]) && in_group[0] > 0,
        "should grow with every batch: {after_batches:?}"
    );
    // The 50th row completes the row group, which is flushed to the output.
    assert_eq!(rest[0], 0, "{after_batches:?}");
    assert!(rest[1] > 0, "{after_batches:?}");
    writer.finish().unwrap();
}
