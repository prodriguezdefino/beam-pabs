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

//! Reading a hand-encoded container file we did not write.

use std::fs;
use std::sync::Arc;

use arrow_io::schema::{date_type, decimal_type, micros_instant_type};
use arrow_io::{BeamRowCodec, RowCodec, SchemaRowCodec};
use avro_io::avroio::{AvroRecordReader, AvroSink, schema_of};
use beam::schema::{BeamRow, FieldType, FieldValue, Schema};
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker};
use chrono::{DateTime, NaiveDate, Utc};
use file::filesystem::LocalFileSystem;

use crate::common::{Simple, TempDir, bytes, long, read_all, simple, write_to_bytes};

const GOLDEN_SCHEMA: &str = concat!(
    r#"{"type":"record","name":"Golden","namespace":"org.example","fields":["#,
    r#"{"name":"id","type":"long"},"#,
    r#"{"name":"name","type":"string"},"#,
    r#"{"name":"score","type":["null","double"]},"#,
    r#"{"name":"note","type":["string","null"]},"#,
    r#"{"name":"day","type":{"type":"int","logicalType":"date"}},"#,
    r#"{"name":"at","type":{"type":"long","logicalType":"timestamp-micros"}},"#,
    r#"{"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}}"#,
    r#"]}"#,
);

const GOLDEN_SYNC: [u8; 16] = [
    0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb,
];

/// A two-block, uncompressed container with three records, encoded by hand.
fn golden_container() -> Vec<u8> {
    let mut out = b"Obj\x01".to_vec();
    // Metadata map, written as one block with a negative count (count, size) to
    // exercise that form of the spec.
    let mut entries = Vec::new();
    entries.extend(bytes(b"avro.schema"));
    entries.extend(bytes(GOLDEN_SCHEMA.as_bytes()));
    entries.extend(bytes(b"avro.codec"));
    entries.extend(bytes(b"null"));
    out.extend(long(-2));
    out.extend(long(entries.len() as i64));
    out.extend(entries);
    out.push(0x00);
    out.extend(GOLDEN_SYNC);

    // id=1, name="ab", score=null, note="hi", day=2024-01-01,
    // at=1_700_000_000.123456s, price=123.45
    let mut rec1 = vec![0x02, 0x04, b'a', b'b', 0x00, 0x00, 0x04, b'h', b'i'];
    rec1.extend([0x96, 0xb4, 0x02]);
    rec1.extend(long(1_700_000_000_123_456));
    rec1.extend([0x04, 0x30, 0x39]); // unscaled 12345
    // id=-2, name="", score=1.5, note=null, day=1969-12-31, at=-1µs, price=-1.00
    let mut rec2 = vec![0x03, 0x00, 0x02];
    rec2.extend([0, 0, 0, 0, 0, 0, 0xf8, 0x3f]);
    rec2.extend([0x02, 0x01, 0x01]);
    rec2.extend([0x02, 0x9c]); // unscaled -100
    // id=64, name="z", score=-0.25, note="", day=1970-01-01, at=0, price=0.00
    let mut rec3 = vec![0x80, 0x01, 0x02, b'z', 0x02];
    rec3.extend((-0.25f64).to_le_bytes());
    rec3.extend([0x00, 0x00, 0x00, 0x00]);
    rec3.extend([0x02, 0x00]);

    for (count, data) in [(2, [rec1, rec2].concat()), (1, rec3)] {
        out.extend(long(count));
        out.extend(bytes(&data));
        out.extend(GOLDEN_SYNC);
    }
    out
}

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Golden {
    id: i64,
    name: String,
    score: Option<f64>,
    note: Option<String>,
    day: NaiveDate,
    at: DateTime<Utc>,
}

#[test]
fn reads_hand_encoded_container_exactly() {
    let dir = TempDir::new("golden");
    let path = dir.file("golden.avro");
    fs::write(&path, golden_container()).unwrap();

    let read = read_all(Arc::new(BeamRowCodec::<Golden>::new()), &path);
    assert_eq!(
        read,
        vec![
            Golden {
                id: 1,
                name: "ab".into(),
                score: None,
                note: Some("hi".into()),
                day: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                at: DateTime::from_timestamp(1_700_000_000, 123_456_000).unwrap(),
            },
            Golden {
                id: -2,
                name: String::new(),
                score: Some(1.5),
                note: None,
                day: NaiveDate::from_ymd_opt(1969, 12, 31).unwrap(),
                at: DateTime::from_timestamp(-1, 999_999_000).unwrap(),
            },
            Golden {
                id: 64,
                name: "z".into(),
                score: Some(-0.25),
                note: Some(String::new()),
                day: NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
                at: DateTime::from_timestamp(0, 0).unwrap(),
            },
        ]
    );

    // Decimals decode to Beam decimal payloads: scale, length, and digits.
    let prices = Arc::new(Schema::builder().field("price", decimal_type()).build());
    let rows = read_all(Arc::new(SchemaRowCodec::new(Arc::clone(&prices))), &path);
    let prices: Vec<_> = rows.iter().map(|r| r.values()[0].clone()).collect();
    assert_eq!(
        prices,
        vec![
            Some(FieldValue::Bytes(vec![0x02, 0x02, 0x30, 0x39])),
            Some(FieldValue::Bytes(vec![0x02, 0x01, 0x9c])),
            Some(FieldValue::Bytes(vec![0x02, 0x01, 0x00])),
        ]
    );
}

#[test]
fn schema_of_hand_encoded_container_maps_logical_types() {
    let dir = TempDir::new("golden_schema");
    let path = dir.file("golden.avro");
    fs::write(&path, golden_container()).unwrap();
    assert_eq!(
        schema_of(&path).unwrap(),
        Schema::builder()
            .field("id", FieldType::int64())
            .field("name", FieldType::string())
            .nullable_field("score", FieldType::double())
            .nullable_field("note", FieldType::string())
            .field("day", date_type())
            .field("at", micros_instant_type())
            .field("price", decimal_type())
            .build()
    );
}

#[test]
fn damaged_final_sync_marker_is_rejected() {
    fn read_err<T: 'static>(codec: Arc<dyn RowCodec<T>>, name: &str, mut buf: Vec<u8>, flip: u8) {
        let dir = TempDir::new(name);
        let path = dir.file("bad_sync.avro");
        let last = buf.len() - 1;
        buf[last] ^= flip;
        fs::write(&path, buf).unwrap();
        let len = i64::try_from(fs::metadata(&path).unwrap().len()).unwrap();
        let reader = AvroRecordReader::new(codec, 100);
        let tracker = OffsetRangeTracker::new(OffsetRange::new(0, len));
        let err = reader
            .read_with_tracker(&LocalFileSystem::new(), &path, &tracker, |_| Ok(()))
            .unwrap_err();
        assert!(err.to_string().contains("sync marker"), "{name}: {err}");
    }

    read_err(
        Arc::new(BeamRowCodec::<Golden>::new()),
        "hand_encoded",
        golden_container(),
        0x01,
    );
    let sink_written = write_to_bytes(
        &AvroSink::<Simple>::new()
            .with_block_size(10)
            .with_compression(None),
        &vec![simple(); 30],
    );
    read_err(
        Arc::new(BeamRowCodec::<Simple>::new()),
        "sink_written",
        sink_written,
        0xff,
    );
}
