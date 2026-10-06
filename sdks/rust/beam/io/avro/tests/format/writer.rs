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

//! What the sink writes: header schema/codec and block bytes.

use std::collections::BTreeMap;

use avro_io::avroio::{AvroSink, CompressionCodec};
use beam::schema::BeamRow;
use chrono::{DateTime, NaiveDate, Utc};

use crate::common::{Simple, bytes, long, parse_blocks, parse_header, read_long, write_to_bytes};

#[test]
fn encoding_helpers_match_hand_computed_bytes() {
    // Values from the Avro specification's zig-zag table, plus a 3-byte varint.
    assert_eq!(long(0), [0x00]);
    assert_eq!(long(-1), [0x01]);
    assert_eq!(long(1), [0x02]);
    assert_eq!(long(-2), [0x03]);
    assert_eq!(long(-64), [0x7f]);
    assert_eq!(long(64), [0x80, 0x01]);
    assert_eq!(long(19_723), [0x96, 0xb4, 0x02]);
    assert_eq!(bytes(b"ab"), [0x04, b'a', b'b']);
    let mut pos = 0;
    assert_eq!(read_long(&[0x96, 0xb4, 0x02], &mut pos), 19_723);
}

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

// ---------------------------------------------------------------------------
// What the sink writes
// ---------------------------------------------------------------------------

/// The writer schema embedded in the container header, verbatim. Guards the
/// logical-type annotations (`date`, `timestamp-micros`), the `["null", T]`
/// union order for nullable fields, record/field names, and the narrowing of
/// Beam BYTE/INT16 to Avro `int`.
const EVENT_WRITER_SCHEMA: &str = concat!(
    r#"{"fields":["#,
    r#"{"name":"id","type":"long"},"#,
    r#"{"name":"small","type":"int"},"#,
    r#"{"name":"medium","type":"int"},"#,
    r#"{"name":"user","type":"string"},"#,
    r#"{"name":"score","type":["null","double"]},"#,
    r#"{"name":"ratio","type":"float"},"#,
    r#"{"name":"active","type":"boolean"},"#,
    r#"{"name":"tags","type":{"items":"string","type":"array"}},"#,
    r#"{"name":"attrs","type":{"type":"map","values":"long"}},"#,
    r#"{"name":"home","type":{"fields":[{"name":"city","type":"string"},{"name":"zip","type":["null","int"]}],"name":"home","type":"record"}},"#,
    r#"{"name":"previous","type":["null",{"fields":[{"name":"city","type":"string"},{"name":"zip","type":["null","int"]}],"name":"previous","type":"record"}]},"#,
    r#"{"name":"day","type":{"logicalType":"date","type":"int"}},"#,
    r#"{"name":"at","type":{"logicalType":"timestamp-micros","type":"long"}},"#,
    r#"{"name":"blob","type":"bytes"}"#,
    r#"],"name":"topLevelRecord","type":"record"}"#,
);

#[test]
fn header_carries_exact_writer_schema_and_codec() {
    for (compression, codec_name) in [
        (None, "null"),
        (Some(CompressionCodec::Snappy), "snappy"),
        (Some(CompressionCodec::Deflate), "deflate"),
        (Some(CompressionCodec::ZStandard), "zstandard"),
    ] {
        let sink = AvroSink::<Event>::new().with_compression(compression);
        let buf = write_to_bytes(&sink, &[]);
        let header = parse_header(&buf);
        assert_eq!(
            header.metadata.keys().collect::<Vec<_>>(),
            ["avro.codec", "avro.schema"],
            "{codec_name}"
        );
        assert_eq!(header.meta("avro.codec"), codec_name);
        assert_eq!(header.meta("avro.schema"), EVENT_WRITER_SCHEMA);
        // No data: the file ends right after the header.
        assert_eq!(buf.len(), header.len, "{codec_name}");
    }
}

#[test]
fn default_codec_is_snappy() {
    let buf = write_to_bytes(&AvroSink::<Event>::new(), &[]);
    assert_eq!(parse_header(&buf).meta("avro.codec"), "snappy");
}

#[test]
fn uncompressed_block_bytes_match_hand_encoding() {
    let items = vec![
        Simple {
            id: 1,
            name: "ab".into(),
            score: None,
            day: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            at: DateTime::from_timestamp(1_700_000_000, 123_456_000).unwrap(),
        },
        Simple {
            id: -2,
            name: String::new(),
            score: Some(1.5),
            day: NaiveDate::from_ymd_opt(1969, 12, 31).unwrap(),
            at: DateTime::from_timestamp(-1, 999_999_000).unwrap(),
        },
        Simple {
            id: 300,
            name: "é".into(),
            score: Some(-0.25),
            day: NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
            at: DateTime::from_timestamp(0, 0).unwrap(),
        },
    ];
    let sink = AvroSink::<Simple>::new()
        .with_compression(None)
        .with_block_size(2);
    let buf = write_to_bytes(&sink, &items);
    let header = parse_header(&buf);
    assert_eq!(header.meta("avro.codec"), "null");

    let mut rec1 = Vec::new();
    rec1.push(0x02); // id 1
    rec1.extend(bytes(b"ab"));
    rec1.push(0x00); // score: union branch 0 = null
    rec1.extend([0x96, 0xb4, 0x02]); // day 19723 = 2024-01-01
    rec1.extend(long(1_700_000_000_123_456)); // micros, not millis
    let mut rec2 = Vec::new();
    rec2.push(0x03); // id -2
    rec2.push(0x00); // ""
    rec2.push(0x02); // score: union branch 1 = double
    rec2.extend([0, 0, 0, 0, 0, 0, 0xf8, 0x3f]); // 1.5, little endian
    rec2.push(0x01); // day -1
    rec2.push(0x01); // at -1 µs
    let mut rec3 = Vec::new();
    rec3.extend([0xd8, 0x04]); // id 300
    rec3.extend([0x04, 0xc3, 0xa9]); // "é" as UTF-8
    rec3.push(0x02);
    rec3.extend((-0.25f64).to_le_bytes());
    rec3.push(0x00); // day 0
    rec3.push(0x00); // at 0

    let blocks = parse_blocks(&buf, &header);
    assert_eq!(
        blocks,
        vec![(2, [rec1, rec2].concat()), (1, rec3)],
        "block framing or record encoding differs from the Avro spec"
    );
}
