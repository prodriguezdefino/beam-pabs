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

//! Tests for generic `#[derive(BeamRow)]` structs, direct wire codecs and field-name mapping.

use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;

use beam::Instant;
use beam::coders::{Coder, CoderError, Context, DefaultCoder, RowCoder, RowStructCoder};
use beam::schema::{BeamEnum, BeamRow, Field, FieldType, Row, Schema, SchemaError};
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;

#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam")]
struct Pair<T> {
    key: String,
    value: T,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam")]
struct GenericPojo<T> {
    t: Option<T>,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam")]
struct TwoParam<K, V> {
    first: K,
    second: Option<V>,
    items: Vec<K>,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam")]
struct Bounded<T: Clone> {
    item: T,
}

struct UnmappedMarker;

#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam")]
struct WithPhantom<T> {
    id: i64,
    #[beam(skip)]
    _marker: PhantomData<fn() -> T>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, BeamEnum)]
#[beam(crate = "::beam")]
enum Status {
    Pending,
    Active,
    Closed,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam")]
struct Leaf {
    code: i32,
    label: String,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam")]
struct AllWireKinds<T> {
    #[beam(encoding_position = 18)]
    flag: bool,
    #[beam(encoding_position = 0)]
    tiny: i8,
    #[beam(encoding_position = 1)]
    short_val: i16,
    #[beam(encoding_position = 2)]
    int_val: i32,
    #[beam(encoding_position = 3)]
    long_val: i64,
    #[beam(encoding_position = 4)]
    u16_val: u16,
    #[beam(encoding_position = 5)]
    u32_val: u32,
    #[beam(encoding_position = 6)]
    float_val: f32,
    #[beam(encoding_position = 7)]
    double_val: f64,
    #[beam(rename = "title", encoding_position = 8)]
    name: String,
    #[beam(encoding_position = 9)]
    raw_bytes: Bytes,
    #[beam(bytes, encoding_position = 10)]
    vec_bytes: Vec<u8>,
    #[beam(encoding_position = 11)]
    present_opt: Option<String>,
    #[beam(encoding_position = 12)]
    absent_opt: Option<i64>,
    #[beam(encoding_position = 13)]
    opt_list: Vec<Option<i32>>,
    #[beam(encoding_position = 14)]
    tree_map: BTreeMap<String, Option<Leaf>>,
    #[beam(encoding_position = 15)]
    status: Status,
    #[beam(encoding_position = 16)]
    at: DateTime<Utc>,
    #[beam(encoding_position = 17)]
    day: NaiveDate,
    #[beam(encoding_position = 19)]
    price: Decimal,
    #[beam(encoding_position = 20)]
    seen: Instant,
    #[beam(encoding_position = 21)]
    generic_payload: T,
    #[beam(skip)]
    local_cache: usize,
}

#[test]
fn generic_instantiations_build_distinct_schemas() {
    let int_schema = Pair::<i64>::beam_schema();
    let str_schema = Pair::<String>::beam_schema();
    let opt_schema = Pair::<Option<i64>>::beam_schema();
    let vec_schema = Pair::<Vec<Leaf>>::beam_schema();

    assert_ne!(int_schema, str_schema);
    assert_ne!(int_schema, opt_schema);
    assert_ne!(str_schema, vec_schema);

    assert_eq!(
        int_schema.field("value").unwrap().field_type,
        FieldType::int64()
    );
    assert_eq!(
        str_schema.field("value").unwrap().field_type,
        FieldType::string()
    );
    assert_eq!(
        opt_schema.field("value").unwrap().field_type,
        FieldType::int64().with_nullable(true)
    );
}

#[test]
fn nested_generic_pojo_matches_expected_schema_and_round_trips() {
    type NestedGeneric = GenericPojo<GenericPojo<Leaf>>;

    let leaf_schema = Leaf::beam_schema();
    let inner_expected = Arc::new(Schema::new(vec![Field::nullable(
        "t",
        FieldType::row(leaf_schema),
    )]));
    let outer_expected = Schema::new(vec![Field::nullable("t", FieldType::row(inner_expected))]);

    assert_eq!(*NestedGeneric::beam_schema(), outer_expected);

    let populated = GenericPojo {
        t: Some(GenericPojo {
            t: Some(Leaf {
                code: 7,
                label: "ok".to_string(),
            }),
        }),
    };
    let empty_inner = GenericPojo::<GenericPojo<Leaf>> {
        t: Some(GenericPojo { t: None }),
    };
    let empty_outer = GenericPojo::<GenericPojo<Leaf>> { t: None };

    for case in [populated, empty_inner, empty_outer] {
        let row = case.to_row().expect("to_row");
        assert_eq!(NestedGeneric::from_row(&row).expect("from_row"), case);

        let bytes = case.encode().expect("encode");
        let row_bytes = row.to_row_bytes().expect("row bytes");
        assert_eq!(bytes, row_bytes);
        assert_eq!(NestedGeneric::decode(&bytes).expect("decode"), case);
    }
}

#[test]
fn generic_map_multi_param_bounded_and_phantom_round_trip() {
    let map_val = GenericPojo {
        t: Some(HashMap::from([
            (
                "k1".to_string(),
                Pair {
                    key: "a".to_string(),
                    value: 10_i64,
                },
            ),
            (
                "k2".to_string(),
                Pair {
                    key: "b".to_string(),
                    value: 20_i64,
                },
            ),
        ])),
    };
    let map_bytes = map_val.encode().expect("encode map");
    assert_eq!(
        GenericPojo::<HashMap<String, Pair<i64>>>::decode(&map_bytes).expect("decode map"),
        map_val
    );

    let two = TwoParam {
        first: 42_i32,
        second: Some("hello".to_string()),
        items: vec![1, 2, 3],
    };
    let two_bytes = two.encode().expect("encode two");
    assert_eq!(two_bytes, two.to_row().unwrap().to_row_bytes().unwrap());
    assert_eq!(
        TwoParam::<i32, String>::decode(&two_bytes).expect("decode two"),
        two
    );

    let bounded = Bounded {
        item: "cloned".to_string(),
    };
    assert_eq!(
        Bounded::<String>::decode(&bounded.encode().unwrap()).unwrap(),
        bounded
    );

    let phantom = WithPhantom::<UnmappedMarker> {
        id: 99,
        _marker: PhantomData,
    };
    let phantom_bytes = phantom.encode().expect("encode phantom");
    let decoded_phantom = WithPhantom::<UnmappedMarker>::decode(&phantom_bytes).expect("decode");
    assert_eq!(decoded_phantom.id, 99);
}

#[test]
fn direct_wire_codec_matches_row_coder_byte_for_byte_across_all_field_kinds() {
    let sample = AllWireKinds {
        flag: true,
        tiny: -12,
        short_val: -3000,
        int_val: 123_456,
        long_val: -9_876_543_210,
        u16_val: 65_000,
        u32_val: 4_000_000_000,
        float_val: -3.25,
        double_val: 1e-9,
        name: "beam-rust".to_string(),
        raw_bytes: Bytes::from_static(b"\x00\x01\xff"),
        vec_bytes: vec![9, 8, 7, 6],
        present_opt: Some("present".to_string()),
        absent_opt: None,
        opt_list: vec![Some(10), None, Some(-20)],
        tree_map: BTreeMap::from([
            (
                "alpha".to_string(),
                Some(Leaf {
                    code: 1,
                    label: "one".to_string(),
                }),
            ),
            ("beta".to_string(), None),
        ]),
        status: Status::Active,
        at: DateTime::from_timestamp(-123, 456_789_000).expect("pre-epoch timestamp"),
        day: NaiveDate::from_ymd_opt(2026, 11, 13).expect("valid date"),
        price: Decimal::new(-123_450, 3),
        seen: Instant::from_millis(-5_000),
        generic_payload: Pair {
            key: "nested".to_string(),
            value: Pair {
                key: "leaf".to_string(),
                value: Leaf {
                    code: 9,
                    label: "nine".to_string(),
                },
            },
        },
        local_cache: 777,
    };

    let direct_bytes = sample.encode().expect("direct encode");
    let mut encode_row_bytes = Vec::new();
    sample
        .encode_row_bytes(&mut encode_row_bytes)
        .expect("encode_row_bytes");
    let mut row_coder_bytes = Vec::new();
    RowCoder::encode_row(&sample.to_row().expect("to_row"), &mut row_coder_bytes)
        .expect("RowCoder::encode_row");

    assert_eq!(direct_bytes, row_coder_bytes);
    assert_eq!(encode_row_bytes, row_coder_bytes);

    let mut expected = sample;
    expected.local_cache = 0;

    let decoded =
        AllWireKinds::<Pair<Pair<Leaf>>>::decode(&direct_bytes).expect("direct decode_element");
    assert_eq!(decoded, expected);

    let runner_schema = Arc::new(
        Schema::from_proto_bytes(&AllWireKinds::<Pair<Pair<Leaf>>>::beam_schema().to_proto_bytes())
            .expect("proto schema"),
    );
    let decoded_with_schema =
        AllWireKinds::<Pair<Pair<Leaf>>>::decode_with_schema(&direct_bytes, Some(&runner_schema))
            .expect("decode_with_schema");
    assert_eq!(decoded_with_schema, expected);
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam")]
struct EvolvedReader {
    id: i64,
    name: String,
    added_optional: Option<String>,
}

#[test]
fn foreign_writer_schema_resolves_fields_by_name_and_defaults_missing_nullable() {
    // The writer puts `name` before `id`, adds a column, and omits `added_optional`.
    let writer_schema = Arc::new(
        Schema::builder()
            .field("name", FieldType::string())
            .field("extra_writer_only", FieldType::int32())
            .field("id", FieldType::int64())
            .build(),
    );
    let writer_row = Row::builder(Arc::clone(&writer_schema))
        .with_value("alice")
        .with_value(999_i32)
        .with_value(42_i64)
        .build()
        .expect("valid writer row");
    let bytes = writer_row.to_row_bytes().expect("encode writer row");

    let expected = EvolvedReader {
        id: 42,
        name: "alice".to_string(),
        added_optional: None,
    };

    assert_eq!(
        EvolvedReader::from_row(&writer_row).expect("from_row by name"),
        expected
    );
    assert_eq!(
        EvolvedReader::decode_with_schema(&bytes, Some(&writer_schema))
            .expect("decode_with_schema by name"),
        expected
    );

    let coder = RowStructCoder::<EvolvedReader>::with_writer_schema(Arc::clone(&writer_schema))
        .expect("coder field map");
    assert_eq!(
        coder
            .decode(&mut bytes.as_slice(), Context::WholeStream)
            .expect("RowStructCoder decode"),
        expected
    );
}

#[test]
fn nested_foreign_schema_with_reordered_inner_fields_decodes_by_name() {
    let reordered_leaf_schema = Arc::new(
        Schema::builder()
            .field("label", FieldType::string())
            .field("code", FieldType::int32())
            .build(),
    );
    let writer_schema = Arc::new(
        Schema::builder()
            .field("key", FieldType::string())
            .field("value", FieldType::row(Arc::clone(&reordered_leaf_schema)))
            .build(),
    );
    assert!(!Pair::<Leaf>::matches_schema(&writer_schema));

    let inner_row = Row::builder(reordered_leaf_schema)
        .with_value("reordered")
        .with_value(17_i32)
        .build()
        .expect("inner row");
    let outer_row = Row::builder(Arc::clone(&writer_schema))
        .with_value("k")
        .with_value(inner_row)
        .build()
        .expect("outer row");
    let bytes = outer_row.to_row_bytes().expect("bytes");

    let decoded = Pair::<Leaf>::decode_with_schema(&bytes, Some(&writer_schema)).expect("decode");
    assert_eq!(
        decoded,
        Pair {
            key: "k".to_string(),
            value: Leaf {
                code: 17,
                label: "reordered".to_string(),
            },
        }
    );
}

#[test]
fn missing_required_field_in_writer_schema_returns_field_not_found() {
    let incomplete_writer = Arc::new(Schema::builder().field("id", FieldType::int64()).build());
    let row = Row::builder(Arc::clone(&incomplete_writer))
        .with_value(1_i64)
        .build()
        .expect("row");
    let bytes = row.to_row_bytes().expect("bytes");

    assert_eq!(
        EvolvedReader::from_row(&row),
        Err(SchemaError::FieldNotFound("name".to_string()))
    );
    assert_eq!(
        RowStructCoder::<EvolvedReader>::with_writer_schema(Arc::clone(&incomplete_writer))
            .unwrap_err(),
        SchemaError::FieldNotFound("name".to_string())
    );
    assert!(matches!(
        EvolvedReader::decode_with_schema(&bytes, Some(&incomplete_writer)),
        Err(CoderError::Schema(SchemaError::FieldNotFound(field))) if field == "name"
    ));
}
