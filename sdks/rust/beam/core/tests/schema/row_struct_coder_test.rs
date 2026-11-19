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

//! Tests for `RowStructCoder` with a writer schema, `FieldMap` resolution and `FieldMapCache`.

use std::sync::Arc;

use beam_core::coders::{
    Coder, CoderError, Context, DefaultCoder, FieldMap, FieldMapCache, RowCoder, RowStructCoder,
};
use beam_core::schema::{BeamRow, Field, FieldType, Row, Schema, SchemaError};

#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "beam_core")]
struct Reader {
    id: i64,
    name: String,
    note: Option<String>,
}

/// `R` is also the name of a generic parameter of a `DefaultCoder` method.
#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "beam_core")]
struct GenericR<R> {
    r: R,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "beam_core")]
struct NoFields {}

fn reader() -> Reader {
    Reader {
        id: 42,
        name: "alice".to_string(),
        note: None,
    }
}

/// The writer puts `name` first, adds a nullable `extra`, and has no `note`.
fn reordered_writer() -> Arc<Schema> {
    Arc::new(
        Schema::builder()
            .field("name", FieldType::string())
            .nullable_field("extra", FieldType::int32())
            .field("id", FieldType::int64())
            .build(),
    )
}

fn round_trip(coder: &RowStructCoder<Reader>, value: &Reader) -> (Vec<u8>, Reader) {
    let mut bytes = Vec::new();
    coder
        .encode(value, &mut bytes, Context::WholeStream)
        .expect("encode");
    let decoded = coder
        .decode(&mut bytes.as_slice(), Context::WholeStream)
        .expect("decode");
    (bytes, decoded)
}

#[test]
fn generic_parameter_named_r_round_trips() {
    let value = GenericR { r: 7_i64 };
    let bytes = value.encode().expect("encode");
    assert_eq!(GenericR::<i64>::decode(&bytes).expect("decode"), value);
}

#[test]
fn zero_field_struct_round_trips() {
    assert!(NoFields::beam_schema().fields.is_empty());
    let bytes = NoFields {}.encode().expect("encode");
    let mut row_bytes = Vec::new();
    RowCoder::encode_row(&NoFields {}.to_row().expect("to_row"), &mut row_bytes).expect("row");
    assert_eq!(bytes, row_bytes);
    assert_eq!(NoFields::decode(&bytes).expect("decode"), NoFields {});
}

#[test]
fn row_struct_coder_with_own_schema_round_trips() {
    let coder = RowStructCoder::<Reader>::new();
    let value = Reader {
        note: Some("n".to_string()),
        ..reader()
    };
    let (bytes, decoded) = round_trip(&coder, &value);
    assert_eq!(decoded, value);
    assert_eq!(bytes, value.encode().expect("encode"));
}

#[test]
fn row_struct_coder_with_reordered_writer_schema_round_trips() {
    let coder =
        RowStructCoder::<Reader>::with_writer_schema(reordered_writer()).expect("writer schema");
    let (bytes, decoded) = round_trip(&coder, &reader());
    assert_eq!(decoded, reader());

    let writer_row = Row::builder(reordered_writer())
        .with_value("alice")
        .with_null()
        .with_value(42_i64)
        .build()
        .expect("writer row");
    let mut expected = Vec::new();
    RowCoder::encode_row(&writer_row, &mut expected).expect("encode writer row");
    assert_eq!(bytes, expected);
}

#[test]
fn row_struct_coder_rejects_a_required_writer_field_that_the_type_lacks() {
    let writer = Arc::new(
        Schema::builder()
            .field("id", FieldType::int64())
            .field("name", FieldType::string())
            .field("required_extra", FieldType::int32())
            .build(),
    );
    let coder = RowStructCoder::<Reader>::with_writer_schema(writer).expect("writer schema");
    let err = coder
        .encode(&reader(), &mut Vec::new(), Context::WholeStream)
        .expect_err("a required writer field with no value must fail");
    assert!(
        matches!(&err, CoderError::Schema(SchemaError::UnexpectedNull { expected })
            if expected.contains("required_extra")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn field_map_matches_writer_fields_by_name() {
    assert_eq!(
        Reader::field_map(&reordered_writer()).expect("field map"),
        FieldMap::ByName(Arc::from([Some(2), Some(0), None]))
    );
}

#[test]
fn derived_schema_after_a_proto_round_trip_takes_the_same_layout() {
    let runner_schema =
        Schema::from_proto_bytes(&Reader::beam_schema().to_proto_bytes()).expect("proto schema");
    assert_eq!(
        Reader::field_map(&runner_schema).expect("field map"),
        FieldMap::Same
    );
    assert_eq!(
        Reader::resolve_field_map(&runner_schema).expect("field map"),
        Some(FieldMap::Same)
    );
}

#[test]
fn runner_schema_with_identity_encoding_positions_decodes() {
    let runner_schema = Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()).with_encoding_position(0),
        Field::new("name", FieldType::string()).with_encoding_position(1),
        Field::nullable("note", FieldType::string()).with_encoding_position(2),
    ]));
    let value = Reader {
        note: Some("n".to_string()),
        ..reader()
    };
    let bytes = value.encode().expect("encode");
    let mut runner_bytes = Vec::new();
    RowCoder::encode_row(
        &Row::new(
            Arc::clone(&runner_schema),
            value.to_row().expect("to_row").into_values(),
        )
        .expect("runner row"),
        &mut runner_bytes,
    )
    .expect("encode runner row");
    assert_eq!(bytes, runner_bytes);

    let mut cache = FieldMapCache::new();
    let decoded: Reader = cache
        .decode(&bytes, Some(&runner_schema), None)
        .expect("decode");
    assert_eq!(decoded, value);
}

#[test]
fn nullable_writer_field_for_a_required_reader_field_fails_only_on_null() {
    let writer = Arc::new(
        Schema::builder()
            .nullable_field("id", FieldType::int64())
            .field("name", FieldType::string())
            .build(),
    );
    assert!(!Reader::matches_schema(&writer));
    let encode = |id: Option<i64>| {
        let row = Row::builder(Arc::clone(&writer));
        let row = match id {
            Some(id) => row.with_value(id),
            None => row.with_null(),
        };
        let mut bytes = Vec::new();
        RowCoder::encode_row(&row.with_value("bob").build().expect("row"), &mut bytes)
            .expect("encode");
        bytes
    };

    let mut cache = FieldMapCache::new();
    let present: Reader = cache
        .decode(&encode(Some(5)), Some(&writer), None)
        .expect("a present value decodes");
    assert_eq!(
        present,
        Reader {
            id: 5,
            name: "bob".to_string(),
            note: None,
        }
    );
    let err = cache
        .decode::<Reader>(&encode(None), Some(&writer), None)
        .expect_err("a null value for a required field must fail");
    assert!(
        matches!(err, CoderError::Schema(SchemaError::UnexpectedNull { .. })),
        "unexpected error: {err:?}"
    );
}

#[test]
fn field_map_cache_resolves_again_for_a_new_schema() {
    let mut cache = FieldMapCache::new();
    let own = Reader::beam_schema();
    assert_eq!(
        cache.get::<Reader>(Some(&own)).expect("own schema"),
        Some(&FieldMap::Same)
    );
    let writer = reordered_writer();
    assert!(matches!(
        cache.get::<Reader>(Some(&writer)).expect("writer schema"),
        Some(FieldMap::ByName(_))
    ));
    assert_eq!(cache.get::<Reader>(None).expect("no schema"), None);
    assert_eq!(
        FieldMapCache::new()
            .get::<i64>(Some(&writer))
            .expect("no map"),
        None
    );
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "beam_core")]
struct Envelope {
    #[beam(bytes)]
    payload: Vec<u8>,
    reader: Reader,
    optional_reader: Option<Reader>,
}

#[test]
fn mapped_decode_skips_extra_writer_fields_and_decodes_nested_reordered_rows() {
    let inner_writer = reordered_writer();
    let extra_nested = Arc::new(Schema::new(vec![Field::new(
        "ignored",
        FieldType::string(),
    )]));
    let writer = Arc::new(Schema::new(vec![
        Field::new("tags", FieldType::array(FieldType::string())).with_encoding_position(2),
        Field::new("reader", FieldType::row(Arc::clone(&inner_writer))).with_encoding_position(0),
        Field::new(
            "counts",
            FieldType::map(FieldType::string(), FieldType::int32()),
        )
        .with_encoding_position(4),
        Field::new("payload", FieldType::bytes()).with_encoding_position(1),
        Field::nullable("extra_row", FieldType::row(Arc::clone(&extra_nested)))
            .with_encoding_position(3),
    ]));

    let inner_row = Row::builder(Arc::clone(&inner_writer))
        .with_value("alice")
        .with_value(99_i32)
        .with_value(42_i64)
        .build()
        .expect("inner row");
    let extra_row = Row::builder(extra_nested)
        .with_value("skip-me")
        .build()
        .expect("extra row");
    let writer_row = Row::new(
        Arc::clone(&writer),
        vec![
            Some(beam_core::schema::FieldValue::Array(vec![
                Some(beam_core::schema::FieldValue::String("a".to_string())),
                Some(beam_core::schema::FieldValue::String("b".to_string())),
            ])),
            Some(beam_core::schema::FieldValue::Row(inner_row)),
            Some(beam_core::schema::FieldValue::Map(vec![(
                beam_core::schema::FieldValue::String("k".to_string()),
                Some(beam_core::schema::FieldValue::Int32(7)),
            )])),
            Some(beam_core::schema::FieldValue::Bytes(vec![1, 2, 3])),
            Some(beam_core::schema::FieldValue::Row(extra_row)),
        ],
    )
    .expect("writer row");

    let mut bytes = Vec::new();
    RowCoder::encode_row(&writer_row, &mut bytes).expect("encode writer row");

    let mut cache = FieldMapCache::new();
    let decoded: Envelope = cache
        .decode(&bytes, Some(&writer), None)
        .expect("decode envelope");
    assert_eq!(
        decoded,
        Envelope {
            payload: vec![1, 2, 3],
            reader: reader(),
            optional_reader: None,
        }
    );

    let empty: NoFields = FieldMapCache::new()
        .decode(&bytes, Some(&writer), None)
        .expect("zero-field struct skips all writer fields");
    assert_eq!(empty, NoFields {});
}

#[test]
fn mapped_decode_reports_value_type_mismatch() {
    let wrong_writer = Arc::new(
        Schema::builder()
            .field("name", FieldType::string())
            .field("id", FieldType::string())
            .build(),
    );
    let row = Row::builder(Arc::clone(&wrong_writer))
        .with_value("alice")
        .with_value("not-an-i64")
        .build()
        .expect("row");
    let mut bytes = Vec::new();
    RowCoder::encode_row(&row, &mut bytes).expect("encode");

    let err = FieldMapCache::new()
        .decode::<Reader>(&bytes, Some(&wrong_writer), None)
        .expect_err("type mismatch must fail");
    assert!(
        matches!(
            err,
            CoderError::Schema(SchemaError::ValueTypeMismatch { .. })
        ),
        "unexpected error: {err:?}"
    );
}
