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

//! The `Typed` codecs of `#[derive(BeamRow)]` values.

use std::sync::Arc;

use beam::Instant;
use beam::prelude::*;
use beam::schema::{BeamRow as _, Field, FieldType, FieldValue, RowElement, Schema};
use beam_io_kafka::codec::{
    Avro, BeamRow, CodecError, Decoder, Encoder, Json, KafkaRecord, Outgoing, Payload, RecordRef,
    RowEncoder, Typed, Utf8, WithMetadata,
};

use super::common::{bind, decode, make_row, record};

#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
struct TypedEvent {
    id: i64,
    label: String,
    note: Option<String>,
}

fn event() -> TypedEvent {
    TypedEvent {
        id: 9,
        label: "alpha".to_string(),
        note: Some("ok".to_string()),
    }
}

/// The value bytes of the record of `element`.
fn encode<E: Encoder>(encoder: &E, element: &E::Element) -> Result<Vec<u8>, CodecError> {
    let mut state = E::State::default();
    let mut buffer = Vec::new();
    let outgoing = encoder.encode(&mut state, element, &mut buffer)?;
    Ok(outgoing.value.unwrap_or_default().to_vec())
}

/// The elements of `records` from one `decode_batch` call, with a state that is not new.
fn decode_batch<D: Decoder>(
    decoder: &D,
    records: &[RecordRef<'_>],
) -> Vec<Result<D::Element, CodecError>> {
    let mut state = D::State::default();
    let mut out = Vec::new();
    decoder.decode_batch(&mut state, records, &mut out);
    out.clear();
    decoder.decode_batch(&mut state, records, &mut out);
    out
}

const EVENT_JSON_SCHEMA: &str = r#"{
  "type": "object",
  "properties": {
    "id": {"type": "integer"},
    "label": {"type": "string"},
    "note": {"type": ["string", "null"]}
  },
  "required": ["id", "label"]
}"#;

const EVENT_AVRO_SCHEMA: &str = r#"{
  "type": "record",
  "name": "TypedEvent",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "label", "type": "string"},
    {"name": "note", "type": ["null", "string"], "default": null}
  ]
}"#;

#[test]
fn beam_row_encoder_reuses_caller_buffer() {
    let mut codec = BeamRow::new();
    let schema = TypedEvent::beam_schema();
    bind(&mut codec, &schema).unwrap();
    let element = RowElement::Row(event().to_row().unwrap());
    let mut buffer = Vec::with_capacity(256);
    let initial_ptr = buffer.as_ptr();
    let outgoing = codec.encode(&mut (), &element, &mut buffer).unwrap();
    assert_eq!(outgoing.value.map(<[u8]>::as_ptr), Some(initial_ptr));
    assert_eq!(buffer.capacity(), 256);
}

#[test]
fn a_typed_beam_row_codec_writes_the_row_coder_bytes_of_the_value() {
    let item = event();
    let mut codec = Typed::<_, TypedEvent>::new(BeamRow::schema(TypedEvent::beam_schema()));
    let p = Pipeline::new();
    codec
        .elements(&p.apply("Items", Create::new([item.clone()])))
        .unwrap();
    let bytes = encode(&codec, &item).unwrap();
    assert_eq!(bytes, item.to_row().unwrap().to_row_bytes().unwrap());
    assert_eq!(decode(&codec, &record(Some(&bytes))).unwrap(), item);
}

#[test]
fn a_typed_beam_row_codec_reads_rows_of_a_reordered_schema_by_name() {
    let writer = Arc::new(Schema::new(vec![
        Field::new("note", FieldType::string().with_nullable(true)),
        Field::new("label", FieldType::string()),
        Field::new("id", FieldType::int64()),
    ]));
    let bytes = make_row(
        &writer,
        vec![
            None,
            Some(FieldValue::String("beta".to_string())),
            Some(FieldValue::Int64(4)),
        ],
    )
    .to_row_bytes()
    .unwrap();
    let codec = Typed::<_, TypedEvent>::new(BeamRow::schema(writer));
    let expected = TypedEvent {
        id: 4,
        label: "beta".to_string(),
        note: None,
    };
    assert_eq!(decode(&codec, &record(Some(&bytes))).unwrap(), expected);
    let records = [record(Some(&bytes)), record(Some(&bytes))];
    let batch: Vec<_> = decode_batch(&codec, &records)
        .into_iter()
        .map(Result::unwrap)
        .collect();
    assert_eq!(batch, [expected.clone(), expected]);
}

#[test]
fn a_typed_codec_fails_for_rows_with_no_required_field() {
    let writer = Arc::new(Schema::new(vec![Field::new("id", FieldType::int64())]));
    let bytes = make_row(&writer, vec![Some(FieldValue::Int64(4))])
        .to_row_bytes()
        .unwrap();
    let codec = Typed::<_, TypedEvent>::new(BeamRow::schema(writer));
    let error = decode(&codec, &record(Some(&bytes))).unwrap_err();
    assert!(error.message().contains("label"), "{error}");

    let json_schema = r#"{"type": "object", "properties": {"id": {"type": "integer"}}}"#;
    let json = Typed::<_, TypedEvent>::new(Json::schema(json_schema).unwrap());
    let error = decode(&json, &record(Some(br#"{"id":1}"#))).unwrap_err();
    assert!(error.message().contains("label"), "{error}");
}

#[test]
fn a_typed_json_codec_maps_reordered_properties_and_omits_optional_ones() {
    let json_schema = r#"{
      "type": "object",
      "properties": {
        "label": {"type": "string"},
        "id": {"type": "integer"}
      },
      "required": ["label", "id"]
    }"#;
    let json = Typed::<_, TypedEvent>::new(Json::schema(json_schema).unwrap());
    let parsed = decode(&json, &record(Some(br#"{"label":"beta","id":11}"#))).unwrap();
    assert_eq!(
        parsed,
        TypedEvent {
            id: 11,
            label: "beta".to_string(),
            note: None,
        }
    );
}

#[test]
fn a_typed_json_codec_writes_the_object_that_it_reads() {
    let item = event();
    let mut json = Typed::<_, TypedEvent>::new(Json::schema(EVENT_JSON_SCHEMA).unwrap());
    json.prepare().unwrap();
    let bytes = encode(&json, &item).unwrap();
    assert_eq!(bytes, br#"{"id":9,"label":"alpha","note":"ok"}"#);
    assert_eq!(decode(&json, &record(Some(&bytes))).unwrap(), item);
}

#[test]
fn a_typed_json_batch_gives_an_error_only_to_the_records_that_do_not_convert() {
    let json = Typed::<_, TypedEvent>::new(Json::schema(EVENT_JSON_SCHEMA).unwrap());
    let good = br#"{"id":1,"label":"a"}"#;
    let records = [
        record(Some(good)),
        record(Some(b"not json")),
        record(None),
        record(Some(good)),
    ];
    let converted: Vec<bool> = decode_batch(&json, &records)
        .iter()
        .map(Result::is_ok)
        .collect();
    assert_eq!(converted, [true, false, false, true]);
}

#[test]
fn a_typed_avro_codec_writes_the_datum_that_it_reads_in_one_batch() {
    let item = event();
    let mut avro = Typed::<_, TypedEvent>::new(Avro::schema(EVENT_AVRO_SCHEMA).unwrap());
    assert!(avro.decodes_batches());
    avro.prepare().unwrap();
    let bytes = encode(&avro, &item).unwrap();
    let records = [
        record(Some(&bytes)),
        record(Some(&bytes[..2])),
        record(Some(&bytes)),
    ];
    let decoded = decode_batch(&avro, &records);
    let converted: Vec<bool> = decoded.iter().map(Result::is_ok).collect();
    assert_eq!(converted, [true, false, true]);
    assert!(decoded.into_iter().flatten().all(|value| value == item));
}

#[test]
fn a_typed_avro_codec_writes_the_datum_of_the_row_of_the_value() {
    let item = event();
    let mut typed = Typed::<_, TypedEvent>::new(Avro::schema(EVENT_AVRO_SCHEMA).unwrap());
    typed.prepare().unwrap();
    let mut rows = Avro::schema(EVENT_AVRO_SCHEMA).unwrap();
    bind(&mut rows, &TypedEvent::beam_schema()).unwrap();
    let row = RowElement::Row(item.to_row().unwrap());
    assert_eq!(encode(&typed, &item).unwrap(), encode(&rows, &row).unwrap());
}

#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
struct Text {
    payload: String,
}

#[test]
fn a_typed_payload_codec_writes_the_one_field() {
    let mut codec = Typed::<_, Text>::new(Payload::string());
    codec.prepare().unwrap();
    let item = Text {
        payload: "héllo".to_string(),
    };
    let bytes = encode(&codec, &item).unwrap();
    assert_eq!(bytes, "héllo".as_bytes());
    assert_eq!(decode(&codec, &record(Some(&bytes))).unwrap(), item);
}

#[test]
fn a_typed_payload_codec_needs_one_field_of_the_type_of_the_codec() {
    let mut codec = Typed::<_, TypedEvent>::new(Payload::string());
    let error = codec.prepare().unwrap_err();
    assert!(error.to_string().contains("exactly one"), "{error}");
}

#[test]
fn a_typed_codec_with_metadata_writes_the_key_the_headers_and_the_timestamp() {
    let item = event();
    let mut codec = WithMetadata::new(Typed::<_, TypedEvent>::new(
        Json::schema(EVENT_JSON_SCHEMA).unwrap(),
    ))
    .with_key(Utf8);
    let element = KafkaRecord::of_value(item.clone())
        .with_key("k1".to_string())
        .with_header("trace", Some(b"t1".to_vec()))
        .with_header("empty", None)
        .with_timestamp(Instant::from_millis(5));
    let p = Pipeline::new();
    codec
        .elements(&p.apply("Records", Create::new([element.clone()])))
        .unwrap();
    let mut state = Default::default();
    let mut buffer = Vec::new();
    let sent = codec.encode(&mut state, &element, &mut buffer).unwrap();
    assert_eq!(sent.key.as_deref(), Some(&b"k1"[..]));
    assert_eq!(
        sent.headers,
        vec![("trace", Some(&b"t1"[..])), ("empty", None)]
    );
    assert_eq!(sent.timestamp, Some(Instant::from_millis(5)));

    let headers = sent.headers.clone();
    let fetched = RecordRef {
        key: sent.key.as_deref(),
        headers: &headers,
        ..record(sent.value)
    };
    let received = decode(&codec, &fetched).unwrap();
    assert_eq!(received.key.as_deref(), Some("k1"));
    assert_eq!(received.value, Some(item));
    assert_eq!(received.headers, element.headers);
}

#[test]
fn a_typed_failure_row_holds_the_value() {
    let codec = Typed::<_, TypedEvent>::new(Json::new());
    assert_eq!(
        codec.failed_type(),
        FieldType::row(Arc::unwrap_or_clone(TypedEvent::beam_schema()))
    );
    assert_eq!(
        codec.failed_value(event()).unwrap(),
        FieldValue::Row(event().to_row().unwrap())
    );
}

/// A codec of a test user: values `id,label` as rows `(id: INT64, label: STRING)`.
#[derive(Clone, Default)]
struct Pair {
    input: Option<Arc<Schema>>,
}

fn pair_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new("label", FieldType::string()),
    ]))
}

impl Decoder for Pair {
    type Element = RowElement;
    type Output = Row;
    type State = ();

    fn decode(&self, _state: &mut (), record: &RecordRef<'_>) -> Result<RowElement, CodecError> {
        let text = std::str::from_utf8(record.value_or_err()?).map_err(|e| e.to_string())?;
        let (id, label) = text.split_once(',').ok_or("no comma")?;
        let id = id.parse::<i64>().map_err(|e| e.to_string())?;
        Ok(RowElement::Row(make_row(
            &pair_schema(),
            vec![
                Some(FieldValue::Int64(id)),
                Some(FieldValue::String(label.to_string())),
            ],
        )))
    }

    fn output(&self, elements: PCollection<RowElement>) -> PCollection<Row> {
        elements.as_rows().with_row_schema(&pair_schema())
    }
}

impl Encoder for Pair {
    type Input = Row;
    type Element = RowElement;
    type State = ();

    fn elements(&mut self, input: &PCollection<Row>) -> Result<PCollection<RowElement>> {
        self.bind_schema(input.row_schema().ok_or_else(|| Error::msg("no schema"))?)?;
        Ok(input.as_row_elements())
    }

    fn encode<'a>(
        &self,
        state: &mut (),
        element: &'a RowElement,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        match element {
            RowElement::Row(row) => self.encode_row(state, row, buffer),
            RowElement::Encoded(_) => Err(CodecError::new("encoded rows are not supported")),
        }
    }

    fn failed_type(&self) -> FieldType {
        FieldType::bytes()
    }

    fn failed_value(&self, _element: RowElement) -> Result<FieldValue, CodecError> {
        Ok(FieldValue::Bytes(Vec::new()))
    }
}

impl RowEncoder for Pair {
    fn bind_schema(&mut self, schema: Arc<Schema>) -> Result {
        self.input = Some(schema);
        Ok(())
    }

    fn encode_row<'a>(
        &self,
        _state: &mut (),
        row: &Row,
        buffer: &'a mut Vec<u8>,
    ) -> Result<Outgoing<'a>, CodecError> {
        let id = row
            .get_i64("id")
            .map_err(|e| e.to_string())?
            .ok_or("no id")?;
        let label = row
            .get_string("label")
            .map_err(|e| e.to_string())?
            .ok_or("no label")?;
        buffer.clear();
        buffer.extend_from_slice(format!("{id},{label}").as_bytes());
        Ok(Outgoing::value(buffer))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
struct Labeled {
    label: String,
    id: i64,
}

#[test]
fn a_typed_codec_of_a_user_codec_round_trips() {
    let mut codec = Typed::<_, Labeled>::new(Pair::default());
    codec.prepare().unwrap();
    let item = Labeled {
        label: "x".to_string(),
        id: 3,
    };
    let bytes = encode(&codec, &item).unwrap();
    assert_eq!(bytes, b"3,x");
    assert_eq!(decode(&codec, &record(Some(&bytes))).unwrap(), item);
}

/// A decoder of a test user that gives encoded rows and no schema for them.
struct NoSchema;

impl Decoder for NoSchema {
    type Element = RowElement;
    type Output = Row;
    type State = ();

    fn decode(&self, _state: &mut (), record: &RecordRef<'_>) -> Result<RowElement, CodecError> {
        record
            .value_or_err()
            .map(|value| RowElement::Encoded(value.to_vec()))
    }

    fn output(&self, elements: PCollection<RowElement>) -> PCollection<Row> {
        elements.as_rows()
    }
}

#[test]
fn a_typed_codec_rejects_encoded_rows_with_no_schema() {
    let bytes = event().to_row().unwrap().to_row_bytes().unwrap();
    let codec = Typed::<_, TypedEvent>::new(NoSchema);
    let error = decode(&codec, &record(Some(&bytes))).unwrap_err();
    assert!(error.message().contains("without a schema"), "{error}");
    let error = decode(
        &Typed::<_, TypedEvent>::new(BeamRow::new()),
        &record(Some(&bytes)),
    )
    .unwrap_err();
    assert!(error.message().contains("schema"), "{error}");
}
