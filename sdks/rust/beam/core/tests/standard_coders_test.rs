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

#![expect(
    clippy::unwrap_used,
    reason = "test fixtures unwrap; a failure is a test failure"
)]

//! Integration tests for Apache Beam Standard Coders.
//!
//! Validates encoding and decoding behavior against the canonical specifications
//! defined in `model/fn-execution/.../standard_coders.yaml`.

use std::collections::BTreeMap;
use std::sync::Arc;

use beam::coders::{
    BoolCoder, BytesCoder, Coder, Context, DoubleCoder, IntervalWindow, IntervalWindowCoder,
    IterableCoder, KvCoder, NullableCoder, PaneInfo, ParamWindowedValueCoder, RowCoder,
    StringUtf8Coder, Timing, VarIntCoder, WindowedValue, WindowedValueCoder,
};
use beam::schema::{BeamField, FieldType, FieldValue, Row, Schema, TypeInfo, URN_DECIMAL};
use model::pipeline as proto;
use prost::Message;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

// ---------------------------------------------------------------------------
// Dynamic test runner driving directly from standard_coders.yaml
// ---------------------------------------------------------------------------

const STANDARD_CODERS_YAML: &str = include_str!(
    "../../../../../model/fn-execution/src/main/resources/org/apache/beam/model/fnexecution/v1/standard_coders.yaml"
);

#[derive(Debug, Deserialize)]
struct StandardCoderDoc {
    coder: CoderDefinition,
    nested: Option<bool>,
    #[serde(default)]
    examples: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
struct CoderDefinition {
    urn: String,
    #[serde(default)]
    components: Vec<CoderDefinition>,
    #[serde(default)]
    payload: Option<String>,
    #[serde(default)]
    non_deterministic: Option<bool>,
}

/// Specification of a standard coder URN not yet implemented in the Rust SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UnsupportedCoderSpec {
    urn: &'static str,
    doc_count: usize,
    description: &'static str,
    prerequisite: &'static str,
}

/// Explicit list of all standard coders from `standard_coders.yaml` that are
/// not yet supported in the Rust SDK, along with their specification document count
/// and prerequisites.
const UNSUPPORTED_CODERS: &[UnsupportedCoderSpec] = &[
    UnsupportedCoderSpec {
        urn: "beam:coder:state_backed_iterable:v1",
        doc_count: 6,
        description: "State-backed iterable coder for paging large iterables",
        prerequisite: "Fn API State service channel integration",
    },
    UnsupportedCoderSpec {
        urn: "beam:coder:sharded_key:v1",
        doc_count: 1,
        description: "Sharded key coder for keyed elements with explicit shard IDs",
        prerequisite: "Sharded key type and partitioning",
    },
    UnsupportedCoderSpec {
        urn: "beam:coder:custom_window:v1",
        doc_count: 1,
        description: "Custom user-defined window coder",
        prerequisite: "Custom window serialization",
    },
];

fn key_to_bytes(key: &str) -> Vec<u8> {
    key.chars().map(|c| c as u8).collect()
}

fn assert_coder_roundtrip<T, C>(
    coder: &C,
    expected: &T,
    expected_bytes: &[u8],
    context: Context,
    non_deterministic: bool,
) where
    C: Coder<T>,
    T: std::fmt::Debug + PartialEq,
{
    let decoded = coder.decode(&mut &expected_bytes[..], context).unwrap();
    assert_eq!(&decoded, expected);
    if !non_deterministic {
        let mut buf = Vec::new();
        coder.encode(expected, &mut buf, context).unwrap();
        assert_eq!(buf.as_slice(), expected_bytes);
    }
}

fn run_single_spec(spec: &StandardCoderDoc) {
    let urn = spec.coder.urn.as_str();

    let contexts = match spec.nested {
        Some(true) => vec![Context::Nested],
        Some(false) => vec![Context::WholeStream],
        None => vec![Context::WholeStream, Context::Nested],
    };

    let non_deterministic = spec.coder.non_deterministic.unwrap_or(false);

    for &context in &contexts {
        for (key, val) in &spec.examples {
            let expected_bytes = key_to_bytes(key);
            match urn {
                "beam:coder:bytes:v1" => {
                    let expected: Vec<u8> = val.as_str().expect("bytes string").as_bytes().to_vec();
                    assert_coder_roundtrip(
                        &BytesCoder,
                        &expected,
                        &expected_bytes,
                        context,
                        non_deterministic,
                    );
                }
                "beam:coder:string_utf8:v1" => {
                    let expected = val.as_str().expect("utf8 string").to_string();
                    assert_coder_roundtrip(
                        &StringUtf8Coder,
                        &expected,
                        &expected_bytes,
                        context,
                        non_deterministic,
                    );
                }
                "beam:coder:bool:v1" => {
                    let expected = val.as_bool().expect("bool");
                    assert_coder_roundtrip(
                        &BoolCoder,
                        &expected,
                        &expected_bytes,
                        context,
                        non_deterministic,
                    );
                }
                "beam:coder:varint:v1" => {
                    let expected: i64 = val.as_i64().expect("varint i64");
                    assert_coder_roundtrip(
                        &VarIntCoder,
                        &expected,
                        &expected_bytes,
                        context,
                        non_deterministic,
                    );
                }
                "beam:coder:double:v1" => {
                    let coder = DoubleCoder;
                    let expected = match val {
                        Value::String(s) => match s.as_str() {
                            "Infinity" => f64::INFINITY,
                            "-Infinity" => f64::NEG_INFINITY,
                            "NaN" => f64::NAN,
                            "-0" => -0.0,
                            s => s.parse::<f64>().expect("parse float string"),
                        },
                        Value::Number(n) => n.as_f64().expect("f64 number"),
                        _ => panic!("unexpected double value: {val:?}"),
                    };
                    let decoded = coder.decode(&mut &expected_bytes[..], context).unwrap();
                    if expected.is_nan() {
                        assert!(decoded.is_nan());
                    } else {
                        assert_eq!(decoded.to_bits(), expected.to_bits());
                    }
                    if !non_deterministic {
                        let mut buf = Vec::new();
                        coder.encode(&expected, &mut buf, context).unwrap();
                        assert_eq!(buf, expected_bytes);
                    }
                }
                "beam:coder:global_window:v1" => {
                    use beam::coders::GlobalWindow;
                    assert_coder_roundtrip(
                        &beam::coders::GlobalWindowCoder,
                        &GlobalWindow,
                        &expected_bytes,
                        context,
                        non_deterministic,
                    );
                }
                "beam:coder:interval_window:v1" => {
                    let end = val["end"].as_i64().expect("end");
                    let span = val["span"].as_i64().expect("span");
                    let expected = IntervalWindow::from_end_and_span(end, span);
                    assert_coder_roundtrip(
                        &IntervalWindowCoder,
                        &expected,
                        &expected_bytes,
                        context,
                        non_deterministic,
                    );
                }
                "beam:coder:kv:v1" => {
                    let comp0 = spec
                        .coder
                        .components
                        .first()
                        .map(|c| c.urn.as_str())
                        .unwrap_or("");
                    let comp1 = spec
                        .coder
                        .components
                        .get(1)
                        .map(|c| c.urn.as_str())
                        .unwrap_or("");
                    match (comp0, comp1) {
                        ("beam:coder:bytes:v1", "beam:coder:varint:v1") => {
                            let k = val["key"].as_str().expect("k").as_bytes().to_vec();
                            let v = val["value"].as_i64().expect("v");
                            assert_coder_roundtrip(
                                &KvCoder::new(BytesCoder, VarIntCoder),
                                &(k, v),
                                &expected_bytes,
                                context,
                                non_deterministic,
                            );
                        }
                        ("beam:coder:bytes:v1", "beam:coder:bytes:v1") => {
                            let k = val["key"].as_str().expect("k").as_bytes().to_vec();
                            let v = val["value"].as_str().expect("v").as_bytes().to_vec();
                            assert_coder_roundtrip(
                                &KvCoder::new(BytesCoder, BytesCoder),
                                &(k, v),
                                &expected_bytes,
                                context,
                                non_deterministic,
                            );
                        }
                        ("beam:coder:bytes:v1", "beam:coder:bool:v1") => {
                            let k = val["key"].as_str().expect("k").as_bytes().to_vec();
                            let v = val["value"].as_bool().expect("v");
                            assert_coder_roundtrip(
                                &KvCoder::new(BytesCoder, BoolCoder),
                                &(k, v),
                                &expected_bytes,
                                context,
                                non_deterministic,
                            );
                        }
                        _ => panic!("Unsupported KV components: ({comp0}, {comp1})"),
                    }
                }
                "beam:coder:iterable:v1" => {
                    let comp0 = spec
                        .coder
                        .components
                        .first()
                        .map(|c| c.urn.as_str())
                        .unwrap_or("");
                    match comp0 {
                        "beam:coder:varint:v1" => {
                            let seq = val.as_array().expect("seq");
                            let expected: Vec<i64> =
                                seq.iter().map(|item| item.as_i64().unwrap()).collect();
                            assert_coder_roundtrip(
                                &IterableCoder::new(VarIntCoder),
                                &expected,
                                &expected_bytes,
                                context,
                                non_deterministic,
                            );
                        }
                        "beam:coder:bytes:v1" => {
                            let seq = val.as_array().expect("seq");
                            let expected: Vec<Vec<u8>> = seq
                                .iter()
                                .map(|item| item.as_str().unwrap().as_bytes().to_vec())
                                .collect();
                            assert_coder_roundtrip(
                                &IterableCoder::new(BytesCoder),
                                &expected,
                                &expected_bytes,
                                context,
                                non_deterministic,
                            );
                        }
                        "beam:coder:bool:v1" => {
                            let seq = val.as_array().expect("seq");
                            let expected: Vec<bool> =
                                seq.iter().map(|item| item.as_bool().unwrap()).collect();
                            assert_coder_roundtrip(
                                &IterableCoder::new(BoolCoder),
                                &expected,
                                &expected_bytes,
                                context,
                                non_deterministic,
                            );
                        }
                        "beam:coder:global_window:v1" => {
                            use beam::coders::GlobalWindow;
                            let seq = val.as_array().expect("seq");
                            let expected: Vec<GlobalWindow> =
                                seq.iter().map(|_| GlobalWindow).collect();
                            assert_coder_roundtrip(
                                &IterableCoder::new(beam::coders::GlobalWindowCoder),
                                &expected,
                                &expected_bytes,
                                context,
                                non_deterministic,
                            );
                        }
                        _ => panic!("Unsupported Iterable component: {comp0}"),
                    }
                }
                "beam:coder:nullable:v1" => {
                    let expected: Option<Vec<u8>> = if val.is_null() {
                        None
                    } else {
                        Some(val.as_str().expect("bytes string").as_bytes().to_vec())
                    };
                    assert_coder_roundtrip(
                        &NullableCoder::new(BytesCoder),
                        &expected,
                        &expected_bytes,
                        context,
                        non_deterministic,
                    );
                }
                "beam:coder:windowed_value:v1" => {
                    let comp1 = spec
                        .coder
                        .components
                        .get(1)
                        .map(|c| c.urn.as_str())
                        .unwrap_or("");
                    let v = val["value"].as_i64().expect("value i64");
                    let ts = val["timestamp"].as_i64().expect("ts");
                    match comp1 {
                        "beam:coder:global_window:v1" => {
                            let expected = WindowedValue::global(v, ts);
                            assert_coder_roundtrip(
                                &WindowedValueCoder::new(VarIntCoder),
                                &expected,
                                &expected_bytes,
                                context,
                                non_deterministic,
                            );
                        }
                        "beam:coder:interval_window:v1" => {
                            let wins: Vec<IntervalWindow> = val["windows"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .map(|w| {
                                    IntervalWindow::from_end_and_span(
                                        w["end"].as_i64().unwrap(),
                                        w["span"].as_i64().unwrap(),
                                    )
                                })
                                .collect();
                            let expected = WindowedValue::new(v, ts, wins, PaneInfo::NO_FIRING);
                            assert_coder_roundtrip(
                                &WindowedValueCoder::with_window_coder(
                                    VarIntCoder,
                                    IntervalWindowCoder,
                                ),
                                &expected,
                                &expected_bytes,
                                context,
                                non_deterministic,
                            );
                        }
                        _ => panic!("Unsupported WindowedValue window component: {comp1}"),
                    }
                }
                "beam:coder:param_windowed_value:v1" => {
                    let comp1 = spec
                        .coder
                        .components
                        .get(1)
                        .map(|c| c.urn.as_str())
                        .unwrap_or("");
                    let payload = key_to_bytes(
                        spec.coder
                            .payload
                            .as_ref()
                            .expect("param_windowed_value specification must carry a payload"),
                    );
                    let v = val["value"].as_i64().expect("value i64");
                    let ts = val["timestamp"].as_i64().expect("ts");
                    let pane = pane_info(&val["pane"]);

                    match comp1 {
                        "beam:coder:global_window:v1" => {
                            use beam::coders::{GlobalWindow, GlobalWindowCoder};
                            let coder =
                                ParamWindowedValueCoder::<i64, _, GlobalWindow>::from_payload(
                                    VarIntCoder,
                                    &GlobalWindowCoder,
                                    &payload,
                                )
                                .expect("payload must parse");

                            // The constants come from the payload rather than the element, so
                            // check them directly: a payload misparse would otherwise surface
                            // only as an opaque whole-value mismatch.
                            assert_eq!(coder.constants().timestamp_millis, ts, "payload timestamp");
                            assert_eq!(
                                coder.constants().windows,
                                [GlobalWindow],
                                "payload windows"
                            );
                            assert_eq!(coder.constants().pane, pane, "payload pane");

                            assert_coder_roundtrip(
                                &coder,
                                &WindowedValue::new(v, ts, vec![GlobalWindow], pane),
                                &expected_bytes,
                                context,
                                non_deterministic,
                            );
                        }
                        "beam:coder:interval_window:v1" => {
                            let wins: Vec<IntervalWindow> = val["windows"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .map(|w| {
                                    IntervalWindow::from_end_and_span(
                                        w["end"].as_i64().unwrap(),
                                        w["span"].as_i64().unwrap(),
                                    )
                                })
                                .collect();
                            let coder =
                                ParamWindowedValueCoder::<i64, _, IntervalWindow>::from_payload(
                                    VarIntCoder,
                                    &IntervalWindowCoder,
                                    &payload,
                                )
                                .expect("payload must parse");

                            assert_eq!(coder.constants().timestamp_millis, ts, "payload timestamp");
                            assert_eq!(coder.constants().windows, wins, "payload windows");
                            assert_eq!(coder.constants().pane, pane, "payload pane");

                            assert_coder_roundtrip(
                                &coder,
                                &WindowedValue::new(v, ts, wins, pane),
                                &expected_bytes,
                                context,
                                non_deterministic,
                            );
                        }
                        _ => panic!("Unsupported ParamWindowedValue window component: {comp1}"),
                    }
                }
                "beam:coder:row:v1" => {
                    let schema = row_schema(spec);
                    let coder = RowCoder::new(Arc::clone(&schema));

                    let decoded = coder.decode(&mut &expected_bytes[..], context).unwrap();
                    assert_row_matches(&decoded, val);

                    if !non_deterministic {
                        let mut buf = Vec::new();
                        coder.encode(&decoded, &mut buf, context).unwrap();
                        assert_eq!(buf.as_slice(), expected_bytes);
                    }
                }
                "beam:coder:timer:v1" => {
                    // Unlike the other standard coders, `TimerCoder` is not a
                    // `Coder<T>`: it resolves its key and window components through a
                    // proto coder table because the harness decodes timers straight off
                    // the data plane. So it needs its own arm, not
                    // `assert_coder_roundtrip`.
                    let comp0 = spec
                        .coder
                        .components
                        .first()
                        .map(|c| c.urn.as_str())
                        .unwrap_or("");
                    let comp1 = spec
                        .coder
                        .components
                        .get(1)
                        .map(|c| c.urn.as_str())
                        .unwrap_or("");
                    assert_eq!(
                        (comp0, comp1),
                        ("beam:coder:string_utf8:v1", "beam:coder:global_window:v1"),
                        "unhandled timer components"
                    );

                    let coders = timer_coder_table(comp0, comp1);
                    let expected = expected_timer_record(val);

                    let mut cursor = std::io::Cursor::new(expected_bytes.as_slice());
                    let decoded =
                        beam::coders::TimerCoder::decode(&mut cursor, "key", "win", &coders)
                            .expect("timer should decode");
                    assert_eq!(decoded, expected);

                    if !non_deterministic {
                        let mut buf = Vec::new();
                        beam::coders::TimerCoder::encode(&decoded, &mut buf)
                            .expect("timer should encode");
                        assert_eq!(buf.as_slice(), expected_bytes);
                    }
                }
                other => panic!("Unexpected supported URN: {other}"),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Timer coder helpers
//
// A timer names its key and window coders as components, and the harness resolves
// them through the bundle's coder table. These helpers rebuild that table and the
// expected `TimerRecord` from the YAML example.
// ---------------------------------------------------------------------------

/// Builds the coder table a timer's components are resolved through.
fn timer_coder_table(
    key_urn: &str,
    window_urn: &str,
) -> std::collections::HashMap<String, proto::Coder> {
    let leaf = |urn: &str| proto::Coder {
        spec: Some(proto::FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: Vec::new(),
    };
    std::collections::HashMap::from([
        ("key".to_string(), leaf(key_urn)),
        ("win".to_string(), leaf(window_urn)),
    ])
}

/// Rebuilds the `PaneInfo` a YAML `pane` mapping describes.
fn pane_info(pane: &Value) -> PaneInfo {
    let timing = match pane["timing"].as_str().expect("timing") {
        "EARLY" => Timing::Early,
        "ON_TIME" => Timing::OnTime,
        "LATE" => Timing::Late,
        "UNKNOWN" => Timing::Unknown,
        other => panic!("unexpected pane timing: {other}"),
    };

    PaneInfo::new(
        pane["is_first"].as_bool().expect("is_first"),
        pane["is_last"].as_bool().expect("is_last"),
        timing,
        pane["index"].as_i64().expect("index"),
        pane["on_time_index"].as_i64().expect("on_time_index"),
    )
}

/// Rebuilds the `TimerRecord` a YAML timer example describes.
fn expected_timer_record(val: &Value) -> beam::coders::TimerRecord {
    // `TimerRecord` carries the key already encoded, mirroring how the harness lifts it
    // straight out of the wire bytes without knowing the key's type.
    let mut user_key = Vec::new();
    StringUtf8Coder
        .encode(
            &val["userKey"].as_str().expect("userKey").to_string(),
            &mut user_key,
            Context::Nested,
        )
        .expect("encode timer user key");

    let windows = val["windows"]
        .as_array()
        .expect("windows")
        .iter()
        .map(|window| {
            assert_eq!(
                window.as_str(),
                Some("global"),
                "only the global window appears in the timer specification"
            );
            // GlobalWindow occupies zero bytes.
            Vec::new()
        })
        .collect();

    let clear = val["clearBit"].as_bool().expect("clearBit");

    // A cleared timer carries no firing metadata; the fields are absent from the
    // example and unread by the decoder.
    beam::coders::TimerRecord {
        user_key,
        dynamic_tag: val["dynamicTimerTag"]
            .as_str()
            .expect("dynamicTimerTag")
            .to_string(),
        windows,
        clear,
        fire_timestamp: if clear {
            0
        } else {
            val["fireTimestamp"].as_i64().expect("fireTimestamp")
        },
        hold_timestamp: if clear {
            0
        } else {
            val["holdTimestamp"].as_i64().expect("holdTimestamp")
        },
        pane: if clear {
            PaneInfo::NO_FIRING
        } else {
            pane_info(&val["pane"])
        },
    }
}

// ---------------------------------------------------------------------------
// Row coder helpers
//
// A row specification carries its schema as a serialized `schema_pb2.Schema`
// rather than as component coder URNs, so the expected values are a tree keyed
// by field name rather than a single scalar.
// ---------------------------------------------------------------------------

/// Reads the `Schema` a row specification encodes in its coder payload.
fn row_schema(spec: &StandardCoderDoc) -> Arc<Schema> {
    let payload = key_to_bytes(
        spec.coder
            .payload
            .as_deref()
            .expect("row coder specification must carry a schema payload"),
    );
    let proto_schema =
        proto::Schema::decode(payload.as_slice()).expect("row coder payload must be a Schema");

    Arc::new(Schema::try_from(proto_schema).expect("schema proto must convert"))
}

/// Asserts that a decoded row carries exactly the values the specification lists.
fn assert_row_matches(row: &Row, expected: &Value) {
    let expected = expected
        .as_object()
        .expect("row example must be a mapping of field name to value");
    let schema = row.schema();
    assert_eq!(
        expected.len(),
        schema.num_fields(),
        "example lists {} fields but the schema declares {}",
        expected.len(),
        schema.num_fields()
    );

    for (index, field) in schema.fields.iter().enumerate() {
        let want = expected
            .get(field.name.as_str())
            .unwrap_or_else(|| panic!("example is missing field '{}'", field.name));
        assert_field_matches(
            &field.field_type,
            row.values()[index].as_ref(),
            want,
            &field.name,
        );
    }
}

/// Asserts that one decoded field value matches the specification, recursing
/// through containers, nested rows and logical types.
fn assert_field_matches(
    field_type: &FieldType,
    actual: Option<&FieldValue>,
    expected: &Value,
    path: &str,
) {
    if expected.is_null() {
        assert!(actual.is_none(), "{path}: expected null, got {actual:?}");
        return;
    }

    let actual = actual.unwrap_or_else(|| panic!("{path}: expected a value, got null"));

    match &field_type.type_info {
        TypeInfo::Atomic(_) => assert_atomic_matches(actual, expected, path),
        TypeInfo::Array(element) | TypeInfo::Iterable(element) => {
            let FieldValue::Array(items) = actual else {
                panic!("{path}: expected an array, got {actual:?}");
            };
            let want = expected
                .as_array()
                .unwrap_or_else(|| panic!("{path}: example must be a sequence"));
            assert_eq!(items.len(), want.len(), "{path}: element count");

            for (index, (item, want)) in items.iter().zip(want).enumerate() {
                assert_field_matches(element, item.as_ref(), want, &format!("{path}[{index}]"));
            }
        }
        TypeInfo::Map(_, value_type) => {
            let FieldValue::Map(entries) = actual else {
                panic!("{path}: expected a map, got {actual:?}");
            };
            let want = expected
                .as_object()
                .unwrap_or_else(|| panic!("{path}: example must be a mapping"));
            assert_eq!(entries.len(), want.len(), "{path}: entry count");

            for (key, value) in entries {
                let FieldValue::String(key) = key else {
                    panic!("{path}: only string-keyed maps appear in the specification");
                };
                let want = want
                    .get(key.as_str())
                    .unwrap_or_else(|| panic!("{path}: example is missing key '{key}'"));
                assert_field_matches(value_type, value.as_ref(), want, &format!("{path}[{key}]"));
            }
        }
        TypeInfo::Row(_) => {
            let FieldValue::Row(inner) = actual else {
                panic!("{path}: expected a row, got {actual:?}");
            };
            assert_row_matches(inner, expected);
        }
        // A decimal's value is not its representation: the bytes carry a scale
        // and an unscaled big integer, so compare the number they denote.
        TypeInfo::Logical { urn, .. } if urn == URN_DECIMAL => {
            let decimal = Decimal::from_field_value(Some(actual))
                .unwrap_or_else(|e| panic!("{path}: {actual:?} is not a decimal: {e}"));
            let want = expected
                .as_str()
                .unwrap_or_else(|| panic!("{path}: decimal example must be a string"));
            assert_eq!(decimal.to_string(), want, "{path}");
        }
        // Every other logical type is transparent: its value is its
        // representation's value.
        TypeInfo::Logical { representation, .. } => {
            assert_field_matches(representation, Some(actual), expected, path);
        }
    }
}

fn assert_atomic_matches(actual: &FieldValue, expected: &Value, path: &str) {
    let as_i64 = || {
        expected
            .as_i64()
            .unwrap_or_else(|| panic!("{path}: example must be an integer"))
    };
    let as_str = || {
        expected
            .as_str()
            .unwrap_or_else(|| panic!("{path}: example must be a string"))
    };

    match actual {
        FieldValue::Boolean(value) => assert_eq!(
            *value,
            expected
                .as_bool()
                .unwrap_or_else(|| panic!("{path}: example must be a boolean")),
            "{path}"
        ),
        FieldValue::Byte(value) => assert_eq!(i64::from(*value), as_i64(), "{path}"),
        FieldValue::Int16(value) => assert_eq!(i64::from(*value), as_i64(), "{path}"),
        FieldValue::Int32(value) => assert_eq!(i64::from(*value), as_i64(), "{path}"),
        FieldValue::Int64(value) => assert_eq!(*value, as_i64(), "{path}"),
        // Compare bit patterns so that signed zero and NaN are not conflated.
        FieldValue::Float(value) => assert_eq!(
            value.to_bits(),
            expected_float(expected, path).to_bits(),
            "{path}"
        ),
        FieldValue::Double(value) => assert_eq!(
            value.to_bits(),
            expected_double(expected, path).to_bits(),
            "{path}"
        ),
        FieldValue::String(value) => assert_eq!(value.as_str(), as_str(), "{path}"),
        FieldValue::Bytes(value) => assert_eq!(value.as_slice(), as_str().as_bytes(), "{path}"),
        other => panic!("{path}: unexpected atomic value {other:?}"),
    }
}

/// Floating point examples are written either as YAML numbers or, where YAML
/// would lose precision or cannot express the value, as strings.
fn expected_float(expected: &Value, path: &str) -> f32 {
    match expected {
        Value::String(text) => text
            .parse()
            .unwrap_or_else(|e| panic!("{path}: '{text}' is not an f32: {e}")),
        Value::Number(number) => number.as_f64().expect("f64 number") as f32,
        other => panic!("{path}: unexpected float example {other:?}"),
    }
}

fn expected_double(expected: &Value, path: &str) -> f64 {
    match expected {
        Value::String(text) => text
            .parse()
            .unwrap_or_else(|e| panic!("{path}: '{text}' is not an f64: {e}")),
        Value::Number(number) => number.as_f64().expect("f64 number"),
        other => panic!("{path}: unexpected double example {other:?}"),
    }
}

#[test]
fn test_standard_coders_yaml_suite() {
    let mut tested_docs = 0;
    let mut skipped_docs = 0;
    let mut unsupported_counts: BTreeMap<&str, usize> = BTreeMap::new();

    let specs: Vec<StandardCoderDoc> = serde_saphyr::from_multiple(STANDARD_CODERS_YAML)
        .expect("failed to deserialize standard coder YAML documents");

    for spec in &specs {
        let urn = spec.coder.urn.as_str();
        if let Some(unsupported) = UNSUPPORTED_CODERS.iter().find(|s| s.urn == urn) {
            *unsupported_counts.entry(unsupported.urn).or_insert(0) += 1;
            skipped_docs += 1;
        } else {
            run_single_spec(spec);
            tested_docs += 1;
        }
    }

    assert_eq!(
        specs.len(),
        45,
        "Expected 45 YAML documents in standard_coders.yaml"
    );
    assert_eq!(
        tested_docs, 37,
        "Expected 37 supported standard coder documents"
    );
    assert_eq!(
        skipped_docs, 8,
        "Expected 8 unsupported standard coder documents"
    );

    // Verify exact breakdown of all missing coders
    for spec in UNSUPPORTED_CODERS {
        let actual_count = unsupported_counts.get(spec.urn).copied().unwrap_or(0);
        assert_eq!(
            actual_count, spec.doc_count,
            "Mismatch in document count for unsupported coder {}",
            spec.urn
        );
    }
}
