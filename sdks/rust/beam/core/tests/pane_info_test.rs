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

//! Tests for `PaneInfo` and element metadata encoding.
//!
//! A pane has variable length, and its leading byte gives the length. A wrong length does
//! not raise an error: the cursor stops inside a VarInt, and the decoder reads all the
//! following fields of the element from the wrong offset. These tests check the byte layout
//! directly, not only round trips, so a change to the tag arithmetic cannot pass unnoticed.

use std::io::Cursor;

use beam::coders::{
    CausedByDrain, Coder, Context, ElementMetadata, GlobalWindow, PaneInfo, StringUtf8Coder,
    Timing, URN_BYTES, URN_GLOBAL_WINDOW, URN_WINDOWED_VALUE, ValueKind, VarIntCoder,
    WindowedValue, WindowedValueCoder, skip_coder_value, skip_pane_info,
};
use model::pipeline::{Coder as ProtoCoder, FunctionSpec};

/// Byte appended after an encoding so tests can prove a read stopped there.
const SENTINEL: u8 = 0xAB;

fn encoded(pane: PaneInfo, element_metadata: bool) -> Vec<u8> {
    let mut buf = Vec::new();
    pane.encode(element_metadata, &mut buf).unwrap();
    buf
}

fn decoded(bytes: &[u8]) -> (PaneInfo, bool) {
    let mut cursor = Cursor::new(bytes);
    let result = PaneInfo::decode(&mut cursor).expect("pane should decode");
    assert_eq!(
        cursor.position() as usize,
        bytes.len(),
        "pane decode must consume exactly the bytes it was given"
    );
    result
}

// ---------------------------------------------------------------------------
// Byte layout
// ---------------------------------------------------------------------------

#[test]
fn common_panes_are_a_single_byte() {
    // Every untriggered element has NO_FIRING (is_first | is_last | timing UNKNOWN(3) << 2).
    assert_eq!(encoded(PaneInfo::NO_FIRING, false), [0x0F]);
    assert_eq!(encoded(PaneInfo::ON_TIME_AND_ONLY_FIRING, false), [0x07]);
}

#[test]
fn equal_indices_take_the_one_index_encoding() {
    // The on-time index equals the index, so a second copy is redundant. The tag alone tells
    // the reader to derive it.
    let pane = PaneInfo::new(false, false, Timing::Late, 5, 5);
    assert_eq!(encoded(pane, false), [0x18, 5]);
    assert_eq!(decoded(&[0x18, 5]), (pane, false));
}

#[test]
fn an_early_pane_takes_the_one_index_encoding() {
    // An early pane has no on-time position, so the reader derives `-1` and the writer does
    // not write it. This matters, because `-1` takes ten VarInt bytes.
    let pane = PaneInfo::new(false, false, Timing::Early, 5, -1);
    assert_eq!(encoded(pane, false), [0x10, 5]);
    assert_eq!(decoded(&[0x10, 5]), (pane, false));
}

#[test]
fn diverging_indices_take_the_two_index_encoding() {
    let pane = PaneInfo::new(true, false, Timing::Late, 5, 3);
    assert_eq!(encoded(pane, false), [0x29, 5, 3]);
    assert_eq!(decoded(&[0x29, 5, 3]), (pane, false));

    // The 0x80 metadata flag shares the tag nibble and must be masked during decode.
    let pane = PaneInfo::new(false, false, Timing::Late, 5, 3);
    assert_eq!(encoded(pane, true), [0xA8, 5, 3]);
    assert_eq!(decoded(&[0xA8, 5, 3]), (pane, true));
}

#[test]
fn only_both_indices_zero_select_the_index_free_encoding() {
    // A single zero index requires encoding both indices to prevent data loss.
    let cases = [
        (
            PaneInfo::new(false, false, Timing::Late, 0, 3),
            [0x28, 0, 3],
        ),
        (
            PaneInfo::new(false, false, Timing::OnTime, 2, 0),
            [0x24, 2, 0],
        ),
    ];
    for (pane, bytes) in cases {
        assert_eq!(encoded(pane, false), bytes, "{pane:?}");
        assert_eq!(decoded(&bytes), (pane, false), "{pane:?}");
    }
}

#[test]
fn an_unknown_pane_always_takes_the_first_encoding() {
    // An UNKNOWN pane always takes the first encoding, and the encoder drops its indices.
    // The Java, Python and Go SDKs encode it the same way. Encoded indices would give bytes
    // that no other SDK reads back the same way, so the lossy encoding is the interoperable one.
    let pane = PaneInfo::new(true, true, Timing::Unknown, 7, 7);
    assert_eq!(encoded(pane, false), [0x0F]);
    assert_eq!(decoded(&[0x0F]), (PaneInfo::NO_FIRING, false));
}

#[test]
fn a_pane_without_indices_decodes_to_its_canonical_ones() {
    // `-1` marks a position that does not exist. A non-first pane has no known index, and an
    // early pane has no on-time index.
    assert_eq!(
        decoded(&[0x00]).0,
        PaneInfo::new(false, false, Timing::Early, -1, -1)
    );
    assert_eq!(
        decoded(&[0x05]).0,
        PaneInfo::new(true, false, Timing::OnTime, 0, 0)
    );
}

#[test]
fn an_unknown_encoding_tag_is_rejected() {
    // Undefined encoding tag 3 must fail to prevent desynchronizing later decodes.
    let err = PaneInfo::decode(&mut Cursor::new(&[0x3F][..]))
        .expect_err("an undefined encoding tag must be rejected");
    assert!(
        err.to_string().contains("Invalid pane encoding 3"),
        "unexpected: {err}"
    );
}

#[test]
fn every_canonical_pane_round_trips() {
    for timing in [Timing::Early, Timing::OnTime, Timing::Late, Timing::Unknown] {
        for is_first in [false, true] {
            for is_last in [false, true] {
                let index = if is_first { 0 } else { -1 };
                let on_time_index = if timing == Timing::Early { -1 } else { 0 };
                let pane = PaneInfo::new(is_first, is_last, timing, index, on_time_index);
                assert_eq!(decoded(&encoded(pane, false)).0, pane, "{pane:?}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Element metadata
// ---------------------------------------------------------------------------

#[test]
fn metadata_round_trips_through_the_windowed_value_coder() {
    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let value = WindowedValue::global("hello".to_string(), 1_000).with_metadata(ElementMetadata {
        drain: CausedByDrain::CausedByDrain,
        value_kind: ValueKind::UpdateAfter,
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
        tracestate: Some("congo=t61rcWkgMzE".to_string()),
    });

    let mut buf = Vec::new();
    coder.encode(&value, &mut buf, Context::Nested).unwrap();

    assert_eq!(buf[12] & 0x80, 0x80, "the pane must announce the metadata");

    let decoded = coder.decode(&mut &buf[..], Context::Nested).unwrap();
    assert_eq!(decoded, value);
}

#[test]
fn default_metadata_writes_no_flag_and_no_blob() {
    // Default metadata writes no flag bit and no blob. On the wire, it is the same as no
    // metadata.
    let value = WindowedValue::global("hi".to_string(), 0);
    assert!(value.metadata.is_default());
    let mut buf = Vec::new();
    WindowedValueCoder::new(StringUtf8Coder)
        .encode(&value, &mut buf, Context::Nested)
        .unwrap();
    assert_eq!(buf[12], 0x0F, "no metadata bit may be set");
    assert_eq!(buf.len(), 8 + 4 + 1 + 3, "no metadata blob may be written");

    let coder = WindowedValueCoder::new(VarIntCoder);
    let explicit = WindowedValue::global(7i64, 0).with_metadata(ElementMetadata {
        drain: CausedByDrain::Normal,
        value_kind: ValueKind::Insert,
        traceparent: None,
        tracestate: None,
    });
    let mut buf = Vec::new();
    coder.encode(&explicit, &mut buf, Context::Nested).unwrap();
    assert_eq!(
        buf[12] & 0x80,
        0,
        "explicitly normal metadata must not set the bit"
    );
    let decoded = coder.decode(&mut &buf[..], Context::Nested).unwrap();
    assert_eq!(decoded.metadata, ElementMetadata::default());
}

#[test]
fn skip_pane_info_consumes_the_metadata_blob() {
    // The harness treats the header as opaque, so only this skip keeps the element bytes
    // aligned. An under-skip does not raise an error: it decodes garbage.
    let pane = PaneInfo::new(false, false, Timing::Late, 5, 3);
    let mut bytes = encoded(pane, true);
    bytes.push(4); // metadata blob length
    bytes.extend_from_slice(&[1, 2, 3, 4]);
    let end = bytes.len();
    bytes.push(SENTINEL);

    let mut cursor = Cursor::new(bytes.as_slice());
    skip_pane_info(&mut cursor).expect("skip should succeed");

    assert_eq!(
        cursor.position() as usize,
        end,
        "skip must stop exactly at the sentinel"
    );
}

#[test]
fn skip_coder_value_handles_a_windowed_value_with_metadata() {
    let leaf = |urn: &str| ProtoCoder {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: Vec::new(),
    };
    let coders = std::collections::HashMap::from([
        (
            "wv".to_string(),
            ProtoCoder {
                spec: Some(FunctionSpec {
                    urn: URN_WINDOWED_VALUE.to_string(),
                    payload: Vec::new(),
                }),
                component_coder_ids: vec!["bytes".to_string(), "win".to_string()],
            },
        ),
        ("bytes".to_string(), leaf(URN_BYTES)),
        ("win".to_string(), leaf(URN_GLOBAL_WINDOW)),
    ]);

    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut bytes = Vec::new();
    coder
        .encode(
            &WindowedValue::<String, GlobalWindow>::global("abc".to_string(), 5).with_metadata(
                ElementMetadata {
                    value_kind: ValueKind::Delete,
                    ..Default::default()
                },
            ),
            &mut bytes,
            Context::Nested,
        )
        .unwrap();
    let end = bytes.len();
    bytes.push(SENTINEL);

    let mut cursor = Cursor::new(bytes.as_slice());
    skip_coder_value(&mut cursor, "wv", &coders, true).expect("skip should succeed");

    assert_eq!(cursor.position() as usize, end);
}
