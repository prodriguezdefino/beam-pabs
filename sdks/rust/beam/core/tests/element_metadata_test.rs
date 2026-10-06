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

//! Element metadata wire format: an unknown drain code decodes as `Normal` and an unknown
//! value kind is an error.

use beam::coders::{
    CausedByDrain, CoderError, ElementMetadata, PaneInfo, ValueKind, WindowedHeader,
};

/// `Elements.ElementMetadata` field numbers for protocol buffer compatibility tests.
const FIELD_DRAIN: u32 = 1;
const FIELD_VALUE_KIND: u32 = 4;

/// Encodes a proto VarInt field: the key `(number << 3) | wire_type`, then the value.
fn varint_field(number: u32, value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    for mut v in [u64::from(number) << 3, value] {
        loop {
            let byte = (v & 0x7F) as u8;
            v >>= 7;
            if v == 0 {
                out.push(byte);
                break;
            }
            out.push(byte | 0x80);
        }
    }
    out
}

/// Wraps proto `body` in the VarInt length prefix that the metadata blob of a pane has.
fn length_delimited(body: &[u8]) -> Vec<u8> {
    let mut out = vec![body.len() as u8];
    out.extend_from_slice(body);
    out
}

fn round_trip(metadata: &ElementMetadata) -> ElementMetadata {
    let mut bytes = Vec::new();
    metadata.encode(&mut bytes).expect("encode");
    ElementMetadata::decode(&mut bytes.as_slice()).expect("decode")
}

#[test]
fn default_metadata_is_normal_insert() {
    let metadata = ElementMetadata::default();
    assert_eq!(metadata.drain, CausedByDrain::Normal);
    assert_eq!(metadata.value_kind, ValueKind::Insert);
    assert!(metadata.is_default());
    assert!(!metadata.is_draining());
}

#[test]
fn every_field_survives_a_round_trip() {
    let metadata = ElementMetadata {
        drain: CausedByDrain::CausedByDrain,
        value_kind: ValueKind::UpdateBefore,
        traceparent: Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".to_string()),
        tracestate: Some("congo=t61rcWkgMzE".to_string()),
    };
    assert_eq!(round_trip(&metadata), metadata);
    assert!(metadata.is_draining());
    assert!(!metadata.is_default());
}

#[test]
fn hand_rolled_protos_decode_unknown_fields_and_drain_codes() {
    let mut unknown_field = varint_field(FIELD_DRAIN, 2);
    unknown_field.extend_from_slice(&varint_field(7, 1234));
    let cases = [
        // An unknown drain code decodes as NORMAL. A false drain would stop a healthy pipeline.
        (
            "unrecognised drain",
            varint_field(FIELD_DRAIN, 99),
            CausedByDrain::Normal,
            ValueKind::Insert,
        ),
        (
            "absent drain",
            varint_field(FIELD_VALUE_KIND, 4),
            CausedByDrain::Normal,
            ValueKind::Delete,
        ),
        // Unrecognized fields from newer runners are ignored.
        (
            "unknown field",
            unknown_field,
            CausedByDrain::CausedByDrain,
            ValueKind::Insert,
        ),
    ];
    for (case, body, drain, value_kind) in cases {
        let decoded = ElementMetadata::decode(&mut length_delimited(&body).as_slice()).expect(case);
        assert_eq!(
            (decoded.drain, decoded.value_kind),
            (drain, value_kind),
            "{case}"
        );
    }
}

#[test]
fn an_unrecognised_value_kind_is_rejected() {
    // Reject unknown value kinds rather than defaulting to INSERT.
    let body = varint_field(FIELD_VALUE_KIND, 99);
    let err = ElementMetadata::decode(&mut length_delimited(&body).as_slice())
        .expect_err("an unknown value kind must be rejected");
    assert!(
        err.to_string().contains("99"),
        "the error should name the value it could not read, got: {err}"
    );
}

#[test]
fn a_blob_shorter_than_its_length_is_an_eof_error() {
    // Declares 8, then 2^60, bytes and supplies three. Reserving 2^60 up front would abort.
    let huge = [
        0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x10, 0x08, 0x02, 0x20,
    ];
    for bytes in [&[8u8, 0x08, 0x02, 0x20][..], &huge[..]] {
        let err = ElementMetadata::decode(&mut &bytes[..]).expect_err("truncated");
        assert!(
            matches!(&err, CoderError::Io(io) if io.kind() == std::io::ErrorKind::UnexpectedEof),
            "{err:?}"
        );
    }
}

#[test]
fn a_negative_declared_length_is_rejected() {
    let mut bytes = vec![0xFF; 9];
    bytes.push(0x01); // VarInt -1.
    let err = ElementMetadata::decode(&mut bytes.as_slice()).expect_err("negative");
    assert!(
        matches!(&err, CoderError::Format(msg) if msg == "element metadata declares -1 bytes"),
        "{err:?}"
    );
}

#[test]
fn the_pane_bit_announces_metadata_only_when_present() {
    let header = WindowedHeader::global(42, PaneInfo::ON_TIME_AND_ONLY_FIRING);
    let pane_byte = *header.as_bytes().last().expect("pane byte");
    assert_eq!(pane_byte & 0x80, 0, "no metadata, no bit: {pane_byte:#04x}");
    assert_eq!(header.metadata(), ElementMetadata::default());

    let metadata = ElementMetadata {
        drain: CausedByDrain::CausedByDrain,
        value_kind: ValueKind::Delete,
        traceparent: Some("trace".to_string()),
        tracestate: None,
    };
    let header =
        WindowedHeader::global_with_metadata(42, PaneInfo::ON_TIME_AND_ONLY_FIRING, &metadata);
    let pane_byte = header.as_bytes()[WindowedHeader::WINDOWS_START];
    assert_eq!(
        pane_byte & 0x80,
        0x80,
        "the pane must announce the metadata that follows it: {pane_byte:#04x}"
    );
    assert_eq!(header.pane(), PaneInfo::ON_TIME_AND_ONLY_FIRING);
    assert_eq!(header.metadata(), metadata);
    assert_eq!(header.timestamp_millis(), 42);
}

#[test]
fn rebuilding_a_header_can_drop_and_restore_metadata() {
    let metadata = ElementMetadata {
        drain: CausedByDrain::CausedByDrain,
        ..Default::default()
    };
    let with = WindowedHeader::global_with_metadata(7, PaneInfo::NO_FIRING, &metadata);

    let without = with.rebuilt(9, PaneInfo::NO_FIRING, &ElementMetadata::default());
    assert_eq!(without.timestamp_millis(), 9);
    assert_eq!(without.metadata(), ElementMetadata::default());
    assert_eq!(
        without.as_bytes().last().expect("pane byte") & 0x80,
        0,
        "dropping the metadata must also clear the bit announcing it"
    );

    let restored = without.rebuilt(9, PaneInfo::NO_FIRING, &metadata);
    assert_eq!(restored.metadata(), metadata);
}

#[test]
fn rebuilding_keeps_the_windows_byte_for_byte() {
    let windows = vec![vec![1u8, 2, 3], vec![4, 5, 6, 7]];
    let original = WindowedHeader::new(100, &windows, PaneInfo::NO_FIRING);
    let rebuilt = original.rebuilt(
        200,
        PaneInfo::ON_TIME_AND_ONLY_FIRING,
        &ElementMetadata::default(),
    );

    assert_eq!(rebuilt.window_bytes(), original.window_bytes());
    assert_eq!(rebuilt.timestamp_millis(), 200);
    assert_eq!(rebuilt.pane(), PaneInfo::ON_TIME_AND_ONLY_FIRING);
    // The window count in the header prefix must match the copied windows.
    assert_eq!(
        &rebuilt.as_bytes()[8..12],
        &2i32.to_be_bytes(),
        "the window count must be copied through, not recomputed from a flattened blob"
    );
}

#[test]
fn rebuilding_an_absent_header_produces_a_global_window_one() {
    let rebuilt =
        WindowedHeader::default().rebuilt(5, PaneInfo::NO_FIRING, &ElementMetadata::default());
    assert!(!rebuilt.is_empty());
    assert_eq!(rebuilt.timestamp_millis(), 5);
    assert!(rebuilt.window_bytes().is_empty());
    assert_eq!(
        &rebuilt.as_bytes()[8..12],
        &1i32.to_be_bytes(),
        "the global window encodes to no bytes, but there is still one of it"
    );
}

#[test]
fn a_header_lifted_off_the_wire_equals_the_one_it_was_encoded_from() {
    let header = WindowedHeader::global(42, PaneInfo::ON_TIME_AND_ONLY_FIRING);
    let pane_start = WindowedHeader::WINDOWS_START; // Global window has zero length.
    let lifted = WindowedHeader::from_wire(header.as_bytes(), pane_start);
    assert_eq!(lifted, header);
    assert_eq!(lifted.pane(), PaneInfo::ON_TIME_AND_ONLY_FIRING);

    // Equality requires identical bytes and identical pane boundaries.
    assert_ne!(
        WindowedHeader::from_wire(header.as_bytes(), pane_start + 1),
        header
    );
    assert_ne!(
        WindowedHeader::global(43, PaneInfo::ON_TIME_AND_ONLY_FIRING),
        header
    );

    assert!(WindowedHeader::EMPTY.is_empty());
    assert!(WindowedHeader::default().is_empty());
    assert!(!header.is_empty());

    let debug = format!("{header:?}");
    assert!(
        debug.starts_with("WindowedHeader {") && debug.contains("pane_start: 12"),
        "{debug}"
    );
}
