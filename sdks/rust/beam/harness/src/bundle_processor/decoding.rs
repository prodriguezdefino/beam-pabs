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

//! Decodes inbound bundle data and its windowed-value framing.

use std::collections::HashMap;
use std::io::{Cursor, ErrorKind, Read};

use super::BundleError;
use beam::coders::{
    PaneInfo, URN_LENGTH_PREFIX, URN_PARAM_WINDOWED_VALUE, URN_WINDOWED_VALUE, WindowedHeader,
    coder_urn, has_nested_row_length_prefix, read_length_prefixed_slice, skip_coder_value,
    skip_pane_info, strip_nested_row_length_prefixes,
};
use model::pipeline::Coder;

/// One element decoded from an inbound chunk: its header and the location of its payload.
pub(super) struct RawElement {
    pub(super) header: WindowedHeader,
    payload: Payload,
}

/// An element payload: a span of its source chunk, or bytes that decoding had to rewrite.
enum Payload {
    Span(usize, usize),
    Owned(Vec<u8>),
}

impl RawElement {
    /// Returns the payload bytes. `chunk` must be the chunk that this element came from.
    pub(super) fn payload<'a>(&'a self, chunk: &'a [u8]) -> &'a [u8] {
        match &self.payload {
            Payload::Span(start, end) => chunk.get(*start..*end).unwrap_or_default(),
            Payload::Owned(bytes) => bytes,
        }
    }
}

/// Decodes as many complete windowed values as `data` holds. Returns the elements and the
/// number of leading bytes they use; a trailing partial element stays for the next chunk.
/// Payloads are spans of `data`: read them with [`RawElement::payload`]. Returns
/// [`BundleError::Coder`] when the root coder is missing or an element is not valid.
pub(super) fn decode_ready_elements(
    data: &[u8],
    root_coder_id: &str,
    coders: &HashMap<String, Coder>,
) -> Result<(Vec<RawElement>, usize), BundleError> {
    let framing = Framing::resolve(root_coder_id, coders)?;
    let mut cursor = Cursor::new(data);
    std::iter::from_fn(|| {
        ((cursor.position() as usize) < data.len()).then(|| {
            framing
                .decode(&mut cursor, data)
                .map(|element| (element, cursor.position() as usize))
        })
    })
    // A partial element waits for the next chunk. The consumed count stops before it.
    .take_while(|decoded| !matches!(decoded, Err(e) if e.kind() == ErrorKind::UnexpectedEof))
    .try_fold((Vec::new(), 0), |(mut elements, _), decoded| {
        let (element, end) = decoded
            .map_err(|e| BundleError::Coder(format!("Failed to decode stream element: {e}")))?;
        elements.push(element);
        Ok((elements, end))
    })
}

/// The framing of the elements of a data port. Resolve it from the coder once per chunk,
/// not per element.
struct Framing<'a> {
    header: HeaderFraming<'a>,
    element: ElementFraming<'a>,
    coders: &'a HashMap<String, Coder>,
}

enum HeaderFraming<'a> {
    /// No header: the port carries bare elements.
    None,
    /// A `beam:coder:windowed_value:v1` header precedes every element.
    Wire { window_coder: &'a str },
    /// A `beam:coder:param_windowed_value:v1` header, fixed by the coder.
    Constant(WindowedHeader),
}

enum ElementFraming<'a> {
    /// The runner frames the element. The payload is exactly the delimited bytes.
    LengthPrefixed,
    /// The SDK walks the element coder to find the payload length.
    Walked {
        coder_id: &'a str,
        nested: bool,
        /// Nested rows carry length prefixes that the SDK row encoding does not have.
        strip_rows: bool,
    },
}

impl<'a> Framing<'a> {
    fn resolve(
        root_coder_id: &'a str,
        coders: &'a HashMap<String, Coder>,
    ) -> Result<Self, BundleError> {
        let root = coders.get(root_coder_id).ok_or_else(|| {
            let mut known: Vec<&String> = coders.keys().collect();
            known.sort();
            BundleError::Coder(format!(
                "Bundle descriptor references coder '{root_coder_id}' but does not define it; \
                 defined coders: {known:?}"
            ))
        })?;
        let component = |i: usize| root.component_coder_ids.get(i).map_or("", String::as_str);
        let (header, element_coder_id, nested) = match coder_urn(root) {
            URN_WINDOWED_VALUE => (
                HeaderFraming::Wire {
                    window_coder: component(1),
                },
                component(0),
                true,
            ),
            URN_PARAM_WINDOWED_VALUE => (
                HeaderFraming::Constant(param_windowed_header(root, coders)),
                component(0),
                true,
            ),
            _ => (HeaderFraming::None, root_coder_id, false),
        };
        let element = if is_length_prefixed(element_coder_id, coders) {
            ElementFraming::LengthPrefixed
        } else {
            ElementFraming::Walked {
                coder_id: element_coder_id,
                nested,
                strip_rows: has_nested_row_length_prefix(element_coder_id, coders),
            }
        };
        Ok(Self {
            header,
            element,
            coders,
        })
    }

    /// Decodes one element (header and payload) at the cursor.
    fn decode(&self, cursor: &mut Cursor<&[u8]>, data: &[u8]) -> std::io::Result<RawElement> {
        let header = match &self.header {
            HeaderFraming::None => WindowedHeader::default(),
            HeaderFraming::Wire { window_coder } => {
                read_wire_header(cursor, data, window_coder, self.coders)?
            }
            HeaderFraming::Constant(header) => header.clone(),
        };
        let payload = self.read_payload(cursor, data)?;
        Ok(RawElement { header, payload })
    }

    fn read_payload(&self, cursor: &mut Cursor<&[u8]>, data: &[u8]) -> std::io::Result<Payload> {
        match self.element {
            ElementFraming::LengthPrefixed => {
                let len = read_length_prefixed_slice(cursor)?.len();
                let end = cursor.position() as usize;
                Ok(Payload::Span(end - len, end))
            }
            ElementFraming::Walked {
                coder_id,
                nested,
                strip_rows,
            } => {
                let start = cursor.position() as usize;
                skip_coder_value(cursor, coder_id, self.coders, nested)?;
                let end = cursor.position() as usize;
                if !strip_rows {
                    return Ok(Payload::Span(start, end));
                }
                let span = data.get(start..end).unwrap_or_default();
                strip_nested_row_length_prefixes(span, coder_id, self.coders).map(Payload::Owned)
            }
        }
    }
}

/// Reads a `beam:coder:windowed_value:v1` header: an 8-byte timestamp, a 4-byte window
/// count and that many windows, then the pane and any element metadata. Only this function
/// has the window coder, so only it can find the pane offset exactly.
fn read_wire_header(
    cursor: &mut Cursor<&[u8]>,
    data: &[u8],
    window_coder: &str,
    coders: &HashMap<String, Coder>,
) -> std::io::Result<WindowedHeader> {
    let start = cursor.position() as usize;
    let mut prefix = [0u8; WindowedHeader::WINDOWS_START];
    cursor.read_exact(&mut prefix)?;
    let window_count = prefix
        .last_chunk::<4>()
        .map_or(0, |count| i32::from_be_bytes(*count));
    (0..window_count).try_for_each(|_| skip_coder_value(cursor, window_coder, coders, true))?;
    let pane_start = cursor.position() as usize - start;
    skip_pane_info(cursor)?;
    let end = cursor.position() as usize;
    Ok(WindowedHeader::from_wire(
        data.get(start..end).unwrap_or_default(),
        pane_start,
    ))
}

/// Returns true when `coder_id` names a `beam:coder:length_prefix:v1` coder.
fn is_length_prefixed(coder_id: &str, coders: &HashMap<String, Coder>) -> bool {
    coders
        .get(coder_id)
        .is_some_and(|c| coder_urn(c) == URN_LENGTH_PREFIX)
}

/// Returns the constant header in the payload of a `beam:coder:param_windowed_value:v1` coder.
///
/// The payload is an encoded `WindowedValue` of bytes with an empty placeholder element:
/// `[8-byte timestamp][4-byte win_count][windows...][pane_info][metadata][placeholder]`.
/// Its header holds the timestamp, windows, pane and metadata of every element on the
/// stream. A payload that does not parse gives the global window.
fn param_windowed_header(root: &Coder, coders: &HashMap<String, Coder>) -> WindowedHeader {
    let payload = root.spec.as_ref().map_or(&[][..], |s| s.payload.as_slice());
    let window_coder = root.component_coder_ids.get(1).map_or("", String::as_str);
    read_wire_header(&mut Cursor::new(payload), payload, window_coder, coders)
        .unwrap_or_else(|_| WindowedHeader::global(0, PaneInfo::NO_FIRING))
}
