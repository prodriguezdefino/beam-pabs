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

use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{self, ErrorKind, Read};
use std::iter;
use std::sync::Arc;

use super::BundleError;
use beam::coders::{
    RowCoder, SkipPlan, URN_LENGTH_PREFIX, URN_PARAM_WINDOWED_VALUE, URN_ROW, URN_WINDOWED_VALUE,
    VarIntCoder, WindowedHeader, add_nested_row_length_prefixes, coder_urn,
    has_nested_row_length_prefix, read_length_prefixed_slice, skip_pane_info,
    strip_nested_row_length_prefixes,
};
use beam::schema::Schema;
use model::pipeline::Coder;

/// The most bytes of a varint that encodes a 64-bit value.
const MAX_VARINT_LEN: usize = 10;

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

/// The framing of the elements of a data port: where each element starts and ends on the
/// wire, and how the SDK encoding differs from the wire encoding. Resolve it once for each
/// bundle descriptor and share it between bundles and split requests.
#[doc(hidden)]
pub struct PortFraming {
    coders: Arc<HashMap<String, Coder>>,
    header: HeaderFraming,
    element: ElementFraming,
}

enum HeaderFraming {
    /// No header: the port carries bare elements.
    None,
    /// A `beam:coder:windowed_value:v1` header precedes every element.
    Wire { window: SkipPlan },
    /// A `beam:coder:param_windowed_value:v1` header, fixed by the coder.
    Constant(WindowedHeader),
}

enum ElementFraming {
    /// The runner frames the element with a length prefix. `nested_rows` names the inner
    /// coder when the runner also puts length prefixes around nested rows.
    LengthPrefixed { nested_rows: Option<String> },
    /// A row with no length prefix. Its schema gives its length.
    Row(Arc<Schema>),
    /// The SDK walks the element coder to find the payload length.
    Walked {
        coder_id: String,
        plan: SkipPlan,
        nested: bool,
        /// Nested rows carry length prefixes that the SDK row encoding does not have.
        nested_rows: bool,
    },
}

impl PortFraming {
    /// Resolves the framing of a port whose elements use `root_coder_id`. Fails if `coders`
    /// does not have that coder, or if a row element has a schema that does not decode.
    pub fn resolve(
        root_coder_id: &str,
        coders: Arc<HashMap<String, Coder>>,
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
                    window: SkipPlan::new(component(1), &coders),
                },
                component(0),
                true,
            ),
            URN_PARAM_WINDOWED_VALUE => (
                HeaderFraming::Constant(param_windowed_header(root, &coders)?),
                component(0),
                true,
            ),
            _ => (HeaderFraming::None, root_coder_id, false),
        };
        let element = element_framing(element_coder_id, nested, &coders)?;
        Ok(Self {
            coders,
            header,
            element,
        })
    }

    /// Decodes as many complete windowed values as `data` holds. Returns the elements and the
    /// number of leading bytes they use; a trailing partial element stays for the next chunk.
    /// Payloads are spans of `data`: read them with [`RawElement::payload`].
    pub(super) fn decode_ready_elements(
        &self,
        data: &[u8],
    ) -> Result<(Vec<RawElement>, usize), BundleError> {
        let mut cursor = data;
        iter::from_fn(|| {
            ((offset(data, cursor)) < data.len()).then(|| {
                self.decode(&mut cursor, data)
                    .map(|element| (element, offset(data, cursor)))
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

    /// Rewrites the `[header][payload]` bytes of a `BundleApplication`, in the SDK encoding,
    /// into the wire encoding of this port. Adds the length prefixes that the runner expects.
    pub fn reframe(&self, element: Vec<u8>) -> Result<Vec<u8>, BundleError> {
        let (root_prefix, nested_rows) = match &self.element {
            ElementFraming::Row(_)
            | ElementFraming::Walked {
                nested_rows: false, ..
            } => return Ok(element),
            ElementFraming::Walked { coder_id, .. } => (false, Some(coder_id.as_str())),
            ElementFraming::LengthPrefixed { nested_rows } => (true, nested_rows.as_deref()),
        };
        let header_end = self.header_len(&element).map_err(|e| {
            BundleError::Coder(format!("Failed to read bundle application header: {e}"))
        })?;
        let (header, payload) = element.split_at(header_end);
        let body = match nested_rows {
            Some(coder_id) => add_nested_row_length_prefixes(payload, coder_id, &self.coders)
                .map(Cow::Owned)
                .map_err(|e| {
                    BundleError::Coder(format!("Failed to reframe bundle application: {e}"))
                })?,
            None => Cow::Borrowed(payload),
        };
        let mut out = Vec::with_capacity(header.len() + MAX_VARINT_LEN + body.len());
        out.extend_from_slice(header);
        if root_prefix {
            VarIntCoder::encode_varint(body.len() as i64, &mut out).map_err(|e| {
                BundleError::Coder(format!("Failed to prefix bundle application: {e}"))
            })?;
        }
        out.extend_from_slice(&body);
        Ok(out)
    }

    /// Returns the length of the windowed-value header at the start of `element`.
    fn header_len(&self, element: &[u8]) -> io::Result<usize> {
        match &self.header {
            HeaderFraming::Wire { window } => {
                let mut cursor = element;
                read_wire_header(&mut cursor, element, window)?;
                Ok(offset(element, cursor))
            }
            HeaderFraming::None | HeaderFraming::Constant(_) => Ok(0),
        }
    }

    /// Decodes one element (header and payload) at the cursor.
    fn decode(&self, cursor: &mut &[u8], data: &[u8]) -> io::Result<RawElement> {
        let header = match &self.header {
            HeaderFraming::None => WindowedHeader::default(),
            HeaderFraming::Wire { window } => read_wire_header(cursor, data, window)?,
            // Cheap: WindowedHeader is 24B + InlineBytes
            HeaderFraming::Constant(header) => header.clone(),
        };
        let payload = self.read_payload(cursor, data)?;
        Ok(RawElement { header, payload })
    }

    fn read_payload(&self, cursor: &mut &[u8], data: &[u8]) -> io::Result<Payload> {
        let start = offset(data, cursor);
        match &self.element {
            ElementFraming::LengthPrefixed { nested_rows } => {
                let slice = read_length_prefixed_slice(cursor)?;
                let end = offset(data, cursor);
                match nested_rows {
                    Some(inner_id) => {
                        strip_nested_row_length_prefixes(slice, inner_id, &self.coders)
                            .map(Payload::Owned)
                    }
                    None => Ok(Payload::Span(end - slice.len(), end)),
                }
            }
            ElementFraming::Row(schema) => {
                RowCoder::skip_row(schema, cursor)?;
                Ok(Payload::Span(start, offset(data, cursor)))
            }
            ElementFraming::Walked {
                coder_id,
                plan,
                nested,
                nested_rows,
            } => {
                plan.skip(cursor, *nested)?;
                let end = offset(data, cursor);
                match nested_rows {
                    false => Ok(Payload::Span(start, end)),
                    true => strip_nested_row_length_prefixes(
                        data.get(start..end).unwrap_or_default(),
                        coder_id,
                        &self.coders,
                    )
                    .map(Payload::Owned),
                }
            }
        }
    }
}

/// How far `cursor`, what is left of `data` after reads, is into `data`.
fn offset(data: &[u8], cursor: &[u8]) -> usize {
    data.len() - cursor.len()
}

/// Chooses how the SDK finds the end of each element of `coder_id`.
fn element_framing(
    coder_id: &str,
    nested: bool,
    coders: &HashMap<String, Coder>,
) -> Result<ElementFraming, BundleError> {
    let coder = coders.get(coder_id);
    let payload = coder
        .and_then(|c| c.spec.as_ref())
        .map_or(&[][..], |spec| spec.payload.as_slice());
    match coder.map(coder_urn) {
        Some(URN_LENGTH_PREFIX) => {
            let inner_id = coder
                .and_then(|c| c.component_coder_ids.first())
                .map_or("", String::as_str);
            Ok(ElementFraming::LengthPrefixed {
                nested_rows: has_nested_row_length_prefix(inner_id, coders)
                    .then(|| inner_id.to_string()),
            })
        }
        // A row coder with no schema cannot measure its rows: the walk takes the rest of
        // the chunk.
        Some(URN_ROW) if !payload.is_empty() => Schema::from_proto_bytes(payload)
            .map(|schema| ElementFraming::Row(Arc::new(schema)))
            .map_err(|e| {
                BundleError::Coder(format!("Row coder '{coder_id}' has no valid schema: {e}"))
            }),
        _ => Ok(ElementFraming::Walked {
            coder_id: coder_id.to_string(),
            plan: SkipPlan::new(coder_id, coders),
            nested,
            nested_rows: has_nested_row_length_prefix(coder_id, coders),
        }),
    }
}

/// Reads a `beam:coder:windowed_value:v1` header: an 8-byte timestamp, a 4-byte window
/// count and that many windows, then the pane and any element metadata. Only this function
/// has the window coder, so only it can find the pane offset exactly.
fn read_wire_header(
    cursor: &mut &[u8],
    data: &[u8],
    window: &SkipPlan,
) -> io::Result<WindowedHeader> {
    let start = offset(data, cursor);
    let mut prefix = [0u8; WindowedHeader::WINDOWS_START];
    cursor.read_exact(&mut prefix)?;
    let window_count = prefix
        .last_chunk::<4>()
        .map_or(0, |count| i32::from_be_bytes(*count));
    (0..window_count).try_for_each(|_| window.skip(cursor, true))?;
    let pane_start = offset(data, cursor) - start;
    skip_pane_info(cursor)?;
    let end = offset(data, cursor);
    Ok(WindowedHeader::from_wire(
        data.get(start..end).unwrap_or_default(),
        pane_start,
    ))
}

/// Returns the constant header in the payload of a `beam:coder:param_windowed_value:v1` coder.
///
/// The payload is an encoded `WindowedValue` of bytes with an empty placeholder element:
/// `[8-byte timestamp][4-byte win_count][windows...][pane_info][metadata][placeholder]`.
/// Its header holds the timestamp, windows, pane and metadata of every element on the
/// stream. Fails if the payload does not parse.
fn param_windowed_header(
    root: &Coder,
    coders: &HashMap<String, Coder>,
) -> Result<WindowedHeader, BundleError> {
    let payload = root.spec.as_ref().map_or(&[][..], |s| s.payload.as_slice());
    let window_coder = root.component_coder_ids.get(1).map_or("", String::as_str);
    read_wire_header(
        &mut &payload[..],
        payload,
        &SkipPlan::new(window_coder, coders),
    )
    .map_err(|e| BundleError::Coder(format!("Invalid param_windowed_value payload: {e}")))
}
