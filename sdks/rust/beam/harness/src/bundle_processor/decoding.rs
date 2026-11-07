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
use std::sync::Arc;

use super::BundleError;
use beam::coders::{
    SkipCursor, SkipPlan, SkipStep, URN_LENGTH_PREFIX, URN_PARAM_WINDOWED_VALUE, URN_ROW,
    URN_WINDOWED_VALUE, VarIntCoder, WindowedHeader, add_nested_row_length_prefixes, coder_urn,
    has_nested_row_length_prefix, read_length_prefixed_slice, skip_pane_info,
    strip_nested_row_length_prefixes,
};
use beam::schema::Schema;
use beam_model::pipeline::Coder;

/// The most bytes of a varint that encodes a 64-bit value.
const MAX_VARINT_LEN: usize = 10;

/// One element decoded from an inbound chunk: its header and the location of its payload.
pub(super) struct RawElement {
    pub(super) header: WindowedHeader,
    payload: Payload,
}

/// The bytes of an unfinished element that one data stream carries from a chunk to the
/// next, with the saved decode state of the element.
#[derive(Default)]
pub(super) struct StreamCarry {
    bytes: Vec<u8>,
    partial: Option<Box<PartialElement>>,
}

impl StreamCarry {
    /// The carried bytes.
    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// The complete elements of one chunk, joined to the bytes that the stream carried.
pub(super) struct ChunkElements<'a> {
    chunk: Cow<'a, [u8]>,
    /// The offset of the next element in `chunk`.
    pos: usize,
    partial: Option<Box<PartialElement>>,
    /// True when the unfinished element needs the next chunk.
    need_more: bool,
}

impl ChunkElements<'_> {
    /// The bytes that the payloads of the elements point into.
    pub(super) fn chunk(&self) -> &[u8] {
        &self.chunk
    }
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

/// Saved decode state for an element split across inbound data chunks.
///
/// Byte offsets are relative to index 0 of the carry buffer, which stays un-drained until
/// this element completes.
pub(super) enum PartialElement {
    /// Waiting for the `beam:coder:windowed_value:v1` header to finish.
    Header,
    /// Header completed; waiting for the payload starting at `payload_start` to finish.
    Payload {
        header: WindowedHeader,
        payload_start: usize,
        state: PartialPayload,
    },
}

pub(super) enum PartialPayload {
    /// `ElementFraming::LengthPrefixed`: waiting for the varint length prefix.
    LengthPrefixVarint,
    /// `ElementFraming::LengthPrefixed`: varint decoded; waiting for `expected_len` bytes.
    LengthPrefixedBody {
        data_start: usize,
        expected_len: usize,
    },
    /// `ElementFraming::Walked` or `ElementFraming::Row`: resumable structural skip.
    Walked(SkipCursor),
}

/// Outcome of [`PortFraming::resume_partial`].
pub(super) enum PartialStep {
    /// The element finished at byte offset `end` of the carry buffer.
    Complete { element: RawElement, end: usize },
    /// Input ended before the element finished.
    NeedMore,
}

enum ElementFraming {
    /// The runner frames the element with a length prefix. `nested_rows` names the inner
    /// coder when the runner also puts length prefixes around nested rows.
    LengthPrefixed { nested_rows: Option<String> },
    /// A row with no length prefix. `plan` skips complete rows and resumes split rows.
    Row { plan: SkipPlan },
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

    /// Decodes the next complete windowed value at `cursor`, advancing `cursor` past it.
    ///
    /// Returns `Ok(None)` when `cursor` is empty or holds only a partial element, leaving
    /// `cursor` unchanged for the next chunk. Payloads are spans of `data`: read them with
    /// [`RawElement::payload`].
    pub(super) fn decode_next<'a>(
        &self,
        cursor: &mut &'a [u8],
        data: &'a [u8],
    ) -> Result<Option<RawElement>, BundleError> {
        if cursor.is_empty() {
            return Ok(None);
        }
        let mut next = *cursor;
        match self.decode(&mut next, data) {
            Ok(element) => {
                *cursor = next;
                Ok(Some(element))
            }
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(BundleError::Coder(format!(
                "Failed to decode stream element: {e}"
            ))),
        }
    }

    /// Starts resumable decoding at byte 0 of `data`, after [`decode_next`](Self::decode_next)
    /// returned `Ok(None)` on a non-empty tail. Advances through every complete token in `data`.
    pub(super) fn begin_partial(&self, data: &[u8]) -> Result<PartialElement, BundleError> {
        let mut partial = self.start_partial();
        match self.resume_partial(&mut partial, data)? {
            PartialStep::NeedMore => Ok(partial),
            PartialStep::Complete { .. } => Err(BundleError::Coder(
                "Resumable decoder completed an element that the fast path marked incomplete"
                    .to_string(),
            )),
        }
    }

    /// Resumes decoding `partial` with the bytes accumulated in `data`.
    pub(super) fn resume_partial(
        &self,
        partial: &mut PartialElement,
        data: &[u8],
    ) -> Result<PartialStep, BundleError> {
        if matches!(partial, PartialElement::Header) {
            let HeaderFraming::Wire { window } = &self.header else {
                *partial = self.start_payload(WindowedHeader::default(), 0);
                return self.resume_partial(partial, data);
            };
            let mut cursor = data;
            match read_wire_header(&mut cursor, data, window) {
                Ok(header) => {
                    let payload_start = offset(data, cursor);
                    *partial = self.start_payload(header, payload_start);
                }
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                    return Ok(PartialStep::NeedMore);
                }
                Err(e) => {
                    return Err(BundleError::Coder(format!(
                        "Failed to decode stream element: {e}"
                    )));
                }
            }
        }
        let PartialElement::Payload {
            header,
            payload_start,
            state,
        } = partial
        else {
            return Ok(PartialStep::NeedMore);
        };
        let payload_start = *payload_start;
        match state {
            PartialPayload::LengthPrefixVarint => {
                let mut cursor = data.get(payload_start..).unwrap_or_default();
                let len = match VarIntCoder::decode_varint(&mut cursor) {
                    Ok(len) => len,
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                        return Ok(PartialStep::NeedMore);
                    }
                    Err(e) => {
                        return Err(BundleError::Coder(format!(
                            "Failed to decode stream element: {e}"
                        )));
                    }
                };
                let expected_len = usize::try_from(len).map_err(|_| {
                    BundleError::Coder(format!(
                        "Failed to decode stream element: Negative length: {len}"
                    ))
                })?;
                let data_start = offset(data, cursor);
                if data.len().saturating_sub(data_start) < expected_len {
                    *state = PartialPayload::LengthPrefixedBody {
                        data_start,
                        expected_len,
                    };
                    return Ok(PartialStep::NeedMore);
                }
                let end = data_start + expected_len;
                let payload = self.finish_length_prefixed(data, data_start, end)?;
                let header = std::mem::take(header);
                Ok(PartialStep::Complete {
                    element: RawElement { header, payload },
                    end,
                })
            }
            PartialPayload::LengthPrefixedBody {
                data_start,
                expected_len,
            } => {
                let data_start = *data_start;
                let expected_len = *expected_len;
                if data.len().saturating_sub(data_start) < expected_len {
                    return Ok(PartialStep::NeedMore);
                }
                let end = data_start + expected_len;
                let payload = self.finish_length_prefixed(data, data_start, end)?;
                let header = std::mem::take(header);
                Ok(PartialStep::Complete {
                    element: RawElement { header, payload },
                    end,
                })
            }
            PartialPayload::Walked(cursor) => match cursor.advance(data) {
                Ok(SkipStep::NeedMore) => Ok(PartialStep::NeedMore),
                Ok(SkipStep::Complete(end)) => {
                    let payload = self.finish_walked(data, payload_start, end)?;
                    let header = std::mem::take(header);
                    Ok(PartialStep::Complete {
                        element: RawElement { header, payload },
                        end,
                    })
                }
                Err(e) => Err(BundleError::Coder(format!(
                    "Failed to decode stream element: {e}"
                ))),
            },
        }
    }

    /// Starts the decode of the next chunk of a stream, after the bytes that `carry` holds.
    pub(super) fn open_chunk<'a>(
        &self,
        carry: &mut StreamCarry,
        bytes: Cow<'a, [u8]>,
    ) -> ChunkElements<'a> {
        let partial = carry.partial.take();
        let chunk = if carry.bytes.is_empty() {
            bytes
        } else {
            let mut joined = std::mem::take(&mut carry.bytes);
            joined.extend_from_slice(&bytes);
            Cow::Owned(joined)
        };
        ChunkElements {
            chunk,
            pos: 0,
            partial,
            need_more: false,
        }
    }

    /// Returns the next complete element of the chunk, or `None` when the chunk has no more.
    pub(super) fn next_element(
        &self,
        elements: &mut ChunkElements<'_>,
    ) -> Result<Option<RawElement>, BundleError> {
        if let Some(mut partial) = elements.partial.take() {
            return match self.resume_partial(&mut partial, &elements.chunk)? {
                PartialStep::Complete { element, end } => {
                    elements.pos = end;
                    Ok(Some(element))
                }
                PartialStep::NeedMore => {
                    elements.partial = Some(partial);
                    elements.need_more = true;
                    Ok(None)
                }
            };
        }
        if elements.need_more {
            return Ok(None);
        }
        let chunk: &[u8] = &elements.chunk;
        let mut cursor = chunk.get(elements.pos..).unwrap_or_default();
        let element = self.decode_next(&mut cursor, chunk)?;
        elements.pos = chunk.len() - cursor.len();
        Ok(element)
    }

    /// Keeps the bytes after the last complete element of the chunk in `carry`. With `resume`,
    /// also keeps the decode state of the unfinished element, so the next chunk continues it.
    pub(super) fn close_chunk(
        &self,
        carry: &mut StreamCarry,
        elements: ChunkElements<'_>,
        resume: bool,
    ) -> Result<(), BundleError> {
        let ChunkElements {
            chunk,
            pos,
            partial,
            need_more,
        } = elements;
        if need_more {
            carry.bytes = chunk.into_owned();
            carry.partial = partial;
            return Ok(());
        }
        if pos >= chunk.len() {
            return Ok(());
        }
        carry.bytes = match chunk {
            Cow::Owned(mut bytes) => {
                bytes.drain(..pos);
                bytes
            }
            Cow::Borrowed(bytes) => bytes.get(pos..).unwrap_or_default().to_vec(),
        };
        if resume {
            carry.partial = Some(Box::new(self.begin_partial(&carry.bytes)?));
        }
        Ok(())
    }

    /// Decodes a sequence of chunks with the loop of the bundle read path, and passes each
    /// element's header and payload to `on_element`. Fails if bytes remain after the last
    /// chunk. It exists for the `data_plane` benchmark, which cannot run a bundle.
    #[doc(hidden)]
    pub fn decode_chunks(
        &self,
        chunks: impl IntoIterator<Item = impl AsRef<[u8]>>,
        mut on_element: impl FnMut(&WindowedHeader, &[u8]),
    ) -> Result<(), BundleError> {
        let mut carry = StreamCarry::default();
        for bytes in chunks {
            let mut elements = self.open_chunk(&mut carry, Cow::Borrowed(bytes.as_ref()));
            while let Some(element) = self.next_element(&mut elements)? {
                on_element(&element.header, element.payload(elements.chunk()));
            }
            self.close_chunk(&mut carry, elements, true)?;
        }
        if !carry.bytes().is_empty() {
            return Err(BundleError::Coder(format!(
                "Stream ended with {} trailing unconsumed bytes",
                carry.bytes().len()
            )));
        }
        Ok(())
    }

    fn start_partial(&self) -> PartialElement {
        match &self.header {
            HeaderFraming::Wire { .. } => PartialElement::Header,
            HeaderFraming::None => self.start_payload(WindowedHeader::default(), 0),
            // Cheap: WindowedHeader is inline or Arc-backed.
            HeaderFraming::Constant(header) => self.start_payload(header.clone(), 0),
        }
    }

    fn start_payload(&self, header: WindowedHeader, payload_start: usize) -> PartialElement {
        let state = match &self.element {
            ElementFraming::LengthPrefixed { .. } => PartialPayload::LengthPrefixVarint,
            ElementFraming::Row { plan, .. } => {
                PartialPayload::Walked(plan.cursor(payload_start, true))
            }
            ElementFraming::Walked { plan, nested, .. } => {
                PartialPayload::Walked(plan.cursor(payload_start, *nested))
            }
        };
        PartialElement::Payload {
            header,
            payload_start,
            state,
        }
    }

    fn finish_length_prefixed(
        &self,
        data: &[u8],
        data_start: usize,
        end: usize,
    ) -> Result<Payload, BundleError> {
        let nested_rows = match &self.element {
            ElementFraming::LengthPrefixed { nested_rows } => nested_rows.as_deref(),
            ElementFraming::Row { .. } | ElementFraming::Walked { .. } => None,
        };
        match nested_rows {
            Some(inner_id) => strip_nested_row_length_prefixes(
                data.get(data_start..end).unwrap_or_default(),
                inner_id,
                &self.coders,
            )
            .map(Payload::Owned)
            .map_err(|e| BundleError::Coder(format!("Failed to decode stream element: {e}"))),
            None => Ok(Payload::Span(data_start, end)),
        }
    }

    fn finish_walked(
        &self,
        data: &[u8],
        payload_start: usize,
        end: usize,
    ) -> Result<Payload, BundleError> {
        match &self.element {
            ElementFraming::Walked {
                coder_id,
                nested_rows: true,
                ..
            } => strip_nested_row_length_prefixes(
                data.get(payload_start..end).unwrap_or_default(),
                coder_id,
                &self.coders,
            )
            .map(Payload::Owned)
            .map_err(|e| BundleError::Coder(format!("Failed to decode stream element: {e}"))),
            ElementFraming::Walked {
                nested_rows: false, ..
            }
            | ElementFraming::Row { .. }
            | ElementFraming::LengthPrefixed { .. } => Ok(Payload::Span(payload_start, end)),
        }
    }

    /// Rewrites the `[header][payload]` bytes of a `BundleApplication`, in the SDK encoding,
    /// into the wire encoding of this port. Adds the length prefixes that the runner expects.
    pub fn reframe(&self, element: Vec<u8>) -> Result<Vec<u8>, BundleError> {
        let (root_prefix, nested_rows) = match &self.element {
            ElementFraming::Row { .. }
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
            ElementFraming::Row { plan, .. } => {
                plan.skip(cursor, true)?;
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
            .map(|schema| ElementFraming::Row {
                plan: SkipPlan::for_row(&schema),
            })
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
