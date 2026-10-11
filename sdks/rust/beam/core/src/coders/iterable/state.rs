// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Reads the inlined prefix of an iterable and the runner state pages after it.

use std::io;
use std::sync::Arc;

use super::{BeamIterable, ElementDecoder, PageStream, StateStreamReader};
use crate::coders::standard::{
    VarIntCoder, read_be_i32, read_exact_vec, state_backed_iterable_message,
};
use crate::coders::traits::CoderError;

/// Reads the inlined elements of an iterable and returns `(elements, continuation_token)`.
fn read_iterable_prefix<T>(
    reader: &mut &[u8],
    mut decode_element: impl FnMut(&mut &[u8]) -> Result<T, CoderError>,
) -> Result<(Vec<T>, Option<Vec<u8>>), CoderError> {
    let count = read_be_i32(reader)?;
    if count >= 0 {
        let elements = (0..count)
            .map(|_| decode_element(&mut *reader))
            .collect::<Result<Vec<_>, _>>()?;
        return Ok((elements, None));
    }
    if count != -1 {
        return Err(CoderError::Format(format!(
            "Invalid iterable count: {count}"
        )));
    }

    let mut prefix = Vec::new();
    loop {
        let chunk_len = VarIntCoder::decode_varint(reader)?;
        match chunk_len {
            0 => return Ok((prefix, None)),
            -1 => {
                let raw_len = VarIntCoder::decode_varint(reader)?;
                let token_len = usize::try_from(raw_len).map_err(|_| {
                    CoderError::Format(format!("Invalid continuation token length: {raw_len}"))
                })?;
                let token = read_exact_vec(reader, token_len)?;
                return Ok((prefix, Some(token)));
            }
            n if n < 0 => {
                return Err(CoderError::Format(format!(
                    "Invalid iterable chunk header: {n}"
                )));
            }
            n => {
                // `n` comes from the wire: reserve at most 1024 elements up front.
                prefix.reserve(usize::try_from(n.min(1024)).unwrap_or(0));
                for _ in 0..n {
                    prefix.push(decode_element(&mut *reader)?);
                }
            }
        }
    }
}

/// Decodes a [`BeamIterable<T>`], with a lazy state suffix if the runner sent a token.
/// Returns an error if a token arrives and `state_reader` is `None`.
pub fn decode_beam_iterable<T: 'static>(
    reader: &mut &[u8],
    decode_element: impl Fn(&mut &[u8]) -> Result<T, CoderError> + Send + Sync + 'static,
    state_reader: Option<&dyn StateStreamReader>,
) -> Result<BeamIterable<T>, CoderError> {
    let (prefix, token) = read_iterable_prefix(reader, |r| decode_element(r))?;
    let Some(token) = token else {
        return Ok(BeamIterable::from_vec(prefix));
    };

    tracing::debug!(
        "BeamIterable: received state-backed continuation token from runner (token_len={}, inlined_prefix_len={})",
        token.len(),
        prefix.len()
    );

    let Some(state_reader) = state_reader else {
        return Err(CoderError::StateStream(state_backed_iterable_message(
            prefix.len(),
        )));
    };
    let page_iter = state_reader
        .stream_runner_pages(&token)
        .map_err(|e| CoderError::StateStream(format!("Failed to open runner state stream: {e}")))?;
    let suffix_iter = StateElementIterator::new(page_iter, Arc::new(decode_element));
    Ok(BeamIterable::with_suffix(prefix, Box::new(suffix_iter)))
}

/// Decodes an iterable into a `Vec<T>` and fetches all state suffix elements. Returns an
/// error if a token arrives and `state_reader` is `None`.
pub fn decode_iterable_with_state<T>(
    reader: &mut &[u8],
    mut decode_element: impl FnMut(&mut &[u8]) -> Result<T, CoderError>,
    state_reader: Option<&dyn StateStreamReader>,
) -> Result<Vec<T>, CoderError> {
    let (mut result, token) = read_iterable_prefix(reader, &mut decode_element)?;
    let Some(token) = token else {
        return Ok(result);
    };

    tracing::debug!(
        "decode_iterable_with_state: received state-backed continuation token (token_len={}, inlined_prefix_len={})",
        token.len(),
        result.len()
    );

    let Some(state_reader) = state_reader else {
        return Err(CoderError::StateStream(state_backed_iterable_message(
            result.len(),
        )));
    };
    let page_iter = state_reader
        .stream_runner_pages(&token)
        .map_err(|e| CoderError::StateStream(format!("Failed to open runner state stream: {e}")))?;
    let mut stream_reader = PageStreamReader::new(page_iter, "Runner state stream error");
    while let Some(item) = stream_reader.decode_next(&mut decode_element) {
        result.push(item?);
    }
    Ok(result)
}

/// Reads runner State API pages as one byte stream. An element can continue on the next page:
/// when it does, the next page is appended and the element is decoded again.
pub struct PageStreamReader {
    page_iter: PageStream,
    /// Bytes not yet decoded, from `pos` on.
    buf: Vec<u8>,
    pos: usize,
    error_prefix: &'static str,
}

impl PageStreamReader {
    pub fn new(page_iter: PageStream, error_prefix: &'static str) -> Self {
        Self {
            page_iter,
            buf: Vec::new(),
            pos: 0,
            error_prefix,
        }
    }

    /// Appends the next non-empty page to the undecoded bytes. Returns `false` at the end of
    /// the stream.
    fn append_page(&mut self) -> Result<bool, CoderError> {
        loop {
            match self.page_iter.next() {
                Some(Ok(page)) if page.is_empty() => {}
                Some(Ok(page)) => {
                    tracing::debug!(
                        "Received a state page of {} bytes from the runner",
                        page.len()
                    );
                    self.buf.drain(..self.pos);
                    self.pos = 0;
                    self.buf.extend_from_slice(&page);
                    return Ok(true);
                }
                Some(Err(e)) => {
                    return Err(CoderError::StateStream(format!(
                        "{}: {e}",
                        self.error_prefix
                    )));
                }
                None => return Ok(false),
            }
        }
    }

    /// Decodes the next element from the page stream, or returns `None` at end-of-stream.
    fn decode_next<T>(
        &mut self,
        mut decode: impl FnMut(&mut &[u8]) -> Result<T, CoderError>,
    ) -> Option<Result<T, CoderError>> {
        loop {
            if self.pos == self.buf.len() {
                match self.append_page() {
                    Ok(true) => continue,
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }
            let mut rest = &self.buf[self.pos..];
            match decode(&mut rest) {
                Ok(value) => {
                    self.pos = self.buf.len() - rest.len();
                    return Some(Ok(value));
                }
                Err(CoderError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    match self.append_page() {
                        Ok(true) => {}
                        Ok(false) => return Some(Err(CoderError::Io(e))),
                        Err(page_error) => return Some(Err(page_error)),
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Iterator that lazily decodes elements from a stream of raw byte pages.
pub struct StateElementIterator<T> {
    reader: PageStreamReader,
    decode_element: ElementDecoder<T>,
}

impl<T> StateElementIterator<T> {
    pub fn new(page_iter: PageStream, decode_element: ElementDecoder<T>) -> Self {
        Self {
            reader: PageStreamReader::new(page_iter, "Runner state stream read error"),
            decode_element,
        }
    }
}

impl<T> Iterator for StateElementIterator<T> {
    type Item = Result<T, CoderError>;

    fn next(&mut self) -> Option<Self::Item> {
        let decode = &self.decode_element;
        self.reader.decode_next(|r| decode(r))
    }
}
