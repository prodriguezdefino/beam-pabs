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

use super::{
    BeamIterable, ElementDecoder, PageStream, StateStreamReader, StreamOpener, StreamingSuffix,
};
use crate::coders::standard::{
    VarIntCoder, read_be_i32, read_exact_vec, state_backed_iterable_message,
};
use crate::coders::traits::CoderError;

/// Continuation token and raw inline chunks captured for reopening a state-backed iterable.
struct Continuation {
    token: Vec<u8>,
    prefix_chunks: Vec<u8>,
}

/// Shared state needed to reopen a state-backed iterable from its first inline element.
struct ReopenSource<T> {
    prefix_chunks: Vec<u8>,
    token: Vec<u8>,
    reader: Arc<dyn StateStreamReader>,
    decode: ElementDecoder<T>,
}

/// Replays captured inline chunks before streaming runner state pages on a reopened pass.
struct ReopenedIter<T> {
    source: Arc<ReopenSource<T>>,
    prefix_pos: usize,
    chunk_remaining: usize,
    suffix: StateElementIterator<T>,
}

impl<T> Iterator for ReopenedIter<T> {
    type Item = Result<T, CoderError>;

    fn next(&mut self) -> Option<Self::Item> {
        let prefix = self.source.prefix_chunks.as_slice();
        while self.chunk_remaining == 0 && self.prefix_pos < prefix.len() {
            let mut slice = &prefix[self.prefix_pos..];
            let chunk_len = match VarIntCoder::decode_varint(&mut slice) {
                Ok(n) => n,
                Err(e) => {
                    self.prefix_pos = prefix.len();
                    return Some(Err(e.into()));
                }
            };
            self.prefix_pos = prefix.len().saturating_sub(slice.len());
            match usize::try_from(chunk_len).ok().filter(|&n| n > 0) {
                Some(n) => self.chunk_remaining = n,
                None => {
                    self.prefix_pos = prefix.len();
                    return Some(Err(CoderError::Format(format!(
                        "Invalid iterable chunk header: {chunk_len}"
                    ))));
                }
            }
        }
        if self.chunk_remaining > 0 {
            self.chunk_remaining -= 1;
            let mut slice = &prefix[self.prefix_pos..];
            let item = (self.source.decode)(&mut slice);
            self.prefix_pos = prefix.len().saturating_sub(slice.len());
            if item.is_err() {
                self.chunk_remaining = 0;
                self.prefix_pos = prefix.len();
            }
            return Some(item);
        }
        self.suffix.next()
    }
}

/// Reads the inlined elements of an iterable and returns `(elements, continuation)`.
fn read_iterable_prefix<T>(
    reader: &mut &[u8],
    mut decode_element: impl FnMut(&mut &[u8]) -> Result<T, CoderError>,
    capture_prefix_chunks: bool,
) -> Result<(Vec<T>, Option<Continuation>), CoderError> {
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

    let chunks_start = *reader;
    let mut prefix = Vec::new();
    loop {
        let before_header = *reader;
        let chunk_len = VarIntCoder::decode_varint(reader)?;
        match chunk_len {
            0 => return Ok((prefix, None)),
            -1 => {
                let raw_len = VarIntCoder::decode_varint(reader)?;
                let token_len = usize::try_from(raw_len).map_err(|_| {
                    CoderError::Format(format!("Invalid continuation token length: {raw_len}"))
                })?;
                let token = read_exact_vec(reader, token_len)?;
                let prefix_chunks = if capture_prefix_chunks {
                    let prefix_bytes_len = chunks_start.len().saturating_sub(before_header.len());
                    chunks_start[..prefix_bytes_len].to_vec()
                } else {
                    Vec::new()
                };
                return Ok((
                    prefix,
                    Some(Continuation {
                        token,
                        prefix_chunks,
                    }),
                ));
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

fn open_runner_pages(
    state_reader: &dyn StateStreamReader,
    token: &[u8],
) -> Result<PageStream, CoderError> {
    state_reader
        .stream_runner_pages(token)
        .map_err(|e| CoderError::StateStream(format!("Failed to open runner state stream: {e}")))
}

fn open_reopened<T: 'static>(
    source: &Arc<ReopenSource<T>>,
) -> Result<StreamingSuffix<T>, CoderError> {
    let page_iter = open_runner_pages(source.reader.as_ref(), &source.token)?;
    let suffix = StateElementIterator::new(page_iter, Arc::clone(&source.decode));
    Ok(Box::new(ReopenedIter {
        source: Arc::clone(source),
        prefix_pos: 0,
        chunk_remaining: 0,
        suffix,
    }))
}

/// Decodes a [`BeamIterable<T>`], with a lazy state suffix if the runner sent a token.
/// Returns an error if a token arrives and `state_reader` is `None`.
pub fn decode_beam_iterable<T: 'static>(
    reader: &mut &[u8],
    decode_element: impl Fn(&mut &[u8]) -> Result<T, CoderError> + Send + Sync + 'static,
    state_reader: Option<&dyn StateStreamReader>,
) -> Result<BeamIterable<T>, CoderError> {
    let (prefix, continuation) = read_iterable_prefix(reader, |r| decode_element(r), false)?;
    let Some(Continuation { token, .. }) = continuation else {
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
    let page_iter = open_runner_pages(state_reader, &token)?;
    let suffix_iter = StateElementIterator::new(page_iter, Arc::new(decode_element));
    Ok(BeamIterable::with_suffix(prefix, Box::new(suffix_iter)))
}

/// Decodes a [`BeamIterable<T>`] and keeps a reopen closure when a continuation token is
/// present. Opens the pass-1 state stream during decode so stream-open errors surface early.
pub(crate) fn decode_beam_iterable_reopenable<T: 'static>(
    reader: &mut &[u8],
    decode_element: impl Fn(&mut &[u8]) -> Result<T, CoderError> + Send + Sync + 'static,
    state_reader: Option<&Arc<dyn StateStreamReader>>,
) -> Result<BeamIterable<T>, CoderError> {
    let (prefix, continuation) = read_iterable_prefix(reader, |r| decode_element(r), true)?;
    let Some(Continuation {
        token,
        prefix_chunks,
    }) = continuation
    else {
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
    let page_iter = open_runner_pages(state_reader.as_ref(), &token)?;
    let decode: ElementDecoder<T> = Arc::new(decode_element);
    let suffix_iter = StateElementIterator::new(page_iter, Arc::clone(&decode));
    let source = Arc::new(ReopenSource {
        prefix_chunks,
        token,
        reader: Arc::clone(state_reader),
        decode,
    });
    let reopen: StreamOpener<T> = Arc::new(move || open_reopened(&source));
    Ok(BeamIterable::with_reopenable_suffix(
        prefix,
        Box::new(suffix_iter),
        reopen,
    ))
}

/// Decodes an iterable into a `Vec<T>` and fetches all state suffix elements. Returns an
/// error if a token arrives and `state_reader` is `None`.
pub fn decode_iterable_with_state<T>(
    reader: &mut &[u8],
    mut decode_element: impl FnMut(&mut &[u8]) -> Result<T, CoderError>,
    state_reader: Option<&dyn StateStreamReader>,
) -> Result<Vec<T>, CoderError> {
    let (mut result, continuation) = read_iterable_prefix(reader, &mut decode_element, false)?;
    let Some(Continuation { token, .. }) = continuation else {
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
    let page_iter = open_runner_pages(state_reader, &token)?;
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
