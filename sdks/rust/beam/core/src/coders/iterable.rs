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

//! Iterables for `beam:coder:state_backed_iterable:v1`. The runner can split a large iterable
//! (for example a hot key after `GroupByKey`): inlined elements come first, then a `-1` chunk
//! header and a state continuation token. [`BeamIterable`] reads the rest lazily from the
//! runner State API.

use std::fmt;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use super::URN_ITERABLE;
use super::standard::{VarIntCoder, read_be_i32, read_exact_vec, state_backed_iterable_message};
use super::traits::{CoderError, CoderRegistry, DefaultCoder};

/// Iterator over raw byte pages from runner state.
pub type PageStream = Box<dyn Iterator<Item = Result<Vec<u8>, String>> + Send>;

/// Closure that decodes one element from a byte reader.
pub type ElementDecoder<T> = Arc<dyn Fn(&mut &[u8]) -> Result<T, CoderError> + Send + Sync>;

/// Shared, thread-safe iterator over the state-backed suffix of an iterable.
pub type StreamingSuffix<T> = Arc<Mutex<Box<dyn Iterator<Item = Result<T, CoderError>> + Send>>>;

/// Opens runner state streams for continuation tokens.
pub trait StateStreamReader: Send + Sync {
    /// Opens the state pages for `token`. Each page holds nested-context elements.
    fn stream_runner_pages(&self, token: &[u8]) -> Result<PageStream, String>;
}

/// Beam elements with an in-memory prefix and an optional suffix that is read lazily from
/// runner state when the runner sends a continuation token.
pub struct BeamIterable<T> {
    prefix: Vec<T>,
    suffix: Option<StreamingSuffix<T>>,
}

impl<T> BeamIterable<T> {
    pub fn from_vec(elements: Vec<T>) -> Self {
        Self {
            prefix: elements,
            suffix: None,
        }
    }

    /// Creates an iterable with an inlined prefix and a lazy streaming suffix.
    pub fn with_suffix(
        prefix: Vec<T>,
        suffix: Box<dyn Iterator<Item = Result<T, CoderError>> + Send>,
    ) -> Self {
        Self {
            prefix,
            suffix: Some(Arc::new(Mutex::new(suffix))),
        }
    }

    pub fn is_in_memory(&self) -> bool {
        self.suffix.is_none()
    }

    pub fn inlined_prefix(&self) -> &[T] {
        &self.prefix
    }

    /// Returns the element count, or `None` if a state suffix exists: the length is unknown
    /// until the suffix is drained.
    #[must_use]
    pub fn in_memory_len(&self) -> Option<usize> {
        self.is_in_memory().then_some(self.prefix.len())
    }

    /// Collects all elements, and fetches the state suffix.
    pub fn into_vec(self) -> Result<Vec<T>, CoderError> {
        let mut result = self.prefix;
        if let Some(suffix) = self.suffix {
            let mut lock = suffix
                .lock()
                .map_err(|e| CoderError::Format(format!("Mutex lock error: {e}")))?;
            for item in lock.by_ref() {
                result.push(item?);
            }
        }
        Ok(result)
    }

    /// An iterable with a state suffix is never empty: the runner sends a continuation token
    /// only when more elements exist.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.prefix.is_empty() && self.suffix.is_none()
    }

    #[must_use]
    pub fn has_state_suffix(&self) -> bool {
        self.suffix.is_some()
    }

    /// Converts the iterable into an iterator that yields `Result<T, CoderError>`.
    pub fn try_into_iter(self) -> BeamIterableTryIntoIter<T> {
        BeamIterableTryIntoIter {
            prefix: self.prefix.into_iter(),
            suffix: self.suffix,
        }
    }
}

impl<T: Clone> Clone for BeamIterable<T> {
    fn clone(&self) -> Self {
        Self {
            prefix: self.prefix.clone(),
            suffix: self.suffix.clone(),
        }
    }
}

impl<T: PartialEq> PartialEq for BeamIterable<T> {
    /// Compares the prefixes and the suffix identity. Different suffixes are never equal: a
    /// comparison would consume the single-pass state streams.
    fn eq(&self, other: &Self) -> bool {
        let suffixes_match = match (&self.suffix, &other.suffix) {
            (None, None) => true,
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            _ => false,
        };
        suffixes_match && self.prefix == other.prefix
    }
}

impl<T: Eq> Eq for BeamIterable<T> {}

impl<T: fmt::Debug> fmt::Debug for BeamIterable<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.suffix.is_some() {
            f.debug_struct("BeamIterable")
                .field("inlined_prefix", &self.prefix)
                .field("has_state_suffix", &true)
                .finish()
        } else {
            f.debug_struct("BeamIterable")
                .field("elements", &self.prefix)
                .finish()
        }
    }
}

impl<T> Default for BeamIterable<T> {
    fn default() -> Self {
        Self::from_vec(Vec::new())
    }
}

impl<T> From<Vec<T>> for BeamIterable<T> {
    fn from(v: Vec<T>) -> Self {
        Self::from_vec(v)
    }
}

impl<T> FromIterator<T> for BeamIterable<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::from_vec(iter.into_iter().collect())
    }
}

/// Fallible iterator over elements of a [`BeamIterable`].
pub struct BeamIterableTryIntoIter<T> {
    prefix: std::vec::IntoIter<T>,
    suffix: Option<StreamingSuffix<T>>,
}

impl<T> Iterator for BeamIterableTryIntoIter<T> {
    type Item = Result<T, CoderError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.prefix.next() {
            return Some(Ok(item));
        }
        // A poisoned mutex is an error: `None` would truncate the stream without a signal.
        let suffix = self.suffix.as_mut()?;
        match suffix.lock() {
            Ok(mut iter) => iter.next(),
            Err(e) => Some(Err(CoderError::Format(format!(
                "state-backed iterable suffix mutex was poisoned by a panicking reader: {e}"
            )))),
        }
    }
}

/// Consuming iterator that panics with the [`CoderError`] if a state fetch or decode fails.
/// Use [`BeamIterable::try_into_iter`] to handle errors.
pub struct BeamIterableIntoIter<T> {
    inner: BeamIterableTryIntoIter<T>,
}

impl<T> Iterator for BeamIterableIntoIter<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next()? {
            Ok(item) => Some(item),
            Err(e) => panic!("Failed to stream state-backed iterable element: {e}"),
        }
    }
}

impl<T> IntoIterator for BeamIterable<T> {
    type Item = T;
    type IntoIter = BeamIterableIntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        BeamIterableIntoIter {
            inner: self.try_into_iter(),
        }
    }
}

impl<T: DefaultCoder> DefaultCoder for BeamIterable<T> {
    type Coder = super::composite::IterableCoder<T, T::Coder>;

    fn coder() -> Self::Coder {
        super::composite::IterableCoder::new(T::coder())
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        // Encoding only the prefix would write a wrong count and lose the suffix elements.
        if self.has_state_suffix() {
            return Err(CoderError::Format(
                "cannot encode a state-backed BeamIterable: drain it with `into_vec` first"
                    .to_string(),
            ));
        }
        let count = i32::try_from(self.prefix.len()).map_err(|_| {
            CoderError::Format(format!(
                "iterable length {} exceeds the i32 wire limit",
                self.prefix.len()
            ))
        })?;
        writer.write_all(&count.to_be_bytes())?;
        self.prefix
            .iter()
            .try_for_each(|item| item.encode_element(writer))
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        Self::decode_element_with_context(reader, None, None)
    }

    fn decode_element_with_context(
        reader: &mut &[u8],
        schema: Option<&Arc<crate::schema::Schema>>,
        state_reader: Option<&Arc<dyn StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        let schema_cloned = schema.cloned();
        let state_reader_cloned = state_reader.cloned();
        decode_beam_iterable(
            reader,
            Arc::new(move |r| {
                T::decode_element_with_context(
                    r,
                    schema_cloned.as_ref(),
                    state_reader_cloned.as_ref(),
                )
            }),
            state_reader.map(|r| r.as_ref()),
        )
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let elem_id = T::register_coder(registry);
        registry.register_coder(URN_ITERABLE, vec![elem_id])
    }
}

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
    decode_element: ElementDecoder<T>,
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
        return Err(CoderError::Format(state_backed_iterable_message(
            prefix.len(),
        )));
    };
    let page_iter = state_reader
        .stream_runner_pages(&token)
        .map_err(|e| CoderError::Format(format!("Failed to open runner state stream: {e}")))?;
    let suffix_iter = StateElementIterator::new(page_iter, decode_element);
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
        return Err(CoderError::Format(state_backed_iterable_message(
            result.len(),
        )));
    };
    let page_iter = state_reader
        .stream_runner_pages(&token)
        .map_err(|e| CoderError::Format(format!("Failed to open runner state stream: {e}")))?;
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
                    return Err(CoderError::Format(format!(
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
