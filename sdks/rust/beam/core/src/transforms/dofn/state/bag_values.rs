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

//! Packed byte storage for bag user state values.

use std::fmt;
use std::iter::FusedIterator;
use std::ops::Range;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use crate::coders::CoderError;

/// Maximum number of chunks before [`StateBag::append`] compacts into one buffer.
const MAX_CHUNKS: usize = 32;

/// Contiguous byte buffer with element byte spans.
#[derive(Debug)]
struct BagChunk {
    data: Bytes,
    spans: Box<[Range<usize>]>,
}

/// Shared chunk list and aggregate sizes for a non-empty [`StateBag`].
struct StateBagInner {
    chunks: Box<[Arc<BagChunk>]>,
    len: usize,
    byte_len: usize,
}

/// Packed byte elements of a bag state cell, shared by `Arc` with zero-copy slicing.
#[derive(Clone, Default)]
pub struct StateBag {
    inner: Option<Arc<StateBagInner>>,
}

impl StateBag {
    /// Creates a bag from `data` and the element `spans` inside `data`.
    ///
    /// # Errors
    ///
    /// Returns [`CoderError::Format`] for a span that does not fit inside `data`.
    pub fn from_packed(data: Bytes, spans: Box<[Range<usize>]>) -> Result<Self, CoderError> {
        if let Some(span) = spans
            .iter()
            .find(|span| span.start > span.end || span.end > data.len())
        {
            return Err(CoderError::Format(format!(
                "Bag element span {span:?} is outside the {} data bytes",
                data.len()
            )));
        }
        Ok(Self::packed(data, spans))
    }

    /// Creates a bag from `data` and `spans` that fit inside `data`.
    fn packed(data: Bytes, spans: Box<[Range<usize>]>) -> Self {
        if spans.is_empty() {
            return Self::default();
        }
        let len = spans.len();
        let byte_len = spans
            .iter()
            .map(|span| span.end.saturating_sub(span.start))
            .sum();
        let chunk = Arc::new(BagChunk { data, spans });
        Self {
            inner: Some(Arc::new(StateBagInner {
                chunks: Box::new([chunk]),
                len,
                byte_len,
            })),
        }
    }

    /// Creates a bag with one element backed by `element`.
    pub fn from_single(element: Bytes) -> Self {
        let byte_len = element.len();
        let span = 0..byte_len;
        let chunk = Arc::new(BagChunk {
            data: element,
            spans: Box::new([span]),
        });
        Self {
            inner: Some(Arc::new(StateBagInner {
                chunks: Box::new([chunk]),
                len: 1,
                byte_len,
            })),
        }
    }

    /// Returns a bag containing the elements of `self` followed by `other`.
    pub fn append(&self, other: &Self) -> Self {
        let (Some(left), Some(right)) = (&self.inner, &other.inner) else {
            if self.inner.is_some() {
                return self.clone();
            }
            return other.clone();
        };
        let len = left.len + right.len;
        let byte_len = left.byte_len + right.byte_len;
        if left.chunks.len() + right.chunks.len() <= MAX_CHUNKS {
            let chunks: Box<[Arc<BagChunk>]> = left
                .chunks
                .iter()
                .chain(right.chunks.iter())
                .map(Arc::clone)
                .collect();
            return Self {
                inner: Some(Arc::new(StateBagInner {
                    chunks,
                    len,
                    byte_len,
                })),
            };
        }
        let mut data = BytesMut::with_capacity(byte_len);
        let mut spans = Vec::with_capacity(len);
        for element in self.iter().chain(other.iter()) {
            let start = data.len();
            data.extend_from_slice(element);
            spans.push(start..data.len());
        }
        Self::packed(data.freeze(), spans.into_boxed_slice())
    }

    /// Number of elements in the bag.
    pub fn len(&self) -> usize {
        self.inner.as_ref().map_or(0, |inner| inner.len)
    }

    /// Whether the bag holds no elements.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Sum of the byte lengths of all elements.
    pub fn byte_len(&self) -> usize {
        self.inner.as_ref().map_or(0, |inner| inner.byte_len)
    }

    /// Borrows each element in order.
    pub fn iter(&self) -> StateBagIter<'_> {
        let (chunks, remaining) = match &self.inner {
            Some(inner) => (&inner.chunks[..], inner.len),
            None => (&[][..], 0),
        };
        StateBagIter {
            chunks,
            chunk_index: 0,
            span_index: 0,
            remaining,
        }
    }

    /// Borrows the first element, or `None` when empty.
    pub fn first(&self) -> Option<&[u8]> {
        let chunk = self.inner.as_ref()?.chunks.first()?;
        let span = chunk.spans.first()?;
        chunk.data.get(span.clone())
    }

    /// Slices the first element without copying bytes, or returns `None` when empty.
    pub fn first_bytes(&self) -> Option<Bytes> {
        let chunk = self.inner.as_ref()?.chunks.first()?;
        let span = chunk.spans.first()?;
        let slice = chunk.data.get(span.clone())?;
        Some(chunk.data.slice_ref(slice))
    }

    /// Copies every element into owned byte vectors.
    pub fn to_vecs(&self) -> Vec<Vec<u8>> {
        self.iter().map(<[u8]>::to_vec).collect()
    }
}

/// Packs owned byte vectors into a bag.
impl From<Vec<Vec<u8>>> for StateBag {
    fn from(elements: Vec<Vec<u8>>) -> Self {
        let mut iter = elements.into_iter();
        let Some(first) = iter.next() else {
            return Self::default();
        };
        if iter.len() == 0 {
            return Self::from_single(Bytes::from(first));
        }
        let count = 1 + iter.len();
        let byte_len = first.len() + iter.as_slice().iter().map(Vec::len).sum::<usize>();
        let mut data = BytesMut::with_capacity(byte_len);
        let mut spans = Vec::with_capacity(count);
        for element in std::iter::once(first).chain(iter) {
            let start = data.len();
            data.extend_from_slice(&element);
            spans.push(start..data.len());
        }
        Self::packed(data.freeze(), spans.into_boxed_slice())
    }
}

impl fmt::Debug for StateBag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl PartialEq for StateBag {
    fn eq(&self, other: &Self) -> bool {
        match (&self.inner, &other.inner) {
            (None, None) => true,
            (Some(a), Some(b)) if Arc::ptr_eq(a, b) => true,
            (Some(a), Some(b)) => {
                a.len == b.len && a.byte_len == b.byte_len && self.iter().eq(other.iter())
            }
            (None, Some(_)) | (Some(_), None) => false,
        }
    }
}

impl Eq for StateBag {}

impl<T: AsRef<[u8]>> PartialEq<[T]> for StateBag {
    fn eq(&self, other: &[T]) -> bool {
        self.len() == other.len() && self.iter().zip(other).all(|(lhs, rhs)| lhs == rhs.as_ref())
    }
}

impl<T: AsRef<[u8]>> PartialEq<&[T]> for StateBag {
    fn eq(&self, other: &&[T]) -> bool {
        <Self as PartialEq<[T]>>::eq(self, *other)
    }
}

impl<T: AsRef<[u8]>, const N: usize> PartialEq<[T; N]> for StateBag {
    fn eq(&self, other: &[T; N]) -> bool {
        <Self as PartialEq<[T]>>::eq(self, other.as_slice())
    }
}

impl<T: AsRef<[u8]>> PartialEq<Vec<T>> for StateBag {
    fn eq(&self, other: &Vec<T>) -> bool {
        <Self as PartialEq<[T]>>::eq(self, other.as_slice())
    }
}

/// Iterator over the encoded byte slices of a [`StateBag`].
#[derive(Clone, Debug)]
pub struct StateBagIter<'a> {
    chunks: &'a [Arc<BagChunk>],
    chunk_index: usize,
    span_index: usize,
    remaining: usize,
}

impl<'a> Iterator for StateBagIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(chunk) = self.chunks.get(self.chunk_index) {
            if let Some(span) = chunk.spans.get(self.span_index) {
                self.span_index += 1;
                self.remaining -= 1;
                return chunk.data.get(span.clone());
            }
            self.chunk_index += 1;
            self.span_index = 0;
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for StateBagIter<'_> {
    fn len(&self) -> usize {
        self.remaining
    }
}

impl FusedIterator for StateBagIter<'_> {}

impl<'a> IntoIterator for &'a StateBag {
    type Item = &'a [u8];
    type IntoIter = StateBagIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
