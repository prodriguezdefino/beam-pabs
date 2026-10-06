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

use super::cache::{CacheKey, CachedPart, CachedValue, StateCache};
use super::channel::StateChannel;
use super::tables::{FastHashMap, StateCellKey};
use bytes::{Buf, Bytes, BytesMut};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use beam::coders::skip_coder_value;
use beam::internals::{
    SideInputElements, SideInputError, SideInputReader, extract_side_input_tags,
};
use model::fn_execution::{ProcessBundleDescriptor, StateKey, state_key};
use model::pipeline::Coder;

/// The maximum bytes of side input pages that one bundle keeps. The oldest pages go first.
const SIDE_INPUT_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone)]
struct SideInputMeta {
    transform_id: String,
    side_input_id: String,
    elem_coder_id: String,
    value_coder_id: String,
    /// The runner cache token of the side input in this bundle, if it sent one.
    cache_token: Option<Bytes>,
}

pub struct FnApiSideInputReader {
    by_transform_and_tag: FastHashMap<String, FastHashMap<String, SideInputMeta>>,
    by_tag: FastHashMap<String, SideInputMeta>,
    coders: HashMap<String, Coder>,
    channel: Option<StateChannel>,
    /// Pages of side inputs without a cache token. They live for one bundle.
    bundle_pages: StateCache,
    /// Pages of side inputs with a cache token. They live across bundles.
    shared_pages: Option<Arc<StateCache>>,
}

impl FnApiSideInputReader {
    pub fn from_descriptor(
        instruction_id: &str,
        descriptor: &ProcessBundleDescriptor,
        worker_id: &str,
    ) -> Option<Self> {
        let channel = StateChannel::from_descriptor(instruction_id, descriptor, worker_id);
        Self::from_channel(descriptor, channel)
    }

    pub fn from_channel(
        descriptor: &ProcessBundleDescriptor,
        channel: Option<StateChannel>,
    ) -> Option<Self> {
        let metas: Vec<SideInputMeta> = descriptor
            .transforms
            .iter()
            .flat_map(|(t_id, transform)| {
                extract_side_input_tags(transform)
                    .into_iter()
                    .filter_map(move |tag| {
                        let pcol_id = transform.inputs.get(&tag)?;
                        let elem_coder_id = descriptor
                            .pcollections
                            .get(pcol_id)
                            .map(|p| p.coder_id.clone())
                            .unwrap_or_default();
                        let value_coder_id = descriptor
                            .coders
                            .get(&elem_coder_id)
                            .and_then(|c| c.component_coder_ids.get(1).cloned())
                            .unwrap_or_else(|| elem_coder_id.clone());
                        Some(SideInputMeta {
                            transform_id: t_id.clone(),
                            side_input_id: tag,
                            elem_coder_id,
                            value_coder_id,
                            cache_token: None,
                        })
                    })
            })
            .collect();

        if metas.is_empty() {
            return None;
        }

        let by_transform_and_tag: FastHashMap<String, FastHashMap<String, SideInputMeta>> =
            metas.iter().fold(FastHashMap::default(), |mut acc, m| {
                acc.entry(m.transform_id.clone())
                    .or_default()
                    .insert(m.side_input_id.clone(), m.clone());
                acc
            });
        let by_tag = metas
            .into_iter()
            .map(|m| (m.side_input_id.clone(), m))
            .collect();

        Some(Self {
            by_transform_and_tag,
            by_tag,
            coders: descriptor.coders.clone(),
            channel,
            bundle_pages: StateCache::new(SIDE_INPUT_CACHE_MAX_BYTES),
            shared_pages: None,
        })
    }

    /// Keeps the pages of side inputs with a token in `tokens` in `cache`, for later bundles.
    pub fn with_shared_cache(
        mut self,
        cache: Arc<StateCache>,
        tokens: &FastHashMap<(String, String), Bytes>,
    ) -> Self {
        let token_of = |meta: &SideInputMeta| {
            tokens
                .get(&(meta.transform_id.clone(), meta.side_input_id.clone()))
                .cloned()
        };
        self.by_transform_and_tag
            .values_mut()
            .flat_map(FastHashMap::values_mut)
            .chain(self.by_tag.values_mut())
            .for_each(|meta| meta.cache_token = token_of(meta));
        self.shared_pages = Some(cache);
        self
    }

    fn resolve_meta_for_transform(
        &self,
        transform_id: Option<&str>,
        tag: &str,
    ) -> Result<&SideInputMeta, SideInputError> {
        transform_id
            .and_then(|curr_t| self.by_transform_and_tag.get(curr_t)?.get(tag))
            .or_else(|| self.by_tag.get(tag))
            .ok_or_else(|| SideInputError::MissingReader {
                tag: tag.to_string(),
            })
    }

    /// Starts a lazy read of one side input cell, from the cache or from the runner.
    fn read_cell<'a>(
        &'a self,
        tag: &str,
        meta: &'a SideInputMeta,
        cell: StateCellKey,
        state_key: StateKey,
        coder_id: &'a str,
    ) -> Result<SideInputElements<'a>, SideInputError> {
        let channel = self
            .channel
            .as_ref()
            .ok_or_else(|| SideInputError::Runner {
                tag: tag.to_string(),
                reason: "ProcessBundleDescriptor has no state_api_service_descriptor".to_string(),
            })?;
        let tag: Arc<str> = Arc::from(tag);
        let (store, cache_token) = match (&self.shared_pages, &meta.cache_token) {
            (Some(shared), Some(token)) => (shared.as_ref(), token.clone()),
            _ => (&self.bundle_pages, Bytes::new()),
        };
        let pages = CachedPageIterator {
            store,
            cache_token,
            channel,
            state_key,
            cell: Arc::new(cell),
            next_token: Some(Bytes::new()),
            tag: Arc::clone(&tag),
        };
        Ok(Box::new(ElementFramingIterator {
            pages,
            framing: Framing::new(coder_id, &self.coders),
            buffer: Bytes::new(),
            tag,
        }))
    }
}

/// The pages of one side input cell, in runner order.
struct CachedPageIterator<'a> {
    store: &'a StateCache,
    /// The runner cache token of the side input, or empty for a cache of one bundle.
    cache_token: Bytes,
    channel: &'a StateChannel,
    state_key: StateKey,
    cell: Arc<StateCellKey>,
    /// The token of the next page; `None` after the last page.
    next_token: Option<Bytes>,
    tag: Arc<str>,
}

impl Iterator for CachedPageIterator<'_> {
    type Item = Result<Bytes, SideInputError>;

    fn next(&mut self) -> Option<Self::Item> {
        let page_token = self.next_token.take()?;
        let key = CacheKey {
            token: self.cache_token.clone(),
            cell: Arc::clone(&self.cell),
            part: CachedPart::SideInputPage(page_token.clone()),
        };
        let (data, next_token) = match self.store.get(&key) {
            Some(CachedValue::Page { data, next_token }) => (data, next_token),
            // Only pages are stored under a page key, so another value is a miss too.
            Some(CachedValue::Bag(_) | CachedValue::Map(_)) | None => {
                let (data, next_token) = match self
                    .channel
                    .fetch_page(&self.state_key, page_token.to_vec())
                {
                    Ok(page) => page,
                    Err(reason) => {
                        return Some(Err(SideInputError::Runner {
                            tag: self.tag.to_string(),
                            reason,
                        }));
                    }
                };
                let data = Bytes::from(data);
                let next_token = next_token.map(Bytes::from);
                self.store.insert(
                    key,
                    CachedValue::Page {
                        data: data.clone(),
                        next_token: next_token.clone(),
                    },
                );
                (data, next_token)
            }
        };
        self.next_token = next_token;
        Some(Ok(data))
    }
}

/// How the elements of a side input are delimited in the page bytes.
enum Framing<'a> {
    /// Each element is a varint length followed by that many bytes.
    LengthPrefixed,
    /// Each element is one value of the coder, read to find its end.
    Coder {
        coder_id: &'a str,
        coders: &'a HashMap<String, Coder>,
    },
}

impl<'a> Framing<'a> {
    fn new(coder_id: &'a str, coders: &'a HashMap<String, Coder>) -> Self {
        let is_length_prefix = coders
            .get(coder_id)
            .and_then(|c| c.spec.as_ref())
            .is_some_and(|s| s.urn == "beam:coder:length_prefix:v1");
        if is_length_prefix {
            Self::LengthPrefixed
        } else {
            Self::Coder { coder_id, coders }
        }
    }

    /// The first element of `data`: the range of its bytes, and the number of bytes it uses.
    /// An error means that `data` does not hold a complete element.
    fn first_element(&self, data: &[u8]) -> Result<(Range<usize>, usize), String> {
        let mut cursor = std::io::Cursor::new(data);
        match self {
            Self::LengthPrefixed => {
                let len = beam::coders::VarIntCoder::decode_varint(&mut cursor)
                    .map_err(|e| format!("Failed to read length prefix: {e}"))?;
                let start = cursor.position() as usize;
                usize::try_from(len)
                    .ok()
                    .and_then(|len| start.checked_add(len))
                    .filter(|&end| end <= data.len())
                    .map(|end| (start..end, end))
                    .ok_or_else(|| {
                        format!(
                            "Length prefix {len} exceeds remaining data length {}",
                            data.len() - start
                        )
                    })
            }
            Self::Coder { coder_id, coders } => {
                skip_coder_value(&mut cursor, coder_id, coders, true)
                    .map(|()| {
                        let end = cursor.position() as usize;
                        (0..end, end)
                    })
                    .map_err(|e| format!("Failed to frame element with coder '{coder_id}': {e}"))
            }
        }
    }
}

/// The elements of a side input cell. An element can span pages.
struct ElementFramingIterator<'a> {
    pages: CachedPageIterator<'a>,
    framing: Framing<'a>,
    /// Bytes of the current page that are not yet returned.
    buffer: Bytes,
    tag: Arc<str>,
}

impl Iterator for ElementFramingIterator<'_> {
    type Item = Result<Bytes, SideInputError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let incomplete = if self.buffer.is_empty() {
                None
            } else {
                match self.framing.first_element(&self.buffer) {
                    Ok((element, used)) => {
                        let item = self.buffer.slice(element);
                        self.buffer.advance(used);
                        return Some(Ok(item));
                    }
                    Err(reason) => Some(reason),
                }
            };
            match (self.pages.next(), incomplete) {
                (Some(Ok(page)), None) => self.buffer = page,
                (Some(Ok(page)), Some(_)) => self.buffer = join(&self.buffer, &page),
                (Some(Err(e)), _) => return Some(Err(e)),
                (None, None) => return None,
                (None, Some(reason)) => {
                    self.buffer = Bytes::new();
                    return Some(Err(SideInputError::Framing {
                        tag: self.tag.to_string(),
                        reason,
                    }));
                }
            }
        }
    }
}

/// `head` followed by `tail` in one buffer.
fn join(head: &[u8], tail: &[u8]) -> Bytes {
    let mut joined = BytesMut::with_capacity(head.len() + tail.len());
    joined.extend_from_slice(head);
    joined.extend_from_slice(tail);
    joined.freeze()
}

impl SideInputReader for FnApiSideInputReader {
    fn get_iterable<'a>(
        &'a self,
        tag: &str,
        window_bytes: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        self.get_iterable_for_transform("", tag, window_bytes)
    }

    fn get_multimap<'a>(
        &'a self,
        tag: &str,
        window_bytes: &[u8],
        key_bytes: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        self.get_multimap_for_transform("", tag, window_bytes, key_bytes)
    }

    fn get_iterable_for_transform<'a>(
        &'a self,
        transform_id: &str,
        tag: &str,
        window_bytes: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        let meta = self
            .resolve_meta_for_transform((!transform_id.is_empty()).then_some(transform_id), tag)?;
        let cell = StateCellKey::new(&meta.transform_id, &meta.side_input_id, window_bytes, &[]);
        let state_key = StateKey {
            r#type: Some(state_key::Type::IterableSideInput(
                state_key::IterableSideInput {
                    transform_id: meta.transform_id.clone(),
                    side_input_id: meta.side_input_id.clone(),
                    window: window_bytes.to_vec(),
                },
            )),
        };
        self.read_cell(tag, meta, cell, state_key, &meta.elem_coder_id)
    }

    fn get_multimap_for_transform<'a>(
        &'a self,
        transform_id: &str,
        tag: &str,
        window_bytes: &[u8],
        key_bytes: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        let meta = self
            .resolve_meta_for_transform((!transform_id.is_empty()).then_some(transform_id), tag)?;
        let cell = StateCellKey::new(
            &meta.transform_id,
            &meta.side_input_id,
            window_bytes,
            key_bytes,
        );
        let state_key = StateKey {
            r#type: Some(state_key::Type::MultimapSideInput(
                state_key::MultimapSideInput {
                    transform_id: meta.transform_id.clone(),
                    side_input_id: meta.side_input_id.clone(),
                    window: window_bytes.to_vec(),
                    key: key_bytes.to_vec(),
                },
            )),
        };
        self.read_cell(tag, meta, cell, state_key, &meta.value_coder_id)
    }
}

pub(crate) fn split_concatenated_elements(
    data: &[u8],
    coder_id: &str,
    coders: &HashMap<String, Coder>,
) -> Result<Vec<Vec<u8>>, String> {
    let mut cursor = std::io::Cursor::new(data);
    let is_length_prefix = coders
        .get(coder_id)
        .and_then(|c| c.spec.as_ref())
        .map(|s| s.urn.as_str() == "beam:coder:length_prefix:v1")
        .unwrap_or(false);

    std::iter::from_fn(|| {
        let start = cursor.position() as usize;
        (start < data.len()).then(|| {
            if is_length_prefix {
                let len = beam::coders::VarIntCoder::decode_varint(&mut cursor)
                    .map_err(|e| format!("Failed to read length prefix: {e}"))?
                    as usize;
                let payload_start = cursor.position() as usize;
                let payload_end = payload_start + len;
                if payload_end > data.len() {
                    return Err(format!(
                        "Length prefix {} exceeds data length {}",
                        len,
                        data.len()
                    ));
                }
                cursor.set_position(payload_end as u64);
                Ok(data[payload_start..payload_end].to_vec())
            } else {
                skip_coder_value(&mut cursor, coder_id, coders, true)
                    .map(|()| data[start..cursor.position() as usize].to_vec())
                    .map_err(|e| {
                        format!("Failed to frame side input element with coder '{coder_id}': {e}")
                    })
            }
        })
    })
    .collect()
}
