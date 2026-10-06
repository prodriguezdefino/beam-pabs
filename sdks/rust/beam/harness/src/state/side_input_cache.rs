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

use super::cache::{CachedPart, CachedValue, StateCache};
use super::channel::StateChannel;
use super::tables::{FastHashMap, StateCellKey, StateCellKeyParts};
use bytes::{Buf, Bytes, BytesMut};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use beam::coders::SkipPlan;
use beam::internals::{SideInputElements, SideInputError, SideInputReader, extract_side_inputs};
use beam::values::SideInputWindowing;
use model::fn_execution::{ProcessBundleDescriptor, StateKey};
use model::pipeline::Coder;

/// The maximum bytes of side input pages that one bundle keeps. The oldest pages go first.
const SIDE_INPUT_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

struct SideInputMeta {
    transform_id: String,
    side_input_id: String,
    elem_coder_id: String,
    value_coder_id: String,
    /// The `window_mapping_fn` of the side input, decoded once for the descriptor.
    windowing: Result<SideInputWindowing, String>,
}

/// The side inputs that the transforms of a descriptor read, with their coders. A processor
/// builds it once and its bundles share it.
pub struct SideInputSpecs {
    metas: Vec<SideInputMeta>,
    /// Maps a transform id and a tag to an index in `metas`.
    by_transform_and_tag: FastHashMap<String, FastHashMap<String, usize>>,
    /// Maps a tag to an index in `metas`, for a read that does not name its transform.
    by_tag: FastHashMap<String, usize>,
    coders: Arc<HashMap<String, Coder>>,
}

impl SideInputSpecs {
    /// The side inputs of `descriptor`, or `None` if no transform reads one.
    pub fn from_descriptor(
        descriptor: &ProcessBundleDescriptor,
        coders: Arc<HashMap<String, Coder>>,
    ) -> Option<Self> {
        let metas: Vec<SideInputMeta> = descriptor
            .transforms
            .iter()
            .flat_map(|(t_id, transform)| {
                extract_side_inputs(transform)
                    .into_iter()
                    .filter_map(move |(tag, side_input)| {
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
                        let windowing = side_input
                            .window_mapping_fn
                            .as_ref()
                            .ok_or_else(|| "the descriptor has no window_mapping_fn".to_string())
                            .and_then(SideInputWindowing::from_proto);
                        Some(SideInputMeta {
                            transform_id: t_id.clone(),
                            side_input_id: tag,
                            elem_coder_id,
                            value_coder_id,
                            windowing,
                        })
                    })
            })
            .collect();

        if metas.is_empty() {
            return None;
        }

        let by_transform_and_tag = metas.iter().enumerate().fold(
            FastHashMap::<String, FastHashMap<String, usize>>::default(),
            |mut acc, (index, m)| {
                acc.entry(m.transform_id.clone())
                    .or_default()
                    .insert(m.side_input_id.clone(), index);
                acc
            },
        );
        let by_tag = metas
            .iter()
            .enumerate()
            .map(|(index, m)| (m.side_input_id.clone(), index))
            .collect();

        Some(Self {
            metas,
            by_transform_and_tag,
            by_tag,
            coders,
        })
    }

    /// The side input that `tag` names, with its index in `metas`. A read in a transform
    /// looks first among the side inputs of that transform.
    fn resolve(
        &self,
        transform_id: Option<&str>,
        tag: &str,
    ) -> Result<(usize, &SideInputMeta), SideInputError> {
        transform_id
            .and_then(|curr_t| self.by_transform_and_tag.get(curr_t)?.get(tag))
            .or_else(|| self.by_tag.get(tag))
            .and_then(|&index| Some((index, self.metas.get(index)?)))
            .ok_or_else(|| SideInputError::MissingReader {
                tag: tag.to_string(),
            })
    }
}

/// Reads the side inputs of one bundle from the runner, and caches their pages.
pub struct FnApiSideInputReader {
    specs: Arc<SideInputSpecs>,
    channel: Option<StateChannel>,
    /// The runner cache token of each side input, in the order of the specs. Empty if the
    /// runner sent no tokens.
    cache_tokens: Vec<Option<Bytes>>,
    /// Pages of side inputs without a cache token. They live for one bundle.
    bundle_pages: StateCache,
    /// Pages of side inputs with a cache token. They live across bundles.
    shared_pages: Option<Arc<StateCache>>,
}

impl FnApiSideInputReader {
    /// A reader for one bundle. Without `channel`, every read returns an error.
    pub fn new(specs: Arc<SideInputSpecs>, channel: Option<StateChannel>) -> Self {
        Self {
            specs,
            channel,
            cache_tokens: Vec::new(),
            bundle_pages: StateCache::new(SIDE_INPUT_CACHE_MAX_BYTES),
            shared_pages: None,
        }
    }

    /// Keeps the pages of side inputs with a token in `tokens` in `cache`, for later bundles.
    pub fn with_shared_cache(
        mut self,
        cache: Arc<StateCache>,
        tokens: &FastHashMap<(String, String), Bytes>,
    ) -> Self {
        self.cache_tokens = self
            .specs
            .metas
            .iter()
            .map(|meta| {
                tokens
                    .get(&(meta.transform_id.clone(), meta.side_input_id.clone()))
                    .cloned()
            })
            .collect();
        self.shared_pages = Some(cache);
        self
    }

    /// Starts a lazy read of one side input cell, from the cache or from the runner.
    fn read_cell<'a>(
        &'a self,
        tag: &str,
        index: usize,
        cell: StateCellKey,
        state_key_fn: fn(&StateCellKey) -> StateKey,
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
        let (store, cache_token) = match (&self.shared_pages, self.cache_tokens.get(index)) {
            (Some(shared), Some(Some(token))) => (shared.as_ref(), token.clone()),
            _ => (&self.bundle_pages, Bytes::new()),
        };
        let pages = CachedPageIterator {
            store,
            cache_token,
            channel,
            state_key_fn,
            cell: Arc::new(cell),
            next_token: Some(Bytes::new()),
            tag: Arc::clone(&tag),
        };
        Ok(Box::new(ElementFramingIterator {
            pages,
            framing: Framing::new(coder_id, &self.specs.coders),
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
    state_key_fn: fn(&StateCellKey) -> StateKey,
    cell: Arc<StateCellKey>,
    /// The token of the next page; `None` after the last page.
    next_token: Option<Bytes>,
    tag: Arc<str>,
}

impl Iterator for CachedPageIterator<'_> {
    type Item = Result<Bytes, SideInputError>;

    fn next(&mut self) -> Option<Self::Item> {
        let page_token = self.next_token.take()?;
        let part = CachedPart::SideInputPage(page_token.clone());
        let (data, next_token) = match self.store.get(&crate::state::cache::CacheKeyRef {
            token: &self.cache_token,
            cell: self.cell.parts(),
            part: &part,
        }
            as &dyn crate::state::cache::CacheKeyParts)
        {
            Some(CachedValue::Page { data, next_token }) => (data, next_token),
            // Only pages are stored under a page key, so another value is a miss too.
            Some(CachedValue::Bag(_) | CachedValue::Map(_)) | None => {
                let state_key = (self.state_key_fn)(&self.cell);
                let (data, next_token) =
                    match self.channel.fetch_page(&state_key, page_token.to_vec()) {
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
                    crate::state::cache::CacheKey {
                        token: self.cache_token.clone(),
                        cell: Arc::clone(&self.cell),
                        part,
                    },
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
    Coder { coder_id: &'a str, plan: SkipPlan },
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
            Self::Coder {
                coder_id,
                plan: SkipPlan::new(coder_id, coders),
            }
        }
    }

    /// The first element of `data`: the range of its bytes, and the number of bytes it uses.
    /// An error means that `data` does not hold a complete element.
    fn first_element(&self, data: &[u8]) -> Result<(Range<usize>, usize), String> {
        let mut cursor = data;
        match self {
            Self::LengthPrefixed => {
                let len = beam::coders::VarIntCoder::decode_varint(&mut cursor)
                    .map_err(|e| format!("Failed to read length prefix: {e}"))?;
                let start = data.len() - cursor.len();
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
            Self::Coder { coder_id, plan } => plan
                .skip(&mut cursor, true)
                .map(|()| {
                    let end = data.len() - cursor.len();
                    (0..end, end)
                })
                .map_err(|e| format!("Failed to frame element with coder '{coder_id}': {e}")),
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

    fn window_mapping(&self, tag: &str) -> Result<&SideInputWindowing, SideInputError> {
        self.window_mapping_for_transform("", tag)
    }

    fn window_mapping_for_transform(
        &self,
        transform_id: &str,
        tag: &str,
    ) -> Result<&SideInputWindowing, SideInputError> {
        let (_, meta) = self
            .specs
            .resolve((!transform_id.is_empty()).then_some(transform_id), tag)?;
        meta.windowing
            .as_ref()
            .map_err(|reason| SideInputError::Windowing {
                tag: tag.to_string(),
                reason: reason.clone(),
            })
    }

    fn get_iterable_for_transform<'a>(
        &'a self,
        transform_id: &str,
        tag: &str,
        window_bytes: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        let (index, meta) = self
            .specs
            .resolve((!transform_id.is_empty()).then_some(transform_id), tag)?;
        let cell = StateCellKey::new(&meta.transform_id, &meta.side_input_id, window_bytes, &[]);
        self.read_cell(
            tag,
            index,
            cell,
            StateCellKey::iterable_side_input_key,
            &meta.elem_coder_id,
        )
    }

    fn get_multimap_for_transform<'a>(
        &'a self,
        transform_id: &str,
        tag: &str,
        window_bytes: &[u8],
        key_bytes: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        let (index, meta) = self
            .specs
            .resolve((!transform_id.is_empty()).then_some(transform_id), tag)?;
        let cell = StateCellKey::new(
            &meta.transform_id,
            &meta.side_input_id,
            window_bytes,
            key_bytes,
        );
        self.read_cell(
            tag,
            index,
            cell,
            StateCellKey::multimap_side_input_key,
            &meta.value_coder_id,
        )
    }
}

pub(crate) fn split_concatenated_elements(
    data: &[u8],
    coder_id: &str,
    coders: &HashMap<String, Coder>,
) -> Result<Vec<Vec<u8>>, String> {
    let mut cursor = data;
    let is_length_prefix = coders
        .get(coder_id)
        .and_then(|c| c.spec.as_ref())
        .map(|s| s.urn.as_str() == "beam:coder:length_prefix:v1")
        .unwrap_or(false);
    let plan = SkipPlan::new(coder_id, coders);

    std::iter::from_fn(|| {
        let start = data.len() - cursor.len();
        (start < data.len()).then(|| {
            if is_length_prefix {
                let raw_len = beam::coders::VarIntCoder::decode_varint(&mut cursor)
                    .map_err(|e| format!("Failed to read length prefix: {e}"))?;
                let len = usize::try_from(raw_len)
                    .map_err(|_| format!("Length prefix {raw_len} is invalid"))?;
                beam::coders::take_bytes(&mut cursor, len)
                    .map(<[u8]>::to_vec)
                    .map_err(|_| format!("Length prefix {len} exceeds data length {}", data.len()))
            } else {
                plan.skip(&mut cursor, true)
                    .map_err(|e| {
                        format!("Failed to frame side input element with coder '{coder_id}': {e}")
                    })
                    .map(|()| data[start..data.len() - cursor.len()].to_vec())
            }
        })
    })
    .collect()
}
