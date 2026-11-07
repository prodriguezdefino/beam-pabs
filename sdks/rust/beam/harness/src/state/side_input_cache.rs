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

use super::cache::{CacheKey, CacheKeyParts, CacheKeyRef, CachedPart, CachedValue, StateCache};
use super::channel::StateChannel;
use super::tables::{FastHashMap, StateCellKey, StateCellKeyParts, StateCellRef};
use bytes::{Buf, Bytes, BytesMut};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};

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
    /// How the elements of an iterable read are delimited.
    elem_framing: Framing,
    /// How the values of a multimap read are delimited.
    value_framing: Framing,
    /// The `window_mapping_fn` of the side input, decoded once for the descriptor.
    windowing: Result<SideInputWindowing, String>,
}

/// The side inputs that the transforms of a descriptor read, with their framings. A
/// processor builds it once and its bundles share it.
pub struct SideInputSpecs {
    metas: Vec<SideInputMeta>,
    /// Maps a transform id and a tag to an index in `metas`.
    by_transform_and_tag: FastHashMap<String, FastHashMap<String, usize>>,
    /// Maps a tag to an index in `metas`, for a read that does not name its transform.
    by_tag: FastHashMap<String, usize>,
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
                let coders = &coders;
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
                            elem_framing: Framing::new(&elem_coder_id, coders),
                            value_framing: Framing::new(&value_coder_id, coders),
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

/// The kind of a side input cell. An iterable cell and a multimap cell of the same side
/// input and window are different cells.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CellKind {
    Iterable,
    Multimap,
}

impl CellKind {
    fn state_key_fn(self) -> fn(&StateCellKey) -> StateKey {
        match self {
            Self::Iterable => StateCellKey::iterable_side_input_key,
            Self::Multimap => StateCellKey::multimap_side_input_key,
        }
    }
}

/// The last cell of one page that a bundle read for one side input. A `DoFn` reads the side
/// input for each element, and consecutive elements are mostly in one window. A hit skips
/// the lock and the LRU update of the [`StateCache`]. A cell of more pages goes through the
/// cache only, so that the cache limit holds for large side inputs.
struct PageMemo {
    kind: CellKind,
    window: Vec<u8>,
    key: Vec<u8>,
    data: Bytes,
}

/// The memo of one side input. Only the thread of the bundle uses it, so `try_lock` does not
/// fail. If it fails, the read uses the [`StateCache`] and stays correct.
#[derive(Default)]
struct MemoSlot(Mutex<Option<PageMemo>>);

impl MemoSlot {
    fn recall(&self, kind: CellKind, window: &[u8], key: &[u8]) -> Option<Bytes> {
        let memo = self.0.try_lock().ok()?;
        memo.as_ref()
            .filter(|m| m.kind == kind && m.window == window && m.key == key)
            // A clone of `Bytes` only increments a reference count.
            .map(|m| m.data.clone())
    }

    fn remember(&self, kind: CellKind, window: &[u8], key: &[u8], data: &Bytes) {
        if let Ok(mut memo) = self.0.try_lock() {
            *memo = Some(PageMemo {
                kind,
                window: window.to_vec(),
                key: key.to_vec(),
                data: data.clone(),
            });
        }
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
    /// The memo of each side input, in the order of the specs. It lives for one bundle,
    /// as the reader does.
    memos: Box<[MemoSlot]>,
}

impl FnApiSideInputReader {
    /// A reader for one bundle. Without `channel`, every read returns an error.
    pub fn new(specs: Arc<SideInputSpecs>, channel: Option<StateChannel>) -> Self {
        let memos = specs.metas.iter().map(|_| MemoSlot::default()).collect();
        Self {
            specs,
            channel,
            cache_tokens: Vec::new(),
            bundle_pages: StateCache::new(SIDE_INPUT_CACHE_MAX_BYTES),
            shared_pages: None,
            memos,
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

    /// Starts a read of one side input cell. The first page comes from the memo, the cache
    /// or the runner at once, so a cell of one cached page needs no owned key. Later pages
    /// come when the iterator reaches them.
    fn read_cell<'a>(
        &'a self,
        index: usize,
        meta: &'a SideInputMeta,
        cell: StateCellRef<'_>,
        kind: CellKind,
        framing: &'a Framing,
    ) -> Result<SideInputElements<'a>, SideInputError> {
        let tag = meta.side_input_id.as_str();
        let channel = self
            .channel
            .as_ref()
            .ok_or_else(|| SideInputError::Runner {
                tag: tag.to_string(),
                reason: "ProcessBundleDescriptor has no state_api_service_descriptor".to_string(),
            })?;
        let (store, cache_token) = match (&self.shared_pages, self.cache_tokens.get(index)) {
            (Some(shared), Some(Some(token))) => (shared.as_ref(), token.clone()),
            _ => (&self.bundle_pages, Bytes::new()),
        };
        let mut pages = CachedPageIterator {
            store,
            cache_token,
            channel,
            kind,
            tag,
            first: None,
            rest: None,
        };
        let (transform_id, state_id, window, key) = cell;
        let memo = self.memos.get(index);
        let first = match memo.and_then(|memo| memo.recall(kind, window, key)) {
            Some(data) => Ok(data),
            None => {
                let mut owned = None;
                pages
                    .load(cell, &mut owned, Bytes::new())
                    .map(|(data, next_token)| {
                        pages.rest = match (next_token, memo) {
                            (Some(token), _) => {
                                let cell = owned.unwrap_or_else(|| {
                                    Arc::new(StateCellKey::new(transform_id, state_id, window, key))
                                });
                                Some((cell, token))
                            }
                            (None, Some(memo)) => {
                                memo.remember(kind, window, key, &data);
                                None
                            }
                            (None, None) => None,
                        };
                        data
                    })
            }
        };
        pages.first = Some(first);
        Ok(Box::new(ElementFramingIterator {
            pages,
            framing,
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
    kind: CellKind,
    tag: &'a str,
    /// The first page, which the read loads when it starts.
    first: Option<Result<Bytes, SideInputError>>,
    /// The owned key of the cell and the token of the next page; `None` after the last page.
    rest: Option<(Arc<StateCellKey>, Bytes)>,
}

impl CachedPageIterator<'_> {
    /// The page of `cell` at `page_token` and the token of the next page, from the cache or
    /// from the runner. A page from the runner goes into the cache under the owned key in
    /// `owned`, which this call makes if it is `None`.
    fn load(
        &self,
        cell: StateCellRef<'_>,
        owned: &mut Option<Arc<StateCellKey>>,
        page_token: Bytes,
    ) -> Result<(Bytes, Option<Bytes>), SideInputError> {
        let part = CachedPart::SideInputPage(self.kind, page_token.clone());
        let key = CacheKeyRef {
            token: &self.cache_token,
            cell,
            part: &part,
        };
        // Only pages are stored under a page key, so another value is a miss too.
        if let Some(CachedValue::Page { data, next_token }) =
            self.store.get(&key as &dyn CacheKeyParts)
        {
            return Ok((data, next_token));
        }
        let (transform_id, state_id, window, user_key) = cell;
        let owned = owned.get_or_insert_with(|| {
            Arc::new(StateCellKey::new(transform_id, state_id, window, user_key))
        });
        let state_key = (self.kind.state_key_fn())(owned);
        let (data, next_token) = self
            .channel
            .fetch_page(&state_key, page_token.to_vec())
            .map_err(|reason| SideInputError::Runner {
                tag: self.tag.to_string(),
                reason,
            })?;
        let data = Bytes::from(data);
        let next_token = next_token.map(Bytes::from);
        self.store.insert(
            CacheKey {
                token: self.cache_token.clone(),
                cell: Arc::clone(owned),
                part,
            },
            CachedValue::Page {
                data: data.clone(),
                next_token: next_token.clone(),
            },
        );
        Ok((data, next_token))
    }
}

impl Iterator for CachedPageIterator<'_> {
    type Item = Result<Bytes, SideInputError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(first) = self.first.take() {
            return Some(first);
        }
        let (cell, page_token) = self.rest.take()?;
        let mut owned = Some(Arc::clone(&cell));
        let loaded = self.load(cell.parts(), &mut owned, page_token);
        Some(loaded.map(|(data, next_token)| {
            self.rest = next_token.map(|token| (cell, token));
            data
        }))
    }
}

/// How the elements of a side input are delimited in the page bytes.
enum Framing {
    /// Each element is a varint length followed by that many bytes.
    LengthPrefixed,
    /// Each element is one value of the coder, read to find its end.
    Coder { coder_id: String, plan: SkipPlan },
}

impl Framing {
    fn new(coder_id: &str, coders: &HashMap<String, Coder>) -> Self {
        let is_length_prefix = coders
            .get(coder_id)
            .and_then(|c| c.spec.as_ref())
            .is_some_and(|s| s.urn == "beam:coder:length_prefix:v1");
        if is_length_prefix {
            Self::LengthPrefixed
        } else {
            Self::Coder {
                coder_id: coder_id.to_string(),
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
    framing: &'a Framing,
    /// Bytes of the current page that are not yet returned.
    buffer: Bytes,
    tag: &'a str,
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
        self.read_cell(
            index,
            meta,
            (&meta.transform_id, &meta.side_input_id, window_bytes, &[]),
            CellKind::Iterable,
            &meta.elem_framing,
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
        self.read_cell(
            index,
            meta,
            (
                &meta.transform_id,
                &meta.side_input_id,
                window_bytes,
                key_bytes,
            ),
            CellKind::Multimap,
            &meta.value_framing,
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
