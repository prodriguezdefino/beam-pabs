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

use std::borrow::{Borrow, Cow};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use beam::internals::{
    SideInputElements, SideInputError, SideInputReader, SingletonBytes, extract_side_inputs,
};
use beam::values::{SideInputWindowing, URN_SIDE_INPUT_MULTIMAP};
use beam_model::fn_execution::{ProcessBundleDescriptor, StateKey};
use beam_model::pipeline::Coder;
use bytes::{Buf, Bytes, BytesMut};

use super::cache::{CacheKey, CacheKeyParts, CacheKeyRef, CachedPart, CachedValue, StateCache};
use super::channel::StateChannel;
use super::framing::RunnerFraming;
use super::tables::{FastHashMap, StateCellKey, StateCellKeyParts, StateCellRef};

/// Maximum bytes of untokenized side input pages that one bundle keeps.
const SIDE_INPUT_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

/// The number of recent single-page cells kept per side input in one bundle. The pages of
/// all memos of a bundle count against [`SIDE_INPUT_CACHE_MAX_BYTES`].
const MEMO_SLOTS: usize = 8;

/// Identifies one side input by `(transform_id, side_input_id)`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct TransformTagKey {
    transform_id: String,
    tag: String,
}

/// Borrowed view shared by [`TransformTagKey`] and `(&str, &str)` so map lookups do not
/// allocate.
trait TransformTagParts {
    fn parts(&self) -> (&str, &str);
}

impl TransformTagParts for TransformTagKey {
    #[inline]
    fn parts(&self) -> (&str, &str) {
        (&self.transform_id, &self.tag)
    }
}

impl TransformTagParts for (&str, &str) {
    #[inline]
    fn parts(&self) -> (&str, &str) {
        *self
    }
}

impl<'a> Borrow<dyn TransformTagParts + 'a> for TransformTagKey {
    fn borrow(&self) -> &(dyn TransformTagParts + 'a) {
        self
    }
}

impl Hash for TransformTagKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

impl Hash for dyn TransformTagParts + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

impl PartialEq for dyn TransformTagParts + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}

impl Eq for dyn TransformTagParts + '_ {}

struct SideInputMeta {
    transform_id: String,
    side_input_id: String,
    /// The framing of the elements of an iterable read.
    elem_framing: RunnerFraming,
    /// The framing of a multimap key. `None` for a side input that is not a multimap.
    key_framing: Option<RunnerFraming>,
    /// The framing of the values of a multimap read.
    value_framing: RunnerFraming,
    /// The `window_mapping_fn` of the side input, decoded once for the descriptor.
    windowing: Result<SideInputWindowing, String>,
}

/// The side inputs that the transforms of a descriptor read, with their framings. A
/// processor builds it once and its bundles share it.
pub struct SideInputSpecs {
    metas: Vec<SideInputMeta>,
    /// Maps a `(transform id, tag)` pair to an index in `metas`.
    by_transform_and_tag: FastHashMap<TransformTagKey, usize>,
    /// Maps a tag to an index in `metas`, for a read that does not name its transform.
    by_tag: FastHashMap<String, usize>,
}

impl SideInputSpecs {
    /// The side inputs of `descriptor`, or `None` if no transform reads one.
    pub fn from_descriptor(
        descriptor: &ProcessBundleDescriptor,
        coders: &Arc<HashMap<String, Coder>>,
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
                            .map_or("", |p| p.coder_id.as_str());
                        let components = descriptor
                            .coders
                            .get(elem_coder_id)
                            .map_or(&[][..], |c| c.component_coder_ids.as_slice());
                        let value_coder_id =
                            components.get(1).map_or(elem_coder_id, String::as_str);
                        let is_multimap = side_input
                            .access_pattern
                            .as_ref()
                            .is_some_and(|p| p.urn == URN_SIDE_INPUT_MULTIMAP);
                        let key_framing = components
                            .first()
                            .filter(|_| is_multimap)
                            .map(|key_coder_id| RunnerFraming::new(key_coder_id, coders));
                        let windowing = side_input
                            .window_mapping_fn
                            .as_ref()
                            .ok_or_else(|| "the descriptor has no window_mapping_fn".to_string())
                            .and_then(SideInputWindowing::from_proto);
                        Some(SideInputMeta {
                            transform_id: t_id.clone(),
                            side_input_id: tag,
                            elem_framing: RunnerFraming::new(elem_coder_id, coders),
                            key_framing,
                            value_framing: RunnerFraming::new(value_coder_id, coders),
                            windowing,
                        })
                    })
            })
            .collect();

        if metas.is_empty() {
            return None;
        }

        let by_transform_and_tag = metas
            .iter()
            .enumerate()
            .map(|(index, m)| {
                (
                    TransformTagKey {
                        transform_id: m.transform_id.clone(),
                        tag: m.side_input_id.clone(),
                    },
                    index,
                )
            })
            .collect();
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
            .and_then(|curr_t| {
                let key = (curr_t, tag);
                self.by_transform_and_tag
                    .get(&key as &dyn TransformTagParts)
            })
            .or_else(|| self.by_tag.get(tag))
            .and_then(|&index| Some((index, self.metas.get(index)?)))
            .ok_or_else(|| SideInputError::MissingReader {
                tag: tag.to_string(),
            })
    }
}

/// The kind of a side input cell. The iterable, multimap and multimap-keys cells of one side
/// input and window are different cells.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CellKind {
    Iterable,
    Multimap,
    MultimapKeys,
}

impl CellKind {
    fn state_key_fn(self) -> fn(&StateCellKey) -> StateKey {
        match self {
            Self::Iterable => StateCellKey::iterable_side_input_key,
            Self::Multimap => StateCellKey::multimap_side_input_key,
            Self::MultimapKeys => StateCellKey::multimap_keys_side_input_key,
        }
    }
}

/// One cached single-page cell that a bundle read for one side input. A `DoFn` reads the
/// side input for each element across a small set of active windows. A hit skips the lock
/// and the LRU update of the [`StateCache`]. A cell of more pages goes through the cache
/// only, so that the cache limit holds for large side inputs.
struct PageMemo {
    kind: CellKind,
    window: Vec<u8>,
    key: Vec<u8>,
    data: Bytes,
}

/// Ring buffer of up to [`MEMO_SLOTS`] single-page side input cells.
#[derive(Default)]
struct MemoRing {
    entries: Vec<PageMemo>,
    /// Next slot index to overwrite once `entries.len() == MEMO_SLOTS`.
    next_evict: usize,
}

/// The memo of one side input. Only the thread of the bundle uses it, so `try_lock` does not
/// fail. If it fails, the read uses the [`StateCache`] and stays correct.
#[derive(Default)]
struct MemoSlot(Mutex<MemoRing>);

impl MemoSlot {
    fn recall(&self, kind: CellKind, window: &[u8], key: &[u8]) -> Option<Bytes> {
        let ring = self.0.try_lock().ok()?;
        ring.entries
            .iter()
            .find(|m| m.kind == kind && m.window == window && m.key == key)
            // A clone of `Bytes` only increments a reference count.
            .map(|m| m.data.clone())
    }

    /// Keeps `data` in the slot of the cell, or in place of the oldest cell when the ring is
    /// full. Skips `data` when the memos of the bundle would pass `budget`.
    fn remember(
        &self,
        kind: CellKind,
        window: &[u8],
        key: &[u8],
        data: &Bytes,
        budget: &MemoBudget,
    ) {
        let Ok(mut ring) = self.0.try_lock() else {
            return;
        };
        let existing = ring
            .entries
            .iter()
            .position(|m| m.kind == kind && m.window == window && m.key == key);
        let slot = match existing {
            Some(slot) => Some(slot),
            None if ring.entries.len() < MEMO_SLOTS => None,
            None => Some(ring.next_evict),
        };
        let freed = slot
            .and_then(|slot| ring.entries.get(slot))
            .map_or(0, |entry| entry.data.len());
        if !budget.replace(freed, data.len()) {
            return;
        }
        match slot {
            Some(slot) => {
                if existing.is_none() {
                    ring.next_evict = (slot + 1) % MEMO_SLOTS;
                }
                if let Some(entry) = ring.entries.get_mut(slot) {
                    entry.kind = kind;
                    entry.window.clear();
                    entry.window.extend_from_slice(window);
                    entry.key.clear();
                    entry.key.extend_from_slice(key);
                    // A clone of `Bytes` only increments a reference count.
                    entry.data = data.clone();
                }
            }
            None => ring.entries.push(PageMemo {
                kind,
                window: window.to_vec(),
                key: key.to_vec(),
                // A clone of `Bytes` only increments a reference count.
                data: data.clone(),
            }),
        }
    }
}

/// The page bytes that the memos of one bundle hold, at most [`SIDE_INPUT_CACHE_MAX_BYTES`].
#[derive(Debug, Default)]
struct MemoBudget(AtomicUsize);

impl MemoBudget {
    /// Replaces `freed` held bytes with `added` bytes. Returns false, and changes nothing,
    /// when the total would pass the limit.
    fn replace(&self, freed: usize, added: usize) -> bool {
        self.0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |held| {
                let total = held.saturating_sub(freed).saturating_add(added);
                (total <= SIDE_INPUT_CACHE_MAX_BYTES).then_some(total)
            })
            .is_ok()
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
    bundle_pages: OnceLock<StateCache>,
    /// Pages of side inputs with a cache token. They live across bundles.
    shared_pages: Option<Arc<StateCache>>,
    /// The memo of each side input, in the order of the specs. It lives for one bundle,
    /// as the reader does.
    memos: Box<[MemoSlot]>,
    memo_bytes: MemoBudget,
}

impl FnApiSideInputReader {
    /// A reader for one bundle. Without `channel`, every read returns an error.
    pub fn new(specs: Arc<SideInputSpecs>, channel: Option<StateChannel>) -> Self {
        let memos = specs.metas.iter().map(|_| MemoSlot::default()).collect();
        Self {
            specs,
            channel,
            cache_tokens: Vec::new(),
            bundle_pages: OnceLock::new(),
            shared_pages: None,
            memos,
            memo_bytes: MemoBudget::default(),
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

    /// Builds a [`CachedPageIterator`] ready to load pages of `kind` for side input `index`.
    fn page_iterator<'a>(
        &'a self,
        index: usize,
        tag: &'a str,
        kind: CellKind,
    ) -> Result<CachedPageIterator<'a>, SideInputError> {
        let channel = self
            .channel
            .as_ref()
            .ok_or_else(|| SideInputError::Runner {
                tag: tag.to_string(),
                reason: "ProcessBundleDescriptor has no state_api_service_descriptor".to_string(),
            })?;
        let (store, cache_token) = match (&self.shared_pages, self.cache_tokens.get(index)) {
            (Some(shared), Some(Some(token))) => (shared.as_ref(), token.clone()),
            _ => (
                self.bundle_pages
                    .get_or_init(|| StateCache::new(SIDE_INPUT_CACHE_MAX_BYTES)),
                Bytes::new(),
            ),
        };
        Ok(CachedPageIterator {
            store,
            cache_token,
            channel,
            kind,
            tag,
            first: None,
            rest: None,
        })
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
        framing: &'a RunnerFraming,
    ) -> Result<SideInputElements<'a>, SideInputError> {
        let tag = meta.side_input_id.as_str();
        let (transform_id, state_id, window, key) = cell;
        let memo = self.memos.get(index);
        if let Some(data) = memo.and_then(|memo| memo.recall(kind, window, key)) {
            return Ok(Box::new(SinglePageFramingIterator {
                framing,
                buffer: data,
                tag,
            }));
        }
        let mut pages = self.page_iterator(index, tag, kind)?;
        let mut owned = None;
        match pages.load(cell, &mut owned, &Bytes::new()) {
            Ok((data, None)) => {
                if let Some(memo) = memo {
                    memo.remember(kind, window, key, &data, &self.memo_bytes);
                }
                Ok(Box::new(SinglePageFramingIterator {
                    framing,
                    buffer: data,
                    tag,
                }))
            }
            Ok((data, Some(token))) => {
                let cell = owned.unwrap_or_else(|| {
                    Arc::new(StateCellKey::new(transform_id, state_id, window, key))
                });
                pages.first = Some(Ok(data));
                pages.rest = Some((cell, token));
                Ok(Box::new(ElementFramingIterator {
                    pages,
                    framing,
                    buffer: Bytes::new(),
                    tag,
                }))
            }
            Err(err) => {
                pages.first = Some(Err(err));
                Ok(Box::new(ElementFramingIterator {
                    pages,
                    framing,
                    buffer: Bytes::new(),
                    tag,
                }))
            }
        }
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
        page_token: &Bytes,
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
        let loaded = self.load(cell.parts(), &mut owned, &page_token);
        Some(loaded.map(|(data, next_token)| {
            self.rest = next_token.map(|token| (cell, token));
            data
        }))
    }
}

/// The elements of a side input cell that fits in one page.
struct SinglePageFramingIterator<'a> {
    framing: &'a RunnerFraming,
    /// Bytes of the page that are not yet returned.
    buffer: Bytes,
    tag: &'a str,
}

impl Iterator for SinglePageFramingIterator<'_> {
    type Item = Result<Bytes, SideInputError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            return None;
        }
        match self.framing.first_value(&self.buffer) {
            Ok((element, used)) => {
                let item = self.buffer.slice(element);
                self.buffer.advance(used);
                Some(
                    self.framing
                        .unframe_rows(item)
                        .map_err(|reason| SideInputError::Framing {
                            tag: self.tag.to_string(),
                            reason,
                        }),
                )
            }
            Err(reason) => {
                self.buffer = Bytes::new();
                Some(Err(SideInputError::Framing {
                    tag: self.tag.to_string(),
                    reason,
                }))
            }
        }
    }
}

/// Parses one complete side input page as a singleton without boxing an iterator.
fn parse_singleton_page(
    page: &Bytes,
    framing: &RunnerFraming,
    tag: &str,
) -> Result<SingletonBytes, SideInputError> {
    if page.is_empty() {
        return Ok(SingletonBytes::Empty);
    }
    let (element, used) = framing
        .first_value(page)
        .map_err(|reason| SideInputError::Framing {
            tag: tag.to_string(),
            reason,
        })?;
    let item = framing
        .unframe_rows(page.slice(element))
        .map_err(|reason| SideInputError::Framing {
            tag: tag.to_string(),
            reason,
        })?;
    if used == page.len() {
        return Ok(SingletonBytes::One(item));
    }
    let rest = page.slice(used..);
    let (second, _) = framing
        .first_value(&rest)
        .map_err(|reason| SideInputError::Framing {
            tag: tag.to_string(),
            reason,
        })?;
    let _ = framing
        .unframe_rows(rest.slice(second))
        .map_err(|reason| SideInputError::Framing {
            tag: tag.to_string(),
            reason,
        })?;
    Ok(SingletonBytes::Multiple)
}

/// The elements of a side input cell. An element can span pages.
struct ElementFramingIterator<'a> {
    pages: CachedPageIterator<'a>,
    framing: &'a RunnerFraming,
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
                match self.framing.first_value(&self.buffer) {
                    Ok((element, used)) => {
                        let item = self.buffer.slice(element);
                        self.buffer.advance(used);
                        return Some(self.framing.unframe_rows(item).map_err(|reason| {
                            SideInputError::Framing {
                                tag: self.tag.to_string(),
                                reason,
                            }
                        }));
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

    fn get_singleton(
        &self,
        tag: &str,
        window_bytes: &[u8],
    ) -> Result<SingletonBytes, SideInputError> {
        self.get_singleton_for_transform("", tag, window_bytes)
    }

    fn get_multimap<'a>(
        &'a self,
        tag: &str,
        window_bytes: &[u8],
        key_bytes: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        self.get_multimap_for_transform("", tag, window_bytes, key_bytes)
    }

    fn get_multimap_keys<'a>(
        &'a self,
        tag: &str,
        window_bytes: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        self.get_multimap_keys_for_transform("", tag, window_bytes)
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

    fn get_singleton_for_transform(
        &self,
        transform_id: &str,
        tag: &str,
        window_bytes: &[u8],
    ) -> Result<SingletonBytes, SideInputError> {
        let (index, meta) = self
            .specs
            .resolve((!transform_id.is_empty()).then_some(transform_id), tag)?;
        let side_tag = meta.side_input_id.as_str();
        let memo = self.memos.get(index);
        if let Some(page) = memo.and_then(|memo| memo.recall(CellKind::Iterable, window_bytes, &[]))
        {
            return parse_singleton_page(&page, &meta.elem_framing, side_tag);
        }
        let mut pages = self.page_iterator(index, side_tag, CellKind::Iterable)?;
        let cell: StateCellRef<'_> = (&meta.transform_id, &meta.side_input_id, window_bytes, &[]);
        let mut owned = None;
        let (page, next_token) = pages.load(cell, &mut owned, &Bytes::new())?;
        let Some(token) = next_token else {
            if let Some(memo) = memo {
                memo.remember(
                    CellKind::Iterable,
                    window_bytes,
                    &[],
                    &page,
                    &self.memo_bytes,
                );
            }
            return parse_singleton_page(&page, &meta.elem_framing, side_tag);
        };
        let cell = owned.unwrap_or_else(|| {
            Arc::new(StateCellKey::new(
                &meta.transform_id,
                &meta.side_input_id,
                window_bytes,
                &[],
            ))
        });
        pages.first = Some(Ok(page));
        pages.rest = Some((cell, token));
        let mut iter = ElementFramingIterator {
            pages,
            framing: &meta.elem_framing,
            buffer: Bytes::new(),
            tag: side_tag,
        };
        let Some(first) = iter.next().transpose()? else {
            return Ok(SingletonBytes::Empty);
        };
        match iter.next().transpose()? {
            Some(_) => Ok(SingletonBytes::Multiple),
            None => Ok(SingletonBytes::One(first)),
        }
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
        let framed_key = match &meta.key_framing {
            Some(framing) => framing.frame(Cow::Borrowed(key_bytes)).map_err(|reason| {
                SideInputError::Framing {
                    tag: tag.to_string(),
                    reason: format!("Failed to frame the multimap key: {reason}"),
                }
            })?,
            None => Cow::Borrowed(key_bytes),
        };
        self.read_cell(
            index,
            meta,
            (
                &meta.transform_id,
                &meta.side_input_id,
                window_bytes,
                &framed_key,
            ),
            CellKind::Multimap,
            &meta.value_framing,
        )
    }

    fn get_multimap_keys_for_transform<'a>(
        &'a self,
        transform_id: &str,
        tag: &str,
        window_bytes: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        let (index, meta) = self
            .specs
            .resolve((!transform_id.is_empty()).then_some(transform_id), tag)?;
        let key_framing = meta
            .key_framing
            .as_ref()
            .ok_or_else(|| SideInputError::Framing {
                tag: tag.to_string(),
                reason: "the side input is not a multimap, so it has no keys".to_string(),
            })?;
        self.read_cell(
            index,
            meta,
            (&meta.transform_id, &meta.side_input_id, window_bytes, &[]),
            CellKind::MultimapKeys,
            key_framing,
        )
    }
}
