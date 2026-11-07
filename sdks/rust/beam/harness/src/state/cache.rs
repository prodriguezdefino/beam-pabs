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

//! The state cache that bundles share.
//!
//! Data read under a cache token stays valid while the runner sends the same token. Old
//! tokens age out of the cache.

use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use beam::internals::StateBag;
use beam_model::fn_execution::ProcessBundleRequest;
use beam_model::fn_execution::process_bundle_request::cache_token;
use bytes::Bytes;
use moka::notification::RemovalCause;
use moka::sync::Cache;

use super::side_input_cache::CellKind;
use super::tables::{FastHashMap, FastHashSet, StateCellKey, StateCellKeyParts};

/// The default maximum size of the [`StateCache`], in bytes.
pub const DEFAULT_STATE_CACHE_BYTES: usize = 100 << 20;

/// A key or a value that knows how many bytes it holds.
pub(crate) trait Weight {
    fn weight(&self) -> usize;
}

/// Estimated per-entry overhead in bytes, apart from key and value payload bytes.
const ENTRY_OVERHEAD_BYTES: usize = 512;

const COUNTER_STRIPES: usize = 16;

/// Cache-line-aligned hit and miss counters so concurrent reads do not contend on one line.
#[repr(align(128))]
#[derive(Default)]
struct CounterStripe {
    hits: AtomicU64,
    misses: AtomicU64,
}

fn stripe_index() -> usize {
    static NEXT_STRIPE: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static STRIPE: usize =
            NEXT_STRIPE.fetch_add(1, Ordering::Relaxed) & (COUNTER_STRIPES - 1);
    }
    STRIPE.with(|&slot| slot)
}

/// The size and the counts of a cache, for the status page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CacheStats {
    pub entries: usize,
    pub bytes: usize,
    pub max_bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

/// The part of a state cell that a cache entry holds.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CachedPart {
    /// The elements of a bag or value cell.
    Bag,
    /// The keys and the known entries of a map or set cell.
    Map,
    /// One page of a side input read of one kind. The bytes are the continuation token of
    /// the page. An iterable cell and a multimap cell with an empty key have the same cell
    /// key, so the kind keeps their pages apart.
    SideInputPage(CellKind, Bytes),
}

/// The key of a [`StateCache`] entry: the runner token, the cell and the part of the cell.
#[derive(Clone, Debug)]
pub(crate) struct CacheKey {
    pub(crate) token: Bytes,
    pub(crate) cell: Arc<StateCellKey>,
    pub(crate) part: CachedPart,
}

pub(crate) trait CacheKeyParts {
    fn token(&self) -> &[u8];
    fn cell(&self) -> super::tables::StateCellRef<'_>;
    fn part(&self) -> &CachedPart;
}

impl CacheKeyParts for CacheKey {
    #[inline]
    fn token(&self) -> &[u8] {
        &self.token
    }
    #[inline]
    fn cell(&self) -> super::tables::StateCellRef<'_> {
        self.cell.parts()
    }
    #[inline]
    fn part(&self) -> &CachedPart {
        &self.part
    }
}

impl<'a> std::borrow::Borrow<dyn CacheKeyParts + 'a> for CacheKey {
    fn borrow(&self) -> &(dyn CacheKeyParts + 'a) {
        self
    }
}

impl Hash for dyn CacheKeyParts + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.token().hash(state);
        self.cell().hash(state);
        self.part().hash(state);
    }
}

impl PartialEq for dyn CacheKeyParts + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.token() == other.token() && self.cell() == other.cell() && self.part() == other.part()
    }
}

impl Eq for dyn CacheKeyParts + '_ {}

impl Hash for CacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        CacheKeyParts::token(self).hash(state);
        CacheKeyParts::cell(self).hash(state);
        CacheKeyParts::part(self).hash(state);
    }
}

impl PartialEq for CacheKey {
    fn eq(&self, other: &Self) -> bool {
        CacheKeyParts::token(self) == CacheKeyParts::token(other)
            && CacheKeyParts::cell(self) == CacheKeyParts::cell(other)
            && CacheKeyParts::part(self) == CacheKeyParts::part(other)
    }
}

impl Eq for CacheKey {}

impl Weight for CacheKey {
    fn weight(&self) -> usize {
        let (transform_id, state_id, window, key) = self.cell.parts();
        let part = match &self.part {
            CachedPart::Bag | CachedPart::Map => 0,
            CachedPart::SideInputPage(_, token) => token.len(),
        };
        self.token.len() + transform_id.len() + state_id.len() + window.len() + key.len() + part
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CacheKeyRef<'a> {
    pub(crate) token: &'a [u8],
    pub(crate) cell: super::tables::StateCellRef<'a>,
    pub(crate) part: &'a CachedPart,
}

impl CacheKeyParts for CacheKeyRef<'_> {
    #[inline]
    fn token(&self) -> &[u8] {
        self.token
    }
    #[inline]
    fn cell(&self) -> super::tables::StateCellRef<'_> {
        self.cell
    }
    #[inline]
    fn part(&self) -> &CachedPart {
        self.part
    }
}

/// The known content of a map or set cell.
#[derive(Clone, Debug, Default)]
pub(crate) struct MapSnapshot {
    /// All keys of the cell, when they are known.
    pub(crate) keys: Option<FastHashSet<Vec<u8>>>,
    /// The values of the keys that were read or written. An empty list is an absent key.
    pub(crate) entries: FastHashMap<Vec<u8>, Vec<Vec<u8>>>,
}

impl MapSnapshot {
    fn weight(&self) -> usize {
        let keys: usize = self.keys.iter().flatten().map(Vec::len).sum();
        let entries: usize = self
            .entries
            .iter()
            .map(|(key, values)| key.len() + values.iter().map(Vec::len).sum::<usize>())
            .sum();
        keys + entries
    }
}

/// A cached value. Reference-counted storage lets a bundle hold the value without a copy.
#[derive(Clone, Debug)]
pub(crate) enum CachedValue {
    Bag(StateBag),
    Map(Arc<MapSnapshot>),
    /// A side input page and the token of the next page.
    Page {
        data: Bytes,
        next_token: Option<Bytes>,
    },
}

impl Weight for CachedValue {
    fn weight(&self) -> usize {
        match self {
            Self::Bag(bag) => bag.byte_len() + bag.len() * size_of::<Range<usize>>(),
            Self::Map(snapshot) => snapshot.weight(),
            Self::Page { data, next_token } => {
                data.len() + next_token.as_ref().map_or(0, Bytes::len)
            }
        }
    }
}

/// The cache of runner state that bundles share, valid per runner cache token.
pub struct StateCache {
    entries: Cache<CacheKey, CachedValue, foldhash::fast::RandomState>,
    max_bytes: usize,
    counters: Box<[CounterStripe; COUNTER_STRIPES]>,
    evictions: Arc<AtomicU64>,
}

impl StateCache {
    /// A cache that keeps at most `max_bytes` of state.
    pub fn new(max_bytes: usize) -> Self {
        let evictions = Arc::new(AtomicU64::new(0));
        let listener_evictions = Arc::clone(&evictions);
        let entries = Cache::builder()
            .max_capacity(max_bytes as u64)
            .weigher(|key: &CacheKey, value: &CachedValue| -> u32 {
                let weight = ENTRY_OVERHEAD_BYTES
                    .saturating_add(key.weight())
                    .saturating_add(value.weight());
                u32::try_from(weight).unwrap_or(u32::MAX)
            })
            .eviction_listener(move |_key, _value, cause| {
                if cause == RemovalCause::Size {
                    listener_evictions.fetch_add(1, Ordering::Relaxed);
                }
            })
            .build_with_hasher(foldhash::fast::RandomState::default());
        Self {
            entries,
            max_bytes,
            counters: Box::new(std::array::from_fn(|_| CounterStripe::default())),
            evictions,
        }
    }

    /// The bytes that the cache holds.
    pub fn total_bytes(&self) -> usize {
        self.entries.run_pending_tasks();
        self.entries.weighted_size() as usize
    }

    /// The size and the hit, miss and eviction counts of the cache.
    pub fn stats(&self) -> CacheStats {
        self.entries.run_pending_tasks();
        let (hits, misses) = self.counters.iter().fold((0u64, 0u64), |(h, m), stripe| {
            (
                h + stripe.hits.load(Ordering::Relaxed),
                m + stripe.misses.load(Ordering::Relaxed),
            )
        });
        CacheStats {
            entries: self.entries.entry_count() as usize,
            bytes: self.entries.weighted_size() as usize,
            max_bytes: self.max_bytes,
            hits,
            misses,
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    /// Reads a cached bag cell for the `state_cache` benchmark.
    pub fn get_bag(
        &self,
        token: &[u8],
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Option<StateBag> {
        let key_ref = CacheKeyRef {
            token,
            cell: (transform_id, state_id, window, key),
            part: &CachedPart::Bag,
        };
        match self.get(&key_ref as &dyn CacheKeyParts) {
            Some(CachedValue::Bag(bag)) => Some(bag),
            Some(CachedValue::Map(_) | CachedValue::Page { .. }) | None => None,
        }
    }

    /// Inserts a cached bag cell for the `state_cache` benchmark.
    pub fn insert_bag(
        &self,
        token: Bytes,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        bag: StateBag,
    ) {
        self.insert(
            CacheKey {
                token,
                cell: Arc::new(StateCellKey::new(transform_id, state_id, window, key)),
                part: CachedPart::Bag,
            },
            CachedValue::Bag(bag),
        );
    }

    pub(crate) fn get<Q>(&self, key: &Q) -> Option<CachedValue>
    where
        CacheKey: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let stripe = &self.counters[stripe_index()];
        let found = self.entries.get(key);
        if found.is_some() {
            stripe.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            stripe.misses.fetch_add(1, Ordering::Relaxed);
        }
        found
    }

    pub(crate) fn insert(&self, key: CacheKey, value: CachedValue) {
        let weight = ENTRY_OVERHEAD_BYTES
            .saturating_add(key.weight())
            .saturating_add(value.weight());
        if weight > self.max_bytes {
            self.entries.invalidate(&key);
            return;
        }
        self.entries.insert(key, value);
    }

    pub(crate) fn remove<Q>(&self, key: &Q)
    where
        CacheKey: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.entries.invalidate(key);
    }
}

impl Default for StateCache {
    fn default() -> Self {
        Self::new(DEFAULT_STATE_CACHE_BYTES)
    }
}

/// The cache tokens of one `ProcessBundleRequest`.
#[derive(Clone, Debug, Default)]
pub struct CacheTokens {
    /// The token for user state, if the runner sent one.
    pub user_state_token: Option<Bytes>,
    /// The token per `(transform id, side input id)`.
    pub side_input_tokens: FastHashMap<(String, String), Bytes>,
}

impl CacheTokens {
    /// Reads the cache tokens of `request`.
    pub fn from_request(request: &ProcessBundleRequest) -> Self {
        let (user_state_token, side_input_tokens) = request.cache_tokens.iter().fold(
            (None, FastHashMap::default()),
            |(user_state, mut side_inputs), token| match &token.r#type {
                Some(cache_token::Type::UserState(_)) => {
                    (Some(Bytes::from(token.token.clone())), side_inputs)
                }
                Some(cache_token::Type::SideInput(side_input)) => {
                    side_inputs.insert(
                        (
                            side_input.transform_id.clone(),
                            side_input.side_input_id.clone(),
                        ),
                        Bytes::from(token.token.clone()),
                    );
                    (user_state, side_inputs)
                }
                None => (user_state, side_inputs),
            },
        );
        Self {
            user_state_token,
            side_input_tokens,
        }
    }
}
