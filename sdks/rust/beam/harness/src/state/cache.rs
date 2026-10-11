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

use std::collections::VecDeque;
use std::hash::{Hash, Hasher};

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use model::fn_execution::ProcessBundleRequest;
use model::fn_execution::process_bundle_request::cache_token;

use super::tables::{FastHashMap, FastHashSet, StateCellKey, StateCellKeyParts};

/// The default maximum size of the [`StateCache`], in bytes.
pub const DEFAULT_STATE_CACHE_BYTES: usize = 100 << 20;

/// A value that knows how many bytes it holds.
pub(crate) trait Weight {
    fn weight(&self) -> usize;
}

/// A map with a maximum size in bytes. The least recently used entries go first.
pub(crate) struct WeightedCache<K, V> {
    entries: FastHashMap<K, Entry<K, V>>,
    /// Keys from least to most recently used. Only the position with the entry stamp counts.
    order: VecDeque<(K, u64)>,
    next_stamp: u64,
    total_bytes: usize,
    max_bytes: usize,
    hits: u64,
    misses: u64,
    evictions: u64,
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

struct Entry<K, V> {
    key: K,
    value: V,
    weight: usize,
    stamp: u64,
}

impl<K: Clone + Eq + Hash, V: Clone + Weight> WeightedCache<K, V> {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            entries: FastHashMap::default(),
            order: VecDeque::new(),
            next_stamp: 0,
            total_bytes: 0,
            max_bytes,
            hits: 0,
            misses: 0,
            evictions: 0,
        }
    }

    /// The value of `key`, which becomes the most recently used entry.
    pub(crate) fn get<Q>(&mut self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let stamp = self.stamp();
        let Some(entry) = self.entries.get_mut(key) else {
            self.misses += 1;
            return None;
        };
        entry.stamp = stamp;
        let k = entry.key.clone();
        let value = entry.value.clone();
        self.hits += 1;
        self.order.push_back((k, stamp));
        self.compact();
        Some(value)
    }

    /// Stores `value` under `key`, unless it is larger than the whole cache.
    pub(crate) fn insert(&mut self, key: K, value: V) {
        let weight = value.weight();
        if weight > self.max_bytes {
            self.remove(&key);
            return;
        }
        let stamp = self.stamp();
        let replaced = self.entries.insert(
            key.clone(),
            Entry {
                key: key.clone(),
                value,
                weight,
                stamp,
            },
        );
        self.total_bytes = self.total_bytes - replaced.map_or(0, |old| old.weight) + weight;
        self.order.push_back((key, stamp));
        self.evict();
        self.compact();
    }

    pub(crate) fn remove<Q>(&mut self, key: &Q)
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Some(old) = self.entries.remove(key) {
            self.total_bytes -= old.weight;
        }
    }

    pub(crate) fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    pub(crate) fn stats(&self) -> CacheStats {
        CacheStats {
            entries: self.entries.len(),
            bytes: self.total_bytes,
            max_bytes: self.max_bytes,
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
        }
    }

    fn stamp(&mut self) -> u64 {
        self.next_stamp += 1;
        self.next_stamp
    }

    fn evict(&mut self) {
        while self.total_bytes > self.max_bytes {
            let Some((key, stamp)) = self.order.pop_front() else {
                return;
            };
            if self.entries.get(&key).is_some_and(|e| e.stamp == stamp) {
                self.remove(&key);
                self.evictions += 1;
            }
        }
    }

    /// Drops the outdated queue positions when they outnumber the entries.
    fn compact(&mut self) {
        if self.order.len() > 2 * self.entries.len() + 64 {
            let entries = &self.entries;
            self.order
                .retain(|(key, stamp)| entries.get(key).is_some_and(|e| e.stamp == *stamp));
        }
    }
}

/// The part of a state cell that a cache entry holds.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum CachedPart {
    /// The elements of a bag or value cell.
    Bag,
    /// The keys and the known entries of a map or set cell.
    Map,
    /// One page of a side input. The bytes are the continuation token of the page.
    SideInputPage(Bytes),
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

/// A cached value. The `Arc`s let a bundle hold the value without a copy.
#[derive(Clone, Debug)]
pub(crate) enum CachedValue {
    Bag(Arc<[Vec<u8>]>),
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
            Self::Bag(elements) => elements.iter().map(Vec::len).sum(),
            Self::Map(snapshot) => snapshot.weight(),
            Self::Page { data, next_token } => {
                data.len() + next_token.as_ref().map_or(0, Bytes::len)
            }
        }
    }
}

/// The cache of runner state that bundles share, valid per runner cache token.
pub struct StateCache {
    entries: Mutex<WeightedCache<CacheKey, CachedValue>>,
}

impl StateCache {
    /// A cache that keeps at most `max_bytes` of state.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            entries: Mutex::new(WeightedCache::new(max_bytes)),
        }
    }

    /// The bytes that the cache holds.
    pub fn total_bytes(&self) -> usize {
        self.lock().total_bytes()
    }

    /// The size and the hit, miss and eviction counts of the cache.
    pub fn stats(&self) -> CacheStats {
        self.lock().stats()
    }

    pub(crate) fn get<Q>(&self, key: &Q) -> Option<CachedValue>
    where
        CacheKey: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.lock().get(key)
    }

    pub(crate) fn insert(&self, key: CacheKey, value: CachedValue) {
        self.lock().insert(key, value);
    }

    pub(crate) fn remove<Q>(&self, key: &Q)
    where
        CacheKey: std::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.lock().remove(key);
    }

    /// Locks the entries. A panic elsewhere cannot leave them invalid, so poison is ignored.
    fn lock(&self) -> MutexGuard<'_, WeightedCache<CacheKey, CachedValue>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
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
