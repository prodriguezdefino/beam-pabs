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

//! Deferred commit of user state.
//!
//! [`BundleUserState`] manages the state reads and mutations of one bundle. It keeps
//! mutations in memory and commits them to the runner when the bundle completes. The
//! first read of a cell fetches its value from the runner. The bundle mutations are then
//! merged with that value, so a read always sees the writes before it.
//!
//! # Semantics
//!
//! * **Read-your-writes:** In one bundle, each mutation (`put`, `remove`, `clear`) is
//!   visible to all later reads (`get`, `keys`, `entries`, `contains`, `read`). A read does
//!   not need a round trip to the runner.
//! * **Value replacement:** `MapState::put(k, v)` replaces the earlier value of `k`. Values
//!   do not accumulate. `MultimapState::put(k, v)` adds `v` after the earlier values of `k`.
//! * **Single-key removal:** `MapState::remove(&k)`, `MultimapState::remove(&k)` and
//!   `SetState::remove(&elem)` remove the entry. A present key disappears from later reads.
//!   Removing an absent key has no effect and does not create a phantom key.
//! * **Whole-cell clear:** `clear()` of a map, multimap or set cell empties the cell.
//!   Later reads return empty collections (`keys()` returns `[]`).
//! * **Cross-bundle persistence:** On a conforming runner, all state that a bundle commits
//!   is available to later bundles.
//!
//! # Runner conformance
//!
//! Behavior across bundles depends on how the runner implements the Fn API state
//! protocol. Dataflow persists and isolates all cross-bundle mutations. Prism keeps a
//! removed map key as an empty entry, so later bundles still see the key in `keys()`.
//!
//! The ValidatesRunner tests `map_and_set_state_clear`, `map_and_set_state_remove` and
//! `multimap_state_clear_remove` in `runner-tests` check these mutations across bundles.
//!

use std::sync::{Arc, Mutex};

use beam::internals::{UserStateBackend, UserStateError};
use model::fn_execution::{StateKey, state_key};

use crate::state::cache::{CachedPart, CachedValue};
use crate::state::tables::{FastHashMap, StateCellKey, StateCellKeyParts, StateCellRef, with_cell};
use crate::state::{StateChannel, split_concatenated_elements};
use crate::user_state::cache::UserStateCache;
use crate::user_state::cells::{MapCell, MapStateCoders, OrderedListCell, StateCell};
use crate::user_state::scoped::ScopedUserState;
use crate::user_state::specs::UserStateSpecs;

type CellTable<V> = FastHashMap<StateCellKey, V>;

pub(super) struct BundleUserStateInner {
    pub(super) channel: StateChannel,
    /// Present when the runner sent a user state token.
    pub(super) cache: Option<UserStateCache>,
    pub(super) specs: Arc<UserStateSpecs>,
    cells: Mutex<CellTable<StateCell>>,
    map_cells: Mutex<CellTable<MapCell>>,
    ordered_list_cells: Mutex<CellTable<OrderedListCell>>,
}

impl BundleUserStateInner {
    pub(super) fn lock_cells(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, CellTable<StateCell>>, UserStateError> {
        self.cells
            .lock()
            .map_err(|e| UserStateError::Other(format!("Failed to lock bundle state cells: {e}")))
    }

    pub(super) fn lock_map_cells(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, CellTable<MapCell>>, UserStateError> {
        self.map_cells.lock().map_err(|e| {
            UserStateError::Other(format!("Failed to lock bundle map state cells: {e}"))
        })
    }

    /// A new bag cell, with the cached content when there is one.
    pub(super) fn new_bag_cell(&self, key: StateCellRef<'_>) -> StateCell {
        let cached = self
            .cached(key, CachedPart::Bag)
            .and_then(|value| match value {
                CachedValue::Bag(elements) => Some(elements),
                CachedValue::Map(_) | CachedValue::Page { .. } => None,
            });
        StateCell::with_initial(cached)
    }

    /// A new map cell, which starts from the cached content if there is one.
    pub(super) fn new_map_cell(&self, key: StateCellRef<'_>) -> MapCell {
        self.cached(key, CachedPart::Map)
            .and_then(|value| match value {
                CachedValue::Map(snapshot) => Some(MapCell::seeded(snapshot)),
                CachedValue::Bag(_) | CachedValue::Page { .. } => None,
            })
            .unwrap_or_default()
    }

    pub(super) fn cached(&self, key: StateCellRef<'_>, part: CachedPart) -> Option<CachedValue> {
        let UserStateCache { cache, token } = self.cache.as_ref()?;
        cache.get(&crate::state::cache::CacheKeyRef {
            token,
            cell: key,
            part: &part,
        } as &dyn crate::state::cache::CacheKeyParts)
    }

    /// A new bag cell for `key`, or `None` if the bundle has it. The cell lock is free meanwhile.
    pub(super) fn bag_seed(
        &self,
        key: StateCellRef<'_>,
    ) -> Result<Option<StateCell>, UserStateError> {
        if self
            .lock_cells()?
            .contains_key(&key as &dyn StateCellKeyParts)
        {
            return Ok(None);
        }
        Ok(Some(self.new_bag_cell(key)))
    }

    /// [`Self::bag_seed`] for a map cell.
    pub(super) fn map_seed(
        &self,
        key: StateCellRef<'_>,
    ) -> Result<Option<MapCell>, UserStateError> {
        if self
            .lock_map_cells()?
            .contains_key(&key as &dyn StateCellKeyParts)
        {
            return Ok(None);
        }
        Ok(Some(self.new_map_cell(key)))
    }

    pub(super) fn get_state_coder(
        &self,
        transform_id: &str,
        state_id: &str,
    ) -> Result<&str, UserStateError> {
        self.specs
            .state_coders
            .get(transform_id)
            .and_then(|states| states.get(state_id))
            .map(|s| s.as_str())
            .ok_or_else(|| {
                UserStateError::Other(format!(
                    "Unknown state '{state_id}' on transform '{transform_id}'"
                ))
            })
    }

    pub(super) fn get_map_state_coders(
        &self,
        transform_id: &str,
        state_id: &str,
    ) -> Result<&MapStateCoders, UserStateError> {
        self.specs
            .map_state_coders
            .get(transform_id)
            .and_then(|states| states.get(state_id))
            .ok_or_else(|| {
                UserStateError::Other(format!(
                    "Unknown map state '{state_id}' on transform '{transform_id}'"
                ))
            })
    }

    pub(super) fn get_ordered_list_state_coder(
        &self,
        transform_id: &str,
        state_id: &str,
    ) -> Result<&str, UserStateError> {
        self.specs
            .ordered_list_state_coders
            .get(transform_id)
            .and_then(|states| states.get(state_id))
            .map(|s| s.as_str())
            .ok_or_else(|| {
                UserStateError::Other(format!(
                    "Unknown ordered list state '{state_id}' on transform '{transform_id}'"
                ))
            })
    }

    pub(super) fn lock_ordered_list_cells(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, CellTable<OrderedListCell>>, UserStateError> {
        self.ordered_list_cells.lock().map_err(|e| {
            UserStateError::Other(format!(
                "Failed to lock bundle ordered list state cells: {e}"
            ))
        })
    }
}

/// Manages the user state of all transforms in one bundle.
#[derive(Clone)]
pub struct BundleUserState {
    pub(super) inner: Arc<BundleUserStateInner>,
}

impl BundleUserState {
    /// The user state of one bundle on `channel`. A bundle with a user state cache token
    /// passes `cache`, so that it reads and fills the shared cache.
    pub fn new(
        specs: Arc<UserStateSpecs>,
        channel: StateChannel,
        cache: Option<UserStateCache>,
    ) -> Self {
        Self {
            inner: Arc::new(BundleUserStateInner {
                channel,
                cache,
                specs,
                cells: Mutex::default(),
                map_cells: Mutex::default(),
                ordered_list_cells: Mutex::default(),
            }),
        }
    }

    /// Returns a [`UserStateBackend`] scoped to the given transform id.
    pub fn scoped(&self, transform_id: &str) -> Arc<dyn UserStateBackend> {
        Arc::new(ScopedUserState {
            bundle_state: self.clone(),
            transform_id: transform_id.to_string(),
        })
    }

    pub(super) fn get_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        let cell_key = (transform_id, state_id, window, key);
        let seed = self.inner.bag_seed(cell_key)?;
        {
            let mut guard = self.inner.lock_cells()?;
            let cached = with_cell(
                &mut guard,
                cell_key,
                move || seed.unwrap_or_default(),
                |cell: &mut StateCell| cell.read_if_present(),
            );
            if let Some(elements) = cached {
                return Ok(elements);
            }
        }
        let state_key = StateKey {
            r#type: Some(state_key::Type::BagUserState(state_key::BagUserState {
                transform_id: transform_id.to_string(),
                user_state_id: state_id.to_string(),
                window: window.to_vec(),
                key: key.to_vec(),
            })),
        };
        let raw_bytes = self
            .inner
            .channel
            .get(state_key)
            .map_err(UserStateError::Runner)?;
        let coder_id = self.inner.get_state_coder(transform_id, state_id)?;
        let fetched = split_concatenated_elements(&raw_bytes, coder_id, &self.inner.specs.coders)
            .map_err(UserStateError::Format)?;
        let mut guard = self.inner.lock_cells()?;
        let elements = with_cell(
            &mut guard,
            cell_key,
            StateCell::default,
            |cell: &mut StateCell| cell.set_initial_and_read(fetched),
        );
        Ok(elements)
    }

    pub(super) fn append_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        let cell_key = (transform_id, state_id, window, key);
        let seed = self.inner.bag_seed(cell_key)?;
        let mut guard = self.inner.lock_cells()?;
        with_cell(
            &mut guard,
            cell_key,
            move || seed.unwrap_or_default(),
            |cell: &mut StateCell| {
                cell.append(element);
            },
        );
        Ok(())
    }

    pub(super) fn clear_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<(), UserStateError> {
        let cell_key = (transform_id, state_id, window, key);
        let seed = self.inner.bag_seed(cell_key)?;
        let mut guard = self.inner.lock_cells()?;
        with_cell(
            &mut guard,
            cell_key,
            move || seed.unwrap_or_default(),
            StateCell::clear,
        );
        Ok(())
    }
}
