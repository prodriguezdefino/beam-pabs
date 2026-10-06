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
//!   do not accumulate.
//! * **Single-key removal:** `MapState::remove(&k)` and `SetState::remove(&elem)` remove
//!   the entry. A present key disappears from later reads. Removing an absent key has no
//!   effect and does not create a phantom key.
//! * **Whole-cell clear:** `MapState::clear()` and `SetState::clear()` empty the cell.
//!   Later reads return empty collections (`keys()` returns `[]`).
//! * **Cross-bundle persistence:** On a conforming runner, all state that a bundle commits
//!   is available to later bundles.
//!
//! # Runner conformance
//!
//! Behavior across bundles depends on how the runner implements the Fn API state
//! protocol. Dataflow persists and isolates all cross-bundle mutations. Prism keeps a
//! removed map key as an empty entry, so later bundles still see the key.
//!
//! For comparison tables, job logs on Dataflow and Prism, and an end-to-end test suite,
//! see `examples/state_conformance`.
//!

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use prost::Message;

use beam::internals::{UserStateBackend, UserStateError};
use beam::pipeline::URN_PAR_DO;
use model::fn_execution::{ProcessBundleDescriptor, StateKey, state_key};
use model::pipeline::{self as proto, Coder};

use bytes::Bytes;

use crate::state::cache::{CacheKey, CachedPart, CachedValue, MapSnapshot, StateCache};
use crate::state::channel::PendingReply;
use crate::state::tables::{FastHashMap, StateCellKey, StateCellKeyParts, StateCellRef, with_cell};
use crate::state::{StateChannel, split_concatenated_elements};

use crate::user_state::cells::{
    BagCommit, MapCell, MapCommit, MapStateCoders, OrderedListCell, StateCell, StateMutation,
    ordered_list_mutations, split_ordered_list_elements,
};

type CellTable<V> = FastHashMap<StateCellKey, V>;

/// The state cache that bundles share, and the user state token of one bundle.
#[derive(Clone)]
pub struct UserStateCache {
    pub cache: Arc<StateCache>,
    pub token: Bytes,
}

struct BundleUserStateInner {
    channel: StateChannel,
    /// Present when the runner sent a user state token.
    cache: Option<UserStateCache>,
    state_coders: HashMap<String, HashMap<String, String>>,
    map_state_coders: HashMap<String, HashMap<String, MapStateCoders>>,
    ordered_list_state_coders: HashMap<String, HashMap<String, String>>,
    coders: Arc<HashMap<String, Coder>>,
    cells: Mutex<CellTable<StateCell>>,
    map_cells: Mutex<CellTable<MapCell>>,
    ordered_list_cells: Mutex<CellTable<OrderedListCell>>,
}

impl BundleUserStateInner {
    fn lock_cells(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, CellTable<StateCell>>, UserStateError> {
        self.cells
            .lock()
            .map_err(|e| UserStateError::Other(format!("Failed to lock bundle state cells: {e}")))
    }

    fn lock_map_cells(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, CellTable<MapCell>>, UserStateError> {
        self.map_cells.lock().map_err(|e| {
            UserStateError::Other(format!("Failed to lock bundle map state cells: {e}"))
        })
    }

    /// A new bag cell, with the cached content when there is one.
    fn new_bag_cell(&self, key: StateCellRef<'_>) -> StateCell {
        let cached = self
            .cached(key, CachedPart::Bag)
            .and_then(|value| match value {
                CachedValue::Bag(elements) => Some(elements),
                CachedValue::Map(_) | CachedValue::Page { .. } => None,
            });
        StateCell::with_initial(cached)
    }

    /// A new map cell, which starts from the cached content if there is one.
    fn new_map_cell(&self, key: StateCellRef<'_>) -> MapCell {
        self.cached(key, CachedPart::Map)
            .and_then(|value| match value {
                CachedValue::Map(snapshot) => Some(MapCell::seeded(snapshot)),
                CachedValue::Bag(_) | CachedValue::Page { .. } => None,
            })
            .unwrap_or_default()
    }

    fn cached(&self, key: StateCellRef<'_>, part: CachedPart) -> Option<CachedValue> {
        let UserStateCache { cache, token } = self.cache.as_ref()?;
        cache.get(&crate::state::cache::CacheKeyRef {
            token,
            cell: key,
            part: &part,
        } as &dyn crate::state::cache::CacheKeyParts)
    }

    /// A new bag cell for `key`, or `None` if the bundle has it. The cell lock is free meanwhile.
    fn bag_seed(&self, key: StateCellRef<'_>) -> Result<Option<StateCell>, UserStateError> {
        if self
            .lock_cells()?
            .contains_key(&key as &dyn StateCellKeyParts)
        {
            return Ok(None);
        }
        Ok(Some(self.new_bag_cell(key)))
    }

    /// [`Self::bag_seed`] for a map cell.
    fn map_seed(&self, key: StateCellRef<'_>) -> Result<Option<MapCell>, UserStateError> {
        if self
            .lock_map_cells()?
            .contains_key(&key as &dyn StateCellKeyParts)
        {
            return Ok(None);
        }
        Ok(Some(self.new_map_cell(key)))
    }

    fn get_state_coder(&self, transform_id: &str, state_id: &str) -> Result<&str, UserStateError> {
        self.state_coders
            .get(transform_id)
            .and_then(|states| states.get(state_id))
            .map(|s| s.as_str())
            .ok_or_else(|| {
                UserStateError::Other(format!(
                    "Unknown state '{state_id}' on transform '{transform_id}'"
                ))
            })
    }

    fn get_map_state_coders(
        &self,
        transform_id: &str,
        state_id: &str,
    ) -> Result<&MapStateCoders, UserStateError> {
        self.map_state_coders
            .get(transform_id)
            .and_then(|states| states.get(state_id))
            .ok_or_else(|| {
                UserStateError::Other(format!(
                    "Unknown map state '{state_id}' on transform '{transform_id}'"
                ))
            })
    }

    fn get_ordered_list_state_coder(
        &self,
        transform_id: &str,
        state_id: &str,
    ) -> Result<&str, UserStateError> {
        self.ordered_list_state_coders
            .get(transform_id)
            .and_then(|states| states.get(state_id))
            .map(|s| s.as_str())
            .ok_or_else(|| {
                UserStateError::Other(format!(
                    "Unknown ordered list state '{state_id}' on transform '{transform_id}'"
                ))
            })
    }

    fn lock_ordered_list_cells(
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
    inner: Arc<BundleUserStateInner>,
}

impl BundleUserState {
    /// Creates a `BundleUserState` for the bundle if a transform declares state specs.
    pub fn from_descriptor(
        instruction_id: &str,
        descriptor: &ProcessBundleDescriptor,
        worker_id: &str,
    ) -> Option<Self> {
        let channel = StateChannel::from_descriptor(instruction_id, descriptor, worker_id)?;
        Self::from_channel_with_cache(
            descriptor,
            channel,
            Arc::new(descriptor.coders.clone()),
            None,
        )
    }

    /// Creates a `BundleUserState` on an existing `StateChannel` if state specs exist.
    pub fn from_channel(
        descriptor: &ProcessBundleDescriptor,
        channel: StateChannel,
    ) -> Option<Self> {
        Self::from_channel_with_cache(
            descriptor,
            channel,
            Arc::new(descriptor.coders.clone()),
            None,
        )
    }

    /// [`Self::from_channel`] for a bundle that reads and fills `cache`.
    pub fn from_channel_with_cache(
        descriptor: &ProcessBundleDescriptor,
        channel: StateChannel,
        coders: Arc<HashMap<String, Coder>>,
        cache: Option<UserStateCache>,
    ) -> Option<Self> {
        let pardo_specs = descriptor.transforms.iter().filter_map(|(t_id, t)| {
            let spec = t
                .spec
                .as_ref()
                .filter(|s| s.urn == URN_PAR_DO && !s.payload.is_empty())?;
            let pardo = proto::ParDoPayload::decode(spec.payload.as_slice()).ok()?;
            Some((t_id, pardo.state_specs))
        });

        let (state_coders, map_state_coders, ordered_list_state_coders) = pardo_specs.fold(
            (
                HashMap::<String, HashMap<String, String>>::new(),
                HashMap::<String, HashMap<String, MapStateCoders>>::new(),
                HashMap::<String, HashMap<String, String>>::new(),
            ),
            |(mut state_coders, mut map_state_coders, mut ordered_list_state_coders),
             (t_id, state_specs)| {
                for (state_id, spec) in state_specs {
                    match spec.spec {
                        Some(proto::state_spec::Spec::BagSpec(b)) => {
                            state_coders
                                .entry(t_id.clone())
                                .or_default()
                                .insert(state_id, b.element_coder_id);
                        }
                        Some(proto::state_spec::Spec::ReadModifyWriteSpec(r)) => {
                            state_coders
                                .entry(t_id.clone())
                                .or_default()
                                .insert(state_id, r.coder_id);
                        }
                        Some(proto::state_spec::Spec::MapSpec(m)) => {
                            map_state_coders.entry(t_id.clone()).or_default().insert(
                                state_id,
                                MapStateCoders {
                                    key_coder_id: m.key_coder_id,
                                    value_coder_id: m.value_coder_id,
                                },
                            );
                        }
                        Some(proto::state_spec::Spec::SetSpec(s)) => {
                            map_state_coders.entry(t_id.clone()).or_default().insert(
                                state_id,
                                MapStateCoders {
                                    key_coder_id: s.element_coder_id.clone(),
                                    value_coder_id: s.element_coder_id,
                                },
                            );
                        }
                        Some(proto::state_spec::Spec::MultimapSpec(m)) => {
                            map_state_coders.entry(t_id.clone()).or_default().insert(
                                state_id,
                                MapStateCoders {
                                    key_coder_id: m.key_coder_id,
                                    value_coder_id: m.value_coder_id,
                                },
                            );
                        }
                        Some(proto::state_spec::Spec::OrderedListSpec(o)) => {
                            ordered_list_state_coders
                                .entry(t_id.clone())
                                .or_default()
                                .insert(state_id, o.element_coder_id);
                        }
                        _ => {}
                    }
                }
                (state_coders, map_state_coders, ordered_list_state_coders)
            },
        );

        if state_coders.is_empty()
            && map_state_coders.is_empty()
            && ordered_list_state_coders.is_empty()
        {
            return None;
        }

        Some(Self {
            inner: Arc::new(BundleUserStateInner {
                channel,
                cache,
                state_coders,
                map_state_coders,
                ordered_list_state_coders,
                coders,
                cells: Mutex::default(),
                map_cells: Mutex::default(),
                ordered_list_cells: Mutex::default(),
            }),
        })
    }

    /// Returns a [`UserStateBackend`] scoped to the given transform id.
    pub fn scoped(&self, transform_id: &str) -> Arc<dyn UserStateBackend> {
        Arc::new(ScopedUserState {
            bundle_state: self.clone(),
            transform_id: transform_id.to_string(),
        })
    }

    /// Commits all buffered state clears and appends to the runner through the State API.
    ///
    /// The bundle sends all mutations before it waits for the replies. After all replies
    /// succeed, the shared cache takes the content of each cell.
    ///
    /// # Errors
    ///
    /// Returns an error when a lock is poisoned or a state request fails.
    pub fn commit(&self) -> Result<(), String> {
        let bags: Vec<(StateCellKey, BagCommit)> = self
            .inner
            .lock_cells()
            .map_err(|e| e.to_string())?
            .drain()
            .map(|(key, cell)| (key, cell.into_commit()))
            .collect();
        let maps: Vec<(StateCellKey, MapCommit, MapSnapshot)> = self
            .inner
            .lock_map_cells()
            .map_err(|e| e.to_string())?
            .drain()
            .map(|(key, cell)| {
                let (commit, snapshot) = cell.into_commit();
                (key, commit, snapshot)
            })
            .collect();
        let ordered_lists: Vec<(StateCellKey, OrderedListCell)> = self
            .inner
            .lock_ordered_list_cells()
            .map_err(|e| e.to_string())?
            .drain()
            .collect();

        let channel = &self.inner.channel;
        let (bag_sent, bag_contents): (Vec<Vec<_>>, Vec<_>) = bags
            .into_iter()
            .map(|(cell, commit)| {
                let BagCommit {
                    cleared,
                    batches,
                    content,
                } = commit;
                let mutations = bag_mutations(channel, &cell, cleared, batches)?;
                let cached = (cell, CachedPart::Bag, content.map(CachedValue::Bag));
                Ok((mutations, cached))
            })
            .collect::<Result<Vec<_>, String>>()?
            .into_iter()
            .unzip();

        let (map_sent, map_contents): (Vec<Vec<_>>, Vec<_>) = maps
            .into_iter()
            .map(|(cell, commit, snapshot)| {
                let mutations = map_mutations(channel, &cell, commit)?;
                let cached = (
                    cell,
                    CachedPart::Map,
                    Some(CachedValue::Map(Arc::new(snapshot))),
                );
                Ok((mutations, cached))
            })
            .collect::<Result<Vec<_>, String>>()?
            .into_iter()
            .unzip();

        // Ordered lists are not cached: a read asks the runner for a range each time.
        let list_sent: Vec<_> = ordered_lists
            .into_iter()
            .flat_map(|(cell, list)| ordered_list_mutations(&cell, list.into_commit()))
            .map(|(state_key, mutation)| match mutation {
                StateMutation::Clear => channel.start_clear(state_key),
                StateMutation::Append(data) => channel.start_append(state_key, data),
            })
            .collect::<Result<Vec<_>, String>>()?;

        bag_sent
            .into_iter()
            .flatten()
            .chain(map_sent.into_iter().flatten())
            .chain(list_sent)
            .try_for_each(PendingReply::finish)?;

        if let Some(UserStateCache { cache, token }) = &self.inner.cache {
            bag_contents
                .into_iter()
                .chain(map_contents)
                .for_each(|(cell, part, content)| match content {
                    Some(content) => {
                        let key = CacheKey {
                            token: token.clone(),
                            cell: Arc::new(cell),
                            part,
                        };
                        cache.insert(key, content);
                    }
                    None => {
                        cache.remove(&crate::state::cache::CacheKeyRef {
                            token,
                            cell: cell.parts(),
                            part: &part,
                        }
                            as &dyn crate::state::cache::CacheKeyParts);
                    }
                });
        }
        Ok(())
    }

    fn get_state(
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
        let fetched = split_concatenated_elements(&raw_bytes, coder_id, &self.inner.coders)
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

    fn append_state(
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

    fn clear_state(
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

    fn get_map_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        let cell_key = (transform_id, state_id, window, key);
        let seed = self.inner.map_seed(cell_key)?;
        {
            let mut guard = self.inner.lock_map_cells()?;
            let cached = with_cell(
                &mut guard,
                cell_key,
                move || seed.unwrap_or_default(),
                |cell: &mut MapCell| cell.get_if_present(map_key),
            );
            if let Some(elements) = cached {
                return Ok(elements);
            }
        }
        let state_key = StateKey {
            r#type: Some(state_key::Type::MultimapUserState(
                state_key::MultimapUserState {
                    transform_id: transform_id.to_string(),
                    user_state_id: state_id.to_string(),
                    window: window.to_vec(),
                    key: key.to_vec(),
                    map_key: map_key.to_vec(),
                },
            )),
        };
        let raw_bytes = self
            .inner
            .channel
            .get(state_key)
            .map_err(UserStateError::Runner)?;
        let fetched = if raw_bytes.is_empty() {
            Vec::new()
        } else {
            let coders = self.inner.get_map_state_coders(transform_id, state_id)?;
            split_concatenated_elements(&raw_bytes, &coders.value_coder_id, &self.inner.coders)
                .map_err(UserStateError::Format)?
        };
        let mut guard = self.inner.lock_map_cells()?;
        let elements = with_cell(
            &mut guard,
            cell_key,
            MapCell::default,
            |cell: &mut MapCell| cell.install_fetched_entry(map_key, fetched),
        );
        Ok(elements)
    }

    fn put_map_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: Vec<u8>,
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        let cell_key = (transform_id, state_id, window, key);
        let seed = self.inner.map_seed(cell_key)?;
        let mut guard = self.inner.lock_map_cells()?;
        with_cell(
            &mut guard,
            cell_key,
            move || seed.unwrap_or_default(),
            |cell: &mut MapCell| {
                cell.insert(map_key, element);
            },
        );
        Ok(())
    }

    fn remove_map_key(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<(), UserStateError> {
        let cell_key = (transform_id, state_id, window, key);
        let seed = self.inner.map_seed(cell_key)?;
        let mut guard = self.inner.lock_map_cells()?;
        with_cell(
            &mut guard,
            cell_key,
            move || seed.unwrap_or_default(),
            |cell: &mut MapCell| {
                cell.remove(map_key);
            },
        );
        Ok(())
    }

    fn clear_map_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<(), UserStateError> {
        let cell_key = (transform_id, state_id, window, key);
        let seed = self.inner.map_seed(cell_key)?;
        let mut guard = self.inner.lock_map_cells()?;
        with_cell(
            &mut guard,
            cell_key,
            move || seed.unwrap_or_default(),
            MapCell::clear,
        );
        Ok(())
    }

    fn get_map_keys(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        let cell_key = (transform_id, state_id, window, key);
        let seed = self.inner.map_seed(cell_key)?;
        {
            let mut guard = self.inner.lock_map_cells()?;
            let cached = with_cell(
                &mut guard,
                cell_key,
                move || seed.unwrap_or_default(),
                |cell: &mut MapCell| cell.keys_if_present(),
            );
            if let Some(keys) = cached {
                return Ok(keys);
            }
        }
        let state_key = StateKey {
            r#type: Some(state_key::Type::MultimapKeysUserState(
                state_key::MultimapKeysUserState {
                    transform_id: transform_id.to_string(),
                    user_state_id: state_id.to_string(),
                    window: window.to_vec(),
                    key: key.to_vec(),
                },
            )),
        };
        let raw_bytes = self
            .inner
            .channel
            .get(state_key)
            .map_err(UserStateError::Runner)?;
        let fetched = if raw_bytes.is_empty() {
            Vec::new()
        } else {
            let coders = self.inner.get_map_state_coders(transform_id, state_id)?;
            split_concatenated_elements(&raw_bytes, &coders.key_coder_id, &self.inner.coders)
                .map_err(UserStateError::Format)?
        };
        let mut guard = self.inner.lock_map_cells()?;
        let keys = with_cell(
            &mut guard,
            cell_key,
            MapCell::default,
            |cell: &mut MapCell| cell.install_fetched_keys(fetched),
        );
        Ok(keys)
    }

    fn get_ordered_list_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<Vec<(i64, Vec<u8>)>, UserStateError> {
        let _coder_id = self
            .inner
            .get_ordered_list_state_coder(transform_id, state_id)?;
        let cell_key = (transform_id, state_id, window, key);
        {
            let mut guard = self.inner.lock_ordered_list_cells()?;
            let is_cleared = with_cell(
                &mut guard,
                cell_key,
                OrderedListCell::default,
                |cell: &mut OrderedListCell| cell.is_cleared(),
            );
            if is_cleared {
                return Ok(with_cell(
                    &mut guard,
                    cell_key,
                    OrderedListCell::default,
                    |cell: &mut OrderedListCell| cell.combine_with_fetched(from, to, Vec::new()),
                ));
            }
        }
        let state_key = StateKey {
            r#type: Some(state_key::Type::OrderedListUserState(
                state_key::OrderedListUserState {
                    transform_id: transform_id.to_string(),
                    user_state_id: state_id.to_string(),
                    window: window.to_vec(),
                    key: key.to_vec(),
                    range: Some(model::fn_execution::OrderedListRange {
                        start: from,
                        end: to,
                    }),
                },
            )),
        };
        let raw_bytes = self
            .inner
            .channel
            .get(state_key)
            .map_err(UserStateError::Runner)?;
        let fetched = if raw_bytes.is_empty() {
            Vec::new()
        } else {
            split_ordered_list_elements(&raw_bytes).map_err(UserStateError::Format)?
        };
        let mut guard = self.inner.lock_ordered_list_cells()?;
        let entries = with_cell(
            &mut guard,
            cell_key,
            OrderedListCell::default,
            |cell: &mut OrderedListCell| cell.combine_with_fetched(from, to, fetched),
        );
        Ok(entries)
    }

    fn append_ordered_list_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        timestamp: i64,
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        let mut guard = self.inner.lock_ordered_list_cells()?;
        with_cell(
            &mut guard,
            (transform_id, state_id, window, key),
            OrderedListCell::default,
            |cell: &mut OrderedListCell| {
                cell.add(timestamp, element);
            },
        );
        Ok(())
    }

    fn clear_ordered_list_range(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<(), UserStateError> {
        let mut guard = self.inner.lock_ordered_list_cells()?;
        with_cell(
            &mut guard,
            (transform_id, state_id, window, key),
            OrderedListCell::default,
            |cell: &mut OrderedListCell| {
                cell.clear_range(from, to);
            },
        );
        Ok(())
    }

    fn clear_ordered_list_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<(), UserStateError> {
        let mut guard = self.inner.lock_ordered_list_cells()?;
        with_cell(
            &mut guard,
            (transform_id, state_id, window, key),
            OrderedListCell::default,
            OrderedListCell::clear,
        );
        Ok(())
    }
}

struct ScopedUserState {
    bundle_state: BundleUserState,
    transform_id: String,
}

impl UserStateBackend for ScopedUserState {
    fn get_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        self.bundle_state
            .get_state(&self.transform_id, state_id, window, key)
    }

    fn append_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        self.bundle_state
            .append_state(&self.transform_id, state_id, window, key, element)
    }

    fn clear_state(&self, state_id: &str, window: &[u8], key: &[u8]) -> Result<(), UserStateError> {
        self.bundle_state
            .clear_state(&self.transform_id, state_id, window, key)
    }

    fn get_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        self.bundle_state
            .get_map_state(&self.transform_id, state_id, window, key, map_key)
    }

    fn get_map_keys(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        self.bundle_state
            .get_map_keys(&self.transform_id, state_id, window, key)
    }

    fn put_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: Vec<u8>,
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        self.bundle_state
            .put_map_state(&self.transform_id, state_id, window, key, map_key, element)
    }

    fn remove_map_key(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<(), UserStateError> {
        self.bundle_state
            .remove_map_key(&self.transform_id, state_id, window, key, map_key)
    }

    fn clear_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<(), UserStateError> {
        self.bundle_state
            .clear_map_state(&self.transform_id, state_id, window, key)
    }

    fn get_ordered_list_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<Vec<(i64, Vec<u8>)>, UserStateError> {
        self.bundle_state.get_ordered_list_state(
            &self.transform_id,
            state_id,
            window,
            key,
            from,
            to,
        )
    }

    fn append_ordered_list_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        timestamp: i64,
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        self.bundle_state.append_ordered_list_state(
            &self.transform_id,
            state_id,
            window,
            key,
            timestamp,
            element,
        )
    }

    fn clear_ordered_list_range(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<(), UserStateError> {
        self.bundle_state.clear_ordered_list_range(
            &self.transform_id,
            state_id,
            window,
            key,
            from,
            to,
        )
    }

    fn clear_ordered_list_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<(), UserStateError> {
        self.bundle_state
            .clear_ordered_list_state(&self.transform_id, state_id, window, key)
    }
}

/// Sends the mutations of one bag cell: a clear if the bundle cleared it, then the appends.
fn bag_mutations<'c>(
    channel: &'c StateChannel,
    cell: &StateCellKey,
    cleared: bool,
    batches: Vec<Vec<u8>>,
) -> Result<Vec<PendingReply<'c>>, String> {
    let clear = cleared.then(|| channel.start_clear(cell.bag_state_key()));
    let appends = batches
        .into_iter()
        .map(|batch| channel.start_append(cell.bag_state_key(), batch));
    clear.into_iter().chain(appends).collect()
}

/// Sends the mutations of one map cell: a whole-cell clear or the clears of removed keys,
/// then one replace per written key.
fn map_mutations<'c>(
    channel: &'c StateChannel,
    cell: &StateCellKey,
    commit: MapCommit,
) -> Result<Vec<PendingReply<'c>>, String> {
    let MapCommit {
        cleared,
        cleared_keys,
        entries,
    } = commit;
    let clears: Vec<_> = match cleared {
        true => vec![channel.start_clear(cell.multimap_keys_state_key())],
        false => cleared_keys
            .into_iter()
            .map(|key| channel.start_clear(cell.multimap_entry_state_key(key)))
            .collect(),
    };
    // Without a whole-cell clear, a clear of the entry makes the append a replace.
    let writes = entries.into_iter().flat_map(|(map_key, value)| {
        let replace =
            (!cleared).then(|| channel.start_clear(cell.multimap_entry_state_key(map_key.clone())));
        let append = channel.start_append(cell.multimap_entry_state_key(map_key), value);
        replace.into_iter().chain(std::iter::once(append))
    });
    clears.into_iter().chain(writes).collect()
}
