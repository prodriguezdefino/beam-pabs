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
//! protocol. Dataflow persists and isolates all cross-bundle mutations. Prism has
//! known defects in cross-bundle multimap state. For example, it drops whole-cell
//! clears and keeps tombstones for removed keys.
//!
//! For comparison tables, job logs on Dataflow and Prism, and an end-to-end test suite,
//! see `examples/state_conformance`.
//!

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use prost::Message;

use beam::internals::UserStateReader;
use beam::pipeline::constants::URN_PAR_DO;
use model::fn_execution::{ProcessBundleDescriptor, StateKey, state_key};
use model::pipeline::{self as proto, Coder};

use crate::state::tables::{FastHashMap, StateCellKey, with_cell};
use crate::state::{StateChannel, split_concatenated_elements};

use crate::user_state::cells::{MapCell, MapStateCoders, StateCell};

type CellTable<V> = FastHashMap<StateCellKey, V>;

struct BundleUserStateInner {
    channel: StateChannel,
    state_coders: HashMap<String, HashMap<String, String>>,
    map_state_coders: HashMap<String, HashMap<String, MapStateCoders>>,
    coders: HashMap<String, Coder>,
    cells: Mutex<CellTable<StateCell>>,
    map_cells: Mutex<CellTable<MapCell>>,
}

impl BundleUserStateInner {
    #[inline]
    fn lock_cells(&self) -> Result<std::sync::MutexGuard<'_, CellTable<StateCell>>, String> {
        self.cells
            .lock()
            .map_err(|e| format!("Failed to lock bundle state cells: {e}"))
    }

    #[inline]
    fn lock_map_cells(&self) -> Result<std::sync::MutexGuard<'_, CellTable<MapCell>>, String> {
        self.map_cells
            .lock()
            .map_err(|e| format!("Failed to lock bundle map state cells: {e}"))
    }

    #[inline]
    fn get_state_coder(&self, transform_id: &str, state_id: &str) -> Result<&str, String> {
        self.state_coders
            .get(transform_id)
            .and_then(|states| states.get(state_id))
            .map(|s| s.as_str())
            .ok_or_else(|| format!("Unknown state '{state_id}' on transform '{transform_id}'"))
    }

    #[inline]
    fn get_map_state_coders(
        &self,
        transform_id: &str,
        state_id: &str,
    ) -> Result<&MapStateCoders, String> {
        self.map_state_coders
            .get(transform_id)
            .and_then(|states| states.get(state_id))
            .ok_or_else(|| format!("Unknown map state '{state_id}' on transform '{transform_id}'"))
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
        Self::from_channel(descriptor, channel)
    }

    /// Creates a `BundleUserState` on an existing `StateChannel` if state specs exist.
    pub fn from_channel(
        descriptor: &ProcessBundleDescriptor,
        channel: StateChannel,
    ) -> Option<Self> {
        let pardo_specs = descriptor.transforms.iter().filter_map(|(t_id, t)| {
            let spec = t
                .spec
                .as_ref()
                .filter(|s| s.urn == URN_PAR_DO && !s.payload.is_empty())?;
            let pardo = proto::ParDoPayload::decode(spec.payload.as_slice()).ok()?;
            Some((t_id, pardo.state_specs))
        });

        let (state_coders, map_state_coders) = pardo_specs.fold(
            (
                HashMap::<String, HashMap<String, String>>::new(),
                HashMap::<String, HashMap<String, MapStateCoders>>::new(),
            ),
            |(mut state_coders, mut map_state_coders), (t_id, state_specs)| {
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
                        _ => {}
                    }
                }
                (state_coders, map_state_coders)
            },
        );

        if state_coders.is_empty() && map_state_coders.is_empty() {
            return None;
        }

        Some(Self {
            inner: Arc::new(BundleUserStateInner {
                channel,
                state_coders,
                map_state_coders,
                coders: descriptor.coders.clone(),
                cells: Mutex::default(),
                map_cells: Mutex::default(),
            }),
        })
    }

    /// Returns a [`UserStateReader`] scoped to the given transform id.
    pub fn scoped(&self, transform_id: &str) -> Arc<dyn UserStateReader> {
        Arc::new(ScopedUserState {
            bundle_state: self.clone(),
            transform_id: transform_id.to_string(),
        })
    }

    /// Commits all buffered state clears and appends to the runner through the State API.
    ///
    /// # Errors
    ///
    /// Returns an error when a lock is poisoned or a state request fails.
    pub fn commit(&self) -> Result<(), String> {
        let mut guard = self.inner.lock_cells()?;

        guard
            .drain()
            .try_for_each(|(cell_key, cell)| match cell.into_commit() {
                (false, None) => Ok(()),
                (true, None) => self.inner.channel.clear(cell_key.into_bag_state_key()),
                (false, Some(data)) => self
                    .inner
                    .channel
                    .append(cell_key.into_bag_state_key(), data),
                (true, Some(data)) => {
                    let state_key = cell_key.into_bag_state_key();
                    self.inner.channel.clear(state_key.clone())?;
                    self.inner.channel.append(state_key, data)
                }
            })?;

        let mut map_guard = self.inner.lock_map_cells()?;

        map_guard.drain().try_for_each(|(cell_key, cell)| {
            let super::cells::MapCommit {
                cleared,
                cleared_keys,
                entries,
            } = cell.into_commit();
            if cleared && entries.is_empty() {
                return self
                    .inner
                    .channel
                    .clear(cell_key.into_multimap_keys_state_key());
            }
            if cleared {
                self.inner
                    .channel
                    .clear(cell_key.multimap_keys_state_key())?;
            } else {
                cleared_keys.into_iter().try_for_each(|cleared_key| {
                    self.inner
                        .channel
                        .clear(cell_key.multimap_entry_state_key(cleared_key))
                })?;
            }

            entries.into_iter().try_for_each(|(map_key, val)| {
                let state_key = cell_key.multimap_entry_state_key(map_key);
                if !cleared {
                    self.inner.channel.clear(state_key.clone())?;
                }
                self.inner.channel.append(state_key, val)
            })
        })
    }

    fn get_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, String> {
        let mut guard = self.inner.lock_cells()?;
        with_cell(
            &mut guard,
            (transform_id, state_id, window, key),
            |cell: &mut StateCell| {
                cell.read(|| {
                    let state_key = StateKey {
                        r#type: Some(state_key::Type::BagUserState(state_key::BagUserState {
                            transform_id: transform_id.to_string(),
                            user_state_id: state_id.to_string(),
                            window: window.to_vec(),
                            key: key.to_vec(),
                        })),
                    };
                    let raw_bytes = self.inner.channel.get(state_key)?;
                    let coder_id = self.inner.get_state_coder(transform_id, state_id)?;
                    split_concatenated_elements(&raw_bytes, coder_id, &self.inner.coders)
                })
            },
        )
    }

    fn append_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        element: Vec<u8>,
    ) -> Result<(), String> {
        let mut guard = self.inner.lock_cells()?;
        with_cell(
            &mut guard,
            (transform_id, state_id, window, key),
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
    ) -> Result<(), String> {
        let mut guard = self.inner.lock_cells()?;
        with_cell(
            &mut guard,
            (transform_id, state_id, window, key),
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
    ) -> Result<Vec<Vec<u8>>, String> {
        let mut guard = self.inner.lock_map_cells()?;
        with_cell(
            &mut guard,
            (transform_id, state_id, window, key),
            |cell: &mut MapCell| {
                cell.get(map_key, || {
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
                    let raw_bytes = self.inner.channel.get(state_key)?;
                    if raw_bytes.is_empty() {
                        return Ok(Vec::new());
                    }
                    let coders = self.inner.get_map_state_coders(transform_id, state_id)?;
                    split_concatenated_elements(
                        &raw_bytes,
                        &coders.value_coder_id,
                        &self.inner.coders,
                    )
                })
            },
        )
    }

    fn put_map_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: Vec<u8>,
        element: Vec<u8>,
    ) -> Result<(), String> {
        let mut guard = self.inner.lock_map_cells()?;
        with_cell(
            &mut guard,
            (transform_id, state_id, window, key),
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
    ) -> Result<(), String> {
        let mut guard = self.inner.lock_map_cells()?;
        with_cell(
            &mut guard,
            (transform_id, state_id, window, key),
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
    ) -> Result<(), String> {
        let mut guard = self.inner.lock_map_cells()?;
        with_cell(
            &mut guard,
            (transform_id, state_id, window, key),
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
    ) -> Result<Vec<Vec<u8>>, String> {
        let mut guard = self.inner.lock_map_cells()?;
        with_cell(
            &mut guard,
            (transform_id, state_id, window, key),
            |cell: &mut MapCell| {
                cell.keys(|| {
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
                    let raw_bytes = self.inner.channel.get(state_key)?;
                    if raw_bytes.is_empty() {
                        return Ok(Vec::new());
                    }
                    let coders = self.inner.get_map_state_coders(transform_id, state_id)?;
                    split_concatenated_elements(
                        &raw_bytes,
                        &coders.key_coder_id,
                        &self.inner.coders,
                    )
                })
            },
        )
    }
}

struct ScopedUserState {
    bundle_state: BundleUserState,
    transform_id: String,
}

impl UserStateReader for ScopedUserState {
    fn get_state(&self, state_id: &str, window: &[u8], key: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        self.bundle_state
            .get_state(&self.transform_id, state_id, window, key)
    }

    fn append_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        element: Vec<u8>,
    ) -> Result<(), String> {
        self.bundle_state
            .append_state(&self.transform_id, state_id, window, key, element)
    }

    fn clear_state(&self, state_id: &str, window: &[u8], key: &[u8]) -> Result<(), String> {
        self.bundle_state
            .clear_state(&self.transform_id, state_id, window, key)
    }

    fn get_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<Vec<Vec<u8>>, String> {
        self.bundle_state
            .get_map_state(&self.transform_id, state_id, window, key, map_key)
    }

    fn get_map_keys(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, String> {
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
    ) -> Result<(), String> {
        self.bundle_state
            .put_map_state(&self.transform_id, state_id, window, key, map_key, element)
    }

    fn remove_map_key(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<(), String> {
        self.bundle_state
            .remove_map_key(&self.transform_id, state_id, window, key, map_key)
    }

    fn clear_map_state(&self, state_id: &str, window: &[u8], key: &[u8]) -> Result<(), String> {
        self.bundle_state
            .clear_map_state(&self.transform_id, state_id, window, key)
    }
}
