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

//! Per-element tables for user state and cached side inputs.
//!
//! Tables hash with foldhash, and are keyed by an owned [`StateCellKey`] that can be
//! probed with a borrowed [`StateCellRef`], so a lookup never allocates.

use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use model::fn_execution::{OrderedListRange, StateKey, state_key};

pub(crate) type FastHashMap<K, V> = HashMap<K, V, foldhash::fast::RandomState>;
pub(crate) type FastHashSet<T> = HashSet<T, foldhash::fast::RandomState>;

/// Identifies a state cell. For side inputs, `state_id` is the side input id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StateCellKey {
    transform_id: String,
    state_id: String,
    window: Vec<u8>,
    key: Vec<u8>,
}

impl StateCellKey {
    #[inline]
    pub(crate) fn new(transform_id: &str, state_id: &str, window: &[u8], key: &[u8]) -> Self {
        Self {
            transform_id: transform_id.to_string(),
            state_id: state_id.to_string(),
            window: window.to_vec(),
            key: key.to_vec(),
        }
    }

    pub(crate) fn bag_state_key(&self) -> StateKey {
        StateKey {
            r#type: Some(state_key::Type::BagUserState(state_key::BagUserState {
                transform_id: self.transform_id.clone(),
                user_state_id: self.state_id.clone(),
                window: self.window.clone(),
                key: self.key.clone(),
            })),
        }
    }

    pub(crate) fn multimap_keys_state_key(&self) -> StateKey {
        StateKey {
            r#type: Some(state_key::Type::MultimapKeysUserState(
                state_key::MultimapKeysUserState {
                    transform_id: self.transform_id.clone(),
                    user_state_id: self.state_id.clone(),
                    window: self.window.clone(),
                    key: self.key.clone(),
                },
            )),
        }
    }

    pub(crate) fn multimap_entry_state_key(&self, map_key: Vec<u8>) -> StateKey {
        StateKey {
            r#type: Some(state_key::Type::MultimapUserState(
                state_key::MultimapUserState {
                    transform_id: self.transform_id.clone(),
                    user_state_id: self.state_id.clone(),
                    window: self.window.clone(),
                    key: self.key.clone(),
                    map_key,
                },
            )),
        }
    }

    pub(crate) fn ordered_list_state_key(&self, start: i64, end: i64) -> StateKey {
        StateKey {
            r#type: Some(state_key::Type::OrderedListUserState(
                state_key::OrderedListUserState {
                    transform_id: self.transform_id.clone(),
                    user_state_id: self.state_id.clone(),
                    window: self.window.clone(),
                    key: self.key.clone(),
                    range: Some(OrderedListRange { start, end }),
                },
            )),
        }
    }

    pub(crate) fn iterable_side_input_key(&self) -> StateKey {
        StateKey {
            r#type: Some(state_key::Type::IterableSideInput(
                state_key::IterableSideInput {
                    transform_id: self.transform_id.clone(),
                    side_input_id: self.state_id.clone(),
                    window: self.window.clone(),
                },
            )),
        }
    }

    pub(crate) fn multimap_side_input_key(&self) -> StateKey {
        StateKey {
            r#type: Some(state_key::Type::MultimapSideInput(
                state_key::MultimapSideInput {
                    transform_id: self.transform_id.clone(),
                    side_input_id: self.state_id.clone(),
                    window: self.window.clone(),
                    key: self.key.clone(),
                },
            )),
        }
    }
}

/// A borrowed [`StateCellKey`]: `(transform_id, state_id, window, key)`.
pub(crate) type StateCellRef<'a> = (&'a str, &'a str, &'a [u8], &'a [u8]);

/// The view owned and borrowed keys share, so a table keyed by [`StateCellKey`] can be
/// probed with a [`StateCellRef`].
pub(crate) trait StateCellKeyParts {
    fn parts(&self) -> StateCellRef<'_>;
}

impl StateCellKeyParts for StateCellKey {
    fn parts(&self) -> StateCellRef<'_> {
        (&self.transform_id, &self.state_id, &self.window, &self.key)
    }
}

impl StateCellKeyParts for StateCellRef<'_> {
    fn parts(&self) -> StateCellRef<'_> {
        *self
    }
}

impl<'a> Borrow<dyn StateCellKeyParts + 'a> for StateCellKey {
    fn borrow(&self) -> &(dyn StateCellKeyParts + 'a) {
        self
    }
}

// Both sides hash through `parts()`, as `Borrow` requires.
impl Hash for StateCellKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

impl Hash for dyn StateCellKeyParts + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

impl PartialEq for dyn StateCellKeyParts + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}

impl Eq for dyn StateCellKeyParts + '_ {}

/// Runs `f` on the cell for `key`. On first use, `init` creates the cell.
pub(crate) fn with_cell<V, R>(
    cells: &mut FastHashMap<StateCellKey, V>,
    key: StateCellRef<'_>,
    init: impl FnOnce() -> V,
    f: impl FnOnce(&mut V) -> R,
) -> R {
    if let Some(cell) = cells.get_mut(&key as &dyn StateCellKeyParts) {
        return f(cell);
    }
    let (transform_id, state_id, window, user_key) = key;
    f(cells
        .entry(StateCellKey::new(transform_id, state_id, window, user_key))
        .or_insert_with(init))
}
