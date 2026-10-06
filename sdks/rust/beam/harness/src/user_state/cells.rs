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

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::state::cache::MapSnapshot;
use crate::state::tables::{FastHashMap, FastHashSet};

/// The coder ids of the key and value components of a map state cell.
#[derive(Clone, Debug)]
pub(super) struct MapStateCoders {
    pub(super) key_coder_id: String,
    pub(super) value_coder_id: String,
}

/// The bundle view of a bag or value cell.
#[derive(Default, Debug)]
pub(super) struct StateCell {
    /// The runner content of the cell, when it is known.
    initial: Option<Arc<[Vec<u8>]>>,
    cleared: bool,
    appends: Vec<Vec<u8>>,
}

/// The maximum size of one append request. A larger element goes alone.
const APPEND_BATCH_BYTES: usize = 10 << 20;

/// The mutations of a bag cell at the end of a bundle, and its content after them.
pub(super) struct BagCommit {
    pub(super) cleared: bool,
    /// The encoded elements to append, joined into requests of at most
    /// [`APPEND_BATCH_BYTES`].
    pub(super) batches: Vec<Vec<u8>>,
    /// The content of the cell after the commit, when the bundle knows all of it.
    pub(super) content: Option<Arc<[Vec<u8>]>>,
}

impl StateCell {
    /// A cell whose runner content is `initial`, when it is known.
    pub(super) fn with_initial(initial: Option<Arc<[Vec<u8>]>>) -> Self {
        Self {
            initial,
            ..Self::default()
        }
    }

    /// Returns the cell content if its initial runner value is already known.
    pub(super) fn read_if_present(&self) -> Option<Vec<Vec<u8>>> {
        self.initial.as_ref().map(|initial| {
            // The trait contract returns owned vectors so user code can decode and mutate them.
            initial.iter().chain(&self.appends).cloned().collect()
        })
    }

    /// Sets the initial runner value if not already set, and returns the full content.
    pub(super) fn set_initial_and_read(&mut self, fetched: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        let initial = self.initial.get_or_insert_with(|| fetched.into());
        // The trait contract returns owned vectors so user code can decode and mutate them.
        initial.iter().chain(&self.appends).cloned().collect()
    }

    #[inline]
    pub(super) fn append(&mut self, element: Vec<u8>) {
        self.appends.push(element);
    }

    #[inline]
    pub(super) fn clear(&mut self) {
        self.cleared = true;
        self.appends.clear();
        self.initial = Some(Arc::from([]));
    }

    pub(super) fn into_commit(self) -> BagCommit {
        let Self {
            initial,
            cleared,
            appends,
        } = self;
        let content = initial.map(|initial| match appends.is_empty() {
            true => initial,
            false => initial.iter().chain(&appends).cloned().collect(),
        });
        BagCommit {
            cleared,
            batches: append_batches(appends),
            content,
        }
    }
}

/// Joins `elements` in order into requests of at most [`APPEND_BATCH_BYTES`].
fn append_batches(elements: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    elements
        .into_iter()
        .fold(Vec::new(), |mut batches, element| {
            match batches.last_mut() {
                Some(batch) if batch.len() + element.len() <= APPEND_BATCH_BYTES => {
                    batch.extend_from_slice(&element);
                }
                Some(_) | None => batches.push(element),
            }
            batches
        })
}

/// The bundle view of a map or set cell.
#[derive(Default, Debug)]
pub(super) struct MapCell {
    /// The content that an earlier bundle left in the state cache.
    seed: Option<Arc<MapSnapshot>>,
    cleared: bool,
    /// Keys that this bundle removed.
    cleared_keys: FastHashSet<Vec<u8>>,
    /// Values that this bundle wrote.
    entries: FastHashMap<Vec<u8>, Vec<u8>>,
    /// Values that this bundle read from the runner. An empty list is an absent key.
    fetched: FastHashMap<Vec<u8>, Vec<Vec<u8>>>,
    /// All keys, after the bundle read them or cleared the cell.
    persisted_keys: Option<FastHashSet<Vec<u8>>>,
}

impl MapCell {
    /// A cell that starts from `seed`.
    pub(super) fn seeded(seed: Arc<MapSnapshot>) -> Self {
        Self {
            seed: Some(seed),
            ..Self::default()
        }
    }

    /// The values of `map_key` before this bundle, if the bundle knows them without a read.
    fn known_before(&self, map_key: &[u8]) -> Option<Vec<Vec<u8>>> {
        let seed = self.seed.as_deref();
        if let Some(values) = self
            .fetched
            .get(map_key)
            .or_else(|| seed.and_then(|seed| seed.entries.get(map_key)))
        {
            return Some(values.clone());
        }
        let all_keys = self
            .persisted_keys
            .as_ref()
            .or_else(|| seed.and_then(|seed| seed.keys.as_ref()))?;
        (!all_keys.contains(map_key)).then(Vec::new)
    }

    /// Returns the values of `map_key` if already known locally without a read.
    pub(super) fn get_if_present(&self, map_key: &[u8]) -> Option<Vec<Vec<u8>>> {
        if let Some(value) = self.entries.get(map_key) {
            return Some(vec![value.clone()]);
        }
        if self.cleared || self.cleared_keys.contains(map_key) {
            return Some(Vec::new());
        }
        self.known_before(map_key)
    }

    /// Stores fetched values for `map_key` and returns the current entry.
    pub(super) fn install_fetched_entry(
        &mut self,
        map_key: &[u8],
        values: Vec<Vec<u8>>,
    ) -> Vec<Vec<u8>> {
        if let Some(value) = self.entries.get(map_key) {
            return vec![value.clone()];
        }
        if self.cleared || self.cleared_keys.contains(map_key) {
            return Vec::new();
        }
        self.fetched
            .entry(map_key.to_vec())
            .or_insert(values)
            .clone()
    }

    /// Returns all keys if already known without a runner fetch.
    pub(super) fn keys_if_present(&self) -> Option<Vec<Vec<u8>>> {
        if let Some(persisted) = &self.persisted_keys {
            return Some(persisted.iter().cloned().collect());
        }
        if let Some(seeded) = self.seed.as_deref().and_then(|seed| seed.keys.as_ref()) {
            let keys: Vec<Vec<u8>> = seeded
                .iter()
                .filter(|k| !self.cleared_keys.contains(*k))
                .chain(self.entries.keys())
                .cloned()
                .collect();
            return Some(keys);
        }
        None
    }

    /// Stores fetched keys from runner and returns all current keys.
    pub(super) fn install_fetched_keys(&mut self, fetched_keys: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        let keys = self.persisted_keys.get_or_insert_with(|| {
            fetched_keys
                .into_iter()
                .filter(|k| !self.cleared_keys.contains(k))
                .chain(self.entries.keys().cloned())
                .collect()
        });
        keys.iter().cloned().collect()
    }

    #[inline]
    pub(super) fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.cleared_keys.remove(&key);
        if let Some(persisted) = &mut self.persisted_keys {
            persisted.insert(key.clone());
        }
        self.entries.insert(key, value);
    }

    #[inline]
    pub(super) fn remove(&mut self, key: &[u8]) {
        self.entries.remove(key);
        self.fetched.remove(key);
        if !self.cleared {
            self.cleared_keys.insert(key.to_vec());
        }
        if let Some(persisted) = &mut self.persisted_keys {
            persisted.remove(key);
        }
    }

    #[inline]
    pub(super) fn clear(&mut self) {
        self.cleared = true;
        self.cleared_keys.clear();
        self.entries.clear();
        self.fetched.clear();
        self.persisted_keys = Some(FastHashSet::default());
    }

    /// The mutations to send to the runner, and the content of the cell after them.
    pub(super) fn into_commit(self) -> (MapCommit, MapSnapshot) {
        let Self {
            seed,
            cleared,
            cleared_keys,
            entries,
            fetched,
            persisted_keys,
        } = self;
        let seed = seed.filter(|_| !cleared);
        let keys = persisted_keys.or_else(|| {
            let mut keys = seed.as_deref()?.keys.clone()?;
            keys.retain(|k| !cleared_keys.contains(k));
            keys.extend(entries.keys().cloned());
            Some(keys)
        });
        // The seed entries are copied: other bundles can hold the same snapshot.
        let snapshot_entries = seed
            .iter()
            .flat_map(|seed| seed.entries.iter())
            .chain(&fetched)
            .map(|(k, v)| (k.clone(), v.clone()))
            .chain(cleared_keys.iter().map(|k| (k.clone(), Vec::new())))
            .chain(entries.iter().map(|(k, v)| (k.clone(), vec![v.clone()])))
            .collect();
        (
            MapCommit {
                cleared,
                cleared_keys,
                entries,
            },
            MapSnapshot {
                keys,
                entries: snapshot_entries,
            },
        )
    }
}

pub(super) struct MapCommit {
    pub(super) cleared: bool,
    pub(super) cleared_keys: FastHashSet<Vec<u8>>,
    pub(super) entries: FastHashMap<Vec<u8>, Vec<u8>>,
}

/// A cell of ordered list user state.
#[derive(Default, Debug)]
pub(super) struct OrderedListCell {
    /// The bundle cleared the whole cell.
    cleared: bool,
    /// Half-open ranges `[from, to)` cleared in this bundle.
    cleared_ranges: Vec<(i64, i64)>,
    /// Values added during this bundle grouped by timestamp.
    appends: BTreeMap<i64, Vec<Vec<u8>>>,
}

impl OrderedListCell {
    /// True when the whole cell was cleared.
    pub(super) fn is_cleared(&self) -> bool {
        self.cleared
    }

    /// Combines fetched runner entries with local uncommitted appends and clears.
    pub(super) fn combine_with_fetched(
        &self,
        from: i64,
        to: i64,
        fetched: Vec<(i64, Vec<u8>)>,
    ) -> Vec<(i64, Vec<u8>)> {
        let runner_entries = if self.cleared {
            Vec::new()
        } else if self.cleared_ranges.is_empty() {
            fetched
        } else {
            fetched
                .into_iter()
                .filter(|(ts, _)| {
                    !self
                        .cleared_ranges
                        .iter()
                        .any(|&(start, end)| *ts >= start && *ts < end)
                })
                .collect()
        };

        let mut runner = runner_entries.into_iter().peekable();
        let mut local = self
            .appends
            .range(from..to)
            .flat_map(|(&ts, values)| values.iter().map(move |value| (ts, value.clone())))
            .peekable();
        std::iter::from_fn(|| match (runner.peek(), local.peek()) {
            (Some((runner_ts, _)), Some((local_ts, _))) if local_ts < runner_ts => local.next(),
            (Some(_), _) => runner.next(),
            (None, _) => local.next(),
        })
        .collect()
    }

    /// Adds an element at the given timestamp.
    #[inline]
    pub(super) fn add(&mut self, timestamp: i64, element: Vec<u8>) {
        self.appends.entry(timestamp).or_default().push(element);
    }

    /// Clears entries within the half-open range `[from, to)`.
    #[inline]
    pub(super) fn clear_range(&mut self, from: i64, to: i64) {
        if from >= to {
            return;
        }
        self.appends.retain(|&ts, _| ts < from || ts >= to);
        if !self.cleared {
            self.cleared_ranges.push((from, to));
        }
    }

    /// Clears all entries from this cell.
    #[inline]
    pub(super) fn clear(&mut self) {
        self.cleared = true;
        self.cleared_ranges.clear();
        self.appends.clear();
    }

    /// The mutations of the cell at the end of the bundle.
    #[inline]
    pub(super) fn into_commit(self) -> OrderedListCommit {
        OrderedListCommit {
            cleared: self.cleared,
            cleared_ranges: self.cleared_ranges,
            appends: self.appends,
        }
    }
}

/// The committed state changes of an ordered list cell.
pub(super) struct OrderedListCommit {
    /// True when the whole cell was cleared.
    pub(super) cleared: bool,
    /// Ranges `[from, to)` that the bundle cleared.
    pub(super) cleared_ranges: Vec<(i64, i64)>,
    /// Pending appends grouped by timestamp.
    pub(super) appends: BTreeMap<i64, Vec<Vec<u8>>>,
}

/// Splits ordered list entries (varint timestamp, length-prefixed value) into pairs.
pub(crate) fn split_ordered_list_elements(data: &[u8]) -> Result<Vec<(i64, Vec<u8>)>, String> {
    let mut cursor = std::io::Cursor::new(data);
    std::iter::from_fn(|| {
        let start = cursor.position() as usize;
        (start < data.len()).then(|| {
            let ts = beam::coders::VarIntCoder::decode_varint(&mut cursor)
                .map_err(|e| format!("Failed to decode ordered list timestamp: {e}"))?;
            let len = beam::coders::VarIntCoder::decode_varint(&mut cursor)
                .map_err(|e| format!("Failed to decode ordered list value length: {e}"))?
                as usize;
            let value_start = cursor.position() as usize;
            let value = data
                .get(value_start..value_start.saturating_add(len))
                .ok_or_else(|| {
                    format!(
                        "Ordered list value length {len} exceeds remaining bytes {}",
                        data.len() - value_start
                    )
                })?;
            cursor.set_position((value_start + len) as u64);
            Ok((ts, value.to_vec()))
        })
    })
    .collect()
}

/// Encodes an ordered list state entry into an existing buffer.
pub(crate) fn encode_ordered_list_entry_into(timestamp: i64, element: &[u8], buf: &mut Vec<u8>) {
    beam::coders::VarIntCoder::encode_varint(timestamp, buf).expect("writing to Vec does not fail");
    beam::coders::VarIntCoder::encode_varint(element.len() as i64, buf)
        .expect("writing to Vec does not fail");
    buf.extend_from_slice(element);
}

/// A planned state mutation on the Fn API State channel.
pub(crate) enum StateMutation {
    /// Clears the state key.
    Clear,
    /// Appends the byte payload to the state key.
    Append(Vec<u8>),
}

/// Builds the mutations of one ordered list cell: a whole clear or the range clears, then
/// one append with all added entries.
pub(crate) fn ordered_list_mutations(
    cell_key: &crate::state::tables::StateCellKey,
    commit: OrderedListCommit,
) -> Vec<(model::fn_execution::StateKey, StateMutation)> {
    let OrderedListCommit {
        cleared,
        cleared_ranges,
        appends,
    } = commit;
    let clears = match cleared {
        true => vec![(i64::MIN, i64::MAX)],
        false => cleared_ranges,
    };
    let data = appends
        .iter()
        .flat_map(|(&ts, elements)| elements.iter().map(move |element| (ts, element)))
        .fold(Vec::new(), |mut data, (ts, element)| {
            encode_ordered_list_entry_into(ts, element, &mut data);
            data
        });
    clears
        .into_iter()
        .map(|(start, end)| {
            (
                cell_key.ordered_list_state_key(start, end),
                StateMutation::Clear,
            )
        })
        .chain((!data.is_empty()).then(|| {
            (
                cell_key.ordered_list_state_key(i64::MIN, i64::MAX),
                StateMutation::Append(data),
            )
        }))
        .collect()
}
