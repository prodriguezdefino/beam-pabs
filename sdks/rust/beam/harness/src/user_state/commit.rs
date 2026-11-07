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

//! The commit of the user state of a bundle to the runner.

use std::sync::Arc;

use crate::state::StateChannel;
use crate::state::cache::{CacheKey, CachedPart, CachedValue, MapSnapshot};
use crate::state::channel::PendingReply;
use crate::state::tables::{StateCellKey, StateCellKeyParts};
use crate::user_state::bundle::BundleUserState;
use crate::user_state::cache::UserStateCache;
use crate::user_state::cells::{
    BagCommit, KeyWrites, MapCommit, OrderedListCell, StateMutation, ordered_list_mutations,
};

impl BundleUserState {
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
}

/// Sends the mutations of one bag cell: a clear if the bundle cleared it, then the appends.
fn bag_mutations(
    channel: &StateChannel,
    cell: &StateCellKey,
    cleared: bool,
    batches: Vec<Vec<u8>>,
) -> Result<Vec<PendingReply>, String> {
    let clear = cleared.then(|| channel.start_clear(cell.bag_state_key()));
    let appends = batches
        .into_iter()
        .map(|batch| channel.start_append(cell.bag_state_key(), batch));
    clear.into_iter().chain(appends).collect()
}

/// Sends the mutations of one map cell: a whole-cell clear or the clears of removed keys,
/// then the writes. A replaced key gets a clear and an append; an added-to key one append.
fn map_mutations(
    channel: &StateChannel,
    cell: &StateCellKey,
    commit: MapCommit,
) -> Result<Vec<PendingReply>, String> {
    let MapCommit {
        cleared,
        cleared_keys,
        entries,
    } = commit;
    let clears: Vec<_> = if cleared {
        vec![channel.start_clear(cell.multimap_keys_state_key())]
    } else {
        cleared_keys
            .into_iter()
            .map(|key| channel.start_clear(cell.multimap_entry_state_key(key)))
            .collect()
    };
    // Without a whole-cell clear, a clear of the entry makes the append a replace.
    let writes = entries.into_iter().flat_map(|(map_key, writes)| {
        let (replace, values) = match writes {
            KeyWrites::Replace(values) => (!cleared, values),
            KeyWrites::Append(values) => (false, values),
        };
        let replace =
            replace.then(|| channel.start_clear(cell.multimap_entry_state_key(map_key.clone())));
        let append = channel.start_append(cell.multimap_entry_state_key(map_key), joined(values));
        replace.into_iter().chain(std::iter::once(append))
    });
    clears.into_iter().chain(writes).collect()
}

/// Joins encoded values into one append payload. Moves a single value without a copy.
fn joined(values: Vec<Vec<u8>>) -> Vec<u8> {
    match <[Vec<u8>; 1]>::try_from(values) {
        Ok([value]) => value,
        Err(values) => values.concat(),
    }
}
