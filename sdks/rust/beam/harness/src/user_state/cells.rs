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

use crate::state::tables::{FastHashMap, FastHashSet};

/// The coder ids of the key and value components of a map state cell.
#[derive(Clone, Debug)]
pub(super) struct MapStateCoders {
    pub(super) key_coder_id: String,
    pub(super) value_coder_id: String,
}

#[derive(Default, Debug)]
pub(super) struct StateCell {
    initial: Option<Vec<Vec<u8>>>,
    cleared: bool,
    appends: Vec<Vec<u8>>,
}

impl StateCell {
    pub(super) fn read(
        &mut self,
        fetch_initial: impl FnOnce() -> Result<Vec<Vec<u8>>, String>,
    ) -> Result<Vec<Vec<u8>>, String> {
        if self.initial.is_none() && !self.cleared {
            self.initial = Some(fetch_initial()?);
        }
        let initial = (!self.cleared)
            .then_some(self.initial.as_deref())
            .flatten()
            .into_iter()
            .flatten();
        Ok(initial.chain(&self.appends).cloned().collect())
    }

    #[inline]
    pub(super) fn append(&mut self, element: Vec<u8>) {
        self.appends.push(element);
    }

    #[inline]
    pub(super) fn clear(&mut self) {
        self.cleared = true;
        self.appends.clear();
        self.initial = Some(Vec::new());
    }

    #[inline]
    pub(super) fn into_commit(mut self) -> (bool, Option<Vec<u8>>) {
        let data = match self.appends.len() {
            0 => None,
            1 => self.appends.pop(),
            _ => Some(self.appends.concat()),
        };
        (self.cleared, data)
    }
}

#[derive(Default, Debug)]
pub(super) struct MapCell {
    cleared: bool,
    cleared_keys: FastHashSet<Vec<u8>>,
    entries: FastHashMap<Vec<u8>, Vec<u8>>,
    persisted_keys: Option<FastHashSet<Vec<u8>>>,
}

impl MapCell {
    pub(super) fn get(
        &mut self,
        map_key: &[u8],
        fetch_entry: impl FnOnce() -> Result<Vec<Vec<u8>>, String>,
    ) -> Result<Vec<Vec<u8>>, String> {
        if let Some(val) = self.entries.get(map_key) {
            return Ok(vec![val.clone()]);
        }
        if self.cleared || self.cleared_keys.contains(map_key) {
            return Ok(Vec::new());
        }
        let elements = fetch_entry()?;
        if let Some(first) = elements.first() {
            self.entries.insert(map_key.to_vec(), first.clone());
        }
        Ok(elements)
    }

    pub(super) fn keys(
        &mut self,
        fetch_keys: impl FnOnce() -> Result<Vec<Vec<u8>>, String>,
    ) -> Result<Vec<Vec<u8>>, String> {
        if self.persisted_keys.is_none() && !self.cleared {
            let keys = fetch_keys()?;
            self.persisted_keys = Some(keys.into_iter().collect());
        }
        let persisted = (!self.cleared)
            .then_some(self.persisted_keys.as_ref())
            .flatten()
            .into_iter()
            .flatten()
            .filter(|k| !self.cleared_keys.contains(*k));
        let all_keys: FastHashSet<Vec<u8>> =
            persisted.chain(self.entries.keys()).cloned().collect();
        Ok(all_keys.into_iter().collect())
    }

    #[inline]
    pub(super) fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        if !self.cleared_keys.is_empty() {
            self.cleared_keys.remove(&key);
        }
        if let Some(ref mut persisted) = self.persisted_keys
            && !persisted.contains(&key)
        {
            persisted.insert(key.clone());
        }
        self.entries.insert(key, value);
    }

    #[inline]
    pub(super) fn remove(&mut self, key: &[u8]) {
        self.entries.remove(key);
        if !self.cleared {
            self.cleared_keys.insert(key.to_vec());
        }
        if let Some(ref mut persisted) = self.persisted_keys {
            persisted.remove(key);
        }
    }

    #[inline]
    pub(super) fn clear(&mut self) {
        self.cleared = true;
        self.cleared_keys.clear();
        self.entries.clear();
        self.persisted_keys = Some(FastHashSet::default());
    }

    #[inline]
    pub(super) fn into_commit(self) -> MapCommit {
        MapCommit {
            cleared: self.cleared,
            cleared_keys: self.cleared_keys,
            entries: self.entries,
        }
    }
}

pub(super) struct MapCommit {
    pub(super) cleared: bool,
    pub(super) cleared_keys: FastHashSet<Vec<u8>>,
    pub(super) entries: FastHashMap<Vec<u8>, Vec<u8>>,
}
