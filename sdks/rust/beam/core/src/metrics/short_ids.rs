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

//! Cache that maps `MonitoringInfo` descriptors to short IDs.
//!
//! Runners with the `beam:protocol:monitoring_info_short_ids:v1` capability, for example
//! Dataflow, read payloads by short ID in `ProcessBundleResponse.monitoring_data`. For an
//! unknown ID they send a `MonitoringInfosMetadataRequest`; the worker replies with the template.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

use model::pipeline::MonitoringInfo;

/// Key for the metadata of a metric. The payload and the timestamp are not part of it.
#[derive(Debug, Clone)]
struct DescriptorKey {
    urn: String,
    type_urn: String,
    labels: HashMap<String, String>,
}

/// A borrowed view of a metric descriptor key. Updates run for each metric in each bundle,
/// so lookup in the short-ID cache must not allocate.
pub(crate) trait DescriptorKeyParts {
    fn urn(&self) -> &str;
    fn type_urn(&self) -> &str;
    fn labels(&self) -> &HashMap<String, String>;
}

impl DescriptorKeyParts for DescriptorKey {
    fn urn(&self) -> &str {
        &self.urn
    }

    fn type_urn(&self) -> &str {
        &self.type_urn
    }

    fn labels(&self) -> &HashMap<String, String> {
        &self.labels
    }
}

impl DescriptorKeyParts for MonitoringInfo {
    fn urn(&self) -> &str {
        &self.urn
    }

    fn type_urn(&self) -> &str {
        &self.r#type
    }

    fn labels(&self) -> &HashMap<String, String> {
        &self.labels
    }
}

impl<'a> Borrow<dyn DescriptorKeyParts + 'a> for DescriptorKey {
    fn borrow(&self) -> &(dyn DescriptorKeyParts + 'a) {
        self
    }
}

impl PartialEq for dyn DescriptorKeyParts + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.urn() == other.urn()
            && self.type_urn() == other.type_urn()
            && self.labels() == other.labels()
    }
}

impl Eq for dyn DescriptorKeyParts + '_ {}

impl Hash for dyn DescriptorKeyParts + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.urn().hash(state);
        self.type_urn().hash(state);
        self.labels().len().hash(state);
        let mut label_sum: u64 = 0;
        for (k, v) in self.labels() {
            let mut entry_hasher = std::collections::hash_map::DefaultHasher::new();
            k.hash(&mut entry_hasher);
            v.hash(&mut entry_hasher);
            label_sum = label_sum.wrapping_add(entry_hasher.finish());
        }
        label_sum.hash(state);
    }
}

impl PartialEq for DescriptorKey {
    fn eq(&self, other: &Self) -> bool {
        (self as &dyn DescriptorKeyParts) == (other as &dyn DescriptorKeyParts)
    }
}

impl Eq for DescriptorKey {}

impl Hash for DescriptorKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (self as &dyn DescriptorKeyParts).hash(state);
    }
}

#[derive(Debug, Default)]
struct ShortIdCacheInner {
    key_to_id: HashMap<DescriptorKey, String>,
    id_to_info: HashMap<String, MonitoringInfo>,
    next_id: usize,
}

/// Thread-safe cache of short IDs for `MonitoringInfo` descriptors.
#[derive(Debug, Clone, Default)]
pub struct ShortIdCache {
    inner: Arc<RwLock<ShortIdCacheInner>>,
}

impl ShortIdCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the short ID for the URN, type URN and labels of `info`, assigning one if new.
    /// The cached template has an empty `payload` and no `start_time`.
    pub fn get_or_create_short_id(&self, info: &MonitoringInfo) -> String {
        let key_parts = info as &dyn DescriptorKeyParts;

        if let Ok(guard) = self.inner.read()
            && let Some(id) = guard.key_to_id.get(key_parts)
        {
            return id.clone();
        }

        // Check again under the write lock. Another thread can insert the key first.
        // Metrics aggregation is best-effort and continues across poisoned locks.
        let mut guard = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(id) = guard.key_to_id.get(key_parts) {
            return id.clone();
        }

        let key = DescriptorKey {
            urn: info.urn.clone(),
            type_urn: info.r#type.clone(),
            labels: info.labels.clone(),
        };

        let id = format!("mi{}", guard.next_id);
        guard.next_id += 1;

        let template_info = MonitoringInfo {
            urn: info.urn.clone(),
            r#type: info.r#type.clone(),
            payload: Vec::new(),
            labels: info.labels.clone(),
            start_time: None,
        };

        guard.key_to_id.insert(key, id.clone());
        guard.id_to_info.insert(id.clone(), template_info);
        id
    }

    /// Returns the template `MonitoringInfo` for `short_id`, if it is registered.
    pub fn get_info(&self, short_id: &str) -> Option<MonitoringInfo> {
        // Poison recovery via into_inner keeps cache active even after thread panic.
        let guard = self
            .inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.id_to_info.get(short_id).cloned()
    }

    /// Returns a map from each registered short ID in `short_ids` to its template.
    pub fn get_infos(&self, short_ids: &[String]) -> HashMap<String, MonitoringInfo> {
        // Poison recovery via into_inner keeps cache active even after thread panic.
        let guard = self
            .inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        short_ids
            .iter()
            .filter_map(|id| {
                guard
                    .id_to_info
                    .get(id)
                    .map(|info| (id.clone(), info.clone()))
            })
            .collect()
    }

    /// Returns the number of distinct metrics in the cache.
    pub fn len(&self) -> usize {
        // Poison recovery via into_inner keeps cache active even after thread panic.
        let guard = self
            .inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.id_to_info.len()
    }

    /// Returns `true` if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
