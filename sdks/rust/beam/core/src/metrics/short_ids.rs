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

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use model::pipeline::MonitoringInfo;

/// Key for the metadata of a metric. The payload and the timestamp are not part of it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MetricKey {
    urn: String,
    type_urn: String,
    labels: BTreeMap<String, String>,
}

#[derive(Debug, Default)]
struct ShortIdCacheInner {
    key_to_id: HashMap<MetricKey, String>,
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
        let key = MetricKey {
            urn: info.urn.clone(),
            type_urn: info.r#type.clone(),
            labels: info
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };

        if let Ok(guard) = self.inner.read()
            && let Some(id) = guard.key_to_id.get(&key)
        {
            return id.clone();
        }

        // Check again under the write lock. Another thread can insert the key first.
        let mut guard = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(id) = guard.key_to_id.get(&key) {
            return id.clone();
        }

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
        let guard = self
            .inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.id_to_info.get(short_id).cloned()
    }

    /// Returns a map from each registered short ID in `short_ids` to its template.
    pub fn get_infos(&self, short_ids: &[String]) -> HashMap<String, MonitoringInfo> {
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
