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

//! Container that collects user metrics while a bundle runs.

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Mutex, MutexGuard, PoisonError};

use model::pipeline::MonitoringInfo;

use super::values::{
    BoundedTrieValue, DistributionValue, GaugeValue, HistogramBuckets, HistogramValue, MetricCell,
    StringSetValue,
};
use super::{LABEL_NAME, LABEL_NAMESPACE, LABEL_PTRANSFORM};
use crate::internals::FastHashMap;

/// Key that identifies one user metric cell in a PTransform.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MetricKey {
    /// The PTransform ID, or PCollection ID when PTransform is absent.
    pub transform_id: String,
    /// The namespace that groups related metrics.
    pub namespace: String,
    /// The identifier of the metric within its namespace.
    pub name: String,
}

impl MetricKey {
    pub fn new(
        transform_id: impl Into<String>,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        Self {
            transform_id: transform_id.into(),
            namespace: namespace.into(),
            name: name.into(),
        }
    }
}

/// A `(transform_id, namespace, name)` view of owned and borrowed keys. Updates run for each
/// element, so a lookup must not allocate: the owned [`MetricKey`] borrows as
/// `dyn MetricKeyParts`, so a tuple of `&str` finds a cell.
pub(crate) trait MetricKeyParts {
    fn parts(&self) -> (&str, &str, &str);
}

impl MetricKeyParts for MetricKey {
    fn parts(&self) -> (&str, &str, &str) {
        (&self.transform_id, &self.namespace, &self.name)
    }
}

impl MetricKeyParts for (&str, &str, &str) {
    fn parts(&self) -> (&str, &str, &str) {
        *self
    }
}

impl<'a> Borrow<dyn MetricKeyParts + 'a> for MetricKey {
    fn borrow(&self) -> &(dyn MetricKeyParts + 'a) {
        self
    }
}

// `Borrow` needs equal hashes for `MetricKey` and `dyn MetricKeyParts`, so both hash `parts()`.
impl Hash for MetricKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

impl Hash for dyn MetricKeyParts + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parts().hash(state);
    }
}

impl PartialEq for dyn MetricKeyParts + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}

impl Eq for dyn MetricKeyParts + '_ {}

/// Applies `update` to the cell for `key`, or inserts `init()`. Allocates only for a new cell.
fn upsert<V>(
    map: &mut FastHashMap<MetricKey, V>,
    key: (&str, &str, &str),
    update: impl FnOnce(&mut V),
    init: impl FnOnce() -> V,
) {
    if let Some(v) = map.get_mut(&key as &dyn MetricKeyParts) {
        update(v);
    } else {
        map.insert(MetricKey::new(key.0, key.1, key.2), init());
    }
}

/// The cells of a container, one map for each kind.
#[derive(Debug, Default)]
struct Cells {
    counters: FastHashMap<MetricKey, i64>,
    distributions: FastHashMap<MetricKey, DistributionValue>,
    gauges: FastHashMap<MetricKey, GaugeValue>,
    string_sets: FastHashMap<MetricKey, StringSetValue>,
    bounded_tries: FastHashMap<MetricKey, BoundedTrieValue>,
    histograms: FastHashMap<MetricKey, HistogramValue>,
}

/// Collects the user metrics of an active bundle or runner execution. Cells use foldhash maps
/// for speed; getters return `HashMap` copies to keep the hasher out of the public API.
#[derive(Debug, Default)]
pub struct MetricsContainer {
    // The bundle thread writes and the progress reporter reads, so the lock is not contended.
    cells: Mutex<Cells>,
}

fn user_metric_labels(key: &MetricKey) -> HashMap<String, String> {
    let mut labels = HashMap::from([
        (LABEL_NAMESPACE.to_string(), key.namespace.clone()),
        (LABEL_NAME.to_string(), key.name.clone()),
    ]);
    if !key.transform_id.is_empty() {
        labels.insert(LABEL_PTRANSFORM.to_string(), key.transform_id.clone());
    }
    labels
}

fn snapshot<V: Clone>(cells: &FastHashMap<MetricKey, V>) -> HashMap<MetricKey, V> {
    cells.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// The `MonitoringInfo` of each cell in `cells`.
fn monitoring_infos<V: MetricCell>(
    cells: &FastHashMap<MetricKey, V>,
) -> impl Iterator<Item = MonitoringInfo> + '_ {
    cells.iter().map(|(key, value)| MonitoringInfo {
        urn: V::URN.to_string(),
        r#type: V::TYPE_URN.to_string(),
        payload: value.encode_cell(),
        labels: user_metric_labels(key),
        start_time: None,
    })
}

impl MetricsContainer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Locks the cells. Metrics are best effort, so a lock that a panic poisoned still works.
    fn cells(&self) -> MutexGuard<'_, Cells> {
        self.cells.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn inc_counter(&self, transform_id: &str, namespace: &str, name: &str, delta: i64) {
        upsert(
            &mut self.cells().counters,
            (transform_id, namespace, name),
            // Counter wraps on overflow.
            |c| *c = c.wrapping_add(delta),
            || delta,
        );
    }

    pub fn update_distribution(&self, transform_id: &str, namespace: &str, name: &str, value: i64) {
        upsert(
            &mut self.cells().distributions,
            (transform_id, namespace, name),
            |d| d.update(value),
            || DistributionValue::new(value),
        );
    }

    pub fn set_gauge(
        &self,
        transform_id: &str,
        namespace: &str,
        name: &str,
        value: i64,
        timestamp_ms: i64,
    ) {
        upsert(
            &mut self.cells().gauges,
            (transform_id, namespace, name),
            |g| g.set(value, timestamp_ms),
            || GaugeValue::new(value, timestamp_ms),
        );
    }

    /// Adds `value` to a string set. Past [`STRING_SET_MAX_BYTES`](super::STRING_SET_MAX_BYTES)
    /// new strings are dropped, with one warning per process.
    pub fn add_to_string_set(&self, transform_id: &str, namespace: &str, name: &str, value: &str) {
        let mut cells = self.cells();
        let key = (transform_id, namespace, name);
        let kept = match cells.string_sets.get_mut(&key as &dyn MetricKeyParts) {
            Some(set) => set.add(value),
            None => {
                let mut set = StringSetValue::new();
                let kept = set.add(value);
                let key = MetricKey::new(transform_id, namespace, name);
                cells.string_sets.insert(key, set);
                kept
            }
        };
        if !kept {
            static FULL: std::sync::Once = std::sync::Once::new();
            FULL.call_once(|| {
                tracing::warn!(
                    "String set metric {namespace}/{name} is full: new strings are dropped"
                );
            });
        }
    }

    /// Adds the path `segments` to a bounded trie.
    pub fn add_to_bounded_trie(
        &self,
        transform_id: &str,
        namespace: &str,
        name: &str,
        segments: &[&str],
    ) {
        upsert(
            &mut self.cells().bounded_tries,
            (transform_id, namespace, name),
            |trie| trie.add(segments),
            || {
                let mut trie = BoundedTrieValue::default();
                trie.add(segments);
                trie
            },
        );
    }

    /// Counts `value` in a histogram. The first update of a cell sets its buckets.
    pub fn update_histogram(
        &self,
        transform_id: &str,
        namespace: &str,
        name: &str,
        buckets: HistogramBuckets,
        value: f64,
    ) {
        upsert(
            &mut self.cells().histograms,
            (transform_id, namespace, name),
            |histogram| histogram.record(value),
            || {
                let mut histogram = HistogramValue::new(buckets);
                histogram.record(value);
                histogram
            },
        );
    }

    pub fn counters(&self) -> HashMap<MetricKey, i64> {
        snapshot(&self.cells().counters)
    }

    pub fn distributions(&self) -> HashMap<MetricKey, DistributionValue> {
        snapshot(&self.cells().distributions)
    }

    pub fn gauges(&self) -> HashMap<MetricKey, GaugeValue> {
        snapshot(&self.cells().gauges)
    }

    pub fn string_sets(&self) -> HashMap<MetricKey, StringSetValue> {
        snapshot(&self.cells().string_sets)
    }

    pub fn bounded_tries(&self) -> HashMap<MetricKey, BoundedTrieValue> {
        snapshot(&self.cells().bounded_tries)
    }

    pub fn histograms(&self) -> HashMap<MetricKey, HistogramValue> {
        snapshot(&self.cells().histograms)
    }

    /// Converts all collected metrics to `MonitoringInfo` messages.
    pub fn to_monitoring_infos(&self) -> Vec<MonitoringInfo> {
        let cells = self.cells();
        monitoring_infos(&cells.counters)
            .chain(monitoring_infos(&cells.distributions))
            .chain(monitoring_infos(&cells.gauges))
            .chain(monitoring_infos(&cells.string_sets))
            .chain(monitoring_infos(&cells.bounded_tries))
            .chain(monitoring_infos(&cells.histograms))
            .collect()
    }
}
