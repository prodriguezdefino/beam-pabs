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
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

use beam_model::pipeline::MonitoringInfo;

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

/// The cells of a container, one map for each kind. A counter maps to its index in
/// [`CounterValues`].
#[derive(Debug, Default)]
struct Cells {
    counters: FastHashMap<MetricKey, CounterIndex>,
    distributions: FastHashMap<MetricKey, DistributionValue>,
    gauges: FastHashMap<MetricKey, GaugeValue>,
    string_sets: FastHashMap<MetricKey, StringSetValue>,
    bounded_tries: FastHashMap<MetricKey, BoundedTrieValue>,
    histograms: FastHashMap<MetricKey, HistogramValue>,
}

/// The position of a counter value in [`CounterValues`]. An index does not change while its
/// container lives, so a counter handle can keep it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CounterIndex(u32);

impl From<CounterIndex> for u32 {
    fn from(CounterIndex(index): CounterIndex) -> Self {
        index
    }
}

impl From<u32> for CounterIndex {
    fn from(index: u32) -> Self {
        Self(index)
    }
}

/// Values in the first segment of [`CounterValues`]. Each next segment holds twice as many.
const FIRST_SEGMENT: usize = 16;
/// Segments of [`CounterValues`]: room for about 10^9 counters in one container.
const SEGMENTS: usize = 26;

/// The counter values of a container. A value is an atomic in a segment that never moves, so
/// an update through a [`CounterIndex`] takes no lock.
#[derive(Debug, Default)]
struct CounterValues {
    segments: [OnceLock<Box<[AtomicI64]>>; SEGMENTS],
}

impl CounterValues {
    /// The segment of `index` and the position in it.
    fn locate(CounterIndex(index): CounterIndex) -> (usize, usize) {
        let index = index as usize;
        let segment = (index / FIRST_SEGMENT + 1).ilog2() as usize;
        (segment, index - FIRST_SEGMENT * ((1 << segment) - 1))
    }

    fn get(&self, index: CounterIndex) -> Option<&AtomicI64> {
        let (segment, offset) = Self::locate(index);
        self.segments.get(segment)?.get()?.get(offset)
    }

    /// Returns the value at `index`, and allocates its segment on first use.
    fn get_or_allocate(&self, index: CounterIndex) -> Option<&AtomicI64> {
        let (segment, offset) = Self::locate(index);
        self.segments
            .get(segment)?
            .get_or_init(|| {
                (0..FIRST_SEGMENT << segment)
                    .map(|_| AtomicI64::new(0))
                    .collect()
            })
            .get(offset)
    }
}

/// Collects the user metrics of an active bundle or runner execution. Cells use foldhash maps
/// for speed; getters return `HashMap` copies to keep the hasher out of the public API.
#[derive(Debug, Default)]
pub struct MetricsContainer {
    // The bundle thread writes and the progress reporter reads, so the lock is not contended.
    cells: Mutex<Cells>,
    counter_values: CounterValues,
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

/// The `MonitoringInfo` of one cell.
fn monitoring_info<V: MetricCell>(key: &MetricKey, value: &V) -> MonitoringInfo {
    MonitoringInfo {
        urn: V::URN.to_string(),
        r#type: V::TYPE_URN.to_string(),
        payload: value.encode_cell(),
        labels: user_metric_labels(key),
        start_time: None,
    }
}

/// The `MonitoringInfo` of each cell in `cells`.
fn monitoring_infos<V: MetricCell>(
    cells: &FastHashMap<MetricKey, V>,
) -> impl Iterator<Item = MonitoringInfo> + '_ {
    cells.iter().map(|(key, value)| monitoring_info(key, value))
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
        if let Some(index) = self.counter_index(transform_id, namespace, name) {
            self.add_to_counter(index, delta);
        }
    }

    /// The index of a counter, created at zero on first use. `None` when the container has
    /// no room for another counter: then the update is dropped, with one warning per process.
    pub(crate) fn counter_index(
        &self,
        transform_id: &str,
        namespace: &str,
        name: &str,
    ) -> Option<CounterIndex> {
        let mut cells = self.cells();
        let key = (transform_id, namespace, name);
        if let Some(&index) = cells.counters.get(&key as &dyn MetricKeyParts) {
            return Some(index);
        }
        let index = u32::try_from(cells.counters.len())
            .ok()
            .map(CounterIndex)
            .filter(|&index| self.counter_values.get_or_allocate(index).is_some());
        let Some(index) = index else {
            static FULL: std::sync::Once = std::sync::Once::new();
            FULL.call_once(|| {
                tracing::warn!(
                    "A metrics container is full: counter {namespace}/{name} is dropped"
                );
            });
            return None;
        };
        cells
            .counters
            .insert(MetricKey::new(transform_id, namespace, name), index);
        Some(index)
    }

    /// Adds `delta` to the counter at `index`. A counter wraps on overflow.
    pub(crate) fn add_to_counter(&self, index: CounterIndex, delta: i64) {
        if let Some(value) = self.counter_values.get(index) {
            value.fetch_add(delta, Ordering::Relaxed);
        }
    }

    fn counter_value(&self, index: CounterIndex) -> i64 {
        self.counter_values
            .get(index)
            .map_or(0, |value| value.load(Ordering::Relaxed))
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
        self.cells()
            .counters
            .iter()
            .map(|(key, &index)| (key.clone(), self.counter_value(index)))
            .collect()
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
        let counters = cells
            .counters
            .iter()
            .map(|(key, &index)| monitoring_info(key, &self.counter_value(index)));
        counters
            .chain(monitoring_infos(&cells.distributions))
            .chain(monitoring_infos(&cells.gauges))
            .chain(monitoring_infos(&cells.string_sets))
            .chain(monitoring_infos(&cells.bounded_tries))
            .chain(monitoring_infos(&cells.histograms))
            .collect()
    }
}
