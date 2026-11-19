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
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use beam_model::pipeline::MonitoringInfo;

use super::io::{RequestCell, RequestLabels};
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

/// The cells of a container, one map for each kind. Counters, distributions, gauges and
/// histograms map to their index in [`SegmentedCells`].
#[derive(Debug, Default)]
struct Cells {
    counters: FastHashMap<MetricKey, CellIndex>,
    distributions: FastHashMap<MetricKey, CellIndex>,
    gauges: FastHashMap<MetricKey, CellIndex>,
    string_sets: FastHashMap<MetricKey, StringSetValue>,
    bounded_tries: FastHashMap<MetricKey, BoundedTrieValue>,
    histograms: FastHashMap<MetricKey, CellIndex>,
    api_requests: FastHashMap<Arc<RequestLabels>, RequestCell>,
}

/// The position of a metric cell in [`SegmentedCells`]. An index does not change while its
/// container lives, so a metric handle can keep it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CellIndex(u32);

impl From<CellIndex> for u32 {
    fn from(CellIndex(index): CellIndex) -> Self {
        index
    }
}

impl From<u32> for CellIndex {
    fn from(index: u32) -> Self {
        Self(index)
    }
}

/// Values in the first segment of [`SegmentedCells`]. Each next segment holds twice as many.
const FIRST_SEGMENT: usize = 16;
/// Segments of [`SegmentedCells`]: room for about 10^9 cells in one container.
const SEGMENTS: usize = 26;

/// Segmented storage for metric cells. Segments never move, so an update through a
/// [`CellIndex`] skips the container map lock.
#[derive(Debug)]
struct SegmentedCells<T> {
    segments: [OnceLock<Box<[T]>>; SEGMENTS],
}

impl<T> Default for SegmentedCells<T> {
    fn default() -> Self {
        Self {
            segments: [const { OnceLock::new() }; SEGMENTS],
        }
    }
}

impl<T: Default> SegmentedCells<T> {
    /// The segment of `index` and the position in it.
    fn locate(CellIndex(index): CellIndex) -> (usize, usize) {
        let index = index as usize;
        let segment = (index / FIRST_SEGMENT + 1).ilog2() as usize;
        (segment, index - FIRST_SEGMENT * ((1 << segment) - 1))
    }

    fn get(&self, index: CellIndex) -> Option<&T> {
        let (segment, offset) = Self::locate(index);
        self.segments.get(segment)?.get()?.get(offset)
    }

    /// Returns the cell at `index`, and allocates its segment on first use.
    fn get_or_allocate(&self, index: CellIndex) -> Option<&T> {
        let (segment, offset) = Self::locate(index);
        self.segments
            .get(segment)?
            .get_or_init(|| {
                (0..FIRST_SEGMENT << segment)
                    .map(|_| T::default())
                    .collect()
            })
            .get(offset)
    }
}

/// Increments `cell` by 1, saturating at `i64::MAX`.
fn saturating_inc(cell: &AtomicI64, ordering: Ordering) {
    let _ = cell.fetch_update(ordering, Ordering::Relaxed, |current| {
        Some(current.saturating_add(1))
    });
}

/// Lock-free statistics of one distribution cell.
#[derive(Debug)]
struct AtomicDistribution {
    count: AtomicI64,
    sum: AtomicI64,
    min: AtomicI64,
    max: AtomicI64,
}

impl Default for AtomicDistribution {
    fn default() -> Self {
        Self {
            count: AtomicI64::new(0),
            sum: AtomicI64::new(0),
            min: AtomicI64::new(i64::MAX),
            max: AtomicI64::new(i64::MIN),
        }
    }
}

impl AtomicDistribution {
    fn update(&self, value: i64) {
        self.sum.fetch_add(value, Ordering::Relaxed);
        if value < self.min.load(Ordering::Relaxed) {
            self.min.fetch_min(value, Ordering::Relaxed);
        }
        if value > self.max.load(Ordering::Relaxed) {
            self.max.fetch_max(value, Ordering::Relaxed);
        }
        saturating_inc(&self.count, Ordering::Release);
    }

    fn snapshot(&self) -> Option<DistributionValue> {
        let count = self.count.load(Ordering::Acquire);
        if count == 0 {
            return None;
        }
        Some(DistributionValue {
            count,
            sum: self.sum.load(Ordering::Relaxed),
            min: self.min.load(Ordering::Relaxed),
            max: self.max.load(Ordering::Relaxed),
        })
    }
}

/// One gauge cell guarded by its own lock, so `(value, timestamp_ms)` updates atomically
/// without locking the container map.
#[derive(Debug, Default)]
struct GaugeCell(Mutex<Option<GaugeValue>>);

impl GaugeCell {
    fn lock(&self) -> MutexGuard<'_, Option<GaugeValue>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn set(&self, value: i64, timestamp_ms: i64) {
        let mut cell = self.lock();
        match cell.as_mut() {
            Some(gauge) => gauge.set(value, timestamp_ms),
            None => *cell = Some(GaugeValue::new(value, timestamp_ms)),
        }
    }

    fn snapshot(&self) -> Option<GaugeValue> {
        self.lock().clone()
    }
}

/// Lock-free bucket counts and total of one histogram cell.
#[derive(Debug)]
struct AtomicHistogram {
    buckets: HistogramBuckets,
    counts: Box<[AtomicI64]>,
    total: AtomicI64,
}

impl AtomicHistogram {
    fn new(buckets: HistogramBuckets) -> Self {
        let counts = (0..buckets.count()).map(|_| AtomicI64::new(0)).collect();
        Self {
            buckets,
            counts,
            total: AtomicI64::new(0),
        }
    }

    fn record(&self, value: f64) {
        if value.is_nan() {
            return;
        }
        saturating_inc(&self.total, Ordering::Relaxed);
        if let Some(index) = self.buckets.bucket_index(value)
            && let Some(count) = self.counts.get(index)
        {
            saturating_inc(count, Ordering::Release);
        }
    }

    fn snapshot(&self) -> HistogramValue {
        // Acquire on counts pairs with Release in `record`, so `sum(counts) <= total`.
        let counts = self
            .counts
            .iter()
            .map(|c| c.load(Ordering::Acquire))
            .collect();
        let total = self.total.load(Ordering::Relaxed);
        HistogramValue::from_parts(self.buckets, counts, total)
    }
}

/// One histogram cell whose buckets are fixed on first use.
#[derive(Debug, Default)]
struct HistogramCell(OnceLock<AtomicHistogram>);

impl HistogramCell {
    fn init(&self, buckets: HistogramBuckets) {
        let _ = self.0.get_or_init(|| AtomicHistogram::new(buckets));
    }

    fn record(&self, value: f64) {
        if let Some(histogram) = self.0.get() {
            histogram.record(value);
        }
    }

    fn snapshot(&self) -> Option<HistogramValue> {
        self.0.get().map(AtomicHistogram::snapshot)
    }
}

/// Returns the cell index for `key`, allocating a cell on first use and running `init` on it.
/// Returns `None` when the container has no room for another cell, with one warning per process.
fn resolve_index<T: Default>(
    map: &mut FastHashMap<MetricKey, CellIndex>,
    values: &SegmentedCells<T>,
    kind: &str,
    key: (&str, &str, &str),
    init: impl FnOnce(&T),
) -> Option<CellIndex> {
    if let Some(&index) = map.get(&key as &dyn MetricKeyParts) {
        return Some(index);
    }
    let index = u32::try_from(map.len()).ok().map(CellIndex);
    let cell = index.and_then(|i| values.get_or_allocate(i));
    let Some((index, cell)) = index.zip(cell) else {
        static FULL: std::sync::Once = std::sync::Once::new();
        FULL.call_once(|| {
            let (_, namespace, name) = key;
            tracing::warn!("A metrics container is full: {kind} {namespace}/{name} is dropped");
        });
        return None;
    };
    init(cell);
    map.insert(MetricKey::new(key.0, key.1, key.2), index);
    Some(index)
}

/// Collects the user metrics of an active bundle or runner execution. Cells use foldhash maps
/// for speed; getters return `HashMap` copies to keep the hasher out of the public API.
#[derive(Debug, Default)]
pub struct MetricsContainer {
    // The bundle thread writes and the progress reporter reads, so the lock is not contended.
    cells: Mutex<Cells>,
    counter_values: SegmentedCells<AtomicI64>,
    distribution_values: SegmentedCells<AtomicDistribution>,
    gauge_values: SegmentedCells<GaugeCell>,
    histogram_values: SegmentedCells<HistogramCell>,
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
    ) -> Option<CellIndex> {
        resolve_index(
            &mut self.cells().counters,
            &self.counter_values,
            "counter",
            (transform_id, namespace, name),
            |_| {},
        )
    }

    /// Adds `delta` to the counter at `index`. A counter wraps on overflow.
    pub(crate) fn add_to_counter(&self, index: CellIndex, delta: i64) {
        if let Some(value) = self.counter_values.get(index) {
            value.fetch_add(delta, Ordering::Relaxed);
        }
    }

    fn counter_value(&self, index: CellIndex) -> i64 {
        self.counter_values
            .get(index)
            .map_or(0, |value| value.load(Ordering::Relaxed))
    }

    pub fn update_distribution(&self, transform_id: &str, namespace: &str, name: &str, value: i64) {
        if let Some(index) = self.distribution_index(transform_id, namespace, name) {
            self.record_distribution(index, value);
        }
    }

    /// The index of a distribution, created empty on first use.
    pub(crate) fn distribution_index(
        &self,
        transform_id: &str,
        namespace: &str,
        name: &str,
    ) -> Option<CellIndex> {
        resolve_index(
            &mut self.cells().distributions,
            &self.distribution_values,
            "distribution",
            (transform_id, namespace, name),
            |_| {},
        )
    }

    /// Records `value` in the distribution at `index`.
    pub(crate) fn record_distribution(&self, index: CellIndex, value: i64) {
        if let Some(cell) = self.distribution_values.get(index) {
            cell.update(value);
        }
    }

    fn distribution_value(&self, index: CellIndex) -> Option<DistributionValue> {
        self.distribution_values.get(index)?.snapshot()
    }

    pub fn set_gauge(
        &self,
        transform_id: &str,
        namespace: &str,
        name: &str,
        value: i64,
        timestamp_ms: i64,
    ) {
        if let Some(index) = self.gauge_index(transform_id, namespace, name) {
            self.record_gauge(index, value, timestamp_ms);
        }
    }

    /// The index of a gauge, created empty on first use.
    pub(crate) fn gauge_index(
        &self,
        transform_id: &str,
        namespace: &str,
        name: &str,
    ) -> Option<CellIndex> {
        resolve_index(
            &mut self.cells().gauges,
            &self.gauge_values,
            "gauge",
            (transform_id, namespace, name),
            |_| {},
        )
    }

    /// Sets the gauge at `index` if `timestamp_ms` is at least its current timestamp.
    pub(crate) fn record_gauge(&self, index: CellIndex, value: i64, timestamp_ms: i64) {
        if let Some(cell) = self.gauge_values.get(index) {
            cell.set(value, timestamp_ms);
        }
    }

    fn gauge_value(&self, index: CellIndex) -> Option<GaugeValue> {
        self.gauge_values.get(index)?.snapshot()
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
        if let Some(index) = self.histogram_index(transform_id, namespace, name, buckets) {
            self.record_histogram(index, value);
        }
    }

    /// The index of a histogram. The first call for a key sets its buckets.
    pub(crate) fn histogram_index(
        &self,
        transform_id: &str,
        namespace: &str,
        name: &str,
        buckets: HistogramBuckets,
    ) -> Option<CellIndex> {
        resolve_index(
            &mut self.cells().histograms,
            &self.histogram_values,
            "histogram",
            (transform_id, namespace, name),
            |cell| cell.init(buckets),
        )
    }

    /// Counts `value` in the histogram at `index`. A NaN value is ignored.
    pub(crate) fn record_histogram(&self, index: CellIndex, value: f64) {
        if let Some(cell) = self.histogram_values.get(index) {
            cell.record(value);
        }
    }

    fn histogram_value(&self, index: CellIndex) -> Option<HistogramValue> {
        self.histogram_values.get(index)?.snapshot()
    }

    pub fn counters(&self) -> HashMap<MetricKey, i64> {
        self.cells()
            .counters
            .iter()
            .map(|(key, &index)| (key.clone(), self.counter_value(index)))
            .collect()
    }

    pub fn distributions(&self) -> HashMap<MetricKey, DistributionValue> {
        self.cells()
            .distributions
            .iter()
            .filter_map(|(key, &index)| Some((key.clone(), self.distribution_value(index)?)))
            .collect()
    }

    pub fn gauges(&self) -> HashMap<MetricKey, GaugeValue> {
        let entries: Vec<(MetricKey, CellIndex)> = self
            .cells()
            .gauges
            .iter()
            .map(|(key, &index)| (key.clone(), index))
            .collect();
        entries
            .into_iter()
            .filter_map(|(key, index)| Some((key, self.gauge_value(index)?)))
            .collect()
    }

    pub fn string_sets(&self) -> HashMap<MetricKey, StringSetValue> {
        snapshot(&self.cells().string_sets)
    }

    pub fn bounded_tries(&self) -> HashMap<MetricKey, BoundedTrieValue> {
        snapshot(&self.cells().bounded_tries)
    }

    pub fn histograms(&self) -> HashMap<MetricKey, HistogramValue> {
        self.cells()
            .histograms
            .iter()
            .filter_map(|(key, &index)| Some((key.clone(), self.histogram_value(index)?)))
            .collect()
    }

    /// Counts one call with `labels` that ended with `status`, and records its latency.
    pub(crate) fn record_api_request(
        &self,
        labels: &Arc<RequestLabels>,
        status: &'static str,
        latency_ms: f64,
    ) {
        let mut cells = self.cells();
        match cells.api_requests.get_mut(labels.as_ref()) {
            Some(cell) => cell.record(status, latency_ms),
            None => {
                let mut cell = RequestCell::default();
                cell.record(status, latency_ms);
                cells.api_requests.insert(Arc::clone(labels), cell);
            }
        }
    }

    /// Converts all collected metrics to `MonitoringInfo` messages.
    pub fn to_monitoring_infos(&self) -> Vec<MonitoringInfo> {
        let (mut infos, gauges) = {
            let cells = self.cells();
            let counters = cells
                .counters
                .iter()
                .map(|(key, &index)| monitoring_info(key, &self.counter_value(index)));
            let distributions = cells.distributions.iter().filter_map(|(key, &index)| {
                let value = self.distribution_value(index)?;
                Some(monitoring_info(key, &value))
            });
            let histograms = cells.histograms.iter().filter_map(|(key, &index)| {
                let value = self.histogram_value(index)?;
                Some(monitoring_info(key, &value))
            });
            let infos: Vec<MonitoringInfo> = counters
                .chain(distributions)
                .chain(monitoring_infos(&cells.string_sets))
                .chain(monitoring_infos(&cells.bounded_tries))
                .chain(histograms)
                .chain(
                    cells
                        .api_requests
                        .iter()
                        .flat_map(|(labels, cell)| cell.monitoring_infos(labels)),
                )
                .collect();
            let gauges: Vec<(MetricKey, CellIndex)> = cells
                .gauges
                .iter()
                .map(|(key, &index)| (key.clone(), index))
                .collect();
            (infos, gauges)
        };
        infos.extend(gauges.into_iter().filter_map(|(key, index)| {
            let value = self.gauge_value(index)?;
            Some(monitoring_info(&key, &value))
        }));
        infos
    }
}
