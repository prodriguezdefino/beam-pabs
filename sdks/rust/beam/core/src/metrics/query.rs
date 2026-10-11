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

//! Query results and filters for pipeline metrics.

use std::collections::{HashMap, HashSet};

use model::pipeline::MonitoringInfo;

use super::URN_ELEMENT_COUNT;
use super::context::{MetricKey, MetricKeyParts, MetricsContainer};
use super::values::{
    BoundedTrieValue, DistributionValue, GaugeValue, HistogramValue, MetricCell, StringSetValue,
};
use super::{LABEL_NAME, LABEL_NAMESPACE, LABEL_PCOLLECTION, LABEL_PTRANSFORM};

/// Selects metrics by namespace, name or transform ID. A `None` field matches all values; a
/// set matches only its members.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricFilter {
    pub namespaces: Option<HashSet<String>>,
    pub names: Option<HashSet<String>>,
    pub transform_ids: Option<HashSet<String>>,
}

fn allows(constraint: &Option<HashSet<String>>, value: &str) -> bool {
    constraint.as_ref().is_none_or(|set| set.contains(value))
}

impl MetricFilter {
    /// Adds `namespace` to the set of namespaces that the filter matches.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespaces
            .get_or_insert_with(HashSet::new)
            .insert(namespace.into());
        self
    }

    /// Adds `name` to the set of metric names that the filter matches.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.names
            .get_or_insert_with(HashSet::new)
            .insert(name.into());
        self
    }

    /// Adds `transform_id` to the set of PTransforms that the filter matches.
    pub fn with_transform(mut self, transform_id: impl Into<String>) -> Self {
        self.transform_ids
            .get_or_insert_with(HashSet::new)
            .insert(transform_id.into());
        self
    }

    /// Returns `true` if `key` passes all constraints of this filter.
    pub fn matches(&self, key: &MetricKey) -> bool {
        allows(&self.namespaces, &key.namespace)
            && allows(&self.names, &key.name)
            && allows(&self.transform_ids, &key.transform_id)
    }
}

/// The committed and attempted values of one metric cell. Each value is optional.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricResult<T> {
    pub key: MetricKey,
    pub committed: Option<T>,
    pub attempted: Option<T>,
}

impl<T> MetricResult<T> {
    pub fn new(key: MetricKey, committed: Option<T>, attempted: Option<T>) -> Self {
        Self {
            key,
            committed,
            attempted,
        }
    }
}

impl<T: Clone> MetricResult<T> {
    /// Returns the committed value if present. Otherwise returns the attempted value.
    pub fn result(&self) -> Option<T> {
        self.committed.as_ref().or(self.attempted.as_ref()).cloned()
    }
}

/// A snapshot of all metrics that a pipeline execution collected.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricResults {
    counters: HashMap<MetricKey, MetricResult<i64>>,
    distributions: HashMap<MetricKey, MetricResult<DistributionValue>>,
    gauges: HashMap<MetricKey, MetricResult<GaugeValue>>,
    string_sets: HashMap<MetricKey, MetricResult<StringSetValue>>,
    bounded_tries: HashMap<MetricKey, MetricResult<BoundedTrieValue>>,
    histograms: HashMap<MetricKey, MetricResult<HistogramValue>>,
}

/// Whether a runner reports a metric value as final or as provisional.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricPhase {
    /// Counted only from work the runner committed.
    Committed,
    /// Counted from every attempt, including work that may be retried.
    Attempted,
}

/// The value of one metric cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricValue {
    Counter(i64),
    Distribution(DistributionValue),
    Gauge(GaugeValue),
    StringSet(StringSetValue),
    BoundedTrie(BoundedTrieValue),
    Histogram(HistogramValue),
}

/// One value a runner reported for a metric cell. A runner with its own metrics format can
/// collect readings into a [`MetricResults`]:
///
/// ```
/// use beam::metrics::{MetricKey, MetricPhase, MetricReading, MetricResults, MetricValue};
///
/// let results: MetricResults = [MetricReading {
///     key: MetricKey::new("step", "ns", "count"),
///     phase: MetricPhase::Committed,
///     value: MetricValue::Counter(3),
/// }]
/// .into_iter()
/// .collect();
/// assert_eq!(results.counter("ns", "count"), Some(3));
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricReading {
    pub key: MetricKey,
    pub phase: MetricPhase,
    pub value: MetricValue,
}

impl MetricReading {
    /// Decodes a `MonitoringInfo` that a runner reported in `phase`. Returns `None` for
    /// malformed payloads and for kinds that [`MetricValue`] does not hold.
    pub fn from_monitoring_info(info: &MonitoringInfo, phase: MetricPhase) -> Option<Self> {
        let value = decode_value(info)?;
        Some(Self {
            key: key_from_labels(info),
            phase,
            value,
        })
    }

    /// Returns the committed and attempted readings of a final value.
    fn settled(key: MetricKey, value: MetricValue) -> [Self; 2] {
        [
            Self {
                key: key.clone(),
                phase: MetricPhase::Committed,
                value: value.clone(),
            },
            Self {
                key,
                phase: MetricPhase::Attempted,
                value,
            },
        ]
    }
}

/// The cell of kind `V` in `info`, if the URN or the type names `V`. `Some(None)` for a
/// malformed payload.
fn cell<V: MetricCell>(info: &MonitoringInfo) -> Option<Option<V>> {
    (info.urn == V::URN || info.r#type == V::TYPE_URN).then(|| V::decode_cell(&info.payload).ok())
}

/// Decodes the value of `info`, or `None` for a kind that [`MetricValue`] does not hold.
fn decode_value(info: &MonitoringInfo) -> Option<MetricValue> {
    let element_count = (info.urn == URN_ELEMENT_COUNT)
        .then(|| i64::decode_cell(&info.payload).ok())
        .or_else(|| cell::<i64>(info));
    element_count
        .map(|v| v.map(MetricValue::Counter))
        .or_else(|| cell(info).map(|v| v.map(MetricValue::Distribution)))
        .or_else(|| cell(info).map(|v| v.map(MetricValue::Gauge)))
        .or_else(|| cell(info).map(|v| v.map(MetricValue::StringSet)))
        .or_else(|| cell(info).map(|v| v.map(MetricValue::BoundedTrie)))
        .or_else(|| cell(info).map(|v| v.map(MetricValue::Histogram)))
        .flatten()
}

fn label<'a>(info: &'a MonitoringInfo, name: &str) -> Option<&'a str> {
    info.labels.get(name).map(String::as_str)
}

/// Returns the key of the cell that a `MonitoringInfo` reports. A system metric has no
/// namespace or name labels, so it gets `beam` as namespace and its URN as name.
fn key_from_labels(info: &MonitoringInfo) -> MetricKey {
    let name = label(info, LABEL_NAME);
    let namespace = match (label(info, LABEL_NAMESPACE), name) {
        (Some(namespace), _) => namespace,
        (None, Some(_)) => "",
        (None, None) => "beam",
    };
    let transform_id = label(info, LABEL_PTRANSFORM)
        .or_else(|| label(info, LABEL_PCOLLECTION))
        .unwrap_or_default();
    MetricKey::new(transform_id, namespace, name.unwrap_or(&info.urn))
}

/// Sets the `phase` value of the cell `key` in `map`, creating the cell if needed.
fn upsert<T: Clone>(
    map: &mut HashMap<MetricKey, MetricResult<T>>,
    key: MetricKey,
    phase: MetricPhase,
    value: T,
) {
    let cell = map
        .entry(key.clone())
        .or_insert_with(|| MetricResult::new(key, None, None));
    match phase {
        MetricPhase::Committed => cell.committed = Some(value),
        MetricPhase::Attempted => cell.attempted = Some(value),
    }
}

/// The values of the cells in `map` named `namespace`/`name`, across all transforms.
fn matching<'a, T: Clone>(
    map: &'a HashMap<MetricKey, MetricResult<T>>,
    namespace: &'a str,
    name: &'a str,
) -> impl Iterator<Item = (&'a MetricKey, T)> + 'a {
    map.iter()
        .filter(move |(key, _)| key.namespace == namespace && key.name == name)
        .filter_map(|(key, cell)| cell.result().map(|value| (key, value)))
}

/// The value of the single cell `transform_id`/`namespace`/`name` in `map`.
fn get_for<T: Clone>(
    map: &HashMap<MetricKey, MetricResult<T>>,
    transform_id: &str,
    namespace: &str,
    name: &str,
) -> Option<T> {
    let key = (transform_id, namespace, name);
    map.get(&key as &dyn MetricKeyParts)
        .and_then(MetricResult::result)
}

fn query<T: Clone>(
    map: &HashMap<MetricKey, MetricResult<T>>,
    filter: &MetricFilter,
) -> Vec<MetricResult<T>> {
    map.iter()
        .filter(|(key, _)| filter.matches(key))
        .map(|(_, cell)| cell.clone())
        .collect()
}

/// The cells in `map` named `namespace`/`name`, combined in transform order.
fn aggregate<V: MetricCell>(
    map: &HashMap<MetricKey, MetricResult<V>>,
    namespace: &str,
    name: &str,
) -> Option<V> {
    let mut cells: Vec<(&MetricKey, V)> = matching(map, namespace, name).collect();
    cells.sort_unstable_by_key(|(key, _)| *key);
    cells
        .into_iter()
        .map(|(_, value)| value)
        .reduce(|mut acc, value| {
            acc.combine(&value);
            acc
        })
}

/// The four accessors of one kind: combined by name, for one transform, by filter, and all.
macro_rules! accessors {
    ($field:ident: $value:ty, $combined:literal, $get:ident, $get_for:ident, $query:ident, $all:ident) => {
        #[doc = concat!("Returns the ", $combined, " of all cells with this namespace and name.")]
        pub fn $get(&self, namespace: &str, name: &str) -> Option<$value> {
            aggregate(&self.$field, namespace, name)
        }

        /// Returns the value for one transform or PCollection, namespace and name. Matches
        /// PCollection IDs when PTransform is absent.
        pub fn $get_for(&self, transform_id: &str, namespace: &str, name: &str) -> Option<$value> {
            get_for(&self.$field, transform_id, namespace, name)
        }

        /// Returns the cells that `filter` matches.
        pub fn $query(&self, filter: &MetricFilter) -> Vec<MetricResult<$value>> {
            query(&self.$field, filter)
        }

        /// Returns all cells of this kind.
        pub fn $all(&self) -> Vec<MetricResult<$value>> {
            query(&self.$field, &MetricFilter::default())
        }
    };
}

impl Extend<MetricReading> for MetricResults {
    fn extend<I: IntoIterator<Item = MetricReading>>(&mut self, readings: I) {
        readings.into_iter().for_each(|r| self.record(r));
    }
}

impl FromIterator<MetricReading> for MetricResults {
    fn from_iter<I: IntoIterator<Item = MetricReading>>(readings: I) -> Self {
        let mut results = Self::new();
        results.extend(readings);
        results
    }
}

impl MetricResults {
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if there are no tracked metrics.
    pub fn is_empty(&self) -> bool {
        self.counters.is_empty()
            && self.distributions.is_empty()
            && self.gauges.is_empty()
            && self.string_sets.is_empty()
            && self.bounded_tries.is_empty()
            && self.histograms.is_empty()
    }

    /// Records one reading. It replaces the earlier value of the same phase for its cell.
    fn record(&mut self, reading: MetricReading) {
        let MetricReading { key, phase, value } = reading;
        match value {
            MetricValue::Counter(v) => upsert(&mut self.counters, key, phase, v),
            MetricValue::Distribution(v) => upsert(&mut self.distributions, key, phase, v),
            MetricValue::Gauge(v) => upsert(&mut self.gauges, key, phase, v),
            MetricValue::StringSet(v) => upsert(&mut self.string_sets, key, phase, v),
            MetricValue::BoundedTrie(v) => upsert(&mut self.bounded_tries, key, phase, v),
            MetricValue::Histogram(v) => upsert(&mut self.histograms, key, phase, v),
        }
    }

    /// Builds a [`MetricResults`] from lists of attempted and committed `MonitoringInfo`s.
    /// Skips unindexed kinds and malformed payloads. For a repeated cell the last value wins.
    pub fn from_monitoring_infos(
        attempted: &[MonitoringInfo],
        committed: &[MonitoringInfo],
    ) -> Self {
        attempted
            .iter()
            .map(|info| (info, MetricPhase::Attempted))
            .chain(committed.iter().map(|info| (info, MetricPhase::Committed)))
            .filter_map(|(info, phase)| MetricReading::from_monitoring_info(info, phase))
            .collect()
    }

    /// Builds a snapshot from a worker's [`MetricsContainer`](crate::internals::MetricsContainer).
    /// Each value is final, so it is both committed and attempted.
    pub fn from_container(container: &MetricsContainer) -> Self {
        fn settled<V>(
            cells: HashMap<MetricKey, V>,
            wrap: fn(V) -> MetricValue,
        ) -> impl Iterator<Item = MetricReading> {
            cells
                .into_iter()
                .flat_map(move |(key, value)| MetricReading::settled(key, wrap(value)))
        }
        settled(container.counters(), MetricValue::Counter)
            .chain(settled(
                container.distributions(),
                MetricValue::Distribution,
            ))
            .chain(settled(container.gauges(), MetricValue::Gauge))
            .chain(settled(container.string_sets(), MetricValue::StringSet))
            .chain(settled(container.bounded_tries(), MetricValue::BoundedTrie))
            .chain(settled(container.histograms(), MetricValue::Histogram))
            .collect()
    }

    accessors!(counters: i64, "sum", counter, counter_for_transform, query_counters, all_counters);
    accessors!(
        distributions: DistributionValue,
        "merged distribution",
        distribution,
        distribution_for_transform,
        query_distributions,
        all_distributions
    );
    accessors!(
        gauges: GaugeValue,
        "latest value (a tie goes to the greatest transform ID)",
        gauge,
        gauge_for_transform,
        query_gauges,
        all_gauges
    );
    accessors!(
        string_sets: StringSetValue,
        "union",
        string_set,
        string_set_for_transform,
        query_string_sets,
        all_string_sets
    );
    accessors!(
        bounded_tries: BoundedTrieValue,
        "merged trie",
        bounded_trie,
        bounded_trie_for_transform,
        query_bounded_tries,
        all_bounded_tries
    );
    accessors!(
        histograms: HistogramValue,
        "merged histogram (cells with other buckets are skipped)",
        histogram,
        histogram_for_transform,
        query_histograms,
        all_histograms
    );
}
