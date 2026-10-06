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

use super::context::{DistributionValue, GaugeValue, MetricKey, MetricsContainer};
use super::{
    LABEL_NAME, LABEL_NAMESPACE, LABEL_PCOLLECTION, LABEL_PTRANSFORM, TYPE_DISTRIBUTION_INT64,
    TYPE_LATEST_INT64, TYPE_SUM_INT64, URN_ELEMENT_COUNT, URN_USER_DISTRIBUTION_INT64,
    URN_USER_LATEST_INT64, URN_USER_SUM_INT64,
};
use crate::coders::VarIntCoder;

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
    /// Creates a filter that matches all metrics. Same as [`MetricFilter::all`].
    pub fn empty() -> Self {
        Self::all()
    }

    /// Creates a filter that matches all metrics.
    pub fn all() -> Self {
        Self::default()
    }

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

impl<T: Clone> MetricResult<T> {
    pub fn new(key: MetricKey, committed: Option<T>, attempted: Option<T>) -> Self {
        Self {
            key,
            committed,
            attempted,
        }
    }

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
    /// malformed payloads and for kinds other than int64 sums, distributions and latest values.
    pub fn from_monitoring_info(info: &MonitoringInfo, phase: MetricPhase) -> Option<Self> {
        let value = MetricKind::of(info)?.decode(&info.payload)?;
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

/// The kind of cell that a `MonitoringInfo` reports. The URN or the type sets the kind.
#[derive(Debug, Clone, Copy)]
enum MetricKind {
    Sum,
    Distribution,
    Latest,
}

impl MetricKind {
    fn of(info: &MonitoringInfo) -> Option<Self> {
        match (info.urn.as_str(), info.r#type.as_str()) {
            (URN_USER_SUM_INT64 | URN_ELEMENT_COUNT, _) | (_, TYPE_SUM_INT64) => Some(Self::Sum),
            (URN_USER_DISTRIBUTION_INT64, _) | (_, TYPE_DISTRIBUTION_INT64) => {
                Some(Self::Distribution)
            }
            (URN_USER_LATEST_INT64, _) | (_, TYPE_LATEST_INT64) => Some(Self::Latest),
            _ => None,
        }
    }

    fn decode(self, payload: &[u8]) -> Option<MetricValue> {
        match self {
            Self::Sum => VarIntCoder::decode_varint(&mut &payload[..])
                .ok()
                .map(MetricValue::Counter),
            Self::Distribution => DistributionValue::decode_payload(payload)
                .ok()
                .map(MetricValue::Distribution),
            Self::Latest => GaugeValue::decode_payload(payload)
                .ok()
                .map(MetricValue::Gauge),
        }
    }
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
    map.get(&MetricKey::new(transform_id, namespace, name))
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
        self.counters.is_empty() && self.distributions.is_empty() && self.gauges.is_empty()
    }

    /// Records one reading. It replaces the earlier value of the same phase for its cell.
    fn record(&mut self, reading: MetricReading) {
        let MetricReading { key, phase, value } = reading;
        match value {
            MetricValue::Counter(v) => upsert(&mut self.counters, key, phase, v),
            MetricValue::Distribution(v) => upsert(&mut self.distributions, key, phase, v),
            MetricValue::Gauge(v) => upsert(&mut self.gauges, key, phase, v),
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

    /// Builds a snapshot from a worker's [`MetricsContainer`]. Each value is final, so it is
    /// both committed and attempted.
    pub fn from_container(container: &MetricsContainer) -> Self {
        let counters = container
            .counters()
            .into_iter()
            .map(|(key, v)| (key, MetricValue::Counter(v)));
        let distributions = container
            .distributions()
            .into_iter()
            .map(|(key, v)| (key, MetricValue::Distribution(v)));
        let gauges = container
            .gauges()
            .into_iter()
            .map(|(key, v)| (key, MetricValue::Gauge(v)));
        counters
            .chain(distributions)
            .chain(gauges)
            .flat_map(|(key, value)| MetricReading::settled(key, value))
            .collect()
    }

    /// Returns the sum of all counter cells with this namespace and name.
    pub fn counter(&self, namespace: &str, name: &str) -> Option<i64> {
        matching(&self.counters, namespace, name)
            .map(|(_, value)| value)
            .reduce(|total, value| total + value)
    }

    /// Returns the counter value for one transform, namespace and name.
    pub fn counter_for_transform(
        &self,
        transform_id: &str,
        namespace: &str,
        name: &str,
    ) -> Option<i64> {
        get_for(&self.counters, transform_id, namespace, name)
    }

    /// Returns the merged distribution of all cells with this namespace and name.
    pub fn distribution(&self, namespace: &str, name: &str) -> Option<DistributionValue> {
        matching(&self.distributions, namespace, name)
            .map(|(_, dist)| dist)
            .reduce(|mut acc, dist| {
                acc.merge(&dist);
                acc
            })
    }

    /// Returns the distribution for one transform, namespace and name.
    pub fn distribution_for_transform(
        &self,
        transform_id: &str,
        namespace: &str,
        name: &str,
    ) -> Option<DistributionValue> {
        get_for(&self.distributions, transform_id, namespace, name)
    }

    /// Returns the latest gauge value of all cells with this namespace and name. On equal
    /// timestamps the greatest transform ID wins, so the result is deterministic.
    pub fn gauge(&self, namespace: &str, name: &str) -> Option<GaugeValue> {
        matching(&self.gauges, namespace, name)
            .max_by_key(|(key, gauge)| (gauge.timestamp_ms, &key.transform_id))
            .map(|(_, gauge)| gauge)
    }

    /// Returns the gauge value for one transform, namespace and name.
    pub fn gauge_for_transform(
        &self,
        transform_id: &str,
        namespace: &str,
        name: &str,
    ) -> Option<GaugeValue> {
        get_for(&self.gauges, transform_id, namespace, name)
    }

    /// Returns the counter cells that `filter` matches.
    pub fn query_counters(&self, filter: &MetricFilter) -> Vec<MetricResult<i64>> {
        query(&self.counters, filter)
    }

    /// Returns the distribution cells that `filter` matches.
    pub fn query_distributions(
        &self,
        filter: &MetricFilter,
    ) -> Vec<MetricResult<DistributionValue>> {
        query(&self.distributions, filter)
    }

    /// Returns the gauge cells that `filter` matches.
    pub fn query_gauges(&self, filter: &MetricFilter) -> Vec<MetricResult<GaugeValue>> {
        query(&self.gauges, filter)
    }

    pub fn all_counters(&self) -> Vec<MetricResult<i64>> {
        query(&self.counters, &MetricFilter::all())
    }

    pub fn all_distributions(&self) -> Vec<MetricResult<DistributionValue>> {
        query(&self.distributions, &MetricFilter::all())
    }

    pub fn all_gauges(&self) -> Vec<MetricResult<GaugeValue>> {
        query(&self.gauges, &MetricFilter::all())
    }
}
