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
use std::sync::{Arc, Mutex};

use model::pipeline::MonitoringInfo;

use super::{
    LABEL_NAME, LABEL_NAMESPACE, LABEL_PTRANSFORM, TYPE_DISTRIBUTION_INT64, TYPE_LATEST_INT64,
    TYPE_SUM_INT64, URN_USER_DISTRIBUTION_INT64, URN_USER_LATEST_INT64, URN_USER_SUM_INT64,
};
use crate::coders::VarIntCoder;
use crate::internals::FastHashMap;

/// Key that identifies one user metric cell in a PTransform.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MetricKey {
    pub transform_id: String,
    pub namespace: String,
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
trait MetricKeyParts {
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

/// Statistics tracked by an integer distribution metric cell.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DistributionValue {
    pub count: i64,
    pub sum: i64,
    pub min: i64,
    pub max: i64,
}

impl DistributionValue {
    pub fn new(value: i64) -> Self {
        Self {
            count: 1,
            sum: value,
            min: value,
            max: value,
        }
    }

    pub fn update(&mut self, value: i64) {
        if self.count == 0 {
            *self = Self::new(value);
        } else {
            self.count += 1;
            self.sum += value;
            self.min = self.min.min(value);
            self.max = self.max.max(value);
        }
    }

    pub fn merge(&mut self, other: &Self) {
        if other.count == 0 {
            return;
        }
        if self.count == 0 {
            *self = other.clone();
            return;
        }
        self.count += other.count;
        self.sum += other.sum;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
    }

    /// Encodes the distribution as Fn API bytes: `<count><sum><min><max>` VarInts.
    pub fn encode_payload(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        let _ = VarIntCoder::encode_varint(self.count, &mut payload);
        let _ = VarIntCoder::encode_varint(self.sum, &mut payload);
        let _ = VarIntCoder::encode_varint(self.min, &mut payload);
        let _ = VarIntCoder::encode_varint(self.max, &mut payload);
        payload
    }

    /// Decodes a distribution from Fn API bytes: `<count><sum><min><max>` VarInts.
    pub fn decode_payload(payload: &[u8]) -> Result<Self, String> {
        let mut reader = payload;
        let count = VarIntCoder::decode_varint(&mut reader)
            .map_err(|e| format!("Failed to decode distribution count: {e}"))?;
        let sum = VarIntCoder::decode_varint(&mut reader)
            .map_err(|e| format!("Failed to decode distribution sum: {e}"))?;
        let min = VarIntCoder::decode_varint(&mut reader)
            .map_err(|e| format!("Failed to decode distribution min: {e}"))?;
        let max = VarIntCoder::decode_varint(&mut reader)
            .map_err(|e| format!("Failed to decode distribution max: {e}"))?;
        Ok(Self {
            count,
            sum,
            min,
            max,
        })
    }
}

/// A gauge value and its timestamp in milliseconds. The value with the latest timestamp wins.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GaugeValue {
    pub value: i64,
    pub timestamp_ms: i64,
}

impl GaugeValue {
    pub fn new(value: i64, timestamp_ms: i64) -> Self {
        Self {
            value,
            timestamp_ms,
        }
    }

    pub fn set(&mut self, value: i64, timestamp_ms: i64) {
        if timestamp_ms >= self.timestamp_ms {
            self.value = value;
            self.timestamp_ms = timestamp_ms;
        }
    }

    /// Encodes the gauge as Fn API bytes: `<timestamp><value>` VarInts.
    pub fn encode_payload(&self) -> Vec<u8> {
        let mut payload = Vec::new();
        let _ = VarIntCoder::encode_varint(self.timestamp_ms, &mut payload);
        let _ = VarIntCoder::encode_varint(self.value, &mut payload);
        payload
    }

    /// Decodes a gauge from Fn API bytes: `<timestamp><value>` VarInts.
    pub fn decode_payload(payload: &[u8]) -> Result<Self, String> {
        let mut reader = payload;
        let timestamp_ms = VarIntCoder::decode_varint(&mut reader)
            .map_err(|e| format!("Failed to decode gauge timestamp_ms: {e}"))?;
        let value = VarIntCoder::decode_varint(&mut reader)
            .map_err(|e| format!("Failed to decode gauge value: {e}"))?;
        Ok(Self {
            value,
            timestamp_ms,
        })
    }
}

/// Collects the user metrics of an active bundle or runner execution. Cells use foldhash maps
/// for speed; getters return `HashMap` copies to keep the hasher out of the public API.
#[derive(Debug, Default)]
pub struct MetricsContainer {
    counters: Mutex<FastHashMap<MetricKey, i64>>,
    distributions: Mutex<FastHashMap<MetricKey, DistributionValue>>,
    gauges: Mutex<FastHashMap<MetricKey, GaugeValue>>,
}

fn snapshot<V: Clone>(cells: &Mutex<FastHashMap<MetricKey, V>>) -> HashMap<MetricKey, V> {
    cells
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

impl MetricsContainer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_counter(&self, transform_id: &str, namespace: &str, name: &str, delta: i64) {
        let mut counters = self.counters.lock().unwrap_or_else(|p| p.into_inner());
        upsert(
            &mut counters,
            (transform_id, namespace, name),
            |c| *c += delta,
            || delta,
        );
    }

    pub fn update_distribution(&self, transform_id: &str, namespace: &str, name: &str, value: i64) {
        let mut dists = self.distributions.lock().unwrap_or_else(|p| p.into_inner());
        upsert(
            &mut dists,
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
        let mut gauges = self.gauges.lock().unwrap_or_else(|p| p.into_inner());
        upsert(
            &mut gauges,
            (transform_id, namespace, name),
            |g| g.set(value, timestamp_ms),
            || GaugeValue::new(value, timestamp_ms),
        );
    }

    pub fn counters(&self) -> HashMap<MetricKey, i64> {
        snapshot(&self.counters)
    }

    pub fn distributions(&self) -> HashMap<MetricKey, DistributionValue> {
        snapshot(&self.distributions)
    }

    pub fn gauges(&self) -> HashMap<MetricKey, GaugeValue> {
        snapshot(&self.gauges)
    }

    /// Converts all collected metrics to `MonitoringInfo` messages.
    pub fn to_monitoring_infos(&self) -> Vec<MonitoringInfo> {
        let mut infos = Vec::new();

        let counters = self.counters();
        for (key, val) in counters {
            let mut payload = Vec::new();
            if VarIntCoder::encode_varint(val, &mut payload).is_ok() {
                infos.push(MonitoringInfo {
                    urn: URN_USER_SUM_INT64.to_string(),
                    r#type: TYPE_SUM_INT64.to_string(),
                    payload,
                    labels: HashMap::from([
                        (LABEL_PTRANSFORM.to_string(), key.transform_id),
                        (LABEL_NAMESPACE.to_string(), key.namespace),
                        (LABEL_NAME.to_string(), key.name),
                    ]),
                    start_time: None,
                });
            }
        }

        let dists = self.distributions();
        for (key, dist) in dists {
            infos.push(MonitoringInfo {
                urn: URN_USER_DISTRIBUTION_INT64.to_string(),
                r#type: TYPE_DISTRIBUTION_INT64.to_string(),
                payload: dist.encode_payload(),
                labels: HashMap::from([
                    (LABEL_PTRANSFORM.to_string(), key.transform_id),
                    (LABEL_NAMESPACE.to_string(), key.namespace),
                    (LABEL_NAME.to_string(), key.name),
                ]),
                start_time: None,
            });
        }

        let gauges = self.gauges();
        for (key, gauge) in gauges {
            infos.push(MonitoringInfo {
                urn: URN_USER_LATEST_INT64.to_string(),
                r#type: TYPE_LATEST_INT64.to_string(),
                payload: gauge.encode_payload(),
                labels: HashMap::from([
                    (LABEL_PTRANSFORM.to_string(), key.transform_id),
                    (LABEL_NAMESPACE.to_string(), key.namespace),
                    (LABEL_NAME.to_string(), key.name),
                ]),
                start_time: None,
            });
        }

        infos
    }

    /// Returns a [`Counter`](super::Counter) bound to this container and `transform_id`.
    pub fn counter(
        self: &Arc<Self>,
        transform_id: impl Into<String>,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> super::Counter {
        super::Counter::with_container(namespace, name, Arc::clone(self), transform_id)
    }

    /// Returns a [`Distribution`](super::Distribution) bound to this container and `transform_id`.
    pub fn distribution(
        self: &Arc<Self>,
        transform_id: impl Into<String>,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> super::Distribution {
        super::Distribution::with_container(namespace, name, Arc::clone(self), transform_id)
    }

    /// Returns a [`Gauge`](super::Gauge) bound to this container and `transform_id`.
    pub fn gauge(
        self: &Arc<Self>,
        transform_id: impl Into<String>,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> super::Gauge {
        super::Gauge::with_container(namespace, name, Arc::clone(self), transform_id)
    }
}
