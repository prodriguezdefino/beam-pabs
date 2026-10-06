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

//! Job metrics of the API and their conversion into Beam metric results.

use beam::metrics::{
    BOUNDED_TRIE_DEFAULT_BOUND, BoundedTrieValue, MetricKey, MetricPhase, MetricReading,
    MetricResults, MetricValue, StringSetValue,
};
use serde::{Deserialize, Serialize};

/// Response returned by `jobs.metrics.get`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct JobMetricsResponse {
    #[serde(default)]
    pub metrics: Vec<MetricUpdateItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metric_time: Option<String>,
}

/// Individual metric update from Dataflow v1b3 `JobMetrics`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricUpdateItem {
    pub name: Option<MetricStructuredName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scalar: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distribution: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gauge: Option<serde_json::Value>,
    /// The strings of a string set metric.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub set: Option<serde_json::Value>,
    /// A bounded trie metric, as the JSON form of the `BoundedTrie` message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bounded_trie: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_time: Option<String>,
}

/// Structured identity of a metric in Dataflow v1b3.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricStructuredName {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    pub name: String,
    #[serde(default)]
    pub context: std::collections::HashMap<String, String>,
}

/// Suffixes of the metrics that Dataflow derives from distributions for its UI. They are
/// skipped as duplicates. Dataflow marks them only by name, so a user metric with one of
/// these suffixes is also skipped.
const SYNTHETIC_SUFFIXES: [&str; 4] = ["_MIN", "_MAX", "_MEAN", "_COUNT"];

/// Parses an int64 that Dataflow encodes as a JSON number or a string. A fractional number
/// is rejected, not truncated: no metric read here is fractional.
fn parse_scalar_i64(val: &serde_json::Value) -> Option<i64> {
    val.as_i64()
        .or_else(|| val.as_u64().and_then(|u| i64::try_from(u).ok()))
        .or_else(|| {
            val.as_f64()
                .filter(|f| f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64)
                .map(|f| f as i64)
        })
        .or_else(|| val.as_str()?.parse().ok())
}

/// Distribution as Dataflow reports it. The wire field stays a `serde_json::Value`, so one
/// unexpected int64 encoding does not fail the whole metrics response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DistributionUpdate {
    count: i64,
    sum: i64,
    min: i64,
    max: i64,
}

impl DistributionUpdate {
    fn from_value(value: &serde_json::Value) -> Option<Self> {
        let obj = value.as_object()?;
        let field = |name: &str| obj.get(name).and_then(parse_scalar_i64).unwrap_or(0);
        Some(Self {
            count: field("count"),
            sum: field("sum"),
            min: field("min"),
            max: field("max"),
        })
    }
}

impl From<DistributionUpdate> for beam::metrics::DistributionValue {
    fn from(d: DistributionUpdate) -> Self {
        Self {
            count: d.count,
            sum: d.sum,
            min: d.min,
            max: d.max,
        }
    }
}

/// Returns the reporting step and the namespace of a Dataflow metric.
fn metric_key(name: &MetricStructuredName) -> MetricKey {
    let context = |key: &str| name.context.get(key);
    let step = context("step")
        .or_else(|| context("output_user_name"))
        .or_else(|| context("original_name"))
        .cloned()
        .unwrap_or_default();
    let namespace = context("namespace")
        .cloned()
        .unwrap_or_else(|| match name.origin.as_deref() {
            Some("user") => String::new(),
            origin => origin.unwrap_or("dataflow/v1b3").to_string(),
        });
    MetricKey::new(step, namespace, name.name.clone())
}

/// Dataflow marks a value that retries can still change as `tentative`: Beam attempted.
fn metric_phase(name: &MetricStructuredName) -> MetricPhase {
    if name.context.get("tentative").is_some_and(|s| s == "true") {
        MetricPhase::Attempted
    } else {
        MetricPhase::Committed
    }
}

fn metric_value(item: &MetricUpdateItem) -> Option<MetricValue> {
    match item {
        MetricUpdateItem {
            scalar: Some(scalar),
            ..
        } => parse_scalar_i64(scalar).map(MetricValue::Counter),
        MetricUpdateItem {
            distribution: Some(dist),
            ..
        } => DistributionUpdate::from_value(dist).map(|d| MetricValue::Distribution(d.into())),
        MetricUpdateItem { set: Some(set), .. } => string_set(set).map(MetricValue::StringSet),
        MetricUpdateItem {
            bounded_trie: Some(trie),
            ..
        } => bounded_trie(trie).map(MetricValue::BoundedTrie),
        MetricUpdateItem { .. } => None,
    }
}

/// A string set from a JSON list of strings.
fn string_set(value: &serde_json::Value) -> Option<StringSetValue> {
    Some(
        value
            .as_array()?
            .iter()
            .filter_map(serde_json::Value::as_str)
            .fold(StringSetValue::new(), |mut set, entry| {
                set.add(entry);
                set
            }),
    )
}

/// A bounded trie from the JSON form of the `BoundedTrie` message.
fn bounded_trie(value: &serde_json::Value) -> Option<BoundedTrieValue> {
    fn node(value: &serde_json::Value) -> model::pipeline::BoundedTrieNode {
        model::pipeline::BoundedTrieNode {
            truncated: value
                .get("truncated")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            children: value
                .get("children")
                .and_then(serde_json::Value::as_object)
                .map(|children| {
                    children
                        .iter()
                        .map(|(segment, child)| (segment.clone(), node(child)))
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
    let trie = model::pipeline::BoundedTrie {
        bound: value
            .get("bound")
            .and_then(serde_json::Value::as_i64)
            .and_then(|bound| i32::try_from(bound).ok())
            .unwrap_or(BOUNDED_TRIE_DEFAULT_BOUND as i32),
        root: value.get("root").map(node),
        singleton: value
            .get("singleton")
            .and_then(serde_json::Value::as_array)
            .map(|segments| {
                segments
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
    };
    BoundedTrieValue::decode_payload(&prost::Message::encode_to_vec(&trie)).ok()
}

/// Converts a Dataflow metric update into a Beam metric reading, if it carries one.
fn parse_metric(item: &MetricUpdateItem) -> Option<MetricReading> {
    let name = item.name.as_ref()?;
    if SYNTHETIC_SUFFIXES.iter().any(|s| name.name.ends_with(s)) {
        return None;
    }
    Some(MetricReading {
        key: metric_key(name),
        phase: metric_phase(name),
        value: metric_value(item)?,
    })
}

impl JobMetricsResponse {
    /// Converts Dataflow v1b3 job metrics into queryable Beam `MetricResults`.
    pub fn to_metric_results(&self) -> MetricResults {
        self.metrics.iter().filter_map(parse_metric).collect()
    }
}
