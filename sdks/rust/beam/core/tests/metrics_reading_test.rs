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

//! Tests for how a `MonitoringInfo` or a `MetricsContainer` becomes `MetricReading`s: the
//! cell key from the labels, the value kind from the URN or type, and the phases in which
//! a container reports a value.
#![expect(clippy::unwrap_used, reason = "test helpers")]

use std::collections::HashMap;

use beam::coders::VarIntCoder;
use beam::metrics::{
    DistributionValue, GaugeValue, LABEL_NAME, LABEL_NAMESPACE, LABEL_PCOLLECTION,
    LABEL_PTRANSFORM, MetricFilter, MetricKey, MetricPhase, MetricReading, MetricResult,
    MetricResults, MetricValue, MetricsContainer, TYPE_SUM_INT64, URN_ELEMENT_COUNT,
    URN_USER_DISTRIBUTION_INT64, URN_USER_LATEST_INT64, URN_USER_SUM_DOUBLE, URN_USER_SUM_INT64,
};
use model::pipeline::MonitoringInfo;

fn varint(value: i64) -> Vec<u8> {
    let mut buf = Vec::new();
    VarIntCoder::encode_varint(value, &mut buf).unwrap();
    buf
}

fn info(urn: &str, kind: &str, labels: &[(&str, &str)], payload: Vec<u8>) -> MonitoringInfo {
    MonitoringInfo {
        urn: urn.to_string(),
        r#type: kind.to_string(),
        labels: labels
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect::<HashMap<_, _>>(),
        payload,
        ..Default::default()
    }
}

fn key_of(info: &MonitoringInfo) -> MetricKey {
    MetricReading::from_monitoring_info(info, MetricPhase::Attempted)
        .unwrap()
        .key
}

#[test]
fn user_metric_keys_come_from_their_labels() {
    let user = info(
        URN_USER_SUM_INT64,
        "",
        &[
            (LABEL_PTRANSFORM, "step"),
            (LABEL_NAMESPACE, "ns"),
            (LABEL_NAME, "count"),
        ],
        varint(1),
    );
    assert_eq!(key_of(&user), MetricKey::new("step", "ns", "count"));
}

#[test]
fn a_named_metric_without_a_namespace_has_an_empty_one() {
    let named = info(
        URN_USER_SUM_INT64,
        "",
        &[(LABEL_PTRANSFORM, "step"), (LABEL_NAME, "count")],
        varint(1),
    );
    assert_eq!(key_of(&named), MetricKey::new("step", "", "count"));
}

#[test]
fn system_metrics_are_named_after_their_urn_in_the_beam_namespace() {
    let system = info(
        URN_ELEMENT_COUNT,
        "",
        &[(LABEL_PCOLLECTION, "pc")],
        varint(7),
    );
    assert_eq!(
        key_of(&system),
        MetricKey::new("pc", "beam", URN_ELEMENT_COUNT)
    );
}

#[test]
fn the_transform_label_wins_over_the_pcollection_label() {
    let both = info(
        URN_ELEMENT_COUNT,
        "",
        &[(LABEL_PTRANSFORM, "step"), (LABEL_PCOLLECTION, "pc")],
        varint(7),
    );
    assert_eq!(key_of(&both).transform_id, "step");
    let neither = info(URN_ELEMENT_COUNT, "", &[], varint(7));
    assert_eq!(key_of(&neither).transform_id, "");
}

#[test]
fn values_are_decoded_by_urn_or_by_type() {
    let decode = |info: &MonitoringInfo| {
        MetricReading::from_monitoring_info(info, MetricPhase::Committed).map(|r| r.value)
    };
    assert_eq!(
        decode(&info(URN_USER_SUM_INT64, "", &[], varint(3))),
        Some(MetricValue::Counter(3))
    );
    assert_eq!(
        decode(&info("custom:urn", TYPE_SUM_INT64, &[], varint(4))),
        Some(MetricValue::Counter(4))
    );
    let dist = DistributionValue::new(5);
    assert_eq!(
        decode(&info(
            URN_USER_DISTRIBUTION_INT64,
            "",
            &[],
            dist.encode_payload()
        )),
        Some(MetricValue::Distribution(dist))
    );
    let gauge = GaugeValue::new(9, 2);
    assert_eq!(
        decode(&info(
            URN_USER_LATEST_INT64,
            "",
            &[],
            gauge.encode_payload()
        )),
        Some(MetricValue::Gauge(gauge))
    );
}

#[test]
fn unindexed_kinds_and_malformed_payloads_yield_no_reading() {
    let unindexed = info(URN_USER_SUM_DOUBLE, "", &[], 1.0f64.to_be_bytes().to_vec());
    assert_eq!(
        MetricReading::from_monitoring_info(&unindexed, MetricPhase::Committed),
        None
    );
    let truncated = info(URN_USER_SUM_INT64, "", &[], vec![0x80]);
    assert_eq!(
        MetricReading::from_monitoring_info(&truncated, MetricPhase::Committed),
        None
    );
}

#[test]
fn the_reading_keeps_the_phase_it_was_reported_in() {
    let counter = info(URN_USER_SUM_INT64, "", &[], varint(1));
    [MetricPhase::Committed, MetricPhase::Attempted]
        .into_iter()
        .for_each(|phase| {
            assert_eq!(
                MetricReading::from_monitoring_info(&counter, phase).map(|r| r.phase),
                Some(phase)
            );
        });
}

#[test]
fn container_values_are_both_committed_and_attempted() {
    let container = MetricsContainer::new();
    container.inc_counter("step", "ns", "count", 2);
    container.inc_counter("step", "ns", "count", 3);
    container.update_distribution("step", "ns", "sizes", 4);
    container.set_gauge("step", "ns", "level", 6, 100);

    let results = MetricResults::from_container(&container);

    assert_eq!(
        results.query_counters(&MetricFilter::all()),
        vec![MetricResult::new(
            MetricKey::new("step", "ns", "count"),
            Some(5),
            Some(5)
        )]
    );
    assert_eq!(
        results.all_distributions(),
        vec![MetricResult::new(
            MetricKey::new("step", "ns", "sizes"),
            Some(DistributionValue::new(4)),
            Some(DistributionValue::new(4))
        )]
    );
    assert_eq!(
        results.all_gauges(),
        vec![MetricResult::new(
            MetricKey::new("step", "ns", "level"),
            Some(GaugeValue::new(6, 100)),
            Some(GaugeValue::new(6, 100))
        )]
    );
}

#[test]
fn an_empty_container_gives_empty_results() {
    assert!(MetricResults::from_container(&MetricsContainer::new()).is_empty());
}

#[test]
fn readings_extend_existing_results() {
    let key = MetricKey::new("step", "ns", "count");
    let mut results: MetricResults = [MetricReading {
        key: key.clone(),
        phase: MetricPhase::Attempted,
        value: MetricValue::Counter(1),
    }]
    .into_iter()
    .collect();
    results.extend([MetricReading {
        key: key.clone(),
        phase: MetricPhase::Committed,
        value: MetricValue::Counter(2),
    }]);
    assert_eq!(
        results.all_counters(),
        vec![MetricResult::new(key, Some(2), Some(1))]
    );
}

#[test]
fn counters_sum_only_cells_that_report_a_value() {
    assert_eq!(MetricResults::new().counter("ns", "count"), None);
    let results: MetricResults = ["a", "b"]
        .into_iter()
        .zip([2, 5])
        .map(|(step, v)| MetricReading {
            key: MetricKey::new(step, "ns", "count"),
            phase: MetricPhase::Committed,
            value: MetricValue::Counter(v),
        })
        .collect();
    assert_eq!(results.counter("ns", "count"), Some(7));
    assert_eq!(results.counter("ns", "other"), None);
    assert_eq!(results.counter_for_transform("b", "ns", "count"), Some(5));
}

#[test]
fn empty_and_all_filters_are_the_same_filter() {
    assert_eq!(MetricFilter::empty(), MetricFilter::all());
}
