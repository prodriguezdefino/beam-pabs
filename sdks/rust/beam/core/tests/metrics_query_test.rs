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

//! Tests for how `MetricResults` indexes monitoring infos and answers queries: committed
//! versus attempted precedence, duplicate keys, gauge selection, system metrics and
//! malformed payloads.
#![expect(clippy::unwrap_used, reason = "test helpers")]

use std::collections::HashMap;

use beam::coders::VarIntCoder;
use beam::metrics::{
    DistributionValue, GaugeValue, LABEL_NAME, LABEL_NAMESPACE, LABEL_PCOLLECTION,
    LABEL_PTRANSFORM, MetricFilter, MetricKey, MetricResult, MetricResults, SUPPORTED_METRIC_URNS,
    TYPE_DISTRIBUTION_INT64, TYPE_LATEST_INT64, TYPE_SUM_INT64, URN_USER_DISTRIBUTION_INT64,
    URN_USER_LATEST_INT64, URN_USER_SUM_INT64, data_channel_read_index, element_count,
    process_bundle_msecs, sampled_byte_size,
};
use model::pipeline::MonitoringInfo;

fn varint(value: i64) -> Vec<u8> {
    let mut buf = Vec::new();
    VarIntCoder::encode_varint(value, &mut buf).unwrap();
    buf
}

fn user_info(
    urn: &str,
    kind: &str,
    transform: &str,
    name: &str,
    payload: Vec<u8>,
) -> MonitoringInfo {
    MonitoringInfo {
        urn: urn.to_string(),
        r#type: kind.to_string(),
        labels: HashMap::from([
            (LABEL_PTRANSFORM.to_string(), transform.to_string()),
            (LABEL_NAMESPACE.to_string(), "ns".to_string()),
            (LABEL_NAME.to_string(), name.to_string()),
        ]),
        payload,
        ..Default::default()
    }
}

fn counter_info(transform: &str, name: &str, value: i64) -> MonitoringInfo {
    user_info(
        URN_USER_SUM_INT64,
        TYPE_SUM_INT64,
        transform,
        name,
        varint(value),
    )
}

fn gauge_info(transform: &str, value: i64, timestamp_ms: i64) -> MonitoringInfo {
    user_info(
        URN_USER_LATEST_INT64,
        TYPE_LATEST_INT64,
        transform,
        "g",
        GaugeValue::new(value, timestamp_ms).encode_payload(),
    )
}

#[test]
fn committed_values_take_precedence_over_attempted() {
    let results = MetricResults::from_monitoring_infos(
        &[counter_info("T", "c", 10), gauge_info("T", 1, 100)],
        &[counter_info("T", "c", 7), gauge_info("T", 2, 50)],
    );

    let counters = results.all_counters();
    assert_eq!(
        counters,
        [MetricResult::new(
            MetricKey::new("T", "ns", "c"),
            Some(7),
            Some(10)
        )]
    );
    assert_eq!(results.counter("ns", "c"), Some(7));
    // Precedence depends on the commit state, not on the order of the reports.
    assert_eq!(
        results.gauge("ns", "g"),
        Some(GaugeValue {
            value: 2,
            timestamp_ms: 50
        })
    );

    // When committed values are absent, queries return attempted values.
    let attempted = MetricResult::new(MetricKey::new("t", "ns", "g"), None, Some(3i64));
    assert_eq!(attempted.result(), Some(3));
    assert_eq!(
        MetricResult::<i64>::new(MetricKey::new("t", "ns", "g"), None, None).result(),
        None
    );
}

#[test]
fn a_repeated_key_keeps_the_last_reported_value() {
    // Monitoring infos carry cumulative values. So a later report replaces an earlier
    // report and is not added to it.
    let results = MetricResults::from_monitoring_infos(
        &[counter_info("T", "c", 5), counter_info("T", "c", 8)],
        &[],
    );
    assert_eq!(results.counter_for_transform("T", "ns", "c"), Some(8));
    assert_eq!(results.all_counters().len(), 1);
}

#[test]
fn counter_and_distribution_queries_aggregate_across_transforms() {
    let dist = |transform: &str, d: DistributionValue| {
        user_info(
            URN_USER_DISTRIBUTION_INT64,
            TYPE_DISTRIBUTION_INT64,
            transform,
            "d",
            d.encode_payload(),
        )
    };
    let results = MetricResults::from_monitoring_infos(
        &[
            counter_info("A", "c", 4),
            counter_info("B", "c", -6),
            dist(
                "A",
                DistributionValue {
                    count: 2,
                    sum: 10,
                    min: 3,
                    max: 7,
                },
            ),
            dist(
                "B",
                DistributionValue {
                    count: 1,
                    sum: -5,
                    min: -5,
                    max: -5,
                },
            ),
        ],
        &[],
    );
    // Negative values encode and decode correctly as 10-byte VarInts.
    assert_eq!(results.counter_for_transform("B", "ns", "c"), Some(-6));
    assert_eq!(results.counter("ns", "c"), Some(-2));
    assert_eq!(
        results.distribution("ns", "d"),
        Some(DistributionValue {
            count: 3,
            sum: 5,
            min: -5,
            max: 7
        })
    );
}

#[test]
fn the_newest_gauge_reading_wins_across_transforms() {
    for infos in [
        [gauge_info("Old", 1, 1_000), gauge_info("New", 2, 2_000)],
        [gauge_info("New", 2, 2_000), gauge_info("Old", 1, 1_000)],
    ] {
        let results = MetricResults::from_monitoring_infos(&infos, &[]);
        assert_eq!(
            results.gauge("ns", "g"),
            Some(GaugeValue {
                value: 2,
                timestamp_ms: 2_000
            })
        );
        assert_eq!(
            results
                .gauge_for_transform("Old", "ns", "g")
                .map(|g| g.value),
            Some(1)
        );
    }
}

#[test]
fn gauge_ties_are_resolved_deterministically() {
    let pick = || {
        MetricResults::from_monitoring_infos(
            &[
                gauge_info("A", 1, 1_000),
                gauge_info("B", 2, 1_000),
                gauge_info("C", 3, 1_000),
            ],
            &[],
        )
        .gauge("ns", "g")
        .unwrap()
        .value
    };
    let first = pick();
    for _ in 0..64 {
        assert_eq!(pick(), first, "tie resolved differently between runs");
    }
}

#[test]
fn system_counters_of_different_pcollections_are_kept_apart() {
    let results = MetricResults::from_monitoring_infos(
        &[
            element_count("pcoll-a", 5).unwrap(),
            element_count("pcoll-b", 7).unwrap(),
        ],
        &[],
    );
    let mut values: Vec<i64> = results
        .all_counters()
        .into_iter()
        .filter_map(|r| r.result())
        .collect();
    values.sort();
    assert_eq!(values, [5, 7]);
}

#[test]
fn every_supported_urn_is_indexed_by_metric_results() {
    let mut infos = vec![
        element_count("pcoll", 11).unwrap(),
        data_channel_read_index("read", 12).unwrap(),
        sampled_byte_size("pcoll", 2, 30, 10, 20).unwrap(),
        process_bundle_msecs("step", 13).unwrap(),
        counter_info("T", "c", 14),
        user_info(
            URN_USER_DISTRIBUTION_INT64,
            TYPE_DISTRIBUTION_INT64,
            "T",
            "d",
            DistributionValue::new(15).encode_payload(),
        ),
        gauge_info("T", 16, 1),
    ];
    let urns: Vec<&str> = infos.iter().map(|i| i.urn.as_str()).collect();
    assert_eq!(urns, SUPPORTED_METRIC_URNS);

    // Index each info alone, because system metrics share a key (see above). Check that
    // each info is in the correct family with the correct value.
    let expected_family = [
        "counter",
        "counter",
        "distribution",
        "counter",
        "counter",
        "distribution",
        "gauge",
    ];
    for (info, family) in infos.drain(..).zip(expected_family) {
        let urn = info.urn.clone();
        let results = MetricResults::from_monitoring_infos(&[info], &[]);
        let found = (
            results.all_counters().len(),
            results.all_distributions().len(),
            results.all_gauges().len(),
        );
        let expected = match family {
            "counter" => (1, 0, 0),
            "distribution" => (0, 1, 0),
            _ => (0, 0, 1),
        };
        assert_eq!(found, expected, "{urn}");
    }

    let byte_size = MetricResults::from_monitoring_infos(
        &[sampled_byte_size("pcoll", 2, 30, 10, 20).unwrap()],
        &[],
    );
    assert_eq!(
        byte_size.all_distributions()[0].result(),
        Some(DistributionValue {
            count: 2,
            sum: 30,
            min: 10,
            max: 20
        })
    );
    let pcoll_label = element_count("pcoll", 11).unwrap().labels[LABEL_PCOLLECTION].clone();
    assert_eq!(pcoll_label, "pcoll");
}

#[test]
fn malformed_payloads_are_dropped() {
    let results = MetricResults::from_monitoring_infos(
        &[
            // Malformed inputs: truncated varint, empty distribution, incomplete gauge.
            user_info(URN_USER_SUM_INT64, TYPE_SUM_INT64, "T", "c", vec![0xFF]),
            user_info(
                URN_USER_DISTRIBUTION_INT64,
                TYPE_DISTRIBUTION_INT64,
                "T",
                "d",
                Vec::new(),
            ),
            user_info(
                URN_USER_LATEST_INT64,
                TYPE_LATEST_INT64,
                "T",
                "g",
                varint(5),
            ),
            // Unrecognized URN and type.
            user_info(
                "beam:metric:unknown:v1",
                "beam:metrics:unknown:v1",
                "T",
                "u",
                varint(1),
            ),
        ],
        &[],
    );
    assert!(results.all_counters().is_empty());
    assert!(results.all_distributions().is_empty());
    assert!(results.all_gauges().is_empty());
}

#[test]
fn metric_filter_requires_every_constraint_to_match() {
    let key = MetricKey::new("T", "ns", "c");
    assert!(MetricFilter::empty().matches(&key));
    assert!(
        MetricFilter::all()
            .with_namespace("ns")
            .with_name("c")
            .with_transform("T")
            .matches(&key)
    );
    assert!(
        !MetricFilter::all()
            .with_namespace("ns")
            .with_name("other")
            .matches(&key)
    );
    assert!(!MetricFilter::all().with_transform("U").matches(&key));
    // Multiple values for a constraint act as alternatives.
    assert!(
        MetricFilter::all()
            .with_name("x")
            .with_name("c")
            .matches(&key)
    );
}
