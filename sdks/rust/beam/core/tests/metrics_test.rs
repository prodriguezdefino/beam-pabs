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

//! Tests for standard metrics specifications and builders.

use beam::coders::VarIntCoder;
use beam::metrics::{
    LABEL_PCOLLECTION, LABEL_PTRANSFORM, Metrics, TYPE_SUM_INT64, URN_DATA_CHANNEL_READ_INDEX,
    URN_ELEMENT_COUNT, URN_SAMPLED_BYTE_SIZE, URN_USER_SUM_INT64, data_channel_read_index,
    element_count,
};

#[test]
fn test_element_count_monitoring_info() {
    let pcol = "pcollection-test-42";
    let count = 1337i64;

    let info = element_count(pcol, count).expect("should build monitoring info");
    assert_eq!(info.urn, URN_ELEMENT_COUNT);
    assert_eq!(info.r#type, TYPE_SUM_INT64);
    assert_eq!(info.labels.get(LABEL_PCOLLECTION), Some(&pcol.to_string()));

    let mut reader = std::io::Cursor::new(info.payload);
    let decoded = VarIntCoder::decode_varint(&mut reader).expect("should decode varint");
    assert_eq!(decoded, count);
}

#[test]
fn test_data_channel_read_index_monitoring_info() {
    let ptransform = "source_read_step";
    let index = 42i64;

    let info = data_channel_read_index(ptransform, index).expect("should build monitoring info");
    assert_eq!(info.urn, URN_DATA_CHANNEL_READ_INDEX);
    assert_eq!(info.r#type, TYPE_SUM_INT64);
    assert_eq!(
        info.labels.get(LABEL_PTRANSFORM),
        Some(&ptransform.to_string())
    );

    let mut reader = std::io::Cursor::new(info.payload);
    let decoded = VarIntCoder::decode_varint(&mut reader).expect("should decode varint");
    assert_eq!(decoded, index);
}

#[test]
fn test_standard_metric_urn_conventions() {
    // The exact strings that runners match (model/pipeline/v1/metrics.proto).
    assert_eq!(URN_ELEMENT_COUNT, "beam:metric:element_count:v1");
    assert_eq!(
        URN_DATA_CHANNEL_READ_INDEX,
        "beam:metric:data_channel:read_index:v1"
    );
    assert_eq!(URN_SAMPLED_BYTE_SIZE, "beam:metric:sampled_byte_size:v1");
    assert_eq!(URN_USER_SUM_INT64, "beam:metric:user:sum_int64:v1");
    assert_eq!(TYPE_SUM_INT64, "beam:metrics:sum_int64:v1");
}

#[test]
fn test_short_id_cache() {
    use beam::metrics::{PROTOCOL_MONITORING_INFO_SHORT_IDS, ShortIdCache};

    assert_eq!(
        PROTOCOL_MONITORING_INFO_SHORT_IDS,
        "beam:protocol:monitoring_info_short_ids:v1"
    );

    let cache = ShortIdCache::new();
    assert!(cache.is_empty());

    let info_a1 = element_count("pcol-1", 10).unwrap();
    let info_a2 = element_count("pcol-1", 20).unwrap();
    let info_b = element_count("pcol-2", 10).unwrap();

    let id_a1 = cache.get_or_create_short_id(&info_a1);
    let id_a2 = cache.get_or_create_short_id(&info_a2);
    let id_b = cache.get_or_create_short_id(&info_b);

    // Identical metrics (same URN, type and labels) must share the same short ID.
    assert_eq!(id_a1, id_a2);
    // Distinct metrics must receive distinct short IDs.
    assert_ne!(id_a1, id_b);
    assert_eq!(cache.len(), 2);

    // Metadata template clears payload and start time.
    let template = cache.get_info(&id_a1).expect("should resolve template");
    assert_eq!(template.urn, URN_ELEMENT_COUNT);
    assert_eq!(template.r#type, TYPE_SUM_INT64);
    assert_eq!(
        template.labels.get(LABEL_PCOLLECTION),
        Some(&"pcol-1".to_string())
    );
    assert!(template.payload.is_empty());
    assert!(template.start_time.is_none());

    let batch = cache.get_infos(&[id_a1.clone(), id_b.clone(), "unknown_id".to_string()]);
    assert_eq!(batch.len(), 2);
    assert!(batch.contains_key(&id_a1));
    assert!(batch.contains_key(&id_b));
    assert!(!batch.contains_key("unknown_id"));
}

#[test]
fn test_user_metrics_with_context_and_container() {
    use beam::metrics::{
        LABEL_NAME, LABEL_NAMESPACE, LABEL_PTRANSFORM, MetricKey, Metrics, MetricsContainer,
        URN_USER_DISTRIBUTION_INT64, URN_USER_LATEST_INT64, URN_USER_SUM_INT64,
    };
    use std::sync::Arc;

    let container = Arc::new(MetricsContainer::new());

    {
        let counter_a = Metrics::counter("app", "events").bind(Arc::clone(&container), "StepA");
        let dist_a = Metrics::distribution("app", "latency").bind(Arc::clone(&container), "StepA");
        let gauge_a = Metrics::gauge("app", "memory").bind(Arc::clone(&container), "StepA");

        counter_a.inc_by(5);
        dist_a.update(10);
        dist_a.update(20);
        gauge_a.set(1024);

        {
            let counter_b = Metrics::counter("app", "events").bind(Arc::clone(&container), "StepB");
            counter_b.inc();
        }

        counter_a.inc_by(2);
    }

    let counters = container.counters();
    assert_eq!(
        counters.get(&MetricKey::new("StepA", "app", "events")),
        Some(&7)
    );
    assert_eq!(
        counters.get(&MetricKey::new("StepB", "app", "events")),
        Some(&1)
    );

    let dists = container.distributions();
    let lat = dists
        .get(&MetricKey::new("StepA", "app", "latency"))
        .expect("latency distribution should exist");
    assert_eq!(lat.count, 2);
    assert_eq!(lat.sum, 30);
    assert_eq!(lat.min, 10);
    assert_eq!(lat.max, 20);

    let gauges = container.gauges();
    let mem = gauges
        .get(&MetricKey::new("StepA", "app", "memory"))
        .expect("memory gauge should exist");
    assert_eq!(mem.value, 1024);
    assert!(mem.timestamp_ms > 0);

    // Two counters (StepA and StepB), one distribution and one gauge give 4 infos.
    let infos = container.to_monitoring_infos();
    assert_eq!(infos.len(), 4);

    let counter_info = infos
        .iter()
        .find(|i| {
            i.urn == URN_USER_SUM_INT64
                && i.labels.get(LABEL_PTRANSFORM).map(String::as_str) == Some("StepA")
        })
        .expect("StepA counter MonitoringInfo must exist");
    assert_eq!(
        counter_info.labels.get(LABEL_NAMESPACE),
        Some(&"app".to_string())
    );
    assert_eq!(
        counter_info.labels.get(LABEL_NAME),
        Some(&"events".to_string())
    );

    let dist_info = infos
        .iter()
        .find(|i| i.urn == URN_USER_DISTRIBUTION_INT64)
        .unwrap();
    let mut reader = std::io::Cursor::new(&dist_info.payload);
    assert_eq!(VarIntCoder::decode_varint(&mut reader).unwrap(), 2);
    assert_eq!(VarIntCoder::decode_varint(&mut reader).unwrap(), 30);
    assert_eq!(VarIntCoder::decode_varint(&mut reader).unwrap(), 10);
    assert_eq!(VarIntCoder::decode_varint(&mut reader).unwrap(), 20);

    let gauge_info = infos
        .iter()
        .find(|i| i.urn == URN_USER_LATEST_INT64)
        .unwrap();
    let mut reader = std::io::Cursor::new(&gauge_info.payload);
    let ts = VarIntCoder::decode_varint(&mut reader).unwrap();
    let val = VarIntCoder::decode_varint(&mut reader).unwrap();
    assert!(ts > 0);
    assert_eq!(val, 1024);
}

#[test]
fn test_system_metrics_additional() {
    use beam::metrics::{
        URN_PROCESS_BUNDLE_MSECS, URN_SAMPLED_BYTE_SIZE, process_bundle_msecs, sampled_byte_size,
    };

    let pardo_info = process_bundle_msecs("transform-1", 1234).unwrap();
    assert_eq!(pardo_info.urn, URN_PROCESS_BUNDLE_MSECS);
    let mut reader = std::io::Cursor::new(pardo_info.payload);
    assert_eq!(VarIntCoder::decode_varint(&mut reader).unwrap(), 1234);

    let byte_info = sampled_byte_size("pcol-1", 10, 1000, 50, 150).unwrap();
    assert_eq!(byte_info.urn, URN_SAMPLED_BYTE_SIZE);
    let mut reader = std::io::Cursor::new(byte_info.payload);
    assert_eq!(VarIntCoder::decode_varint(&mut reader).unwrap(), 10);
    assert_eq!(VarIntCoder::decode_varint(&mut reader).unwrap(), 1000);
    assert_eq!(VarIntCoder::decode_varint(&mut reader).unwrap(), 50);
    assert_eq!(VarIntCoder::decode_varint(&mut reader).unwrap(), 150);
}

#[test]
fn test_distribution_and_gauge_payload_codecs() {
    use beam::metrics::{DistributionValue, GaugeValue};

    let dist = DistributionValue {
        count: 10,
        sum: 500,
        min: 5,
        max: 95,
    };
    let payload = dist.encode_payload();
    let decoded = DistributionValue::decode_payload(&payload).expect("decode dist");
    assert_eq!(dist, decoded);

    let gauge = GaugeValue::new(42, 1700000000);
    let payload = gauge.encode_payload();
    let decoded = GaugeValue::decode_payload(&payload).expect("decode gauge");
    assert_eq!(gauge, decoded);
}

#[test]
fn test_metric_results_querying_and_filtering() {
    use beam::metrics::{MetricFilter, MetricResults, Metrics, MetricsContainer};
    use std::sync::Arc;

    let container = Arc::new(MetricsContainer::new());
    {
        {
            let counter_a =
                Metrics::counter("app", "records_processed").bind(Arc::clone(&container), "StageA");
            counter_a.inc_by(10);
            let dist_a =
                Metrics::distribution("app", "latency_ms").bind(Arc::clone(&container), "StageA");
            dist_a.update(100);
            dist_a.update(200);
            let gauge_a =
                Metrics::gauge("app", "queue_depth").bind(Arc::clone(&container), "StageA");
            gauge_a.set(50);
        }

        {
            let counter_b =
                Metrics::counter("app", "records_processed").bind(Arc::clone(&container), "StageB");
            counter_b.inc_by(15);
            let counter_err =
                Metrics::counter("app", "errors").bind(Arc::clone(&container), "StageB");
            counter_err.inc();
        }
    }

    let results = MetricResults::from_container(&container);

    // Aggregated counter across all transforms.
    assert_eq!(results.counter("app", "records_processed"), Some(25));
    assert_eq!(results.counter("app", "errors"), Some(1));
    assert_eq!(results.counter("app", "nonexistent"), None);

    // Transform-specific queries.
    assert_eq!(
        results.counter_for_transform("StageA", "app", "records_processed"),
        Some(10)
    );
    assert_eq!(
        results.counter_for_transform("StageB", "app", "records_processed"),
        Some(15)
    );
    assert_eq!(
        results.counter_for_transform("StageA", "app", "errors"),
        None
    );

    let dist = results.distribution("app", "latency_ms").unwrap();
    assert_eq!(dist.count, 2);
    assert_eq!(dist.sum, 300);
    assert_eq!(dist.min, 100);
    assert_eq!(dist.max, 200);

    let gauge = results.gauge("app", "queue_depth").unwrap();
    assert_eq!(gauge.value, 50);

    let filter_stage_b = MetricFilter::empty().with_transform("StageB");
    let counters_b = results.query_counters(&filter_stage_b);
    assert_eq!(counters_b.len(), 2);

    let filter_errors = MetricFilter::empty().with_name("errors");
    let counters_err = results.query_counters(&filter_errors);
    assert_eq!(counters_err.len(), 1);
    assert_eq!(counters_err[0].result(), Some(1));
}

#[test]
fn test_metric_results_from_monitoring_infos() {
    use beam::metrics::{MetricResults, Metrics, MetricsContainer};
    use std::sync::Arc;

    let container = Arc::new(MetricsContainer::new());
    {
        let cnt = Metrics::counter("ns", "cnt").bind(Arc::clone(&container), "WorkerTransform");
        let dist =
            Metrics::distribution("ns", "dist").bind(Arc::clone(&container), "WorkerTransform");
        let gauge = Metrics::gauge("ns", "gauge").bind(Arc::clone(&container), "WorkerTransform");
        cnt.inc_by(42);
        dist.update(10);
        dist.update(20);
        gauge.set(100);
    }

    let infos = container.to_monitoring_infos();
    let results = MetricResults::from_monitoring_infos(&infos, &[]);

    assert_eq!(results.counter("ns", "cnt"), Some(42));
    let dist = results.distribution("ns", "dist").unwrap();
    assert_eq!(dist.count, 2);
    assert_eq!(dist.sum, 30);
    let gauge = results.gauge("ns", "gauge").unwrap();
    assert_eq!(gauge.value, 100);
}

#[test]
fn test_metric_results_advanced_queries_and_committed() {
    use beam::metrics::{MetricFilter, MetricResults, Metrics, MetricsContainer};
    use std::sync::Arc;

    let container = Arc::new(MetricsContainer::new());
    {
        let cnt = Metrics::counter("my_ns", "my_cnt").bind(Arc::clone(&container), "Transform1");
        let dist =
            Metrics::distribution("my_ns", "my_dist").bind(Arc::clone(&container), "Transform1");
        let gauge = Metrics::gauge("my_ns", "my_gauge").bind(Arc::clone(&container), "Transform1");
        cnt.inc_by(10);
        dist.update(5);
        gauge.set(200);
    }

    let infos = container.to_monitoring_infos();

    // Test committed monitoring infos (second argument).
    let results = MetricResults::from_monitoring_infos(&[], &infos);

    assert_eq!(results.counter("my_ns", "my_cnt"), Some(10));
    assert_eq!(
        results.counter_for_transform("Transform1", "my_ns", "my_cnt"),
        Some(10)
    );

    let dist = results
        .distribution_for_transform("Transform1", "my_ns", "my_dist")
        .unwrap();
    assert_eq!(dist.count, 1);
    assert_eq!(dist.sum, 5);

    let gauge = results
        .gauge_for_transform("Transform1", "my_ns", "my_gauge")
        .unwrap();
    assert_eq!(gauge.value, 200);

    assert_eq!(results.all_counters().len(), 1);
    assert_eq!(results.all_distributions().len(), 1);
    assert_eq!(results.all_gauges().len(), 1);

    let filter = MetricFilter::all()
        .with_namespace("my_ns")
        .with_name("my_dist");
    let dists = results.query_distributions(&filter);
    assert_eq!(dists.len(), 1);

    let filter_gauge = MetricFilter::empty()
        .with_namespace("my_ns")
        .with_name("my_gauge");
    let gauges = results.query_gauges(&filter_gauge);
    assert_eq!(gauges.len(), 1);

    // Gauge selection across transforms and committed/attempted precedence are
    // covered with explicit timestamps in metrics_query_test.rs.
}

/// Records a metric as a helper function without a context does.
fn count_line(line: &str) {
    Metrics::counter("app", "lines").inc();
    Metrics::distribution("app", "line_len").update(line.len() as i64);
    Metrics::gauge("app", "last_len").set(line.len() as i64);
}

#[test]
fn unbound_metrics_record_into_the_current_scope() {
    use beam::metrics::{MetricResults, MetricsContainer, MetricsScope};
    use std::sync::Arc;

    let container = Arc::new(MetricsContainer::new());
    {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("ExtractWords"));
        count_line("to be");
        count_line("or not");
    }
    let results = MetricResults::from_container(&container);

    assert_eq!(
        results.counter_for_transform("ExtractWords", "app", "lines"),
        Some(2),
        "unbound metrics must be attributed to the scope's transform"
    );
    let dist = results
        .distribution_for_transform("ExtractWords", "app", "line_len")
        .expect("distribution recorded");
    assert_eq!((dist.count, dist.min, dist.max), (2, 5, 6));
    assert_eq!(
        results
            .gauge_for_transform("ExtractWords", "app", "last_len")
            .map(|g| g.value),
        Some(6)
    );
}

#[test]
fn nested_scopes_attribute_to_the_innermost_transform_and_restore() {
    use beam::metrics::{MetricResults, MetricsContainer, MetricsScope};
    use std::sync::Arc;

    let container = Arc::new(MetricsContainer::new());
    {
        let _outer = MetricsScope::enter(Arc::clone(&container), Arc::from("Producer"));
        Metrics::counter("app", "calls").inc();
        {
            // A fused consumer runs inside the call of its producer.
            let _inner = MetricsScope::enter(Arc::clone(&container), Arc::from("Consumer"));
            Metrics::counter("app", "calls").inc_by(10);
        }
        Metrics::counter("app", "calls").inc_by(100);
    }
    let results = MetricResults::from_container(&container);

    assert_eq!(
        results.counter_for_transform("Producer", "app", "calls"),
        Some(101)
    );
    assert_eq!(
        results.counter_for_transform("Consumer", "app", "calls"),
        Some(10)
    );
}

#[test]
fn unbound_metrics_outside_any_scope_are_no_ops_and_bound_ones_ignore_the_scope() {
    use beam::metrics::{MetricResults, MetricsContainer, MetricsScope};
    use std::sync::Arc;

    // Without a scope, there is no container to record into. The call must not panic.
    count_line("orphan");

    let scoped = Arc::new(MetricsContainer::new());
    let explicit = Arc::new(MetricsContainer::new());
    {
        let _scope = MetricsScope::enter(Arc::clone(&scoped), Arc::from("Scoped"));
        Metrics::counter("app", "bound")
            .bind(Arc::clone(&explicit), "Explicit")
            .inc();
    }

    assert_eq!(
        MetricResults::from_container(&explicit).counter_for_transform("Explicit", "app", "bound"),
        Some(1)
    );
    assert_eq!(
        MetricResults::from_container(&scoped).counter("app", "bound"),
        None
    );
}

#[test]
fn selected_transforms_record_while_selected_and_restore_on_return() {
    use beam::metrics::{MetricResults, MetricsContainer, MetricsScope};
    use std::sync::Arc;

    let container = Arc::new(MetricsContainer::new());
    let transforms: Arc<[Arc<str>]> = Arc::from([Arc::from("Producer"), Arc::from("Consumer")]);
    {
        let _scope = MetricsScope::enter_transforms(Arc::clone(&container), transforms);
        // The scope is entered, but no transform is selected, so there is nothing to
        // attribute to.
        Metrics::counter("app", "calls").inc_by(1000);
        {
            let _producer = MetricsScope::select(0);
            Metrics::counter("app", "calls").inc();
            {
                // A fused consumer runs inside the call of its producer.
                let _consumer = MetricsScope::select(1);
                Metrics::counter("app", "calls").inc_by(10);
            }
            Metrics::counter("app", "calls").inc_by(100);
        }
        Metrics::counter("app", "calls").inc_by(1000);
    }
    let results = MetricResults::from_container(&container);

    assert_eq!(
        results.counter_for_transform("Producer", "app", "calls"),
        Some(101)
    );
    assert_eq!(
        results.counter_for_transform("Consumer", "app", "calls"),
        Some(10)
    );
}
