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

//! Cell cache tests for `Distribution`, `Gauge`, and `Histogram`: updates across scopes,
//! transforms and threads, `bind` cache resets, NaN handling, and latest-timestamp semantics.

use std::sync::Arc;

use beam_core::internals::{MetricsContainer, MetricsScope, TransformMetrics};
use beam_core::metrics::{DistributionValue, GaugeValue, HistogramBuckets, MetricResults, Metrics};

fn transforms(ids: &[&str]) -> Arc<[Arc<str>]> {
    ids.iter().map(|&id| Arc::from(id)).collect()
}

#[test]
fn cached_distribution_records_across_transforms_scopes_and_extremes() {
    let dist = Metrics::distribution("cache_dist", "latency");
    let container = Arc::new(MetricsContainer::new());
    let metrics = Arc::new(TransformMetrics::new(
        Arc::clone(&container),
        transforms(&["Producer", "Consumer"]),
    ));

    {
        let _scope = MetricsScope::enter_transforms(&metrics);
        for v in [10_i64, 30, 20] {
            let _producer = metrics.select(0);
            dist.update(v);
            let _consumer = metrics.select(1);
            dist.update(-v);
        }
    }

    // Nested scopes record into separate transforms on the same handle.
    {
        let _outer = MetricsScope::enter(Arc::clone(&container), Arc::from("MaxOnly"));
        dist.update(i64::MAX);
        {
            let _inner = MetricsScope::enter(Arc::clone(&container), Arc::from("MinOnly"));
            dist.update(i64::MIN);
        }
    }

    let expected_producer = DistributionValue {
        count: 3,
        sum: 60,
        min: 10,
        max: 30,
    };
    let expected_consumer = DistributionValue {
        count: 3,
        sum: -60,
        min: -30,
        max: -10,
    };
    let expected_max = DistributionValue::new(i64::MAX);
    let expected_min = DistributionValue::new(i64::MIN);

    for results in [
        MetricResults::from_container(&container),
        MetricResults::from_monitoring_infos(&container.to_monitoring_infos(), &[]),
    ] {
        assert_eq!(
            results.distribution_for_transform("Producer", "cache_dist", "latency"),
            Some(expected_producer.clone())
        );
        assert_eq!(
            results.distribution_for_transform("Consumer", "cache_dist", "latency"),
            Some(expected_consumer.clone())
        );
        assert_eq!(
            results.distribution_for_transform("MaxOnly", "cache_dist", "latency"),
            Some(expected_max.clone())
        );
        assert_eq!(
            results.distribution_for_transform("MinOnly", "cache_dist", "latency"),
            Some(expected_min.clone())
        );
    }
}

#[test]
fn cached_gauge_records_across_transforms_and_keeps_latest_timestamp() {
    let gauge = Metrics::gauge("cache_gauge", "depth");
    let container = Arc::new(MetricsContainer::new());
    let metrics = Arc::new(TransformMetrics::new(
        Arc::clone(&container),
        transforms(&["Producer", "Consumer"]),
    ));

    {
        let _scope = MetricsScope::enter_transforms(&metrics);
        for v in [1_i64, 5, 9] {
            let _producer = metrics.select(0);
            gauge.set(v);
            let _consumer = metrics.select(1);
            gauge.set(v * 10);
        }
    }

    for results in [
        MetricResults::from_container(&container),
        MetricResults::from_monitoring_infos(&container.to_monitoring_infos(), &[]),
    ] {
        assert_eq!(
            results
                .gauge_for_transform("Producer", "cache_gauge", "depth")
                .map(|g| g.value),
            Some(9)
        );
        assert_eq!(
            results
                .gauge_for_transform("Consumer", "cache_gauge", "depth")
                .map(|g| g.value),
            Some(90)
        );
    }

    // Explicit timestamps: older updates do not overwrite, ties overwrite, newer overwrites.
    let ts_container = MetricsContainer::new();
    let gauge_at = |name| {
        MetricResults::from_container(&ts_container).gauge_for_transform(
            "Step",
            "cache_gauge",
            name,
        )
    };
    ts_container.set_gauge("Step", "cache_gauge", "ts", 10, 100);
    ts_container.set_gauge("Step", "cache_gauge", "ts", 20, 50);
    assert_eq!(gauge_at("ts"), Some(GaugeValue::new(10, 100)));
    ts_container.set_gauge("Step", "cache_gauge", "ts", 30, 100);
    assert_eq!(gauge_at("ts"), Some(GaugeValue::new(30, 100)));
    ts_container.set_gauge("Step", "cache_gauge", "ts", 40, 200);
    assert_eq!(gauge_at("ts"), Some(GaugeValue::new(40, 200)));

    // Negative first timestamp is preserved and still compared by timestamp order.
    ts_container.set_gauge("Step", "cache_gauge", "neg", 5, -10);
    ts_container.set_gauge("Step", "cache_gauge", "neg", 6, -20);
    assert_eq!(gauge_at("neg"), Some(GaugeValue::new(5, -10)));
    ts_container.set_gauge("Step", "cache_gauge", "neg", 7, -5);
    assert_eq!(gauge_at("neg"), Some(GaugeValue::new(7, -5)));
}

#[test]
fn cached_histogram_records_across_transforms_and_ignores_nan() {
    let buckets = HistogramBuckets::linear(0.0, 10.0, 3).expect("valid buckets");
    let histogram = Metrics::histogram("cache_hist", "sizes", buckets);
    let container = Arc::new(MetricsContainer::new());
    let metrics = Arc::new(TransformMetrics::new(
        Arc::clone(&container),
        transforms(&["Producer", "Consumer", "NanOnly"]),
    ));

    {
        let _scope = MetricsScope::enter_transforms(&metrics);
        {
            let _producer = metrics.select(0);
            for value in [f64::NAN, 0.0, 9.5, 15.0, 50.0, f64::NAN] {
                histogram.update(value);
            }
        }
        {
            let _consumer = metrics.select(1);
            for value in [-1.0, 20.0, f64::NAN, 29.9] {
                histogram.update(value);
            }
        }
        {
            let _nan_only = metrics.select(2);
            histogram.update(f64::NAN);
        }
    }

    for results in [
        MetricResults::from_container(&container),
        MetricResults::from_monitoring_infos(&container.to_monitoring_infos(), &[]),
    ] {
        let producer = results
            .histogram_for_transform("Producer", "cache_hist", "sizes")
            .expect("producer histogram");
        assert_eq!(producer.buckets(), buckets);
        assert_eq!(producer.bucket_counts(), [2, 1, 0]);
        assert_eq!(producer.total(), 4);

        let consumer = results
            .histogram_for_transform("Consumer", "cache_hist", "sizes")
            .expect("consumer histogram");
        assert_eq!(consumer.bucket_counts(), [0, 0, 2]);
        assert_eq!(consumer.total(), 3);

        let nan_only = results
            .histogram_for_transform("NanOnly", "cache_hist", "sizes")
            .expect("nan-only histogram");
        assert_eq!(nan_only.bucket_counts(), [0, 0, 0]);
        assert_eq!(nan_only.total(), 0);
    }
}

#[test]
fn bind_resets_cell_cache_for_distribution_gauge_and_histogram() {
    let buckets = HistogramBuckets::linear(0.0, 5.0, 2).expect("valid buckets");
    let dist = Metrics::distribution("cache_bind", "dist");
    let gauge = Metrics::gauge("cache_bind", "gauge");
    let hist = Metrics::histogram("cache_bind", "hist", buckets);

    let scoped = Arc::new(MetricsContainer::new());
    let bound_container = Arc::new(MetricsContainer::new());

    {
        let _scope = MetricsScope::enter(Arc::clone(&scoped), Arc::from("Scoped"));
        // Warm each handle's cache on the active scope.
        dist.update(10);
        gauge.set(10);
        hist.update(1.0);

        let bound_dist = dist.bind(Arc::clone(&bound_container), "Bound");
        let bound_gauge = gauge.bind(Arc::clone(&bound_container), "Bound");
        let bound_hist = hist.bind(Arc::clone(&bound_container), "Bound");

        // Equality ignores the binding and cell cache.
        assert_eq!(dist, bound_dist);
        assert_eq!(gauge, bound_gauge);
        assert_eq!(hist, bound_hist);

        // Updates on bound handles must go to the bound container, not the active scope.
        bound_dist.update(99);
        bound_gauge.set(99);
        bound_hist.update(6.0);
    }

    let scoped_results = MetricResults::from_container(&scoped);
    assert_eq!(
        scoped_results.distribution_for_transform("Scoped", "cache_bind", "dist"),
        Some(DistributionValue::new(10))
    );
    assert_eq!(
        scoped_results
            .gauge_for_transform("Scoped", "cache_bind", "gauge")
            .map(|g| g.value),
        Some(10)
    );
    let scoped_hist = scoped_results
        .histogram_for_transform("Scoped", "cache_bind", "hist")
        .expect("scoped histogram");
    assert_eq!(
        (scoped_hist.bucket_counts(), scoped_hist.total()),
        (&[1, 0][..], 1)
    );

    let bound_results = MetricResults::from_container(&bound_container);
    assert_eq!(
        bound_results.distribution_for_transform("Bound", "cache_bind", "dist"),
        Some(DistributionValue::new(99))
    );
    assert_eq!(
        bound_results
            .gauge_for_transform("Bound", "cache_bind", "gauge")
            .map(|g| g.value),
        Some(99)
    );
    let bound_hist = bound_results
        .histogram_for_transform("Bound", "cache_bind", "hist")
        .expect("bound histogram");
    assert_eq!(
        (bound_hist.bucket_counts(), bound_hist.total()),
        (&[0, 1][..], 1)
    );
}

/// Runs `update` on `THREADS` threads that all record into the cells of transform `Shared` in
/// one container, so the threads write the same cell at the same time.
fn update_from_threads(update: impl Fn(i64) + Send + Sync + 'static) -> Arc<MetricsContainer> {
    const THREADS: i64 = 4;
    let container = Arc::new(MetricsContainer::new());
    let update = Arc::new(update);
    let threads: Vec<_> = (0..THREADS)
        .map(|thread| {
            let container = Arc::clone(&container);
            let update = Arc::clone(&update);
            std::thread::spawn(move || {
                let _scope = MetricsScope::enter(container, Arc::from("Shared"));
                (0..UPDATES).for_each(|i| update(thread * UPDATES + i));
            })
        })
        .collect();
    threads
        .into_iter()
        .for_each(|thread| thread.join().expect("update thread"));
    container
}

/// Updates per thread in the many-thread tests.
const UPDATES: i64 = 10_000;

#[test]
fn a_shared_distribution_records_exact_totals_from_many_threads() {
    let dist = Arc::new(Metrics::distribution("cache_threads", "latency"));
    let handle = Arc::clone(&dist);
    let container = update_from_threads(move |value| handle.update(value));
    let values = 4 * UPDATES;
    assert_eq!(
        MetricResults::from_container(&container).distribution_for_transform(
            "Shared",
            "cache_threads",
            "latency"
        ),
        Some(DistributionValue {
            count: values,
            sum: values * (values - 1) / 2,
            min: 0,
            max: values - 1,
        })
    );
}

#[test]
fn a_shared_histogram_records_exact_bucket_totals_from_many_threads() {
    let buckets = HistogramBuckets::linear(0.0, 10_000.0, 4).expect("valid buckets");
    let histogram = Arc::new(Metrics::histogram("cache_threads", "sizes", buckets));
    let handle = Arc::clone(&histogram);
    #[expect(clippy::cast_precision_loss, reason = "test values stay below 2^53")]
    let container = update_from_threads(move |value| handle.update(value as f64));
    let recorded = MetricResults::from_container(&container)
        .histogram_for_transform("Shared", "cache_threads", "sizes")
        .expect("shared histogram");
    assert_eq!(recorded.bucket_counts(), [10_000, 10_000, 10_000, 10_000]);
    assert_eq!(recorded.total(), 4 * UPDATES);
}

#[test]
fn a_shared_gauge_keeps_one_of_the_values_set_from_many_threads() {
    let gauge = Arc::new(Metrics::gauge("cache_threads", "level"));
    let handle = Arc::clone(&gauge);
    let container = update_from_threads(move |value| handle.set(value));
    let GaugeValue { value, .. } = MetricResults::from_container(&container)
        .gauge_for_transform("Shared", "cache_threads", "level")
        .expect("shared gauge");
    assert!((0..4 * UPDATES).contains(&value), "{value}");
}
