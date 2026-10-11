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

//! The cell cache of a counter: one handle that records in many bundles, transforms, scopes
//! and threads gives the same totals as a new handle for each update.

use std::sync::Arc;

use beam::internals::{MetricsContainer, MetricsScope, TransformMetrics, process_wide_container};
use beam::metrics::{Counter, MetricResults, Metrics};

fn transforms(ids: &[&str]) -> Arc<[Arc<str>]> {
    ids.iter().map(|&id| Arc::from(id)).collect()
}

fn total(container: &MetricsContainer, transform: &str, counter: &Counter) -> Option<i64> {
    MetricResults::from_container(container).counter_for_transform(
        transform,
        counter.namespace(),
        counter.name(),
    )
}

#[test]
fn a_counter_field_records_into_the_container_of_each_bundle() {
    let counter = Metrics::counter("cache_bundles", "words");
    let mut bundles = Vec::new();
    for bundle in 1..=3 {
        // Each bundle has a new container, as in the worker.
        let container = Arc::new(MetricsContainer::new());
        let metrics = Arc::new(TransformMetrics::new(
            Arc::clone(&container),
            transforms(&["Count"]),
        ));
        let _scope = MetricsScope::enter_transforms(&metrics);
        let _count = metrics.select(0);
        (0..bundle).for_each(|_| counter.inc_by(10));
        bundles.push(container);
    }

    let totals: Vec<_> = bundles
        .iter()
        .map(|container| total(container, "Count", &counter))
        .collect();
    assert_eq!(totals, [Some(10), Some(20), Some(30)]);
}

#[test]
fn a_counter_field_records_after_its_last_container_is_dropped() {
    let counter = Metrics::counter("cache_dropped", "words");
    for bundle in 1..=5 {
        let container = Arc::new(MetricsContainer::new());
        let metrics = Arc::new(TransformMetrics::new(
            Arc::clone(&container),
            transforms(&["Count"]),
        ));
        {
            let _scope = MetricsScope::enter_transforms(&metrics);
            let _count = metrics.select(0);
            (0..bundle).for_each(|_| counter.inc());
        }
        // The next container can get the address of this one.
        drop(metrics);
        assert_eq!(total(&container, "Count", &counter), Some(bundle));
    }
}

#[test]
fn a_counter_used_by_two_transforms_records_for_each() {
    let counter = Metrics::counter("cache_transforms", "calls");
    let container = Arc::new(MetricsContainer::new());
    let metrics = Arc::new(TransformMetrics::new(
        Arc::clone(&container),
        transforms(&["Producer", "Consumer"]),
    ));
    {
        let _scope = MetricsScope::enter_transforms(&metrics);
        for _ in 0..100 {
            let _producer = metrics.select(0);
            counter.inc();
            let _consumer = metrics.select(1);
            counter.inc_by(2);
        }
    }

    assert_eq!(total(&container, "Producer", &counter), Some(100));
    assert_eq!(total(&container, "Consumer", &counter), Some(200));
}

#[test]
fn a_counter_records_into_each_scope_of_one_container() {
    let counter = Metrics::counter("cache_scopes", "calls");
    let container = Arc::new(MetricsContainer::new());
    for _ in 0..3 {
        let _outer = MetricsScope::enter(Arc::clone(&container), Arc::from("Outer"));
        counter.inc();
        {
            let _inner = MetricsScope::enter(Arc::clone(&container), Arc::from("Inner"));
            counter.inc_by(10);
        }
        counter.inc();
    }

    assert_eq!(total(&container, "Outer", &counter), Some(6));
    assert_eq!(total(&container, "Inner", &counter), Some(30));
}

#[test]
fn a_counter_records_into_the_process_wide_container_outside_a_scope() {
    let counter = Metrics::counter("cache_process_wide", "calls");
    let container = Arc::new(MetricsContainer::new());
    counter.inc();
    {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("Scoped"));
        counter.inc_by(5);
    }
    counter.inc();

    assert_eq!(total(process_wide_container(), "", &counter), Some(2));
    assert_eq!(total(&container, "Scoped", &counter), Some(5));
}

#[test]
fn a_bound_counter_and_its_clones_record_into_one_cell() {
    let container = Arc::new(MetricsContainer::new());
    let bound = Metrics::counter("cache_bound", "rows").bind(Arc::clone(&container), "Write");
    let clone = bound.clone();
    (0..50).for_each(|_| {
        bound.inc();
        clone.inc();
    });

    assert_eq!(total(&container, "Write", &bound), Some(100));
}

#[test]
fn a_shared_counter_records_exact_totals_from_many_threads() {
    const UPDATES: i64 = 10_000;
    let counter = Arc::new(Metrics::counter("cache_threads", "words"));
    let containers: Vec<_> = (0..4)
        .map(|_| {
            let counter = Arc::clone(&counter);
            std::thread::spawn(move || {
                let container = Arc::new(MetricsContainer::new());
                let metrics = Arc::new(TransformMetrics::new(
                    Arc::clone(&container),
                    transforms(&["Count"]),
                ));
                let _scope = MetricsScope::enter_transforms(&metrics);
                let _count = metrics.select(0);
                (0..UPDATES).for_each(|_| counter.inc());
                container
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|thread| thread.join().expect("counter thread"))
        .collect();

    containers.iter().for_each(|container| {
        assert_eq!(total(container, "Count", &counter), Some(UPDATES));
    });
}

#[test]
fn a_counter_reports_its_total_in_the_monitoring_infos() {
    let container = Arc::new(MetricsContainer::new());
    let counter = Metrics::counter("cache_report", "words").bind(Arc::clone(&container), "Count");
    (0..7).for_each(|_| counter.inc());
    counter.dec_by(2);

    let results = MetricResults::from_monitoring_infos(&container.to_monitoring_infos(), &[]);

    assert_eq!(
        results.counter_for_transform("Count", "cache_report", "words"),
        Some(5)
    );
    assert_eq!(
        container.counters().values().copied().collect::<Vec<_>>(),
        [5]
    );
}
