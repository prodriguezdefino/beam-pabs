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

//! User metrics recorded with `Metrics::counter(..)` from plain helper functions reach
//! the runner, attributed to the transform that called them.

use beam::metrics::MetricFilter;
use beam::prelude::*;
use prism::PrismRunner;

/// A helper with no context at hand, as in the WordCount example.
fn split(line: &str) -> Vec<String> {
    Metrics::counter("test", "lines").inc();
    Metrics::distribution("test", "line_len").update(line.len() as i64);
    line.split_whitespace().map(str::to_string).collect()
}

fn format(word: &str) -> String {
    Metrics::counter("test", "formatted").inc();
    Metrics::gauge("test", "last_len").set(word.len() as i64);
    word.to_uppercase()
}

#[tokio::test]
async fn unbound_user_metrics_are_reported_to_the_runner() {
    let p = Pipeline::new();
    let _ = p
        .apply(
            "Create",
            Create::new(vec!["a bb".to_string(), "ccc".to_string()]),
        )
        .par_do_fn("Split", |line: String, out| {
            split(&line).into_iter().try_for_each(|w| out.emit(w))
        })
        .map("Format", |word: String| format(&word));

    let result = p
        .run_with_runner(&PrismRunner::new())
        .await
        .expect("pipeline failed");
    let metrics = result.metrics().expect("prism reports metrics");

    assert_eq!(metrics.counter("test", "lines"), Some(2));
    assert_eq!(metrics.counter("test", "formatted"), Some(3));
    let len = metrics
        .distribution("test", "line_len")
        .expect("distribution reported");
    assert_eq!((len.count, len.sum, len.min, len.max), (2, 7, 3, 4));
    assert!(
        metrics.gauge("test", "last_len").is_some(),
        "gauge reported"
    );

    // Each metric belongs to the transform whose code recorded it, even though Format
    // runs fused inside Split's call.
    let transform_of = |name: &str| -> Vec<String> {
        metrics
            .query_counters(&MetricFilter::default().with_name(name))
            .into_iter()
            .map(|r| r.key.transform_id)
            .collect()
    };
    let (lines, formatted) = (transform_of("lines"), transform_of("formatted"));
    assert_eq!(lines.len(), 1, "lines reported under {lines:?}");
    assert_eq!(formatted.len(), 1, "formatted reported under {formatted:?}");
    assert_ne!(lines, formatted, "each transform reports its own metrics");
}

#[tokio::test]
async fn string_set_metrics_are_reported_to_the_runner() {
    let p = Pipeline::new();
    let _ = p
        .apply(
            "Create",
            Create::new(vec!["a/b".to_string(), "a/c".to_string(), "d".to_string()]),
        )
        .map("Record", |path: String| {
            let segments: Vec<&str> = path.split('/').collect();
            Metrics::string_set("test", "paths").add(&path);
            // Prism drops these two kinds. The pipeline must still succeed.
            Metrics::bounded_trie("test", "tree").add(&segments);
            let buckets = HistogramBuckets::linear(0.0, 2.0, 2).expect("valid buckets");
            Metrics::histogram("test", "len", buckets).update(path.len() as f64);
            path
        });

    let result = p
        .run_with_runner(&PrismRunner::new())
        .await
        .expect("pipeline failed");
    let metrics = result.metrics().expect("prism reports metrics");

    let paths = metrics
        .string_set("test", "paths")
        .expect("string set reported");
    assert_eq!(
        paths
            .values()
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["a/b", "a/c", "d"]
    );
}
