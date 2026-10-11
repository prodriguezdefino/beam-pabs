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

//! The string set, bounded trie and histogram metrics: their payloads, their merge rules and
//! their path from a container to query results.

use std::collections::BTreeSet;
use std::sync::Arc;

use beam::internals::MetricsContainer;
use beam::metrics::{
    BoundedTrieValue, HistogramBuckets, HistogramValue, MetricResults, Metrics,
    STRING_SET_MAX_BYTES, StringSetValue,
};

fn path(segments: &[&str], truncated: bool) -> (Vec<String>, bool) {
    (segments.iter().map(|s| s.to_string()).collect(), truncated)
}

#[test]
fn a_string_set_encodes_as_a_count_and_length_prefixed_strings() {
    let mut set = StringSetValue::new();
    assert!(set.add("bc"));
    assert!(set.add("a"));
    assert!(set.add("a"));

    let payload = set.encode_payload();
    assert_eq!(payload, [0, 0, 0, 2, 1, b'a', 2, b'b', b'c']);
    assert_eq!(StringSetValue::decode_payload(&payload), Ok(set));
    assert!(StringSetValue::decode_payload(&[0, 0, 0, 1, 5, b'a']).is_err());
}

#[test]
fn a_full_string_set_drops_new_strings_and_keeps_known_ones() {
    let mut set = StringSetValue::new();
    let big = "x".repeat(STRING_SET_MAX_BYTES);
    assert!(set.add(&big));
    assert!(!set.add("y"));
    assert!(set.add(&big));

    let mut other = StringSetValue::new();
    other.add("z");
    assert!(!set.merge(&other));
    assert_eq!(set.values().len(), 1);
}

#[test]
fn a_bounded_trie_truncates_the_largest_subtree_past_its_bound() {
    let mut trie = BoundedTrieValue::new(3);
    trie.add(&["a", "x"]);
    trie.add(&["a", "y"]);
    trie.add(&["b"]);
    assert_eq!(trie.paths().len(), 3);
    trie.add(&["c"]);

    assert_eq!(
        trie.paths(),
        BTreeSet::from([path(&["a"], true), path(&["b"], false), path(&["c"], false)])
    );
    // A truncated branch takes no new paths.
    trie.add(&["a", "z"]);
    assert_eq!(trie.paths().len(), 3);
}

#[test]
fn bounded_tries_merge_under_the_smaller_bound_and_round_trip_their_payload() {
    let mut ours = BoundedTrieValue::new(10);
    ours.add(&["bq", "p", "t1"]);
    let mut theirs = BoundedTrieValue::new(2);
    theirs.add(&["bq", "p", "t2"]);
    theirs.add(&["gcs", "bucket"]);

    ours.merge(&theirs);
    assert_eq!(ours.bound(), 2);
    assert_eq!(
        ours.paths(),
        BTreeSet::from([path(&["bq", "p"], true), path(&["gcs", "bucket"], false)])
    );
    assert_eq!(
        BoundedTrieValue::decode_payload(&ours.encode_payload()),
        Ok(ours)
    );
}

#[test]
fn a_bounded_trie_payload_may_hold_a_singleton() {
    let payload = prost::Message::encode_to_vec(&model::pipeline::BoundedTrie {
        bound: 5,
        root: None,
        singleton: vec!["kafka".to_string(), "topic".to_string()],
    });
    let trie = BoundedTrieValue::decode_payload(&payload).expect("a singleton trie");
    assert_eq!(
        trie.paths(),
        BTreeSet::from([path(&["kafka", "topic"], false)])
    );
}

#[test]
fn linear_histograms_count_values_per_bucket_and_outside_only_in_the_total() {
    let buckets = HistogramBuckets::linear(0.0, 10.0, 3).expect("valid buckets");
    let mut histogram = HistogramValue::new(buckets);
    [-1.0, 0.0, 9.9, 10.0, 29.9, 30.0, f64::NAN]
        .into_iter()
        .for_each(|value| histogram.record(value));

    assert_eq!(histogram.bucket_counts(), [2, 1, 1]);
    assert_eq!(histogram.total(), 6);
    assert_eq!(
        HistogramValue::decode_payload(&histogram.encode_payload()),
        Ok(histogram.clone())
    );

    let mut other = HistogramValue::new(HistogramBuckets::linear(0.0, 5.0, 3).expect("valid"));
    other.record(1.0);
    assert!(!histogram.merge(&other), "other buckets do not merge");
    assert_eq!(histogram.total(), 6);
}

#[test]
fn exponential_histograms_grow_by_two_to_the_two_to_the_minus_scale() {
    let buckets = HistogramBuckets::exponential(0, 3).expect("valid buckets");
    let mut histogram = HistogramValue::new(buckets);
    [0.5, 1.0, 1.9, 2.0, 3.9, 4.0, 8.0, -1.0]
        .into_iter()
        .for_each(|value| histogram.record(value));
    assert_eq!(histogram.bucket_counts(), [3, 2, 1]);
    assert_eq!(histogram.total(), 8);
}

#[test]
fn invalid_histogram_buckets_are_errors() {
    assert!(HistogramBuckets::linear(0.0, 0.0, 3).is_err());
    assert!(HistogramBuckets::linear(0.0, 1.0, 0).is_err());
    assert!(HistogramBuckets::linear(f64::NAN, 1.0, 3).is_err());
    assert!(HistogramBuckets::exponential(4, 3).is_err());
    assert!(HistogramBuckets::exponential(0, 0).is_err());
}

#[test]
fn new_metric_types_reach_query_results_through_monitoring_infos() {
    let container = Arc::new(MetricsContainer::new());
    let buckets = HistogramBuckets::linear(0.0, 1.0, 2).expect("valid buckets");
    let set = Metrics::string_set("lineage", "sources").bind(Arc::clone(&container), "read");
    let trie = Metrics::bounded_trie("lineage", "sources_v2").bind(Arc::clone(&container), "read");
    let histogram =
        Metrics::histogram("io", "latency", buckets).bind(Arc::clone(&container), "read");
    set.add("bigquery:p.d.t");
    set.add("bigquery:p.d.t");
    trie.add(&["bigquery", "p", "d.t"]);
    histogram.update(0.5);
    histogram.update(7.0);

    let results = MetricResults::from_monitoring_infos(&[], &container.to_monitoring_infos());

    let sources = results
        .string_set("lineage", "sources")
        .expect("a string set");
    assert_eq!(
        sources.values(),
        &BTreeSet::from(["bigquery:p.d.t".to_string()])
    );
    let trie = results
        .bounded_trie_for_transform("read", "lineage", "sources_v2")
        .expect("a bounded trie");
    assert_eq!(
        trie.paths(),
        BTreeSet::from([path(&["bigquery", "p", "d.t"], false)])
    );
    let latency = results.histogram("io", "latency").expect("a histogram");
    assert_eq!((latency.bucket_counts(), latency.total()), (&[1, 0][..], 2));

    let from_container = MetricResults::from_container(&container);
    assert_eq!(
        from_container.string_set("lineage", "sources"),
        Some(sources)
    );
}
