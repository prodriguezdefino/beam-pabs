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

//! Unit tests for `apache-beam-benchmark-io`.

use beam_benchmark_io::{
    LATENCY_BOUNDS_MS, bucket, bucket_names, parse_consumer_pool_size, parse_property,
};

#[test]
fn bucket_includes_the_bound() {
    assert_eq!(bucket(0), 0);
    assert_eq!(bucket(50), 0);
    assert_eq!(bucket(51), 1);
    assert_eq!(bucket(1_000), 4);
    assert_eq!(bucket(300_000), 11);
    assert_eq!(bucket(300_001), LATENCY_BOUNDS_MS.len());
}

#[test]
fn there_is_one_bucket_for_each_bound_and_inf() {
    let names = bucket_names().collect::<Vec<_>>();
    assert_eq!(names.len(), LATENCY_BOUNDS_MS.len() + 1);
    assert_eq!(names[0], "latency_le_50");
    assert_eq!(names[bucket(300_000)], "latency_le_300000");
    assert_eq!(names[bucket(300_001)], "latency_le_inf");
}

#[test]
fn parse_property_splits_at_first_equals_and_rejects_bad_keys() {
    assert_eq!(
        parse_property("client.id=a=b"),
        Ok(("client.id".to_string(), "a=b".to_string()))
    );
    assert_eq!(
        parse_property("linger.ms="),
        Ok(("linger.ms".to_string(), String::new()))
    );
    for bad in ["no_equals", "=1", " fetch.min.bytes=1", "fetch min=1"] {
        assert!(parse_property(bad).is_err(), "{bad}");
    }
}

#[test]
fn parse_consumer_pool_size_bounds_from_one_to_sixteen() {
    assert_eq!(parse_consumer_pool_size("1"), Ok(1));
    assert_eq!(parse_consumer_pool_size("16"), Ok(16));
    for bad in ["0", "17", "-1", "abc"] {
        assert!(parse_consumer_pool_size(bad).is_err(), "{bad}");
    }
}
