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

//! Tests for 120-bit key-space arithmetic and `x-goog-request-params` target routing headers.

use std::cmp::Ordering;

use gcp::bigtable::key::{
    compare_end, estimate_fraction, interpolate, is_valid_range, next_key, split_evenly,
};
use gcp::bigtable::proto::{MutateRowsRequest, ReadRowsRequest, SampleRowKeysRequest};
use gcp::bigtable::target::{ReadTarget, WriteTarget, percent_encode};

#[test]
fn next_key_appends_zero_byte_as_immediate_lexicographic_successor() {
    assert_eq!(next_key(b""), vec![0]);
    assert_eq!(next_key(b"abc"), b"abc\0".to_vec());
    assert!(b"abc".as_slice() < next_key(b"abc").as_slice());
}

#[test]
fn compare_end_and_is_valid_range_treat_empty_end_as_infinity() {
    assert_eq!(compare_end(b"", b""), Ordering::Equal);
    assert_eq!(compare_end(b"z", b""), Ordering::Less);
    assert_eq!(compare_end(b"", b"z"), Ordering::Greater);
    assert_eq!(compare_end(b"a", b"b"), Ordering::Less);

    assert!(is_valid_range(b"", b""));
    assert!(is_valid_range(b"z", b""));
    assert!(is_valid_range(b"a", b"b"));
    assert!(!is_valid_range(b"b", b"b"));
    assert!(!is_valid_range(b"c", b"b"));
}

#[test]
fn estimate_fraction_and_interpolate_are_consistent_across_prefixes() {
    let start = b"tenant#0001#";
    let end = b"tenant#0009#";
    let mid = interpolate(start, end, 0.5).unwrap();
    assert_eq!(&mid[..], b"tenant#0005#");

    let frac = estimate_fraction(start, end, &mid);
    assert!((frac - 0.5).abs() < 1e-4, "frac = {frac}");
    assert_eq!(estimate_fraction(start, end, start), 0.0);
    assert_eq!(estimate_fraction(start, end, end), 1.0);
}

#[test]
fn split_evenly_partitions_range_into_contiguous_subranges() {
    let ranges = split_evenly(b"a", b"e", 4);
    assert_eq!(ranges.len(), 4);
    assert_eq!(ranges[0].0, b"a");
    assert_eq!(ranges.last().unwrap().1, b"e");
    for pair in ranges.windows(2) {
        assert_eq!(pair[0].1, pair[1].0);
        assert!(is_valid_range(&pair[0].0, &pair[0].1));
    }
}

#[test]
fn read_target_routing_headers_support_tables_materialized_views_and_databoost() {
    let table = ReadTarget::table("proj-1", "inst-1", "tbl-1");
    table.validate().unwrap();
    assert_eq!(
        table.resource_name(),
        "projects/proj-1/instances/inst-1/tables/tbl-1"
    );
    assert_eq!(
        table.routing_header(None),
        "table_name=projects%2Fproj-1%2Finstances%2Finst-1%2Ftables%2Ftbl-1"
    );
    assert_eq!(
        table.routing_header(Some("databoost-profile")),
        "table_name=projects%2Fproj-1%2Finstances%2Finst-1%2Ftables%2Ftbl-1&app_profile_id=databoost-profile"
    );

    let mv = ReadTarget::materialized_view("proj-1", "inst-1", "mv-daily");
    mv.validate().unwrap();
    assert_eq!(
        mv.resource_name(),
        "projects/proj-1/instances/inst-1/materializedViews/mv-daily"
    );
    assert_eq!(
        mv.routing_header(Some("databoost-mv")),
        "name=projects%2Fproj-1%2Finstances%2Finst-1&app_profile_id=databoost-mv"
    );

    let av = ReadTarget::authorized_view("proj-1", "inst-1", "tbl-1", "av-1");
    av.validate().unwrap();
    assert_eq!(
        av.resource_name(),
        "projects/proj-1/instances/inst-1/tables/tbl-1/authorizedViews/av-1"
    );
    assert_eq!(
        av.routing_header(Some("ap#1")),
        "table_name=projects%2Fproj-1%2Finstances%2Finst-1%2Ftables%2Ftbl-1&app_profile_id=ap%231"
    );
}

#[test]
fn targets_populate_sample_read_and_mutate_proto_requests() {
    let mv = ReadTarget::materialized_view("p", "i", "v");
    let mut sample_req = SampleRowKeysRequest::default();
    mv.apply_to_sample(&mut sample_req, Some("boost"));
    assert_eq!(
        sample_req.materialized_view_name,
        "projects/p/instances/i/materializedViews/v"
    );
    assert_eq!(sample_req.app_profile_id, "boost");
    assert!(sample_req.table_name.is_empty());

    let mut read_req = ReadRowsRequest::default();
    mv.apply_to_read(&mut read_req, Some("boost"));
    assert_eq!(
        read_req.materialized_view_name,
        "projects/p/instances/i/materializedViews/v"
    );
    assert_eq!(read_req.app_profile_id, "boost");

    let wt = WriteTarget::authorized_view("p", "i", "t", "av");
    wt.validate().unwrap();
    let mut mutate_req = MutateRowsRequest::default();
    wt.apply_to_mutate(&mut mutate_req, Some("write-ap"));
    assert_eq!(
        mutate_req.authorized_view_name,
        "projects/p/instances/i/tables/t/authorizedViews/av"
    );
    assert_eq!(mutate_req.app_profile_id, "write-ap");
    assert_eq!(
        wt.routing_header(Some("write-ap")),
        "table_name=projects%2Fp%2Finstances%2Fi%2Ftables%2Ft&app_profile_id=write-ap"
    );
    assert_eq!(percent_encode("a/b c~-_.1"), "a%2Fb%20c~-_.1");
}

#[test]
fn routing_cookies_extract_and_inject_cbt_cookie_headers() {
    use gcp::bigtable::client::RoutingCookies;
    use tonic::metadata::{MetadataMap, MetadataValue};

    let mut trailers = MetadataMap::new();
    trailers.insert(
        "x-goog-cbt-cookie-databoost",
        MetadataValue::from_static("route-token-1"),
    );
    trailers.insert("other-header", MetadataValue::from_static("ignored"));

    let mut cookies = RoutingCookies::default();
    cookies.extract(&trailers);

    let mut outgoing = MetadataMap::new();
    cookies.inject(&mut outgoing);
    assert_eq!(
        outgoing
            .get("x-goog-cbt-cookie-databoost")
            .and_then(|v| v.to_str().ok()),
        Some("route-token-1")
    );
    assert!(outgoing.get("other-header").is_none());
}
