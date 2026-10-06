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

//! Tests for CoGroupByKey, KeyedPCollectionTuple, and relational joins.

#![expect(clippy::unwrap_used, reason = "test helpers")]

mod support;

use std::collections::HashMap;

use beam::coders::{BeamIterable, Coder, Context, DefaultCoder};
use beam::pipeline::{URN_FLATTEN, URN_GROUP_BY_KEY, URN_PAR_DO};
use beam::prelude::*;
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::{
    CoGroupByKey, FullOuterJoin, InnerJoin, KeyedPCollectionTuple, LeftJoin, RawUnionValue,
    RightJoin,
};
use testing::{TestPipeline, passert};

fn encode_with<T, C: Coder<T>>(coder: &C, value: &T, context: Context) -> Vec<u8> {
    let mut buf = Vec::new();
    coder.encode(value, &mut buf, context).unwrap();
    buf
}

#[test]
fn raw_union_value_is_laid_out_as_a_kv_of_varint_and_bytes() {
    let value = RawUnionValue {
        tag: 42,
        value: vec![0xDE, 0xAD, 0xBE, 0xEF],
    };
    // The coder declares `beam:coder:kv:v1`. A runner decodes the bytes as
    // `KV<varint, bytes>`, so the layout must match exactly.
    assert_eq!(RawUnionValue::coder().urn(), "beam:coder:kv:v1");
    let nested = [0x2A, 0x04, 0xDE, 0xAD, 0xBE, 0xEF];
    assert_eq!(value.encode().unwrap(), nested);
    assert_eq!(
        value.encode().unwrap(),
        (42i32, vec![0xDEu8, 0xAD, 0xBE, 0xEF]).encode().unwrap()
    );
    for context in [Context::Nested, Context::WholeStream] {
        assert_eq!(
            encode_with(&RawUnionValue::coder(), &value, context),
            encode_with(
                &<(i32, Vec<u8>)>::coder(),
                &(42, vec![0xDE, 0xAD, 0xBE, 0xEF]),
                context
            ),
            "{context:?}"
        );
    }
    let mut elem = Vec::new();
    value.encode_element(&mut elem).unwrap();
    assert_eq!(elem, nested);

    assert_eq!(RawUnionValue::decode(&nested).unwrap(), value);
    assert_eq!(
        RawUnionValue::decode_element(&mut nested.as_slice()).unwrap(),
        value
    );
}

#[test]
fn raw_union_value_registers_the_kv_coder_proto() {
    let p = Pipeline::new();
    let id = RawUnionValue::register_coder(&p);
    assert_eq!(<(i32, Vec<u8>)>::register_coder(&p), id);
    assert_eq!(support::coder_shape(&p.to_proto(), &id), "kv(varint,bytes)");
}

fn sample_result() -> CoGbkResult {
    let values_by_tag = HashMap::from([
        (
            0,
            vec![
                "Alice".to_string().encode().unwrap(),
                "Bob".to_string().encode().unwrap(),
            ],
        ),
        (1, vec![95i64.encode().unwrap()]),
    ]);
    CoGbkResult::new(
        vec!["names".to_string(), "scores".to_string()],
        values_by_tag,
    )
}

#[test]
fn cogbk_result_accessors_decode_each_tag() {
    let result = sample_result();
    assert_eq!(result.tag_names(), &["names", "scores"]);
    assert!(!result.is_empty());

    let names: BeamIterable<String> = result.get("names").expect("names tag lookup");
    assert_eq!(names.into_vec().unwrap(), vec!["Alice", "Bob"]);
    assert_eq!(result.get_vec::<i64>("scores").unwrap(), vec![95]);
    assert_eq!(
        result.get_by_index::<i64>(1).unwrap().into_vec().unwrap(),
        vec![95]
    );
    // An index with no values gives an empty result, not an error.
    assert!(
        result
            .get_by_index::<i64>(7)
            .unwrap()
            .into_vec()
            .unwrap()
            .is_empty()
    );

    let err = result.get::<i64>("unknown").unwrap_err();
    assert_eq!(
        err.to_string(),
        r#"Tag 'unknown' not found in CoGbkResult schema: ["names", "scores"]"#
    );

    // Bytes that do not decode report the tag index. 0xFF is a truncated VarInt.
    let broken = CoGbkResult::new(
        vec!["n".to_string()],
        HashMap::from([(0, vec![vec![0xFF]])]),
    );
    let err = broken.get_vec::<i64>("n").unwrap_err();
    assert!(
        err.to_string()
            .starts_with("Failed to decode CoGbkResult value for tag index 0"),
        "{err}"
    );

    // A declared tag without values returns an empty vector.
    let empty = CoGbkResult::new(vec!["a".to_string(), "orders".to_string()], HashMap::new());
    assert!(empty.get_vec::<i64>("orders").unwrap().is_empty());
    assert!(empty.is_empty());
}

#[test]
fn cogbk_result_bytes_are_the_declared_kv_layout() {
    let result = sample_result();
    assert_eq!(CoGbkResult::coder().urn(), "beam:coder:kv:v1");

    // The layout is `KV<iterable<string>, iterable<KV<varint, iterable<bytes>>>>`. Entries
    // are sorted by tag, so the encoding is deterministic although the source is a HashMap.
    let as_kv = (
        vec!["names".to_string(), "scores".to_string()],
        vec![
            (
                0i32,
                vec![
                    "Alice".to_string().encode().unwrap(),
                    "Bob".to_string().encode().unwrap(),
                ],
            ),
            (1i32, vec![95i64.encode().unwrap()]),
        ],
    );
    let expected = as_kv.encode().unwrap();
    assert_eq!(result.encode().unwrap(), expected);
    for context in [Context::Nested, Context::WholeStream] {
        assert_eq!(
            encode_with(&CoGbkResult::coder(), &result, context),
            encode_with(
                &<(Vec<String>, Vec<(i32, Vec<Vec<u8>>)>)>::coder(),
                &as_kv,
                context
            ),
            "{context:?}"
        );
    }

    // Check the leading bytes: the iterable count (4-byte big-endian), then the
    // length-prefixed tag names.
    assert_eq!(
        &expected[..17],
        &[
            0, 0, 0, 2, 5, b'n', b'a', b'm', b'e', b's', 6, b's', b'c', b'o', b'r', b'e', b's'
        ]
    );

    let decoded = CoGbkResult::decode(&expected).unwrap();
    assert_eq!(decoded, result);
    assert_eq!(
        decoded.get_vec::<String>("names").unwrap(),
        vec!["Alice", "Bob"]
    );

    let p = Pipeline::new();
    let id = CoGbkResult::register_coder(&p);
    assert_eq!(
        support::coder_shape(&p.to_proto(), &id),
        "kv(iterable(string_utf8),iterable(kv(varint,iterable(bytes))))"
    );
}

#[test]
fn cogbk_expands_to_tag_flatten_gbk_construct_chain() {
    let p = Pipeline::new();
    let users = p.apply(Create::new(
        "CreateUsers",
        vec![(1i64, "Alice".to_string()), (2i64, "Bob".to_string())],
    ));
    let orders = p.apply(Create::new(
        "CreateOrders",
        vec![(1i64, 100i64), (1i64, 250i64), (3i64, 50i64)],
    ));

    let grouped = KeyedPCollectionTuple::of("users", &users)
        .and("orders", &orders)
        .apply(CoGroupByKey::new("JoinUsersOrders"));

    let proto = p.to_proto();

    // A separate ParDo tags each input.
    for (tag, source) in [
        ("Tag[users]", "CreateUsers/Process"),
        ("Tag[orders]", "CreateOrders/Process"),
    ] {
        assert_eq!(support::urn(support::transform(&proto, tag)), URN_PAR_DO);
        assert_eq!(support::input_producer_names(&proto, tag), [source]);
        assert_eq!(
            support::output_coder(&proto, tag),
            "kv(varint,kv(varint,bytes))"
        );
    }
    // A Flatten merges the tagged collections.
    let flatten = "JoinUsersOrders/Flatten";
    assert_eq!(
        support::urn(support::transform(&proto, flatten)),
        URN_FLATTEN
    );
    assert_eq!(
        support::input_producer_names(&proto, flatten),
        ["Tag[orders]", "Tag[users]"]
    );
    assert_eq!(
        support::output_coder(&proto, flatten),
        "kv(varint,kv(varint,bytes))"
    );
    // A GroupByKey groups the merged collection.
    let gbk = "JoinUsersOrders/GroupByKey";
    assert_eq!(
        support::urn(support::transform(&proto, gbk)),
        URN_GROUP_BY_KEY
    );
    assert_eq!(support::input_producer_names(&proto, gbk), [flatten]);
    assert_eq!(
        support::output_coder(&proto, gbk),
        "kv(varint,iterable(kv(varint,bytes)))"
    );
    // A ParDo converts each group into a `CoGbkResult`.
    let construct = "JoinUsersOrders/ConstructCoGbkResult";
    assert_eq!(
        support::urn(support::transform(&proto, construct)),
        URN_PAR_DO
    );
    assert_eq!(support::input_producer_names(&proto, construct), [gbk]);
    assert_eq!(support::single_output(&proto, construct), grouped.id());
    assert_eq!(
        support::pcoll_coder(&proto, grouped.id()),
        "kv(varint,kv(iterable(string_utf8),iterable(kv(varint,iterable(bytes)))))"
    );
}

/// Returns `(key, sorted users, sorted orders)` for each grouped element.
fn flatten_result((k, res): (i64, CoGbkResult)) -> (i64, (Vec<String>, Vec<i64>)) {
    let mut users: Vec<String> = res.get_vec("users").unwrap();
    let mut orders: Vec<i64> = res.get_vec("orders").unwrap();
    users.sort();
    orders.sort();
    (k, (users, orders))
}

#[tokio::test]
async fn cogbk_groups_every_input_by_key() {
    let p = TestPipeline::new();
    let users = p.apply(Create::new(
        "CreateUsers",
        vec![(1i64, "Alice".to_string()), (2i64, "Bob".to_string())],
    ));
    let orders = p.apply(Create::new(
        "CreateOrders",
        vec![(1i64, 250i64), (1i64, 100i64), (3i64, 50i64)],
    ));
    let grouped = KeyedPCollectionTuple::of("users", &users)
        .and("orders", &orders)
        .apply(CoGroupByKey::new("Grouped"));
    let rows = grouped.apply(Map::new("Flatten", flatten_result));

    passert::that("AssertRows", &rows).contains_in_any_order([
        (1i64, (vec!["Alice".to_string()], vec![100i64, 250])),
        (2i64, (vec!["Bob".to_string()], vec![])),
        (3i64, (vec![], vec![50i64])),
    ]);
    p.run().await.expect("pipeline and assertions");
}

#[test]
fn cogbk_display_data_names_the_transform() {
    let cogbk = CoGroupByKey::new("MyCustomCoGbk");
    let mut builder = DisplayDataBuilder::new();
    cogbk.populate_display_data(&mut builder);
    let items: Vec<_> = builder
        .build()
        .into_iter()
        .map(|d| (d.key, d.value))
        .collect();
    assert_eq!(
        items,
        [
            ("transform".to_string(), "CoGroupByKey".to_string()),
            ("name".to_string(), "MyCustomCoGbk".to_string()),
        ]
    );
}

#[test]
#[should_panic(expected = "Duplicate tag 'dup' in KeyedPCollectionTuple")]
fn keyed_tuple_rejects_duplicate_tags() {
    let p = Pipeline::new();
    let a = p.apply(Create::new("A", vec![(1i64, 1i64)]));
    let _ = KeyedPCollectionTuple::of("dup", &a).and("dup", &a);
}

#[test]
#[should_panic(expected = "Cannot CoGroupByKey with an empty KeyedPCollectionTuple")]
fn cogbk_rejects_an_empty_tuple() {
    let p = Pipeline::new();
    let _ = KeyedPCollectionTuple::<i64>::empty(p).apply(CoGroupByKey::new("Empty"));
}

#[tokio::test]
async fn relational_joins_produce_exact_rows() {
    let p = TestPipeline::new();
    let left = p.apply(Create::new(
        "Left",
        vec![(1i64, "L1".to_string()), (2i64, "L2".to_string())],
    ));
    let right = p.apply(Create::new(
        "Right",
        vec![(1i64, 10i64), (1i64, 11i64), (3i64, 30i64)],
    ));
    let l1 = || "L1".to_string();

    let inner = left.apply(InnerJoin::new("Inner", &right));
    passert::that("Inner", &inner).contains_in_any_order([(1i64, (l1(), 10i64)), (1, (l1(), 11))]);

    let left_joined = left.apply(LeftJoin::new("LeftJoin", &right));
    passert::that("Left", &left_joined).contains_in_any_order([
        (1i64, (l1(), Some(10i64))),
        (1, (l1(), Some(11))),
        (2, ("L2".to_string(), None)),
    ]);

    let right_joined = left.apply(RightJoin::new("RightJoin", &right));
    passert::that("Right", &right_joined).contains_in_any_order([
        (1i64, (Some(l1()), 10i64)),
        (1, (Some(l1()), 11)),
        (3, (None, 30)),
    ]);

    let full = left.apply(FullOuterJoin::new("FullOuter", &right));
    passert::that("FullOuter", &full).contains_in_any_order([
        (1i64, (Some(l1()), Some(10i64))),
        (1, (Some(l1()), Some(11))),
        (2, (Some("L2".to_string()), None)),
        (3, (None, Some(30))),
    ]);

    p.run().await.expect("pipeline and assertions");
}
