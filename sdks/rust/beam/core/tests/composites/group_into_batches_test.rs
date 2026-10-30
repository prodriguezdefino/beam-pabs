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

#![expect(
    clippy::unwrap_used,
    reason = "test fixtures unwrap; a failure is a test failure"
)]

//! Tests for the graph, the display data and the configuration errors of `GroupIntoBatches`.

use std::collections::HashMap;
use std::time::Duration;

use beam::coders::{DefaultCoder, ShardedKey, URN_SHARDED_KEY};
use beam::pipeline::{Pipeline, PipelineError, URN_PAR_DO};
use beam::transforms::display_data::{DisplayDataItem, DisplayDataValue};
use beam::transforms::{Create, GroupIntoBatches};
use beam::values::PCollection;
use model::pipeline as proto;

fn keyed_input(p: &Pipeline) -> PCollection<(String, i64)> {
    p.apply(
        "Create",
        Create::new(vec![("a".to_string(), 1i64), ("b".to_string(), 2)]),
    )
}

fn transform<'c>(components: &'c proto::Components, name: &str) -> &'c proto::PTransform {
    components
        .transforms
        .values()
        .find(|t| t.unique_name == name)
        .unwrap_or_else(|| panic!("transform '{name}' must exist"))
}

fn display_data(transform: &proto::PTransform) -> HashMap<String, DisplayDataValue> {
    transform
        .display_data
        .iter()
        .map(|item| DisplayDataItem::from_proto(item).unwrap())
        .map(|item| (item.key, item.value))
        .collect()
}

#[test]
fn test_group_into_batches_is_one_stateful_par_do() {
    let p = Pipeline::new();
    let _: PCollection<(String, Vec<i64>)> =
        keyed_input(&p).apply("Batch", GroupIntoBatches::new(10));
    p.validate().unwrap();

    let components = p.to_proto().components.unwrap();
    let composite = transform(&components, "Batch");
    assert_eq!(composite.subtransforms.len(), 1);
    let par_do = &components.transforms[&composite.subtransforms[0]];
    assert_eq!(par_do.spec.as_ref().unwrap().urn, URN_PAR_DO);
    let payload = <proto::ParDoPayload as prost::Message>::decode(
        par_do.spec.as_ref().unwrap().payload.as_slice(),
    )
    .unwrap();
    let mut state_ids: Vec<_> = payload.state_specs.keys().map(String::as_str).collect();
    state_ids.sort_unstable();
    assert_eq!(state_ids, ["bytes", "count", "min_timestamp", "values"]);
    let mut timer_ids: Vec<_> = payload
        .timer_family_specs
        .keys()
        .map(String::as_str)
        .collect();
    timer_ids.sort_unstable();
    assert_eq!(timer_ids, ["buffering", "end_of_window"]);
}

#[test]
fn test_group_into_batches_display_data_has_the_limits() {
    let p = Pipeline::new();
    let _ = keyed_input(&p).apply(
        "Batch",
        GroupIntoBatches::new(10)
            .with_byte_size(1024)
            .with_max_buffering_duration(Duration::from_secs(5)),
    );

    let components = p.to_proto().components.unwrap();
    let items = display_data(transform(&components, "Batch"));
    assert_eq!(items["batch_size"], DisplayDataValue::Integer(10));
    assert_eq!(items["byte_size"], DisplayDataValue::Integer(1024));
    assert_eq!(
        items["max_buffering_duration"],
        DisplayDataValue::Text("5s".to_string())
    );
    assert_eq!(items["sharded_key"], DisplayDataValue::Boolean(false));
}

#[test]
fn test_group_into_batches_display_data_leaves_out_unset_limits() {
    let p = Pipeline::new();
    let _ = keyed_input(&p).apply("Batch", GroupIntoBatches::new(3));

    let components = p.to_proto().components.unwrap();
    let items = display_data(transform(&components, "Batch"));
    assert_eq!(items["batch_size"], DisplayDataValue::Integer(3));
    assert!(!items.contains_key("byte_size"));
    assert!(!items.contains_key("max_buffering_duration"));
}

#[test]
fn test_group_into_batches_with_sharded_key_outputs_a_sharded_key_coder() {
    let p = Pipeline::new();
    let batches: PCollection<(ShardedKey<String>, Vec<i64>)> =
        keyed_input(&p).apply("Batch", GroupIntoBatches::new(10).with_sharded_key());
    p.validate().unwrap();

    let components = p.to_proto().components.unwrap();
    let composite = transform(&components, "Batch");
    let names: Vec<_> = composite
        .subtransforms
        .iter()
        .map(|id| components.transforms[id].unique_name.as_str())
        .collect();
    assert_eq!(names, ["Batch/AssignShard", "Batch/Batch"]);
    assert_eq!(
        display_data(composite)["sharded_key"],
        DisplayDataValue::Boolean(true)
    );

    let kv_coder = &components.coders[batches.coder_id()];
    let key_coder = &components.coders[&kv_coder.component_coder_ids[0]];
    assert_eq!(key_coder.spec.as_ref().unwrap().urn, URN_SHARDED_KEY);
}

fn construction_reason(p: &Pipeline) -> String {
    match p.validate().unwrap_err() {
        PipelineError::InvalidConstruction {
            transform_id,
            reason,
        } => {
            assert_eq!(transform_id, "Batch");
            reason
        }
        other => panic!("expected a construction error, got {other:?}"),
    }
}

#[test]
fn test_group_into_batches_rejects_a_zero_batch_size() {
    let p = Pipeline::new();
    let _ = keyed_input(&p).apply("Batch", GroupIntoBatches::new(0));
    assert!(construction_reason(&p).contains("batch_size"));
}

#[test]
fn test_group_into_batches_rejects_a_zero_byte_size() {
    let p = Pipeline::new();
    let _ = keyed_input(&p).apply("Batch", GroupIntoBatches::new(5).with_byte_size(0));
    assert!(construction_reason(&p).contains("byte size"));
}

#[test]
fn test_group_into_batches_rejects_a_zero_buffering_duration() {
    let p = Pipeline::new();
    let _ = keyed_input(&p).apply(
        "Batch",
        GroupIntoBatches::new(5)
            .with_sharded_key()
            .with_max_buffering_duration(Duration::ZERO),
    );
    assert!(construction_reason(&p).contains("buffering duration"));
}

#[test]
fn test_sharded_key_golden_bytes_and_round_trip() {
    let value = ShardedKey::new("key".to_string(), b"shard_id".to_vec());
    let bytes = value.encode().unwrap();
    assert_eq!(bytes, b"\x08shard_id\x03key");
    assert_eq!(ShardedKey::<String>::decode(&bytes).unwrap(), value);
}

#[test]
fn test_sharded_key_with_a_varint_key_and_an_empty_shard_id() {
    let value = ShardedKey::new(300i64, Vec::new());
    let bytes = value.encode().unwrap();
    assert_eq!(bytes, [0x00, 0xac, 0x02]);
    assert_eq!(ShardedKey::<i64>::decode(&bytes).unwrap(), value);
}

#[test]
fn test_sharded_key_rejects_a_truncated_shard_id() {
    assert!(ShardedKey::<String>::decode(b"\x08shard").is_err());
}
