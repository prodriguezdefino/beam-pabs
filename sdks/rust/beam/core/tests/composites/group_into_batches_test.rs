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

//! Tests for the graph, display data, configuration errors, and timer behavior of
//! `GroupIntoBatches`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beam::Instant;
use beam::coders::{
    DefaultCoder, IntervalWindow, PaneInfo, ShardedKey, TimerAction, TimerRecord, URN_SHARDED_KEY,
    WindowedHeader,
};
use beam::internals::{
    ElementSink, HandlerContext, HandlerInstance, PipelineInternals, TimerCollector,
    UserStateBackend, UserStateError,
};
use beam::pipeline::{Pipeline, PipelineError, URN_PAR_DO};
use beam::transforms::display_data::{DisplayDataItem, DisplayDataValue};
use beam::transforms::{ByKey, Create, GroupIntoBatches, PTransform};
use beam::values::PCollection;
use beam::windowing::BoundedWindow;
use beam_model::pipeline as proto;

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
fn test_group_into_batches_rejects_a_zero_custom_byte_size() {
    for sharded in [false, true] {
        let p = Pipeline::new();
        let batcher = GroupIntoBatches::new(5).with_byte_size_fn(0, |_: &i64| 4);
        if sharded {
            let _ = keyed_input(&p).apply("Batch", batcher.with_sharded_key());
        } else {
            let _ = keyed_input(&p).apply("Batch", batcher);
        }
        assert!(construction_reason(&p).contains("byte size"));
    }
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

type CellKey = (String, Vec<u8>, Vec<u8>);
type StateResult<T> = Result<T, UserStateError>;

/// In-memory bag and value state backend for unit tests of `GroupIntoBatches`.
#[derive(Default)]
struct BagStateBackend {
    cells: Mutex<HashMap<CellKey, Vec<Vec<u8>>>>,
}

fn unsupported_state<T>() -> StateResult<T> {
    Err(UserStateError::Other(
        "GroupIntoBatches uses only bag and value state".to_string(),
    ))
}

impl UserStateBackend for BagStateBackend {
    fn get_state(&self, state_id: &str, window: &[u8], key: &[u8]) -> StateResult<Vec<Vec<u8>>> {
        let cell = (state_id.to_string(), window.to_vec(), key.to_vec());
        Ok(self
            .cells
            .lock()
            .unwrap()
            .get(&cell)
            .cloned()
            .unwrap_or_default())
    }

    fn append_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        element: Vec<u8>,
    ) -> StateResult<()> {
        let cell = (state_id.to_string(), window.to_vec(), key.to_vec());
        self.cells
            .lock()
            .unwrap()
            .entry(cell)
            .or_default()
            .push(element);
        Ok(())
    }

    fn clear_state(&self, state_id: &str, window: &[u8], key: &[u8]) -> StateResult<()> {
        let cell = (state_id.to_string(), window.to_vec(), key.to_vec());
        self.cells.lock().unwrap().remove(&cell);
        Ok(())
    }

    fn get_map_state(&self, _: &str, _: &[u8], _: &[u8], _: &[u8]) -> StateResult<Vec<Vec<u8>>> {
        unsupported_state()
    }
    fn get_map_keys(&self, _: &str, _: &[u8], _: &[u8]) -> StateResult<Vec<Vec<u8>>> {
        unsupported_state()
    }
    fn put_map_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: Vec<u8>,
        _: Vec<u8>,
    ) -> StateResult<()> {
        unsupported_state()
    }
    fn append_map_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: Vec<u8>,
        _: Vec<u8>,
    ) -> StateResult<()> {
        unsupported_state()
    }
    fn remove_map_key(&self, _: &str, _: &[u8], _: &[u8], _: &[u8]) -> StateResult<()> {
        unsupported_state()
    }
    fn clear_map_state(&self, _: &str, _: &[u8], _: &[u8]) -> StateResult<()> {
        unsupported_state()
    }
    fn get_ordered_list_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: i64,
        _: i64,
    ) -> StateResult<Vec<(i64, Vec<u8>)>> {
        unsupported_state()
    }
    fn append_ordered_list_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: i64,
        _: Vec<u8>,
    ) -> StateResult<()> {
        unsupported_state()
    }
    fn clear_ordered_list_range(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: i64,
        _: i64,
    ) -> StateResult<()> {
        unsupported_state()
    }
    fn clear_ordered_list_state(&self, _: &str, _: &[u8], _: &[u8]) -> StateResult<()> {
        unsupported_state()
    }
}

/// Captures emitted batches alongside their output event timestamps.
#[derive(Default)]
struct WindowedBatchSink(Vec<(Instant, (String, Vec<i64>))>);

impl ElementSink for WindowedBatchSink {
    fn push(&mut self, _: Vec<u8>) -> Result<(), String> {
        Err("expected windowed output".to_string())
    }

    fn push_windowed(&mut self, header: &WindowedHeader, element: Vec<u8>) -> Result<(), String> {
        let batch = <(String, Vec<i64>)>::decode(&element).map_err(|e| e.to_string())?;
        self.0.push((header.timestamp(), batch));
        Ok(())
    }
}

type StepResult = (
    Vec<(Instant, (String, Vec<i64>))>,
    Vec<(String, TimerRecord)>,
);

/// Drives the handler of `GroupIntoBatches` over elements and timers in `[0, 10_000)`.
struct BatchHarness {
    stage: HandlerInstance,
    backend: Arc<dyn UserStateBackend>,
    timers: Arc<TimerCollector>,
    window: IntervalWindow,
}

impl BatchHarness {
    fn new<F>(batcher: GroupIntoBatches<ByKey, F>) -> Self
    where
        GroupIntoBatches<ByKey, F>:
            PTransform<PCollection<(String, i64)>, Output = PCollection<(String, Vec<i64>)>>,
    {
        let p = Pipeline::new();
        let _ = keyed_input(&p).apply("Batch", batcher);
        let components = p.to_proto().components.unwrap();
        let id = &transform(&components, "Batch").subtransforms[0];
        let mut stage = p.transform_handlers().remove(id).unwrap().instantiate();
        stage.setup().unwrap();
        stage.start_bundle().unwrap();
        Self {
            stage,
            backend: Arc::new(BagStateBackend::default()),
            timers: Arc::new(TimerCollector::new()),
            window: IntervalWindow::new(Instant::from_millis(0), Instant::from_millis(10_000)),
        }
    }

    fn drain_timers(&self) -> Vec<(String, TimerRecord)> {
        self.timers
            .drain()
            .into_iter()
            .map(|change| (change.family, change.record))
            .collect()
    }

    fn process(&mut self, key: &str, value: i64, timestamp_millis: i64) -> StepResult {
        let encoded = (key.to_string(), value).encode().unwrap();
        let header = WindowedHeader::global(timestamp_millis, PaneInfo::NO_FIRING)
            .with_windows(&[BoundedWindow::Interval(self.window)]);
        let mut sink = WindowedBatchSink::default();
        {
            let mut ctx = HandlerContext::new(&mut sink)
                .with_header(&header)
                .with_state_backend(Some(&self.backend))
                .with_timer_collector(Some(&self.timers));
            self.stage.process(&encoded, &mut ctx).unwrap();
        }
        (sink.0, self.drain_timers())
    }

    fn fire(&mut self, family: &str, record: &TimerRecord) -> StepResult {
        let mut sink = WindowedBatchSink::default();
        {
            let mut ctx = HandlerContext::new(&mut sink)
                .with_state_backend(Some(&self.backend))
                .with_timer_collector(Some(&self.timers));
            self.stage.on_timer(family, record, &mut ctx).unwrap();
        }
        (sink.0, self.drain_timers())
    }
}

#[test]
fn test_buffering_timer_flushes_partial_batch_and_clears_end_of_window_timer() {
    let duration = Duration::from_secs(5);
    let mut harness =
        BatchHarness::new(GroupIntoBatches::new(10).with_max_buffering_duration(duration));

    let before = Instant::now();
    let (outputs, timers) = harness.process("k", 10, 1_000);
    let after = Instant::now();
    assert!(outputs.is_empty());
    let [
        (eow_family, eow_record),
        (buffering_family, buffering_record),
    ] = timers.as_slice()
    else {
        panic!("expected end_of_window and buffering timers, got {timers:?}");
    };
    assert_eq!(eow_family, "end_of_window");
    assert_eq!(buffering_family, "buffering");
    let TimerAction::Set {
        fire_timestamp: eow_fire,
        hold_timestamp: eow_hold,
        ..
    } = eow_record.action
    else {
        panic!("expected set end_of_window timer, got {eow_record:?}");
    };
    assert_eq!(eow_fire, Instant::from_millis(9_999));
    assert_eq!(eow_hold, Instant::from_millis(1_000));
    let TimerAction::Set {
        fire_timestamp: buf_fire,
        hold_timestamp: buf_hold,
        ..
    } = buffering_record.action
    else {
        panic!("expected set buffering timer, got {buffering_record:?}");
    };
    assert!((before + duration) <= buf_fire && buf_fire <= (after + duration));
    assert_eq!(buf_hold, Instant::from_millis(1_000));

    // A second element in the same open batch does not arm the timers again.
    let (outputs, timers) = harness.process("k", 20, 2_000);
    assert!(outputs.is_empty());
    assert!(timers.is_empty());

    // Firing the buffering timer flushes the partial batch at the held timestamp and clears
    // the end-of-window timer and batch state.
    let (outputs, timers) = harness.fire("buffering", buffering_record);
    let expected = (Instant::from_millis(1_000), ("k".to_string(), vec![10, 20]));
    assert_eq!(outputs, vec![expected]);
    let [(family, cleared)] = timers.as_slice() else {
        panic!("expected cleared end_of_window timer, got {timers:?}");
    };
    assert_eq!(family, "end_of_window");
    assert!(matches!(cleared.action, TimerAction::Clear));

    let (outputs, _) = harness.fire("end_of_window", eow_record);
    assert!(outputs.is_empty());
}

#[test]
fn test_out_of_order_element_rearms_end_of_window_timer_with_earlier_hold() {
    let mut harness = BatchHarness::new(GroupIntoBatches::new(10));

    let (outputs, timers) = harness.process("k", 1, 5_000);
    assert!(outputs.is_empty());
    let [(family, first)] = timers.as_slice() else {
        panic!("expected one end_of_window timer, got {timers:?}");
    };
    assert_eq!(family, "end_of_window");
    assert!(matches!(
        first.action,
        TimerAction::Set {
            fire_timestamp,
            hold_timestamp,
            ..
        } if fire_timestamp == Instant::from_millis(9_999)
            && hold_timestamp == Instant::from_millis(5_000)
    ));

    // An earlier timestamp moves the minimum back and re-arms the end-of-window hold.
    let (outputs, timers) = harness.process("k", 2, 2_000);
    assert!(outputs.is_empty());
    let [(family, earlier)] = timers.as_slice() else {
        panic!("expected re-armed end_of_window timer, got {timers:?}");
    };
    assert_eq!(family, "end_of_window");
    assert!(matches!(
        earlier.action,
        TimerAction::Set {
            fire_timestamp,
            hold_timestamp,
            ..
        } if fire_timestamp == Instant::from_millis(9_999)
            && hold_timestamp == Instant::from_millis(2_000)
    ));

    // A later timestamp keeps the earlier minimum hold without re-arming the timer.
    let (outputs, timers) = harness.process("k", 3, 7_000);
    assert!(outputs.is_empty());
    assert!(timers.is_empty());

    let (outputs, timers) = harness.fire("end_of_window", earlier);
    assert!(timers.is_empty());
    let expected = (
        Instant::from_millis(2_000),
        ("k".to_string(), vec![1, 2, 3]),
    );
    assert_eq!(outputs, vec![expected]);
}

#[test]
fn test_count_flush_clears_end_of_window_and_buffering_timers() {
    let mut harness = BatchHarness::new(
        GroupIntoBatches::new(2).with_max_buffering_duration(Duration::from_secs(5)),
    );

    let (outputs, timers) = harness.process("k", 10, 3_000);
    assert!(outputs.is_empty());
    assert_eq!(timers.len(), 2);

    // Reaching batch_size flushes the batch at the minimum timestamp and clears both timers.
    let (outputs, timers) = harness.process("k", 20, 1_500);
    let expected = (Instant::from_millis(1_500), ("k".to_string(), vec![10, 20]));
    assert_eq!(outputs, vec![expected]);
    let [(eow_family, eow_clear), (buf_family, buf_clear)] = timers.as_slice() else {
        panic!("expected cleared end_of_window and buffering timers, got {timers:?}");
    };
    assert_eq!(eow_family, "end_of_window");
    assert!(matches!(eow_clear.action, TimerAction::Clear));
    assert_eq!(buf_family, "buffering");
    assert!(matches!(buf_clear.action, TimerAction::Clear));
}

#[test]
fn test_with_byte_size_fn_flushes_by_custom_size_and_populates_display_data() {
    let batcher = GroupIntoBatches::new(100)
        .with_byte_size_fn(10, |value: &i64| usize::try_from(*value).unwrap_or(1));
    let p = Pipeline::new();
    let _ = keyed_input(&p).apply("Batch", batcher.clone());
    p.validate().unwrap();
    let components = p.to_proto().components.unwrap();
    let items = display_data(transform(&components, "Batch"));
    assert_eq!(items["byte_size"], DisplayDataValue::Integer(10));
    assert_eq!(items["sharded_key"], DisplayDataValue::Boolean(false));

    let mut harness = BatchHarness::new(batcher);
    // `-1` encodes to 10 varint bytes, yet its custom size is 1, so it stays buffered.
    let (outputs, timers) = harness.process("k", -1, 1_000);
    assert!(outputs.is_empty());
    assert_eq!(timers.len(), 1);

    // `9` encodes to 1 varint byte and brings the custom total to 10, flushing the batch.
    let (outputs, timers) = harness.process("k", 9, 2_000);
    assert_eq!(
        outputs,
        vec![(Instant::from_millis(2_000), ("k".to_string(), vec![-1, 9]))]
    );
    let [(family, cleared)] = timers.as_slice() else {
        panic!("expected cleared end_of_window timer, got {timers:?}");
    };
    assert_eq!(family, "end_of_window");
    assert!(matches!(cleared.action, TimerAction::Clear));
}

#[test]
fn test_with_byte_size_fn_and_sharded_key_batches_by_custom_size() {
    let p = Pipeline::new();
    let input = keyed_input(&p);
    let _: PCollection<(ShardedKey<String>, Vec<i64>)> = input.apply(
        "BatchBefore",
        GroupIntoBatches::new(100)
            .with_byte_size_fn(8, |value: &i64| usize::try_from(*value).unwrap_or(0))
            .with_sharded_key(),
    );
    let _: PCollection<(ShardedKey<String>, Vec<i64>)> = input.apply(
        "Batch",
        GroupIntoBatches::new(100)
            .with_sharded_key()
            .with_byte_size_fn(8, |value| usize::try_from(*value).unwrap_or(0)),
    );
    p.validate().unwrap();

    let components = p.to_proto().components.unwrap();
    for name in ["BatchBefore", "Batch"] {
        let items = display_data(transform(&components, name));
        assert_eq!(items["byte_size"], DisplayDataValue::Integer(8));
        assert_eq!(items["sharded_key"], DisplayDataValue::Boolean(true));
    }

    let id = &transform(&components, "Batch").subtransforms[1];
    let mut stage = p.transform_handlers().remove(id).unwrap().instantiate();
    stage.setup().unwrap();
    stage.start_bundle().unwrap();
    let backend: Arc<dyn UserStateBackend> = Arc::new(BagStateBackend::default());
    let timers = Arc::new(TimerCollector::new());
    let window = IntervalWindow::new(Instant::from_millis(0), Instant::from_millis(10_000));
    let key = ShardedKey::new("k".to_string(), b"s0".to_vec());

    let mut send = |value: i64, millis: i64| -> Vec<(ShardedKey<String>, Vec<i64>)> {
        let encoded = (key.clone(), value).encode().unwrap();
        let header = WindowedHeader::global(millis, PaneInfo::NO_FIRING)
            .with_windows(&[BoundedWindow::Interval(window)]);
        let mut raw: Vec<Vec<u8>> = Vec::new();
        let mut ctx = HandlerContext::new(&mut raw)
            .with_header(&header)
            .with_state_backend(Some(&backend))
            .with_timer_collector(Some(&timers));
        stage.process(&encoded, &mut ctx).unwrap();
        raw.into_iter()
            .map(|bytes| <(ShardedKey<String>, Vec<i64>)>::decode(&bytes).unwrap())
            .collect()
    };

    assert!(send(3, 1_000).is_empty());
    assert_eq!(send(5, 2_000), vec![(key, vec![3, 5])]);
}
