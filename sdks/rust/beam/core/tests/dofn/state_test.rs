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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use beam::internals::{StateCell, UserStateReader};
use beam::pipeline::Pipeline;
use beam::pipeline::constants::{
    URN_REQUIREMENT_STATEFUL, URN_USER_STATE_BAG, URN_USER_STATE_MULTIMAP,
    URN_USER_STATE_ORDERED_LIST,
};
use beam::prelude::*;
use beam::transforms::{BagState, MapState, OrderedListState, SetState, ValueState};
use beam::transforms::{Create, ParDo};

type StateMapKey = (String, Vec<u8>, Vec<u8>);
type StateCells = HashMap<StateMapKey, Vec<Vec<u8>>>;
type MapEntries = HashMap<Vec<u8>, Vec<u8>>;
type OrderedListEntries = std::collections::BTreeMap<i64, Vec<Vec<u8>>>;

#[derive(Default, Debug)]
struct TestStateReader {
    cells: Mutex<StateCells>,
    map_cells: Mutex<HashMap<StateMapKey, MapEntries>>,
    ordered_list_cells: Mutex<HashMap<StateMapKey, OrderedListEntries>>,
}

impl UserStateReader for TestStateReader {
    fn get_state(&self, state_id: &str, window: &[u8], key: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        let map_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let guard = self.cells.lock().map_err(|e| format!("Lock error: {e}"))?;
        Ok(guard.get(&map_key).cloned().unwrap_or_default())
    }

    fn append_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        element: Vec<u8>,
    ) -> Result<(), String> {
        let map_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self.cells.lock().map_err(|e| format!("Lock error: {e}"))?;
        guard.entry(map_key).or_default().push(element);
        Ok(())
    }

    fn clear_state(&self, state_id: &str, window: &[u8], key: &[u8]) -> Result<(), String> {
        let map_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self.cells.lock().map_err(|e| format!("Lock error: {e}"))?;
        guard.remove(&map_key);
        Ok(())
    }

    fn get_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<Vec<Vec<u8>>, String> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let guard = self
            .map_cells
            .lock()
            .map_err(|e| format!("Lock error: {e}"))?;
        if let Some(entries) = guard.get(&cell_key)
            && let Some(val) = entries.get(map_key)
        {
            return Ok(vec![val.clone()]);
        }
        Ok(Vec::new())
    }

    fn get_map_keys(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, String> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let guard = self
            .map_cells
            .lock()
            .map_err(|e| format!("Lock error: {e}"))?;
        Ok(guard
            .get(&cell_key)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default())
    }

    fn put_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: Vec<u8>,
        element: Vec<u8>,
    ) -> Result<(), String> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self
            .map_cells
            .lock()
            .map_err(|e| format!("Lock error: {e}"))?;
        guard.entry(cell_key).or_default().insert(map_key, element);
        Ok(())
    }

    fn remove_map_key(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<(), String> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self
            .map_cells
            .lock()
            .map_err(|e| format!("Lock error: {e}"))?;
        if let Some(entries) = guard.get_mut(&cell_key) {
            entries.remove(map_key);
        }
        Ok(())
    }

    fn clear_map_state(&self, state_id: &str, window: &[u8], key: &[u8]) -> Result<(), String> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self
            .map_cells
            .lock()
            .map_err(|e| format!("Lock error: {e}"))?;
        guard.remove(&cell_key);
        Ok(())
    }

    fn get_ordered_list_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<Vec<(i64, Vec<u8>)>, String> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let guard = self
            .ordered_list_cells
            .lock()
            .map_err(|e| format!("Lock error: {e}"))?;
        let Some(entries) = guard.get(&cell_key) else {
            return Ok(Vec::new());
        };
        let mut result = Vec::new();
        for (&ts, values) in entries.range(from..to) {
            for val in values {
                result.push((ts, val.clone()));
            }
        }
        Ok(result)
    }

    fn append_ordered_list_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        timestamp: i64,
        element: Vec<u8>,
    ) -> Result<(), String> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self
            .ordered_list_cells
            .lock()
            .map_err(|e| format!("Lock error: {e}"))?;
        guard
            .entry(cell_key)
            .or_default()
            .entry(timestamp)
            .or_default()
            .push(element);
        Ok(())
    }

    fn clear_ordered_list_range(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<(), String> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self
            .ordered_list_cells
            .lock()
            .map_err(|e| format!("Lock error: {e}"))?;
        if let Some(entries) = guard.get_mut(&cell_key) {
            entries.retain(|&ts, _| ts < from || ts >= to);
        }
        Ok(())
    }

    fn clear_ordered_list_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<(), String> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self
            .ordered_list_cells
            .lock()
            .map_err(|e| format!("Lock error: {e}"))?;
        guard.remove(&cell_key);
        Ok(())
    }
}

#[test]
fn test_state_spec_proto_generation() {
    let p = Pipeline::new();

    let bag_spec = <BagState<i64> as StateCell>::spec("bag_state");
    assert_eq!(bag_spec.name(), "bag_state");
    let proto_bag = bag_spec.register_and_encode(&p);
    assert_eq!(
        proto_bag.protocol.as_ref().map(|f| f.urn.as_str()),
        Some(URN_USER_STATE_BAG)
    );
    assert!(proto_bag.spec.is_some());

    let val_spec = <ValueState<String> as StateCell>::spec("val_state");
    assert_eq!(val_spec.name(), "val_state");
    let proto_val = val_spec.register_and_encode(&p);
    assert_eq!(
        proto_val.protocol.as_ref().map(|f| f.urn.as_str()),
        Some(URN_USER_STATE_BAG)
    );
    // A value cell declares that it holds one value, with the bag protocol on the wire.
    assert!(matches!(
        proto_val.spec,
        Some(model::pipeline::state_spec::Spec::ReadModifyWriteSpec(_))
    ));
}

#[derive(State)]
#[expect(dead_code, reason = "the test reads the declared specs, not the cells")]
struct DummyBagState {
    bag_accum: BagState<i64>,
}

#[derive(Clone)]
struct DummyStatefulFn;

impl StatefulDoFn for DummyStatefulFn {
    type Key = String;
    type Value = i64;
    type Out = (String, i64);
    type State = DummyBagState;
    type Timer = NoTimers;

    fn process_element(
        &mut self,
        key: &Self::Key,
        val: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.emit((key.clone(), val))
    }
}

#[test]
fn test_stateful_pardo_populates_pipeline_requirements() {
    let p = Pipeline::new();

    let input = p.apply("Create", Create::new(vec![("k1".to_string(), 10_i64)]));
    let _output = input.apply("StatefulDoFn", ParDo::new(DummyStatefulFn));

    let proto_pipeline = p.to_proto();
    assert!(
        proto_pipeline
            .requirements
            .contains(&URN_REQUIREMENT_STATEFUL.to_string()),
        "Pipeline requirements must contain stateful requirement URN, got: {:?}",
        proto_pipeline.requirements
    );
}

#[test]
fn test_bag_state_and_value_state_in_memory_lifecycle() {
    let reader = Arc::new(TestStateReader::default());

    let mut bag = BagState::<i64>::new(
        reader.clone(),
        "my_bag".to_string(),
        vec![],
        b"key1".to_vec(),
    );

    assert_eq!(bag.read().unwrap(), Vec::<i64>::new());

    bag.append(10).unwrap();
    bag.append(20).unwrap();
    bag.append(30).unwrap();
    assert_eq!(bag.read().unwrap(), vec![10, 20, 30]);

    // Verify key isolation.
    let mut other_bag = BagState::<i64>::new(
        reader.clone(),
        "my_bag".to_string(),
        vec![],
        b"key2".to_vec(),
    );
    other_bag.append(999).unwrap();
    assert_eq!(other_bag.read().unwrap(), vec![999]);
    assert_eq!(bag.read().unwrap(), vec![10, 20, 30]);

    bag.clear().unwrap();
    assert_eq!(bag.read().unwrap(), Vec::<i64>::new());
    assert_eq!(other_bag.read().unwrap(), vec![999]);

    // ValueState wrapping BagState.
    let val_bag = BagState::<String>::new(reader, "my_val".to_string(), vec![], b"key1".to_vec());
    let mut val_state = ValueState::new(val_bag);

    assert_eq!(val_state.read().unwrap(), None);

    val_state.write("hello".to_string()).unwrap();
    assert_eq!(val_state.read().unwrap(), Some("hello".to_string()));

    val_state.write("world".to_string()).unwrap();
    assert_eq!(val_state.read().unwrap(), Some("world".to_string()));

    val_state.clear().unwrap();
    assert_eq!(val_state.read().unwrap(), None);
}

#[test]
fn test_state_cell_binding_lifecycle() {
    let reader: Arc<dyn UserStateReader> = Arc::new(TestStateReader::default());
    let binding = beam::internals::StateBinding::new(Some(reader), b"win", b"key1");

    let mut bag = <BagState<i64> as StateCell>::bind("bag", &binding).expect("bind");
    bag.append(42).unwrap();
    assert_eq!(bag.read().unwrap(), vec![42]);

    let mut val = <ValueState<String> as StateCell>::bind("val", &binding).expect("bind");
    val.write("beam".to_string()).unwrap();
    assert_eq!(val.read().unwrap(), Some("beam".to_string()));

    let mut map = <MapState<String, i64> as StateCell>::bind("map", &binding).expect("bind");
    map.put("alpha".to_string(), 100).unwrap();
    assert_eq!(map.get(&"alpha".to_string()).unwrap(), Some(100));

    let mut set = <SetState<String> as StateCell>::bind("set", &binding).expect("bind");
    set.insert("item1".to_string()).unwrap();
    assert!(set.contains(&"item1".to_string()).unwrap());
    assert!(!set.contains(&"item2".to_string()).unwrap());
}

#[test]
fn without_a_state_reader_cells_fail_to_bind_and_no_state_binds() {
    use beam::internals::StateCells;

    let binding = beam::internals::StateBinding::new(None, b"win", b"key1");
    let error = <BagState<i64> as StateCell>::bind("bag", &binding)
        .err()
        .expect("a cell needs a state reader");
    assert!(error.to_string().contains("state 'bag'"), "{error}");
    assert!(<() as StateCells>::bind(&binding).is_ok());
}

#[test]
fn test_map_and_set_state_spec_proto_generation() {
    let p = Pipeline::new();

    let map_spec = <MapState<String, i64> as StateCell>::spec("my_map");
    assert_eq!(map_spec.name(), "my_map");
    let proto_map = map_spec.register_and_encode(&p);
    assert_eq!(
        proto_map.protocol.as_ref().map(|f| f.urn.as_str()),
        Some(URN_USER_STATE_MULTIMAP)
    );
    assert!(proto_map.spec.is_some());

    let set_spec = <SetState<String> as StateCell>::spec("my_set");
    assert_eq!(set_spec.name(), "my_set");
    let proto_set = set_spec.register_and_encode(&p);
    assert_eq!(
        proto_set.protocol.as_ref().map(|f| f.urn.as_str()),
        Some(URN_USER_STATE_MULTIMAP)
    );
    assert!(proto_set.spec.is_some());
}

#[test]
fn test_map_state_and_set_state_in_memory_lifecycle() {
    let reader = Arc::new(TestStateReader::default());

    let mut map = MapState::<String, i64>::new(
        reader.clone(),
        "scores".to_string(),
        vec![],
        b"team_red".to_vec(),
    );

    // Initial state is empty
    assert_eq!(map.get(&"alice".to_string()).unwrap(), None);
    assert_eq!(map.keys().unwrap(), Vec::<String>::new());

    // Put entries
    map.put("alice".to_string(), 50).unwrap();
    map.put("bob".to_string(), 75).unwrap();

    assert_eq!(map.get(&"alice".to_string()).unwrap(), Some(50));
    assert_eq!(map.get(&"bob".to_string()).unwrap(), Some(75));
    assert_eq!(map.get(&"charlie".to_string()).unwrap(), None);

    let mut keys = map.keys().unwrap();
    keys.sort();
    assert_eq!(keys, vec!["alice".to_string(), "bob".to_string()]);

    // Overwrite entry
    map.put("alice".to_string(), 100).unwrap();
    assert_eq!(map.get(&"alice".to_string()).unwrap(), Some(100));

    // Remove entry
    map.remove(&"bob".to_string()).unwrap();
    assert_eq!(map.get(&"bob".to_string()).unwrap(), None);
    assert_eq!(map.keys().unwrap(), vec!["alice".to_string()]);

    // Clear map
    map.clear().unwrap();
    assert_eq!(map.get(&"alice".to_string()).unwrap(), None);
    assert_eq!(map.keys().unwrap(), Vec::<String>::new());

    // SetState testing
    let map_for_set =
        MapState::<String, ()>::new(reader, "visited".to_string(), vec![], b"user_123".to_vec());
    let mut set = SetState::new(map_for_set);

    assert!(!set.contains(&"page_a".to_string()).unwrap());
    assert_eq!(set.read().unwrap(), Vec::<String>::new());

    set.insert("page_a".to_string()).unwrap();
    set.insert("page_b".to_string()).unwrap();
    assert!(set.contains(&"page_a".to_string()).unwrap());
    assert!(set.contains(&"page_b".to_string()).unwrap());
    assert!(!set.contains(&"page_c".to_string()).unwrap());

    let mut visited = set.read().unwrap();
    visited.sort();
    assert_eq!(visited, vec!["page_a".to_string(), "page_b".to_string()]);

    set.remove(&"page_a".to_string()).unwrap();
    assert!(!set.contains(&"page_a".to_string()).unwrap());
    assert!(set.contains(&"page_b".to_string()).unwrap());

    set.clear().unwrap();
    assert!(!set.contains(&"page_b".to_string()).unwrap());
    assert_eq!(set.read().unwrap(), Vec::<String>::new());
}

#[test]
fn test_ordered_list_state_spec_proto_generation() {
    let p = Pipeline::new();

    let ordered_spec = <OrderedListState<String> as StateCell>::spec("my_events");
    assert_eq!(ordered_spec.name(), "my_events");
    let proto_ordered = ordered_spec.register_and_encode(&p);
    assert_eq!(
        proto_ordered.protocol.as_ref().map(|f| f.urn.as_str()),
        Some(URN_USER_STATE_ORDERED_LIST)
    );
    assert!(proto_ordered.spec.is_some());
}

#[test]
fn test_ordered_list_state_in_memory_lifecycle() {
    let reader = Arc::new(TestStateReader::default());

    let mut list = OrderedListState::<String>::new(
        reader,
        "events".to_string(),
        vec![],
        b"device_42".to_vec(),
    );

    assert_eq!(list.read().unwrap(), Vec::<(Instant, String)>::new());
    assert_eq!(
        list.read_range(ms(10)..ms(50)).unwrap(),
        Vec::<(Instant, String)>::new()
    );

    list.add(ms(10), "start".to_string()).unwrap();
    list.add(ms(50), "end".to_string()).unwrap();
    list.add(ms(20), "ping".to_string()).unwrap();
    list.add(ms(20), "pong".to_string()).unwrap();

    let all = list.read().unwrap();
    assert_eq!(
        all,
        vec![
            (ms(10), "start".to_string()),
            (ms(20), "ping".to_string()),
            (ms(20), "pong".to_string()),
            (ms(50), "end".to_string()),
        ]
    );

    let middle = list.read_range(ms(15)..ms(30)).unwrap();
    assert_eq!(
        middle,
        vec![(ms(20), "ping".to_string()), (ms(20), "pong".to_string()),]
    );

    list.clear_range(ms(20)..ms(21)).unwrap();
    assert_eq!(
        list.read().unwrap(),
        vec![(ms(10), "start".to_string()), (ms(50), "end".to_string()),]
    );

    list.clear().unwrap();
    assert_eq!(list.read().unwrap(), Vec::<(Instant, String)>::new());
}

fn ms(millis: i64) -> Instant {
    Instant::from_millis(millis)
}

#[derive(State)]
#[expect(dead_code, reason = "the test reads the declared specs, not the cells")]
struct TestDeclaredState {
    count: ValueState<i64>,
    #[state(id = "custom_accum")]
    accum: BagState<String>,
}

#[derive(Timers)]
enum TestDeclaredTimer {
    #[timer(event_time)]
    Flush,
    #[timer(processing_time, id = "custom_timer")]
    Alarm,
}

#[derive(Clone)]
struct TestDeclaredFn;

impl StatefulDoFn for TestDeclaredFn {
    type Key = String;
    type Value = i64;
    type Out = (String, i64);
    type State = TestDeclaredState;
    type Timer = TestDeclaredTimer;

    fn process_element(
        &mut self,
        key: &Self::Key,
        value: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.emit((key.clone(), value))
    }
}

fn pardo_payload(p: &Pipeline, name: &str) -> model::pipeline::ParDoPayload {
    use prost::Message;
    let proto = p.to_proto();
    let transform = proto
        .components
        .as_ref()
        .expect("components")
        .transforms
        .values()
        .find(|t| t.unique_name == name)
        .unwrap_or_else(|| panic!("no transform named {name}"));
    let payload = &transform.spec.as_ref().expect("spec").payload;
    model::pipeline::ParDoPayload::decode(payload.as_slice()).expect("ParDoPayload decodes")
}

#[test]
fn stateful_dofn_populates_state_specs_and_timer_families() {
    let p = Pipeline::new();
    let input = p.apply("Create", Create::new(vec![("k".to_string(), 1i64)]));
    let _ = input.apply("Declared", ParDo::new(TestDeclaredFn));

    let payload = pardo_payload(&p, "Declared");
    assert!(payload.state_specs.contains_key("count"));
    assert!(payload.state_specs.contains_key("custom_accum"));
    assert_eq!(payload.state_specs.len(), 2);

    assert!(payload.timer_family_specs.contains_key("Flush"));
    assert!(payload.timer_family_specs.contains_key("custom_timer"));
    assert_eq!(payload.timer_family_specs.len(), 2);
}

/// A stateful function that asks for stable and time-sorted input.
#[derive(Clone)]
struct SortedCommitFn;

impl StatefulDoFn for SortedCommitFn {
    type Key = String;
    type Value = i64;
    type Out = (String, i64);
    type State = DummyBagState;
    type Timer = NoTimers;
    const REQUIRES_STABLE_INPUT: bool = true;
    const REQUIRES_TIME_SORTED_INPUT: bool = true;

    fn process_element(
        &mut self,
        key: &Self::Key,
        val: Self::Value,
        _state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.emit((key.clone(), val))
    }
}

/// A stateless function that asks for stable input.
#[derive(Clone)]
struct StableSinkFn;

impl DoFn for StableSinkFn {
    type In = i64;
    type Out = i64;
    const REQUIRES_STABLE_INPUT: bool = true;

    fn process_element(&mut self, element: i64, ctx: &mut ProcessContext<'_, i64>) -> Result {
        ctx.emit(element)
    }
}

#[test]
fn test_input_requirements_reach_the_payload_and_the_pipeline() {
    use beam::pipeline::{URN_REQUIREMENT_STABLE_INPUT, URN_REQUIREMENT_TIME_SORTED_INPUT};
    use model::pipeline::ParDoPayload;
    use prost::Message;

    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(vec![("k".to_string(), 1_i64)]))
        .apply("SortedCommit", ParDo::new(SortedCommitFn));
    let _ = p
        .apply("Numbers", Create::new(vec![1_i64]))
        .apply("StableSink", ParDo::new(StableSinkFn));

    let proto = p.to_proto();
    for requirement in [
        URN_REQUIREMENT_STABLE_INPUT,
        URN_REQUIREMENT_TIME_SORTED_INPUT,
    ] {
        assert!(
            proto.requirements.iter().any(|r| r == requirement),
            "{requirement} in {:?}",
            proto.requirements
        );
    }
    let payload = |name: &str| {
        let transforms = &proto.components.as_ref().expect("components").transforms;
        let spec = transforms
            .values()
            .find(|t| t.unique_name == name)
            .and_then(|t| t.spec.as_ref())
            .expect("ParDo spec");
        ParDoPayload::decode(spec.payload.as_slice()).expect("ParDoPayload")
    };
    let sorted = payload("SortedCommit");
    assert!(sorted.requires_stable_input && sorted.requires_time_sorted_input);
    let sink = payload("StableSink");
    assert!(sink.requires_stable_input && !sink.requires_time_sorted_input);
}
