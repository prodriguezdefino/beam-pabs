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

use beam::internals::{AnyStateSpec, UserStateReader};
use beam::pipeline::Pipeline;
use beam::pipeline::constants::{
    URN_REQUIREMENT_STATEFUL, URN_USER_STATE_BAG, URN_USER_STATE_MULTIMAP,
};
use beam::transforms::{
    BagState, BagStateSpec, MapState, MapStateSpec, SetState, SetStateSpec, ValueState,
    ValueStateSpec,
};
use beam::transforms::{Create, DoFn, ParDo, ProcessContext};

type StateMapKey = (String, Vec<u8>, Vec<u8>);
type StateCells = HashMap<StateMapKey, Vec<Vec<u8>>>;
type MapEntries = HashMap<Vec<u8>, Vec<u8>>;

#[derive(Default, Debug)]
struct TestStateReader {
    cells: Mutex<StateCells>,
    map_cells: Mutex<HashMap<StateMapKey, MapEntries>>,
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
}

#[test]
fn test_state_spec_proto_generation() {
    let p = Pipeline::new();

    let bag_spec = BagStateSpec::<i64>::new("bag_state");
    assert_eq!(bag_spec.name(), "bag_state");
    let proto_bag = bag_spec.register_and_encode(&p);
    assert_eq!(
        proto_bag.protocol.as_ref().map(|f| f.urn.as_str()),
        Some(URN_USER_STATE_BAG)
    );
    assert!(proto_bag.spec.is_some());

    let val_spec = ValueStateSpec::<String>::new("val_state");
    assert_eq!(val_spec.name(), "val_state");
    let proto_val = val_spec.register_and_encode(&p);
    assert_eq!(
        proto_val.protocol.as_ref().map(|f| f.urn.as_str()),
        Some(URN_USER_STATE_BAG)
    );
    assert!(proto_val.spec.is_some());
}

#[test]
fn test_stateful_pardo_populates_pipeline_requirements() {
    let p = Pipeline::new();

    let bag_spec = BagStateSpec::<i64>::new("bag_accum");
    let input = p.apply(Create::new("Create", vec![("k1".to_string(), 10_i64)]));

    #[derive(Clone)]
    struct DummyFn;
    impl DoFn for DummyFn {
        type In = (String, i64);
        type Out = (String, i64);

        fn process_element(
            &mut self,
            element: Self::In,
            ctx: &mut ProcessContext<'_, Self::Out>,
        ) -> beam::Result {
            ctx.emit(element)
        }
    }

    let _output = input.apply(ParDo::new("StatefulDoFn", DummyFn).with_state_spec(&bag_spec));

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
fn test_process_context_binds_bag_and_value_state() {
    let reader = Arc::new(TestStateReader::default());

    let mut captured = Vec::new();
    struct MockSink<'a>(&'a mut Vec<Vec<u8>>);
    impl beam::internals::ElementSink for MockSink<'_> {
        fn push(&mut self, elem: Vec<u8>) -> Result<(), String> {
            self.0.push(elem);
            Ok(())
        }
        fn push_tagged(&mut self, _tag: &str, elem: Vec<u8>) -> Result<(), String> {
            self.0.push(elem);
            Ok(())
        }
    }

    let mut sink = MockSink(&mut captured);
    let reader: Arc<dyn UserStateReader> = reader;
    let ctx = ProcessContext::<()>::new(&mut sink).with_state_reader(&reader);

    let bag_spec = BagStateSpec::<i64>::new("bag");
    let val_spec = ValueStateSpec::<String>::new("val");

    let mut bag = ctx.bag_state(&bag_spec, &"my_key".to_string()).unwrap();
    bag.append(42).unwrap();
    assert_eq!(bag.read().unwrap(), vec![42]);

    let mut val = ctx.value_state(&val_spec, &"my_key".to_string()).unwrap();
    val.write("beam".to_string()).unwrap();
    assert_eq!(val.read().unwrap(), Some("beam".to_string()));

    let map_spec = MapStateSpec::<String, i64>::new("map");
    let mut map = ctx.map_state(&map_spec, &"my_key".to_string()).unwrap();
    map.put("alpha".to_string(), 100).unwrap();
    assert_eq!(map.get(&"alpha".to_string()).unwrap(), Some(100));

    let set_spec = SetStateSpec::<String>::new("set");
    let mut set = ctx.set_state(&set_spec, &"my_key".to_string()).unwrap();
    set.insert("item1".to_string()).unwrap();
    assert!(set.contains(&"item1".to_string()).unwrap());
    assert!(!set.contains(&"item2".to_string()).unwrap());
}

#[test]
fn test_map_and_set_state_spec_proto_generation() {
    let p = Pipeline::new();

    let map_spec = MapStateSpec::<String, i64>::new("my_map");
    assert_eq!(map_spec.name(), "my_map");
    let proto_map = map_spec.register_and_encode(&p);
    assert_eq!(
        proto_map.protocol.as_ref().map(|f| f.urn.as_str()),
        Some(URN_USER_STATE_MULTIMAP)
    );
    assert!(proto_map.spec.is_some());

    let set_spec = SetStateSpec::<String>::new("my_set");
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
