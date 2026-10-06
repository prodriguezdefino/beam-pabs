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

//! Tests for `CombiningState`: its spec, its operations and the accumulators it stores.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use beam::coders::DefaultCoder;
use beam::internals::{StateBinding, StateCell, StateCells, UserStateBackend, UserStateError};
use beam::pipeline::{Pipeline, URN_USER_STATE_BAG};
use beam::prelude::*;
use model::pipeline as proto;
use rust_decimal::Decimal;

type CellKey = (String, Vec<u8>, Vec<u8>);

/// A backend with bag cells only, which is what a combining cell uses.
#[derive(Default)]
struct BagBackend {
    cells: Mutex<HashMap<CellKey, Vec<Vec<u8>>>>,
}

impl BagBackend {
    fn elements(&self, state_id: &str) -> Vec<Vec<u8>> {
        self.cells
            .lock()
            .expect("the lock is not poisoned")
            .iter()
            .filter(|((id, _, _), _)| id == state_id)
            .flat_map(|(_, elements)| elements.clone())
            .collect()
    }
}

fn not_a_bag() -> UserStateError {
    UserStateError::Other("a combining cell uses only bag state".to_string())
}

fn lock_error(e: impl std::fmt::Display) -> UserStateError {
    UserStateError::Other(format!("lock error: {e}"))
}

impl UserStateBackend for BagBackend {
    fn get_state(
        &self,
        id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        let cells = self.cells.lock().map_err(lock_error)?;
        Ok(cells
            .get(&(id.to_string(), window.to_vec(), key.to_vec()))
            .cloned()
            .unwrap_or_default())
    }

    fn append_state(
        &self,
        id: &str,
        window: &[u8],
        key: &[u8],
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        let mut cells = self.cells.lock().map_err(lock_error)?;
        cells
            .entry((id.to_string(), window.to_vec(), key.to_vec()))
            .or_default()
            .push(element);
        Ok(())
    }

    fn clear_state(&self, id: &str, window: &[u8], key: &[u8]) -> Result<(), UserStateError> {
        let mut cells = self.cells.lock().map_err(lock_error)?;
        cells.remove(&(id.to_string(), window.to_vec(), key.to_vec()));
        Ok(())
    }

    fn get_map_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        Err(not_a_bag())
    }

    fn get_map_keys(&self, _: &str, _: &[u8], _: &[u8]) -> Result<Vec<Vec<u8>>, UserStateError> {
        Err(not_a_bag())
    }

    fn put_map_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: Vec<u8>,
        _: Vec<u8>,
    ) -> Result<(), UserStateError> {
        Err(not_a_bag())
    }

    fn remove_map_key(&self, _: &str, _: &[u8], _: &[u8], _: &[u8]) -> Result<(), UserStateError> {
        Err(not_a_bag())
    }

    fn clear_map_state(&self, _: &str, _: &[u8], _: &[u8]) -> Result<(), UserStateError> {
        Err(not_a_bag())
    }

    fn get_ordered_list_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: i64,
        _: i64,
    ) -> Result<Vec<(i64, Vec<u8>)>, UserStateError> {
        Err(not_a_bag())
    }

    fn append_ordered_list_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: i64,
        _: Vec<u8>,
    ) -> Result<(), UserStateError> {
        Err(not_a_bag())
    }

    fn clear_ordered_list_range(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: i64,
        _: i64,
    ) -> Result<(), UserStateError> {
        Err(not_a_bag())
    }

    fn clear_ordered_list_state(&self, _: &str, _: &[u8], _: &[u8]) -> Result<(), UserStateError> {
        Err(not_a_bag())
    }
}

/// Binds the cell `id` of type `C` to key `key` of a shared backend.
fn bind<C: StateCell>(backend: &Arc<BagBackend>, id: &str, key: &[u8]) -> C {
    let backend: Arc<dyn UserStateBackend> = backend.clone();
    let binding = StateBinding::new(Some(backend), b"window", key);
    C::bind(id, &binding).expect("the cell binds")
}

#[test]
fn the_spec_is_a_combining_spec_with_the_bag_protocol() {
    let p = Pipeline::new();
    let spec = <CombiningState<Mean<i64>> as StateCell>::spec("average");
    assert_eq!(spec.name(), "average");
    let proto = spec.register_and_encode(&p);
    assert_eq!(
        proto.protocol.as_ref().map(|f| f.urn.as_str()),
        Some(URN_USER_STATE_BAG)
    );
    let Some(proto::state_spec::Spec::CombiningSpec(combining)) = proto.spec else {
        panic!("expected a combining spec, got {:?}", proto.spec);
    };
    // The accumulator of `Mean<i64>` is `(f64, i64)`, registered as a KV coder.
    assert_eq!(
        combining.accumulator_coder_id,
        <(f64, i64)>::register_coder(&p)
    );
    let combine_fn = combining
        .combine_fn
        .expect("the spec names the combine function");
    assert_eq!(combine_fn.urn, "beam:combine_fn:rust:state:v1");
    assert!(String::from_utf8_lossy(&combine_fn.payload).contains("Mean"));
}

#[test]
fn add_read_and_clear() -> Result {
    let backend = Arc::new(BagBackend::default());
    let mut total: CombiningState<Sum<i64>> = bind(&backend, "total", b"k");

    assert!(total.is_empty()?);
    assert_eq!(total.read()?, 0);
    [5, -2, 10].into_iter().try_for_each(|v| total.add(v))?;
    assert!(!total.is_empty()?);
    assert_eq!(total.read()?, 13);

    total.clear()?;
    assert!(total.is_empty()?);
    assert_eq!(total.read()?, 0);
    Ok(())
}

/// Each add replaces the stored accumulator, so the cell holds one accumulator, not one
/// element per input.
#[test]
fn the_cell_stores_one_accumulator() -> Result {
    let backend = Arc::new(BagBackend::default());
    let mut best: CombiningState<Max<i64>> = bind(&backend, "best", b"k");
    (1..=100).try_for_each(|v| best.add(v))?;
    assert_eq!(best.read()?, 100);
    assert_eq!(backend.elements("best"), vec![100i64.encode()?]);
    Ok(())
}

#[test]
fn cells_of_other_keys_and_ids_are_separate() -> Result {
    let backend = Arc::new(BagBackend::default());
    let mut a: CombiningState<Sum<i64>> = bind(&backend, "total", b"a");
    let mut b: CombiningState<Sum<i64>> = bind(&backend, "total", b"b");
    let mut other: CombiningState<Sum<i64>> = bind(&backend, "other", b"a");
    a.add(1)?;
    b.add(10)?;
    other.add(100)?;
    assert_eq!((a.read()?, b.read()?, other.read()?), (1, 10, 100));

    // A new handle on the same cell reads what the first one wrote.
    let again: CombiningState<Sum<i64>> = bind(&backend, "total", b"a");
    assert_eq!(again.read()?, 1);
    Ok(())
}

/// The accumulator of `Mean` is a tuple, and the cell encodes it with the tuple coder.
#[test]
fn a_tuple_accumulator_round_trips() -> Result {
    let backend = Arc::new(BagBackend::default());
    let mut average: CombiningState<Mean<i64>> = bind(&backend, "average", b"k");
    assert!(average.read()?.is_nan());
    [1, 2, 3, 6].into_iter().try_for_each(|v| average.add(v))?;
    assert_eq!(average.read_accumulator()?, (12.0, 4));
    assert_eq!(average.read()?, 3.0);
    Ok(())
}

#[test]
fn a_decimal_accumulator_round_trips() -> Result {
    let backend = Arc::new(BagBackend::default());
    let mut balance: CombiningState<Sum<Decimal>> = bind(&backend, "balance", b"k");
    [Decimal::new(1050, 2), Decimal::new(-325, 2)]
        .into_iter()
        .try_for_each(|v| balance.add(v))?;
    assert_eq!(balance.read()?, Decimal::new(725, 2));
    Ok(())
}

/// `add_accumulator` merges a partial result, for example from another cell.
#[test]
fn add_accumulator_merges_a_partial_result() -> Result {
    let backend = Arc::new(BagBackend::default());
    let mut average: CombiningState<Mean<i64>> = bind(&backend, "average", b"k");
    average.add_accumulator((10.0, 2))?;
    assert_eq!(average.read()?, 5.0);
    average.add(2)?;
    average.add_accumulator((8.0, 1))?;
    assert_eq!(average.read_accumulator()?, (20.0, 4));
    assert_eq!(backend.elements("average").len(), 1);
    Ok(())
}

/// A bag can hold more than one accumulator, for example after a runner merged the
/// state of merging windows. Reads merge them.
#[test]
fn a_read_merges_several_stored_accumulators() -> Result {
    let backend = Arc::new(BagBackend::default());
    let mut bag: BagState<i64> = bind(&backend, "total", b"k");
    bag.append(3)?;
    bag.append(4)?;
    let mut total: CombiningState<Sum<i64>> = bind(&backend, "total", b"k");
    assert_eq!(total.read()?, 7);
    total.add(1)?;
    assert_eq!(total.read()?, 8);
    assert_eq!(backend.elements("total").len(), 1);
    Ok(())
}

#[test]
fn an_add_error_leaves_the_cell_unchanged() -> Result {
    let backend = Arc::new(BagBackend::default());
    let mut total: CombiningState<Sum<i64>> = bind(&backend, "total", b"k");
    total.add(i64::MAX)?;
    let err = total.add(1).expect_err("the sum overflows");
    assert!(err.to_string().contains("overflowed i64"), "{err}");
    assert_eq!(total.read()?, i64::MAX);
    Ok(())
}

#[derive(State)]
#[expect(dead_code, reason = "the test reads the declared specs, not the cells")]
struct ScoreState {
    total: CombiningState<Sum<i64>>,
    best: CombiningState<Max<i64>>,
    average: CombiningState<Mean<i64>>,
}

#[test]
fn derive_state_declares_combining_cells() {
    let p = Pipeline::new();
    let specs = ScoreState::specs();
    let names: Vec<&str> = specs.iter().map(|spec| spec.name()).collect();
    assert_eq!(names, ["total", "best", "average"]);
    assert!(specs.iter().all(|spec| matches!(
        spec.register_and_encode(&p).spec,
        Some(proto::state_spec::Spec::CombiningSpec(_))
    )));
}
