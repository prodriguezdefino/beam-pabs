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

//! Tests for the row schemas of dynamic `Row` state cells: the schema that the expansion
//! gives to each cell, the construction errors, and the check of each state write.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use beam::Instant;
use beam::coders::{DefaultCoder, URN_ROW};
use beam::internals::{StateCells, UserStateBackend};
use beam::pipeline::{Pipeline, PipelineError};
use beam::schema::{FieldType, Row, Schema};
use beam::transforms::{
    BagState, Create, MapState, MultimapState, NoTimers, OrderedListState, ParDo, SetState, State,
    StateRowSchemas, StatefulContext, StatefulDoFn, ValueState,
};
use beam_model::pipeline::state_spec::Spec;
use beam_model::pipeline::{Coder, ParDoPayload};
use prost::Message;

use super::common::TestStateBackend;

/// A function with the state cells `S` and the input values `V`. It gives `schemas`.
struct RowStateFn<S, V> {
    schemas: StateRowSchemas,
    _types: PhantomData<fn() -> (S, V)>,
}

impl<S, V> RowStateFn<S, V> {
    fn new(schemas: StateRowSchemas) -> Self {
        Self {
            schemas,
            _types: PhantomData,
        }
    }
}

impl<S, V> Clone for RowStateFn<S, V> {
    fn clone(&self) -> Self {
        Self::new(self.schemas.clone())
    }
}

impl<S: StateCells + 'static, V: DefaultCoder> StatefulDoFn for RowStateFn<S, V> {
    type Key = String;
    type Value = V;
    type Out = i64;
    type State = S;
    type Timer = NoTimers;

    fn process_element(
        &mut self,
        _key: &String,
        _value: V,
        _state: &mut S,
        _ctx: &mut StatefulContext<'_, Self>,
    ) -> beam::Result {
        Ok(())
    }

    fn state_row_schemas(&self) -> StateRowSchemas {
        self.schemas.clone()
    }
}

#[derive(State)]
#[beam(crate = "::beam")]
#[expect(dead_code, reason = "the expansion reads only the cell types")]
struct DynamicRowCells {
    last: ValueState<Row>,
    seen: BagState<Row>,
}

#[derive(State)]
#[beam(crate = "::beam")]
#[expect(dead_code, reason = "the expansion reads only the cell types")]
struct OneBag {
    seen: BagState<Row>,
}

#[derive(State)]
#[beam(crate = "::beam")]
#[expect(dead_code, reason = "the expansion reads only the cell types")]
struct RowKeyedCells {
    map: MapState<Row, i64>,
    set: SetState<Row>,
    multi: MultimapState<Row, Row>,
}

#[derive(State)]
#[beam(crate = "::beam")]
#[expect(dead_code, reason = "the expansion reads only the cell types")]
struct OneMap {
    map: MapState<Row, i64>,
}

#[derive(State)]
#[beam(crate = "::beam")]
#[expect(dead_code, reason = "the expansion reads only the cell types")]
struct PairBag {
    pair: BagState<(Row, Row)>,
}

#[derive(State)]
#[beam(crate = "::beam")]
#[expect(dead_code, reason = "the expansion reads only the cell types")]
struct RowToRowMap {
    lookup: MapState<Row, Row>,
}

fn schema(field: &str, field_type: FieldType) -> Arc<Schema> {
    Arc::new(Schema::builder().field(field, field_type).build())
}

fn int_row(schema: &Arc<Schema>, value: i64) -> Row {
    Row::builder(Arc::clone(schema))
        .with_value(value)
        .build()
        .expect("row")
}

/// Applies `RowStateFn<S, i64>` to an input without a row schema.
fn apply_without_input_schema<S: StateCells + 'static>(schemas: StateRowSchemas) -> Pipeline {
    let p = Pipeline::new();
    p.apply("In", Create::new(vec![("k".to_string(), 1_i64)]))
        .apply("Stateful", ParDo::new(RowStateFn::<S, i64>::new(schemas)));
    p
}

/// Applies `RowStateFn<S, Row>` to an input whose rows have the schema `input`.
fn apply_with_input_schema<S: StateCells + 'static>(
    input: &Arc<Schema>,
    schemas: StateRowSchemas,
) -> Pipeline {
    let p = Pipeline::new();
    p.apply(
        "In",
        Create::new(vec![("k".to_string(), int_row(input, 1))]),
    )
    .apply("Stateful", ParDo::new(RowStateFn::<S, Row>::new(schemas)));
    p
}

/// The reason of the construction error of `p`.
fn construction_error(p: &Pipeline) -> String {
    match p.validate() {
        Err(PipelineError::InvalidConstruction { reason, .. }) => reason,
        other => panic!("expected an invalid construction, got {other:?}"),
    }
}

/// For each state id of transform `Stateful`, the schema payloads of the `Row` coders of
/// each coder of the cell, in spec order.
fn state_row_payloads(p: &Pipeline) -> HashMap<String, Vec<Vec<Vec<u8>>>> {
    let proto = p.to_proto();
    let components = proto.components.expect("components");
    let transform = components
        .transforms
        .values()
        .find(|t| t.unique_name == "Stateful")
        .expect("the stateful transform");
    let payload = &transform.spec.as_ref().expect("spec").payload;
    let pardo = ParDoPayload::decode(payload.as_slice()).expect("ParDoPayload decodes");
    pardo
        .state_specs
        .into_iter()
        .map(|(state_id, spec)| {
            let coder_ids = match spec.spec.expect("spec") {
                Spec::BagSpec(b) => vec![b.element_coder_id],
                Spec::ReadModifyWriteSpec(r) => vec![r.coder_id],
                Spec::SetSpec(s) => vec![s.element_coder_id],
                Spec::MapSpec(m) => vec![m.key_coder_id, m.value_coder_id],
                Spec::MultimapSpec(m) => vec![m.key_coder_id, m.value_coder_id],
                Spec::OrderedListSpec(o) => vec![o.element_coder_id],
                Spec::CombiningSpec(c) => vec![c.accumulator_coder_id],
            };
            let payloads = coder_ids
                .iter()
                .map(|id| row_payloads(id, &components.coders))
                .collect();
            (state_id, payloads)
        })
        .collect()
}

fn row_payloads(coder_id: &str, coders: &HashMap<String, Coder>) -> Vec<Vec<u8>> {
    let coder = &coders[coder_id];
    let spec = coder.spec.as_ref().expect("coder spec");
    if spec.urn == URN_ROW {
        return vec![spec.payload.clone()];
    }
    coder
        .component_coder_ids
        .iter()
        .flat_map(|id| row_payloads(id, coders))
        .collect()
}

#[test]
fn dynamic_row_state_requires_a_schema_from_input_or_dofn() {
    let bad = apply_without_input_schema::<DynamicRowCells>(StateRowSchemas::new());
    let reason = construction_error(&bad);
    assert!(reason.contains("'last'"), "{reason}");
    assert!(reason.contains("state_row_schemas"), "{reason}");
    assert!(reason.contains("with_value"), "{reason}");

    let schema = schema("n", FieldType::int64());
    let good = apply_without_input_schema::<DynamicRowCells>(
        StateRowSchemas::new()
            .with_value("last", Arc::clone(&schema))
            .with_value("seen", Arc::clone(&schema)),
    );
    good.validate().expect("explicit schemas validate");
    let payloads = state_row_payloads(&good);
    let expected = vec![vec![schema.to_proto_bytes()]];
    assert_eq!(payloads["last"], expected);
    assert_eq!(payloads["seen"], expected);
}

#[test]
fn the_input_schema_does_not_give_the_schema_of_a_row_cell() {
    let input = schema("amount", FieldType::int64());
    let p = apply_with_input_schema::<OneBag>(&input, StateRowSchemas::new());
    let reason = construction_error(&p);
    assert!(reason.contains("'seen'"), "{reason}");
    assert!(reason.contains("with_value"), "{reason}");
}

#[test]
fn explicit_key_schemas_give_map_set_and_multimap_row_keys() {
    let key = schema("region", FieldType::string());
    let value = schema("limit", FieldType::int64());
    let p = apply_without_input_schema::<RowKeyedCells>(
        StateRowSchemas::new()
            .with_key("map", Arc::clone(&key))
            .with_key("set", Arc::clone(&key))
            .with_key("multi", Arc::clone(&key))
            .with_value("multi", Arc::clone(&value)),
    );
    p.validate().expect("every Row coder has a schema");
    let payloads = state_row_payloads(&p);
    let (key, value) = (key.to_proto_bytes(), value.to_proto_bytes());
    assert_eq!(payloads["map"], vec![vec![key.clone()], vec![]]);
    assert_eq!(payloads["set"], vec![vec![key.clone()]]);
    assert_eq!(payloads["multi"], vec![vec![key], vec![value]]);
}

#[test]
fn a_row_key_needs_with_key() {
    let schema = schema("region", FieldType::string());
    let p = apply_without_input_schema::<OneMap>(
        StateRowSchemas::new().with_value("map", Arc::clone(&schema)),
    );
    let reason = construction_error(&p);
    assert!(reason.contains("'map'"), "{reason}");
    assert!(reason.contains("with_key"), "{reason}");
}

#[test]
fn a_coder_with_two_rows_is_an_error() {
    let input = schema("amount", FieldType::int64());
    let pair = apply_with_input_schema::<PairBag>(
        &input,
        StateRowSchemas::new().with_value("pair", Arc::clone(&input)),
    );
    let reason = construction_error(&pair);
    assert!(reason.contains("'pair'"), "{reason}");
    assert!(reason.contains("more than one dynamic Row"), "{reason}");
}

#[test]
fn a_row_to_row_map_needs_a_key_and_a_value_schema() {
    let input = schema("amount", FieldType::int64());
    let key = schema("region", FieldType::string());

    let key_only = apply_with_input_schema::<RowToRowMap>(
        &input,
        StateRowSchemas::new().with_key("lookup", Arc::clone(&key)),
    );
    let reason = construction_error(&key_only);
    assert!(reason.contains("'lookup'"), "{reason}");
    assert!(reason.contains("with_value"), "{reason}");

    let both = apply_with_input_schema::<RowToRowMap>(
        &input,
        StateRowSchemas::new()
            .with_key("lookup", Arc::clone(&key))
            .with_value("lookup", Arc::clone(&input)),
    );
    both.validate().expect("both coders have a schema");
    assert_eq!(
        state_row_payloads(&both)["lookup"],
        vec![vec![key.to_proto_bytes()], vec![input.to_proto_bytes()]]
    );
}

#[test]
fn state_row_schemas_for_an_undeclared_cell_is_an_error() {
    let input = schema("amount", FieldType::int64());
    let p = apply_with_input_schema::<OneBag>(
        &input,
        StateRowSchemas::new()
            .with_value("seen", Arc::clone(&input))
            .with_value("missing", Arc::clone(&input)),
    );
    let reason = construction_error(&p);
    assert!(reason.contains("'missing'"), "{reason}");
}

#[test]
fn a_row_with_another_schema_fails_the_state_write() {
    let cell = schema("n", FieldType::int64());
    let other = schema("s", FieldType::string());
    let key = schema("region", FieldType::string());
    let backend: Arc<dyn UserStateBackend> = Arc::new(
        TestStateBackend::default()
            .with_value_schema("bag", Arc::clone(&cell))
            .with_value_schema("list", Arc::clone(&cell))
            .with_value_schema("map", Arc::clone(&cell))
            .with_key_schema("map", Arc::clone(&key))
            .with_key_schema("set", Arc::clone(&key)),
    );
    // The same layout in another `Arc` passes: the harness decodes the cell schema itself.
    let same = Arc::new((*cell).clone());
    let wrong = Row::builder(Arc::clone(&other))
        .with_value("x")
        .build()
        .expect("row");
    let region = Row::builder(Arc::clone(&key))
        .with_value("us")
        .build()
        .expect("row");

    let mut bag = BagState::<Row>::new(Arc::clone(&backend), "bag", vec![], b"k".to_vec());
    let err = bag.append(&wrong).expect_err("a wrong schema fails");
    assert!(err.to_string().contains("'bag'"), "{err}");
    bag.append(&int_row(&same, 1))
        .expect("the same layout passes");
    assert_eq!(bag.read().expect("read").len(), 1);

    let mut value = ValueState::new(BagState::<Row>::new(
        Arc::clone(&backend),
        "bag",
        vec![],
        b"v".to_vec(),
    ));
    value.write(&wrong).expect_err("a wrong value schema fails");

    let mut list =
        OrderedListState::<Row>::new(Arc::clone(&backend), "list", vec![], b"k".to_vec());
    list.add(Instant::from_millis(1), &wrong)
        .expect_err("a wrong ordered list schema fails");

    let mut map = MapState::<Row, Row>::new(Arc::clone(&backend), "map", vec![], b"k".to_vec());
    map.put(&wrong, &int_row(&cell, 1))
        .expect_err("a wrong key schema fails");
    map.put(&region, &wrong)
        .expect_err("a wrong value schema fails");
    map.put(&region, &int_row(&cell, 1))
        .expect("matching schemas pass");

    let mut set = SetState::new(MapState::<Row, ()>::new(
        Arc::clone(&backend),
        "set",
        vec![],
        b"k".to_vec(),
    ));
    set.insert(&wrong)
        .expect_err("a wrong set element schema fails");
    set.insert(&region).expect("a matching set element passes");
}
