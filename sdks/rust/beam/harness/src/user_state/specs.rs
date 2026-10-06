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

//! The user state cells that the transforms of a descriptor declare.

use std::collections::HashMap;
use std::sync::Arc;

use prost::Message;

use beam::coders::extract_row_schema;
use beam::pipeline::URN_PAR_DO;
use beam::schema::Schema;
use beam_model::fn_execution::ProcessBundleDescriptor;
use beam_model::pipeline::state_spec::Spec;
use beam_model::pipeline::{Coder, ParDoPayload};

use crate::state::framing::RunnerFraming;

/// The runner framing and the row schema of one coder of a state cell.
pub(super) struct StateCoder {
    pub(super) framing: RunnerFraming,
    /// The schema of the rows in a value, when all rows of the coder have one schema.
    pub(super) schema: Option<Arc<Schema>>,
}

impl StateCoder {
    fn new(coder_id: &str, coders: &Arc<HashMap<String, Coder>>) -> Self {
        Self {
            framing: RunnerFraming::new(coder_id, coders),
            schema: extract_row_schema(coder_id, coders),
        }
    }
}

/// The key and value coders of a map, set or multimap cell.
pub(super) struct MapStateCoders {
    pub(super) key: StateCoder,
    pub(super) value: StateCoder,
}

/// The coders of one state cell, by the kind of the cell.
enum CellCoders {
    /// A bag, value or combining cell. A combining cell is a bag of accumulators.
    Bag(StateCoder),
    /// A map, set or multimap cell.
    Map(MapStateCoders),
    OrderedList(StateCoder),
}

/// The cells of each transform, by transform id and state id.
type ByTransform<T> = HashMap<String, HashMap<String, T>>;

/// The user state cells that the transforms of a descriptor declare, with their coders. A
/// processor builds it once and its bundles share it.
pub struct UserStateSpecs {
    bags: ByTransform<StateCoder>,
    maps: ByTransform<MapStateCoders>,
    ordered_lists: ByTransform<StateCoder>,
}

/// The cell `state_id` of transform `transform_id` in `cells`.
fn cell<'a, T>(cells: &'a ByTransform<T>, transform_id: &str, state_id: &str) -> Option<&'a T> {
    cells.get(transform_id)?.get(state_id)
}

impl UserStateSpecs {
    /// The coder of the bag, value or combining cell `state_id`.
    pub(super) fn bag(&self, transform_id: &str, state_id: &str) -> Option<&StateCoder> {
        cell(&self.bags, transform_id, state_id)
    }

    /// The coders of the map, set or multimap cell `state_id`.
    pub(super) fn map(&self, transform_id: &str, state_id: &str) -> Option<&MapStateCoders> {
        cell(&self.maps, transform_id, state_id)
    }

    /// The coder of the ordered list cell `state_id`.
    pub(super) fn ordered_list(&self, transform_id: &str, state_id: &str) -> Option<&StateCoder> {
        cell(&self.ordered_lists, transform_id, state_id)
    }

    /// The cells of `descriptor`, or `None` if no transform declares user state.
    pub fn from_descriptor(
        descriptor: &ProcessBundleDescriptor,
        coders: &Arc<HashMap<String, Coder>>,
    ) -> Option<Self> {
        let cells = descriptor
            .transforms
            .iter()
            .filter_map(|(t_id, t)| {
                let spec = t
                    .spec
                    .as_ref()
                    .filter(|s| s.urn == URN_PAR_DO && !s.payload.is_empty())?;
                let pardo = ParDoPayload::decode(spec.payload.as_slice()).ok()?;
                Some((t_id, pardo.state_specs))
            })
            .flat_map(|(t_id, state_specs)| {
                state_specs.into_iter().filter_map(move |(state_id, spec)| {
                    let cell = match spec.spec? {
                        Spec::BagSpec(b) => {
                            CellCoders::Bag(StateCoder::new(&b.element_coder_id, coders))
                        }
                        Spec::ReadModifyWriteSpec(r) => {
                            CellCoders::Bag(StateCoder::new(&r.coder_id, coders))
                        }
                        Spec::CombiningSpec(c) => {
                            CellCoders::Bag(StateCoder::new(&c.accumulator_coder_id, coders))
                        }
                        Spec::MapSpec(m) => CellCoders::Map(MapStateCoders {
                            key: StateCoder::new(&m.key_coder_id, coders),
                            value: StateCoder::new(&m.value_coder_id, coders),
                        }),
                        Spec::SetSpec(s) => CellCoders::Map(MapStateCoders {
                            key: StateCoder::new(&s.element_coder_id, coders),
                            value: StateCoder::new(&s.element_coder_id, coders),
                        }),
                        Spec::MultimapSpec(m) => CellCoders::Map(MapStateCoders {
                            key: StateCoder::new(&m.key_coder_id, coders),
                            value: StateCoder::new(&m.value_coder_id, coders),
                        }),
                        Spec::OrderedListSpec(o) => {
                            CellCoders::OrderedList(StateCoder::new(&o.element_coder_id, coders))
                        }
                    };
                    Some((t_id, state_id, cell))
                })
            });

        let specs = cells.fold(
            Self {
                bags: HashMap::new(),
                maps: HashMap::new(),
                ordered_lists: HashMap::new(),
            },
            |mut specs, (t_id, state_id, cell)| {
                match cell {
                    CellCoders::Bag(coder) => {
                        specs
                            .bags
                            .entry(t_id.clone())
                            .or_default()
                            .insert(state_id, coder);
                    }
                    CellCoders::Map(coders) => {
                        specs
                            .maps
                            .entry(t_id.clone())
                            .or_default()
                            .insert(state_id, coders);
                    }
                    CellCoders::OrderedList(coder) => {
                        specs
                            .ordered_lists
                            .entry(t_id.clone())
                            .or_default()
                            .insert(state_id, coder);
                    }
                }
                specs
            },
        );
        let empty =
            specs.bags.is_empty() && specs.maps.is_empty() && specs.ordered_lists.is_empty();
        (!empty).then_some(specs)
    }
}
