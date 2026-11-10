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

//! User state specifications and handles for stateful DoFns.
//!
//! Under the Beam portability model, state is scoped to one key and one window. Writes
//! and clears are buffered during the bundle and committed when the bundle completes.

mod backend;
mod bag;
mod map;
mod multimap;
mod ordered_list;
mod row_schemas;
mod set;
mod value;

use beam_model::pipeline as proto;

use crate::pipeline::Pipeline;

pub use backend::{UserStateBackend, UserStateError};
pub use bag::BagState;
pub(crate) use bag::BagStateSpec;
pub use map::MapState;
pub(crate) use map::MapStateSpec;
pub use multimap::MultimapState;
pub(crate) use multimap::MultimapStateSpec;
pub use ordered_list::OrderedListState;
pub(crate) use ordered_list::OrderedListStateSpec;
pub use row_schemas::StateRowSchemas;
use row_schemas::check_row_schema;
pub use set::SetState;
pub(crate) use set::SetStateSpec;
pub use value::ValueState;
pub(crate) use value::ValueStateSpec;

/// Type-erased state specification for pipeline graph construction.
pub trait AnyStateSpec: Send + Sync {
    /// Returns the state ID, which is unique within the transform.
    fn name(&self) -> &str;

    /// Registers the state value coder in the pipeline and encodes the Runner API `StateSpec`.
    fn register_and_encode(&self, pipeline: &Pipeline) -> proto::StateSpec;
}

fn state_spec(spec: proto::state_spec::Spec, protocol_urn: &str) -> proto::StateSpec {
    proto::StateSpec {
        spec: Some(spec),
        protocol: Some(proto::FunctionSpec {
            urn: protocol_urn.to_string(),
            payload: Vec::new(),
        }),
    }
}
