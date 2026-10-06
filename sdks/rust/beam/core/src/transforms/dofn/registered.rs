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

//! The link-time registry of function types that `#[derive(Register)]` fills.
//!
//! [`ParDo`](crate::transforms::ParDo) and
//! [`SplittableParDo`](crate::transforms::sdf::SplittableParDo) find the type of their
//! function here by [`TypeId`] and write the stable id and the serialized fields into the
//! function spec. The worker builds the function from that spec. A type with no entry stays
//! on the rebuild path.

use std::any::{Any, TypeId};
use std::sync::Arc;

use model::pipeline as proto;
use prost::Message;
use serde::de::DeserializeOwned;

use super::pardo::{DoFn, IntoParDoFn, Stateful, Stateless};
use super::sdf::SplittableDoFn;
use super::sdf::splittable_dofn::SplittableDoFnHandler;
use super::stateful::StatefulDoFn;
use crate::pipeline::constants::URN_RUST_DOFN_REGISTERED;
use crate::pipeline::{Pipeline, PipelineError};
use crate::transforms::TransformFn;

/// One registered function type. `#[derive(Register)]` submits it; do not submit it by hand.
pub struct FnRegistration {
    /// Stable id that the worker finds the type by. It is the same on every target.
    pub id: &'static str,
    /// Identifies the type in the process that builds the pipeline.
    pub type_id: fn() -> TypeId,
    /// Encodes an instance of the registered function into bytes.
    pub encode: fn(&dyn Any) -> Result<Vec<u8>, String>,
    /// Reconstructs the registered function handler from serialized bytes.
    pub build: fn(&[u8]) -> Result<Registered, String>,
}

inventory::collect!(FnRegistration);

/// What the worker builds from a registered function.
pub enum Registered {
    /// The handler of a `DoFn`, `StatefulDoFn` or `SplittableDoFn`.
    Handler(TransformFn),
}

/// Selects how a registered [`SplittableDoFn`] becomes a handler. The compiler infers it.
pub struct Splittable;

/// A function type that `#[derive(Register)]` registers: a [`DoFn`] (`M` = [`Stateless`]),
/// a [`StatefulDoFn`] (`M` = [`Stateful`]) or a [`SplittableDoFn`] (`M` = [`Splittable`]).
pub trait RegisteredFn<M>: DeserializeOwned {
    /// Wraps the function in what the worker runs.
    fn into_registered(self) -> Registered;
}

impl<F: DoFn + DeserializeOwned> RegisteredFn<Stateless> for F {
    fn into_registered(self) -> Registered {
        Registered::Handler(IntoParDoFn::<Stateless>::into_handler(self))
    }
}

impl<F: StatefulDoFn + DeserializeOwned> RegisteredFn<Stateful> for F {
    fn into_registered(self) -> Registered {
        Registered::Handler(IntoParDoFn::<Stateful>::into_handler(self))
    }
}

impl<S: SplittableDoFn + DeserializeOwned> RegisteredFn<Splittable> for S {
    fn into_registered(self) -> Registered {
        Registered::Handler(Arc::new(SplittableDoFnHandler::new(self)))
    }
}

/// The `do_fn` payload of [`URN_RUST_DOFN_REGISTERED`].
#[derive(Clone, PartialEq, Message)]
struct RegisteredSpec {
    #[prost(string, tag = "1")]
    id: String,
    #[prost(bytes = "vec", tag = "2")]
    fields: Vec<u8>,
}

/// Serializes the fields of a registered function of type `F`.
pub fn encode_registered<F: serde::Serialize + 'static>(func: &dyn Any) -> Result<Vec<u8>, String> {
    let func = func
        .downcast_ref::<F>()
        .ok_or_else(|| "the registration does not match the function type".to_string())?;
    serde_json::to_vec(func).map_err(|e| format!("the function fields do not serialize: {e}"))
}

/// Builds what the worker runs for a registered function of type `F` from its serialized
/// fields. The compiler infers `M`.
pub fn build_registered<F: RegisteredFn<M>, M>(fields: &[u8]) -> Result<Registered, String> {
    let func: F = serde_json::from_slice(fields)
        .map_err(|e| format!("the function fields do not deserialize: {e}"))?;
    Ok(func.into_registered())
}

/// The stable id and the serialized fields of a registered function.
pub(crate) struct RegisteredFields {
    pub(crate) id: &'static str,
    pub(crate) fields: Vec<u8>,
}

/// Returns the id and the fields of `func` if its type is registered, or `None` for the
/// rebuild path. A registration error is recorded on `pipeline`, so it fails `validate`; the
/// function then takes the rebuild path, so the graph stays complete.
pub(crate) fn registered_fields<F: 'static>(
    pipeline: &Pipeline,
    transform_id: &str,
    func: &F,
) -> Option<RegisteredFields> {
    let type_id = TypeId::of::<F>();
    let registration = inventory::iter::<FnRegistration>
        .into_iter()
        .find(|reg| (reg.type_id)() == type_id)?;
    find_registration(registration.id)
        .and_then(|_| (registration.encode)(func))
        .map(|fields| RegisteredFields {
            id: registration.id,
            fields,
        })
        .inspect_err(|reason| {
            pipeline.record_construction_error(PipelineError::InvalidConstruction {
                transform_id: transform_id.to_string(),
                reason: reason.clone(),
            });
        })
        .ok()
}

/// Returns the `do_fn` spec of `func` if its type is registered, or `None` for the rebuild
/// path. See [`registered_fields`] for errors.
pub(crate) fn graph_spec<F: 'static>(
    pipeline: &Pipeline,
    transform_id: &str,
    func: &F,
) -> Option<proto::FunctionSpec> {
    registered_fields(pipeline, transform_id, func).map(|RegisteredFields { id, fields }| {
        proto::FunctionSpec {
            urn: URN_RUST_DOFN_REGISTERED.to_string(),
            payload: RegisteredSpec {
                id: id.to_string(),
                fields,
            }
            .encode_to_vec(),
        }
    })
}

/// Builds the handler of a [`URN_RUST_DOFN_REGISTERED`] `do_fn` from its payload.
pub fn registered_handler(payload: &[u8]) -> Result<TransformFn, String> {
    let RegisteredSpec { id, fields } = RegisteredSpec::decode(payload)
        .map_err(|e| format!("Failed to decode the registered DoFn payload: {e}"))?;
    let Registered::Handler(handler) = build(&id, &fields)?;
    Ok(handler)
}

/// Builds what the worker runs for the registered function `id` from its fields.
pub(crate) fn build(id: &str, fields: &[u8]) -> Result<Registered, String> {
    (find_registration(id)?.build)(fields).map_err(|e| format!("registered function '{id}': {e}"))
}

/// Returns the only registration with `id`.
fn find_registration(id: &str) -> Result<&'static FnRegistration, String> {
    let mut matching = inventory::iter::<FnRegistration>
        .into_iter()
        .filter(|reg| reg.id == id);
    match (matching.next(), matching.next()) {
        (Some(reg), None) => Ok(reg),
        (None, _) => Err(format!(
            "no function type registered as '{id}'; the worker binary must link the crate \
             that defines it"
        )),
        (Some(_), Some(_)) => Err(format!(
            "more than one function type registered as '{id}'; set a unique \
             #[beam(id = \"..\")]"
        )),
    }
}
