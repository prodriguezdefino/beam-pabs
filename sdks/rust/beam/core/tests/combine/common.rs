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

//! Helpers for the `CombineFn` tests in `core/tests`.

use std::io::{Read, Write};

use beam::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use beam::internals::{BundleHandler, HandlerContext};
use beam::pipeline::URN_PAR_DO;
use beam::prelude::*;
use beam_model::pipeline as proto;
use prost::Message;
use serde::{Deserialize, Serialize};

/// Runs `combine_fn` as a runner does: it deals `inputs` to `parts` partial accumulators in
/// turn, merges the partials and extracts the output. More than one part makes the merge of
/// the function run.
pub(crate) fn combine_in_parts<CF: CombineFn>(
    combine_fn: &CF,
    inputs: Vec<CF::Input>,
    parts: usize,
) -> Result<CF::Output> {
    let groups = inputs.into_iter().enumerate().fold(
        (0..parts).map(|_| Vec::new()).collect::<Vec<_>>(),
        |mut groups, (index, input)| {
            groups[index % parts].push(input);
            groups
        },
    );
    let partials = groups
        .into_iter()
        .map(|group| {
            group
                .into_iter()
                .try_fold(combine_fn.create_accumulator()?, |acc, input| {
                    combine_fn.add_input(acc, input)
                })
        })
        .collect::<Result<Vec<_>>>()?;
    combine_fn.extract_output(combine_fn.merge_accumulators(partials)?)
}

/// Sums the values of a key, each times `factor`.
#[derive(Clone, Serialize, Deserialize, Register)]
pub(crate) struct ScaledSum {
    pub factor: i64,
}

impl CombineFn for ScaledSum {
    type Input = i64;
    type Accum = i64;
    type Output = i64;

    fn create_accumulator(&self) -> Result<i64> {
        Ok(0)
    }

    fn add_input(&self, acc: i64, input: i64) -> Result<i64> {
        Ok(acc + input * self.factor)
    }

    fn merge_accumulators(&self, accs: Vec<i64>) -> Result<i64> {
        Ok(accs.into_iter().sum())
    }

    fn extract_output(&self, acc: i64) -> Result<i64> {
        Ok(acc)
    }
}

/// A key whose coder has a URN that the worker cannot delimit.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct OpaqueKey(pub i64);

pub(crate) struct OpaqueKeyCoder;

impl Coder<OpaqueKey> for OpaqueKeyCoder {
    fn urn(&self) -> &'static str {
        "beam:coder:test:opaque_key:v1"
    }

    fn encode(
        &self,
        value: &OpaqueKey,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        Ok(writer.write_all(&value.0.to_be_bytes())?)
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<OpaqueKey, CoderError> {
        let mut bytes = [0; 8];
        reader.read_exact(&mut bytes)?;
        Ok(OpaqueKey(i64::from_be_bytes(bytes)))
    }
}

impl DefaultCoder for OpaqueKey {
    type Coder = OpaqueKeyCoder;

    fn coder() -> Self::Coder {
        OpaqueKeyCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        OpaqueKeyCoder.encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        OpaqueKeyCoder.decode(reader, Context::Nested)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(OpaqueKeyCoder.urn(), Vec::new())
    }
}

/// The transform named `name`.
pub(crate) fn transform(p: &Pipeline, name: &str) -> proto::PTransform {
    p.to_proto()
        .components
        .expect("components")
        .transforms
        .into_values()
        .find(|t| t.unique_name == name)
        .expect("transform exists")
}

/// The `combine_fn` spec of the `CombinePerKey` named `name`.
pub(crate) fn combine_fn_spec(p: &Pipeline, name: &str) -> proto::FunctionSpec {
    let spec = transform(p, name).spec.expect("spec");
    proto::CombinePayload::decode(spec.payload.as_slice())
        .expect("CombinePayload")
        .combine_fn
        .expect("combine_fn")
}

/// The `do_fn` spec of the `ParDo` named `name`.
pub(crate) fn do_fn_spec(p: &Pipeline, name: &str) -> proto::FunctionSpec {
    let spec = transform(p, name).spec.expect("spec");
    assert_eq!(spec.urn, URN_PAR_DO);
    proto::ParDoPayload::decode(spec.payload.as_slice())
        .expect("ParDoPayload")
        .do_fn
        .expect("do_fn")
}

pub(crate) fn kvs(pairs: &[(&str, i64)]) -> Vec<(String, i64)> {
    pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

#[derive(BeamRow, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct RowKey {
    pub group: String,
    pub bucket: i32,
}

/// Runs `handler` over `elements` as one bundle and returns what it emitted.
pub(crate) fn run_bundle(handler: &dyn BundleHandler, elements: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let mut instance = handler.instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    instance.setup().expect("setup");
    instance.start_bundle().expect("start_bundle");
    {
        let mut ctx = HandlerContext::new(&mut sink);
        for element in elements {
            instance.process(element, &mut ctx).expect("process");
        }
        instance.finish_bundle(&mut ctx).expect("finish_bundle");
    }
    instance.teardown().expect("teardown");
    sink
}
