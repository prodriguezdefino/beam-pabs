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

//! A registered [`CombineFn`] in the pipeline graph.
//!
//! The worker finds the `CombineFn` type in the registry, but the registry does not know the
//! key type of the `CombinePerKey`. So the stages keep each key as the bytes that its coder
//! wrote, and the key coder from the spec finds where the key ends. That works only for a
//! key coder that [`skips_exactly`]; for another key coder, the combine takes the rebuild
//! path.

use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::marker::PhantomData;
use std::sync::Arc;

use model::pipeline as proto;
use prost::Message;

use super::CombineFn;
use super::partial::PartialCombineFn;
use super::per_key::{ExtractOnlyFn, MergeAccumulatorsFn, MergeOnlyFn};
use crate::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use crate::coders::{skip_coder_value, skips_exactly};
use crate::pipeline::{
    COMBINE_STAGE_EXTRACT, COMBINE_STAGE_MERGE, COMBINE_STAGE_MERGE_AND_EXTRACT,
    COMBINE_STAGE_PRECOMBINE, Pipeline, URN_RUST_COMBINE_FN_REGISTERED,
    URN_RUST_DOFN_REGISTERED_COMBINE,
};
use crate::transforms::dofn::registered::{self, Registered, RegisteredFields};
use crate::transforms::dofn::{FinishBundleContext, HandlerContext};
use crate::transforms::{BundleHandler, DoFn, DoFnInput, HandlerInstance, TransformFn};

/// Selects how a registered [`CombineFn`] becomes its stages. The compiler infers it.
pub struct Combining;

/// One stage of a combine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CombineStage {
    /// Combines elements into accumulators before the shuffle.
    Precombine,
    /// Merges the accumulators of a key after the shuffle.
    Merge,
    /// Turns a merged accumulator into the output value.
    Extract,
    /// Merges the accumulators of a key and extracts the output value.
    MergeAndExtract,
}

impl CombineStage {
    fn name(self) -> &'static str {
        match self {
            Self::Precombine => COMBINE_STAGE_PRECOMBINE,
            Self::Merge => COMBINE_STAGE_MERGE,
            Self::Extract => COMBINE_STAGE_EXTRACT,
            Self::MergeAndExtract => COMBINE_STAGE_MERGE_AND_EXTRACT,
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        [
            Self::Precombine,
            Self::Merge,
            Self::Extract,
            Self::MergeAndExtract,
        ]
        .into_iter()
        .find(|stage| stage.name() == name)
    }
}

/// The stages of a registered `CombineFn`, for keys of any coder.
pub trait CombineStages: Send + Sync {
    /// Returns the handler of `stage` for keys that `key` delimits.
    fn stage(&self, stage: CombineStage, key: Arc<KeyCoder>) -> TransformFn;
}

/// Builds the stages of `combine_fn`.
pub(crate) fn registered_combine<CF: CombineFn>(combine_fn: CF) -> Arc<dyn CombineStages> {
    Arc::new(RegisteredCombine {
        combine_fn: Arc::new(combine_fn),
    })
}

struct RegisteredCombine<CF> {
    combine_fn: Arc<CF>,
}

impl<CF: CombineFn> CombineStages for RegisteredCombine<CF> {
    fn stage(&self, stage: CombineStage, key: Arc<KeyCoder>) -> TransformFn {
        let combine_fn = Arc::clone(&self.combine_fn);
        match stage {
            CombineStage::Precombine => Arc::new(ByteKeyed::new(
                PartialCombineFn::<EncodedKey, CF>::new(combine_fn),
                key,
            )),
            CombineStage::Merge => Arc::new(ByteKeyed::new(
                MergeOnlyFn::<EncodedKey, CF>::new(combine_fn),
                key,
            )),
            CombineStage::Extract => Arc::new(ByteKeyed::new(
                ExtractOnlyFn::<EncodedKey, CF>::new(combine_fn),
                key,
            )),
            CombineStage::MergeAndExtract => Arc::new(ByteKeyed::new(
                MergeAccumulatorsFn::<EncodedKey, CF>::new(combine_fn),
                key,
            )),
        }
    }
}

/// Finds where an encoded key ends, from the key coder of the `CombinePerKey`.
pub struct KeyCoder {
    coder_id: String,
    coders: HashMap<String, proto::Coder>,
}

impl KeyCoder {
    /// Splits an encoded `KV` into the key and the value.
    fn split<'e>(&self, element: &'e [u8]) -> Result<(&'e [u8], &'e [u8]), String> {
        let mut cursor = Cursor::new(element);
        skip_coder_value(&mut cursor, &self.coder_id, &self.coders, true)
            .map_err(|e| format!("Failed to find the end of the key: {e}"))?;
        usize::try_from(cursor.position())
            .ok()
            .and_then(|end| element.split_at_checked(end))
            .ok_or_else(|| "the key ends past the element".to_string())
    }
}

/// The bytes of a key, as its key coder wrote them.
///
/// Writing one copies the bytes, so `(EncodedKey, V)` encodes exactly as the `KV` of the
/// original key type. Reading one needs the key coder, so [`KeyCoder::split`] does it, and
/// decoding fails.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct EncodedKey(Vec<u8>);

/// Coder of [`EncodedKey`]. Only the worker uses it, so it is never in a pipeline graph.
pub(super) struct EncodedKeyCoder;

const URN_ENCODED_KEY: &str = "beam:coder:rust:encoded_key:v1";

const DECODE_ERROR: &str = "an encoded key is read with its key coder, not decoded";

impl Coder<EncodedKey> for EncodedKeyCoder {
    fn urn(&self) -> &'static str {
        URN_ENCODED_KEY
    }

    fn encode(
        &self,
        value: &EncodedKey,
        writer: &mut dyn Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        Ok(writer.write_all(&value.0)?)
    }

    fn decode(&self, _reader: &mut dyn Read, _context: Context) -> Result<EncodedKey, CoderError> {
        Err(CoderError::Format(DECODE_ERROR.to_string()))
    }
}

impl DefaultCoder for EncodedKey {
    type Coder = EncodedKeyCoder;

    fn coder() -> Self::Coder {
        EncodedKeyCoder
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        EncodedKeyCoder.encode(self, writer, Context::Nested)
    }

    fn decode_element(_reader: &mut dyn Read) -> Result<Self, CoderError> {
        Err(CoderError::Format(DECODE_ERROR.to_string()))
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_ENCODED_KEY, Vec::new())
    }
}

/// Runs a combine `DoFn` whose input is `(EncodedKey, V)`: splits off the key, decodes the
/// value, and calls the `DoFn` once for each window of the element.
struct ByteKeyed<D, V> {
    func: D,
    key: Arc<KeyCoder>,
    _value: PhantomData<fn() -> V>,
}

impl<D, V> ByteKeyed<D, V> {
    fn new(func: D, key: Arc<KeyCoder>) -> Self {
        Self {
            func,
            key,
            _value: PhantomData,
        }
    }
}

impl<D: Clone, V> Clone for ByteKeyed<D, V> {
    fn clone(&self) -> Self {
        Self::new(self.func.clone(), Arc::clone(&self.key))
    }
}

impl<D, V> BundleHandler for ByteKeyed<D, V>
where
    D: DoFn,
    D::In: DoFnInput<Value = (EncodedKey, V)>,
    V: DefaultCoder,
{
    fn setup(&mut self) -> Result<(), String> {
        Ok(self.func.setup()?)
    }

    fn start_bundle(&mut self) -> Result<(), String> {
        Ok(self.func.start_bundle()?)
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let (key, value) = self.key.split(element)?;
        let header = ctx.header;
        header.explode()?.try_for_each(|header| {
            let value = V::decode_with_context(value, ctx.input_schema, ctx.state_stream_reader)
                .map_err(|e| format!("Failed to decode the combine value: {e}"))?;
            let input = D::In::from_element((EncodedKey(key.to_vec()), value), &header)?;
            let mut pctx = ctx.as_process_context::<D::Out>().with_header(&header);
            Ok(self.func.process_element(input, &mut pctx)?)
        })
    }

    fn finish_bundle(&mut self, ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let mut fctx = FinishBundleContext::new(ctx.as_process_context::<D::Out>());
        Ok(self.func.finish_bundle(&mut fctx)?)
    }

    fn teardown(&mut self) -> Result<(), String> {
        Ok(self.func.teardown()?)
    }

    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }
}

/// The payload of [`URN_RUST_DOFN_REGISTERED_COMBINE`] and [`URN_RUST_COMBINE_FN_REGISTERED`].
#[derive(Clone, PartialEq, Message)]
struct RegisteredCombineSpec {
    #[prost(string, tag = "1")]
    id: String,
    #[prost(bytes = "vec", tag = "2")]
    fields: Vec<u8>,
    #[prost(string, tag = "3")]
    key_coder_id: String,
    /// The key coder and all coders that it refers to.
    #[prost(map = "string, message", tag = "4")]
    coders: HashMap<String, proto::Coder>,
    /// Empty in a `combine_fn` spec, where the lifted stage of the runner selects it.
    #[prost(string, tag = "5")]
    stage: String,
}

/// The function specs of a registered combine.
pub(super) struct CombineSpecs {
    spec: RegisteredCombineSpec,
}

impl CombineSpecs {
    /// Returns the specs of `combine_fn` with keys of coder `key_coder_id`, or `None` for the
    /// rebuild path: the type is not registered, or the key coder does not
    /// [`skips_exactly`].
    pub(super) fn new<CF: 'static>(
        pipeline: &Pipeline,
        transform_id: &str,
        combine_fn: &CF,
        key_coder_id: &str,
    ) -> Option<Self> {
        let coders = coder_closure(pipeline, key_coder_id);
        if !skips_exactly(key_coder_id, &coders) {
            return None;
        }
        let RegisteredFields { id, fields } =
            registered::registered_fields(pipeline, transform_id, combine_fn)?;
        Some(Self {
            spec: RegisteredCombineSpec {
                id: id.to_string(),
                fields,
                key_coder_id: key_coder_id.to_string(),
                coders,
                stage: String::new(),
            },
        })
    }

    /// The `do_fn` spec of the `ParDo` that runs `stage`.
    pub(super) fn do_fn(&self, stage: CombineStage) -> proto::FunctionSpec {
        proto::FunctionSpec {
            urn: URN_RUST_DOFN_REGISTERED_COMBINE.to_string(),
            payload: RegisteredCombineSpec {
                stage: stage.name().to_string(),
                ..self.spec.clone()
            }
            .encode_to_vec(),
        }
    }

    /// The `combine_fn` spec of the `CombinePerKey`.
    pub(super) fn combine_fn(&self) -> proto::FunctionSpec {
        proto::FunctionSpec {
            urn: URN_RUST_COMBINE_FN_REGISTERED.to_string(),
            payload: self.spec.encode_to_vec(),
        }
    }
}

/// Returns `coder_id` and every coder that it refers to, from the components of `pipeline`.
fn coder_closure(pipeline: &Pipeline, coder_id: &str) -> HashMap<String, proto::Coder> {
    let graph = pipeline.lock();
    let all = &graph.components.coders;
    let mut closure = HashMap::new();
    let mut pending = vec![coder_id.to_string()];
    while let Some(id) = pending.pop() {
        if closure.contains_key(&id) {
            continue;
        }
        if let Some(coder) = all.get(&id) {
            pending.extend(coder.component_coder_ids.iter().cloned());
            closure.insert(id, coder.clone());
        }
    }
    closure
}

/// Builds the handler of one stage of a registered combine from its spec payload. `stage`
/// is the lifted stage of the runner; without it, the payload names the stage.
pub fn registered_combine_handler(
    payload: &[u8],
    stage: Option<&str>,
) -> Result<TransformFn, String> {
    let RegisteredCombineSpec {
        id,
        fields,
        key_coder_id,
        coders,
        stage: spec_stage,
    } = RegisteredCombineSpec::decode(payload)
        .map_err(|e| format!("Failed to decode the registered combine payload: {e}"))?;
    let stage_name = stage.unwrap_or(&spec_stage);
    let stage = CombineStage::from_name(stage_name)
        .ok_or_else(|| format!("registered combine '{id}': unknown stage '{stage_name}'"))?;
    let key = Arc::new(KeyCoder {
        coder_id: key_coder_id,
        coders,
    });
    match registered::build(&id, &fields)? {
        Registered::Combine(stages) => Ok(stages.stage(stage, key)),
        Registered::Handler(_) => Err(format!(
            "'{id}' is registered as a DoFn, but the spec runs it as a CombineFn"
        )),
    }
}
