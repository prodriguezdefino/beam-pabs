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

use std::any::TypeId;
use std::collections::HashMap;
use std::io::Write;
use std::marker::PhantomData;
use std::sync::Arc;

use model::pipeline as proto;
use prost::Message;

use super::CombineFn;
use super::partial::PartialCombineFn;
use super::per_key::{
    CombineGroupedValuesFn, ConvertToAccumulatorsFn, ExtractOnlyFn, MergeOnlyFn, typed_stage,
};
use crate::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use crate::coders::{SkipPlan, skips_exactly};
use crate::pipeline::{Pipeline, URN_RUST_COMBINE_FN_REGISTERED};
use crate::transforms::dofn::registered::{self, Registered, RegisteredFields};
use crate::transforms::dofn::{FinishBundleContext, HandlerContext};
use crate::transforms::{
    BundleHandler, DoFn, DoFnInput, HandlerInstance, TransformFn, TypedElement,
};

/// Selects how a registered [`CombineFn`] becomes its stages. The compiler infers it.
pub struct Combining;

pub use crate::pipeline::constants::{CombineStage, UnknownCombineStage};

/// A standard key type whose combine stages run with its concrete Rust type `K`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
enum StandardKey {
    None = 0,
    Unit = 1,
    Bool = 2,
    I32 = 3,
    I64 = 4,
    String = 5,
    Bytes = 6,
}

impl StandardKey {
    /// Returns the variant for `K` when `key_coder_id` is the default coder of `K`.
    fn of<K: DefaultCoder + 'static>(
        key_coder_id: &str,
        coders: &HashMap<String, proto::Coder>,
    ) -> Self {
        match coders.get(key_coder_id) {
            Some(proto::Coder {
                spec: Some(proto::FunctionSpec { urn, .. }),
                component_coder_ids,
            }) if component_coder_ids.is_empty() && urn == K::coder().urn() => {
                match TypeId::of::<K>() {
                    id if id == TypeId::of::<()>() => Self::Unit,
                    id if id == TypeId::of::<bool>() => Self::Bool,
                    id if id == TypeId::of::<i32>() => Self::I32,
                    id if id == TypeId::of::<i64>() => Self::I64,
                    id if id == TypeId::of::<String>() => Self::String,
                    id if id == TypeId::of::<Vec<u8>>() => Self::Bytes,
                    _ => Self::None,
                }
            }
            _ => Self::None,
        }
    }
}

/// The stages of a registered `CombineFn`, for keys of any coder.
pub trait CombineStages: Send + Sync {
    /// Returns the handler of `stage` for keys that `key` describes.
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
        match key.standard {
            StandardKey::Unit => typed_stage::<(), CF>(stage, combine_fn),
            StandardKey::Bool => typed_stage::<bool, CF>(stage, combine_fn),
            StandardKey::I32 => typed_stage::<i32, CF>(stage, combine_fn),
            StandardKey::I64 => typed_stage::<i64, CF>(stage, combine_fn),
            StandardKey::String => typed_stage::<String, CF>(stage, combine_fn),
            StandardKey::Bytes => typed_stage::<Vec<u8>, CF>(stage, combine_fn),
            StandardKey::None => match stage {
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
                CombineStage::ConvertToAccumulators => Arc::new(ByteKeyed::new(
                    ConvertToAccumulatorsFn::<EncodedKey, CF>::new(combine_fn),
                    key,
                )),
                CombineStage::CombineGroupedValues => Arc::new(ByteKeyed::new(
                    CombineGroupedValuesFn::<EncodedKey, CF>::new(combine_fn),
                    key,
                )),
            },
        }
    }
}

/// Finds where an encoded key ends, or names the standard key type of the `CombinePerKey`.
pub struct KeyCoder {
    plan: SkipPlan,
    standard: StandardKey,
}

impl KeyCoder {
    /// Splits an encoded `KV` into the key and the value.
    fn split<'e>(&self, element: &'e [u8]) -> Result<(&'e [u8], &'e [u8]), String> {
        let mut value = element;
        self.plan
            .skip(&mut value, true)
            .map_err(|e| format!("Failed to find the end of the key: {e}"))?;
        Ok((&element[..element.len() - value.len()], value))
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
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        Ok(writer.write_all(&value.0)?)
    }

    fn decode(&self, _reader: &mut &[u8], _context: Context) -> Result<EncodedKey, CoderError> {
        Err(CoderError::Format(DECODE_ERROR.to_string()))
    }
}

impl DefaultCoder for EncodedKey {
    type Coder = EncodedKeyCoder;

    fn coder() -> Self::Coder {
        EncodedKeyCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        EncodedKeyCoder.encode(self, writer, Context::Nested)
    }

    fn decode_element(_reader: &mut &[u8]) -> Result<Self, CoderError> {
        Err(CoderError::Format(DECODE_ERROR.to_string()))
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_ENCODED_KEY, Vec::new())
    }
}

/// Runs a combine `DoFn` whose input is `(EncodedKey, V)`.
struct ByteKeyed<D, V> {
    func: D,
    key: Arc<KeyCoder>,
    explode: bool,
    buf: Vec<u8>,
    _value: PhantomData<fn() -> V>,
}

impl<D: DoFn, V> ByteKeyed<D, V> {
    fn new(func: D, key: Arc<KeyCoder>) -> Self {
        let explode = <D::In as DoFnInput>::OBSERVES_WINDOW || !func.side_inputs().is_empty();
        Self {
            func,
            key,
            explode,
            buf: Vec::new(),
            _value: PhantomData,
        }
    }
}

impl<D: DoFn + Clone, V> Clone for ByteKeyed<D, V> {
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
        // V does not require Clone, so multi-window elements decode V and copy the key slice once
        // per window; single-window elements run the closure once.
        header
            .explode_if(self.explode)
            .map_err(|e| e.to_string())?
            .try_for_each(|header| {
                let value =
                    V::decode_with_context(value, ctx.input_schema, ctx.state_stream_reader)
                        .map_err(|e| format!("Failed to decode the combine value: {e}"))?;
                let input = D::In::from_element((EncodedKey(key.to_vec()), value), &header)?;
                let mut pctx = ctx.as_process_context::<D::Out>().with_header(&header);
                Ok(self.func.process_element(input, &mut pctx)?)
            })
    }

    fn process_value(
        &mut self,
        element: TypedElement<'_>,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        let mut buf = std::mem::take(&mut self.buf);
        let result = element
            .encode_into(&mut buf)
            .and_then(|()| self.process(&buf, ctx));
        self.buf = buf;
        result
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

/// The payload of [`URN_RUST_COMBINE_FN_REGISTERED`].
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
    #[prost(enumeration = "StandardKey", tag = "5")]
    standard_key: i32,
}

/// The function spec of a registered combine.
pub(super) struct CombineSpecs {
    spec: RegisteredCombineSpec,
}

impl CombineSpecs {
    /// Returns the spec of `combine_fn` for key coder `key_coder_id`, or `None` when the
    /// combine takes the rebuild path.
    pub(super) fn new<K: DefaultCoder + 'static, CF: 'static>(
        pipeline: &Pipeline,
        transform_id: &str,
        combine_fn: &CF,
        key_coder_id: Option<&str>,
    ) -> Option<Self> {
        let key_coder_id = key_coder_id?;
        let coders = coder_closure(pipeline, key_coder_id);
        if !skips_exactly(key_coder_id, &coders) {
            return None;
        }
        let standard_key = StandardKey::of::<K>(key_coder_id, &coders) as i32;
        let RegisteredFields { id, fields } =
            registered::registered_fields(pipeline, transform_id, combine_fn)?;
        Some(Self {
            spec: RegisteredCombineSpec {
                id: id.to_string(),
                fields,
                key_coder_id: key_coder_id.to_string(),
                coders,
                standard_key,
            },
        })
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

/// Builds the handler of the combine stage `stage` of a registered combine from its spec
/// payload. The URN of the stage transform gives `stage`.
pub fn registered_combine_handler(payload: &[u8], stage: &str) -> Result<TransformFn, String> {
    let RegisteredCombineSpec {
        id,
        fields,
        key_coder_id,
        coders,
        standard_key,
    } = RegisteredCombineSpec::decode(payload)
        .map_err(|e| format!("Failed to decode the registered combine payload: {e}"))?;
    let stage: CombineStage = stage
        .parse()
        .map_err(|_| format!("registered combine '{id}': unknown stage '{stage}'"))?;
    let standard = StandardKey::try_from(standard_key).unwrap_or(StandardKey::None);
    let key = Arc::new(KeyCoder {
        plan: SkipPlan::new(&key_coder_id, &coders),
        standard,
    });
    match registered::build(&id, &fields)? {
        Registered::Combine(stages) => Ok(stages.stage(stage, key)),
        Registered::Handler(_) => Err(format!(
            "'{id}' is registered as a DoFn, but the spec runs it as a CombineFn"
        )),
    }
}
