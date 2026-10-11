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

//! A registered `CombineFn`: its stages travel in the graph, with the key coder, so the
//! worker needs no rebuilt pipeline for them.

use std::io::{Read, Write};

use beam::coders::{BeamIterable, Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use beam::internals::{
    BundleHandler, HandlerContext, PipelineInternals, TypedElement, registered_combine_handler,
};
use beam::pipeline::{
    CombineStage, URN_COMBINE_GLOBALLY, URN_COMBINE_GROUPED_VALUES, URN_COMBINE_PER_KEY,
    URN_PAR_DO, URN_RUST_COMBINE_FN, URN_RUST_COMBINE_FN_REGISTERED, stage_key,
};
use beam::prelude::*;
use model::pipeline as proto;
use prost::Message;
use serde::{Deserialize, Serialize};
use testing::{TestPipeline, passert};

/// Sums the values of a key, each times `factor`.
#[derive(Clone, Serialize, Deserialize, Register)]
struct ScaledSum {
    factor: i64,
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
struct OpaqueKey(i64);

struct OpaqueKeyCoder;

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
fn transform(p: &Pipeline, name: &str) -> proto::PTransform {
    p.to_proto()
        .components
        .expect("components")
        .transforms
        .into_values()
        .find(|t| t.unique_name == name)
        .expect("transform exists")
}

/// The `combine_fn` spec of the `CombinePerKey` named `name`.
fn combine_fn_spec(p: &Pipeline, name: &str) -> proto::FunctionSpec {
    let spec = transform(p, name).spec.expect("spec");
    proto::CombinePayload::decode(spec.payload.as_slice())
        .expect("CombinePayload")
        .combine_fn
        .expect("combine_fn")
}

/// The `do_fn` spec of the `ParDo` named `name`.
fn do_fn_spec(p: &Pipeline, name: &str) -> proto::FunctionSpec {
    let spec = transform(p, name).spec.expect("spec");
    assert_eq!(spec.urn, URN_PAR_DO);
    proto::ParDoPayload::decode(spec.payload.as_slice())
        .expect("ParDoPayload")
        .do_fn
        .expect("do_fn")
}

fn kvs(pairs: &[(&str, i64)]) -> Vec<(String, i64)> {
    pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect()
}

#[tokio::test]
async fn registered_combine_fn_runs_on_prism_without_registered_handlers() {
    let p = TestPipeline::new();
    let out = p
        .apply("Create", Create::new(kvs(&[("a", 1), ("b", 2), ("a", 3)])))
        .apply("Sum", CombinePerKey::new(ScaledSum { factor: 10 }));
    passert::that("PerKeyTotals", &out).contains_in_any_order(kvs(&[("a", 40), ("b", 20)]));

    assert_eq!(
        combine_fn_spec(p.pipeline(), "Sum").urn,
        URN_RUST_COMBINE_FN_REGISTERED
    );
    assert_eq!(
        transform(p.pipeline(), "Sum/CombineGroupedValues")
            .spec
            .expect("spec")
            .urn,
        URN_COMBINE_GROUPED_VALUES
    );
    assert_eq!(
        combine_fn_spec(p.pipeline(), "Sum/CombineGroupedValues"),
        combine_fn_spec(p.pipeline(), "Sum")
    );
    // The assertions register closure handlers; the combine adds none.
    assert!(
        p.pipeline()
            .transform_handlers()
            .keys()
            .all(|key| !key.starts_with("Sum")),
        "{:?}",
        p.pipeline().transform_handlers().keys()
    );
    p.run().await.expect("pipeline and assertions pass");
}

#[tokio::test]
async fn registered_combine_fn_runs_globally() {
    let p = TestPipeline::new();
    let out = p
        .apply("Create", Create::new(vec![1i64, 2, 3]))
        .apply("Total", CombineGlobally::new(ScaledSum { factor: 2 }));
    passert::that("AssertTotal", &out).contains_in_any_order([12i64]);

    assert_eq!(
        combine_fn_spec(p.pipeline(), "Total/Combine").urn,
        URN_RUST_COMBINE_FN_REGISTERED
    );
    p.run().await.expect("pipeline and assertions pass");
}

#[test]
fn a_key_coder_the_worker_cannot_delimit_keeps_the_rebuild_path() {
    let p = Pipeline::new();
    let _ = p
        .apply(
            "Create",
            Create::new(vec![(OpaqueKey(1), 1i64), (OpaqueKey(1), 2)]),
        )
        .apply("Sum", CombinePerKey::new(ScaledSum { factor: 1 }));

    assert_eq!(combine_fn_spec(&p, "Sum").urn, URN_RUST_COMBINE_FN);
    assert_eq!(
        combine_fn_spec(&p, "Sum/CombineGroupedValues").urn,
        URN_RUST_COMBINE_FN
    );
    let handlers = p.transform_handlers();
    for stage in [
        CombineStage::Precombine,
        CombineStage::Merge,
        CombineStage::Extract,
        CombineStage::ConvertToAccumulators,
        CombineStage::CombineGroupedValues,
    ] {
        assert!(
            handlers.contains_key(&stage_key("Sum", stage)),
            "{stage}: {:?}",
            handlers.keys()
        );
    }
    assert_eq!(p.validate(), Ok(()));
}

/// Runs `handler` over `elements` as one bundle and returns what it emitted.
fn run_bundle(handler: &dyn BundleHandler, elements: &[Vec<u8>]) -> Vec<Vec<u8>> {
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

/// Runs `handler` over `elements` via `process_value` and asserts whether each slot was taken.
fn run_fused_bundle<T: DefaultCoder>(
    handler: &dyn BundleHandler,
    elements: Vec<T>,
    expect_taken: bool,
) -> Vec<Vec<u8>> {
    let mut instance = handler.instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    instance.setup().expect("setup");
    instance.start_bundle().expect("start_bundle");
    {
        let mut ctx = HandlerContext::new(&mut sink);
        for element in elements {
            let mut slot = Some(element);
            instance
                .process_value(TypedElement::new(&mut slot), &mut ctx)
                .expect("process_value");
            assert_eq!(slot.is_none(), expect_taken);
        }
        instance.finish_bundle(&mut ctx).expect("finish_bundle");
    }
    instance.teardown().expect("teardown");
    sink
}

/// Asserts that `Precombine` takes fused `(K, i64)` values for key type `K`.
fn assert_standard_key_takes_fused<K>(k1: K, k2: K)
where
    K: DefaultCoder + Clone + Eq + Ord + std::hash::Hash + std::fmt::Debug,
{
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(vec![(k1.clone(), 1i64)]))
        .apply("Sum", CombinePerKey::new(ScaledSum { factor: 10 }));
    let combine_fn = combine_fn_spec(&p, "Sum");
    let precombine =
        registered_combine_handler(&combine_fn.payload, CombineStage::Precombine.as_str())
            .expect("precombine");
    let inputs = vec![(k1.clone(), 1i64), (k2.clone(), 2), (k1.clone(), 3)];
    let mut partials = run_fused_bundle(precombine.as_ref(), inputs, true)
        .iter()
        .map(|bytes| <(K, i64)>::decode(bytes).expect("decode"))
        .collect::<Vec<_>>();
    partials.sort();
    let mut expected = if k1 == k2 {
        vec![(k1, 60)]
    } else {
        vec![(k1, 40), (k2, 20)]
    };
    expected.sort();
    assert_eq!(partials, expected);
}

#[test]
fn standard_keys_take_fused_values_without_encoding() {
    assert_standard_key_takes_fused((), ());
    assert_standard_key_takes_fused(false, true);
    assert_standard_key_takes_fused(1i32, 2i32);
    assert_standard_key_takes_fused(1i64, 2i64);
    assert_standard_key_takes_fused("a".to_string(), "b".to_string());
    assert_standard_key_takes_fused(b"a".to_vec(), b"b".to_vec());
}

#[derive(BeamRow, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct RowKey {
    group: String,
    bucket: i32,
}

#[test]
fn the_stages_combine_keys_as_encoded_bytes() {
    let k1 = RowKey {
        group: "a".to_string(),
        bucket: 1,
    };
    let k2 = RowKey {
        group: "b".to_string(),
        bucket: 2,
    };
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(vec![(k1.clone(), 1i64)]))
        .apply("Sum", CombinePerKey::new(ScaledSum { factor: 10 }));
    let combine_fn = combine_fn_spec(&p, "Sum/CombineGroupedValues");

    let precombine =
        registered_combine_handler(&combine_fn.payload, CombineStage::Precombine.as_str())
            .expect("precombine");
    let raw = vec![(k1.clone(), 1i64), (k2.clone(), 2), (k1.clone(), 3)];
    let encoded = raw
        .iter()
        .map(|kv| kv.encode().expect("encode"))
        .collect::<Vec<_>>();
    for emitted in [
        run_bundle(precombine.as_ref(), &encoded),
        run_fused_bundle(precombine.as_ref(), raw, false),
    ] {
        let mut partials = emitted
            .iter()
            .map(|bytes| <(RowKey, i64)>::decode(bytes).expect("decode"))
            .collect::<Vec<_>>();
        partials.sort();
        assert_eq!(partials, vec![(k1.clone(), 40), (k2.clone(), 20)]);
    }

    let combine = registered_combine_handler(
        &combine_fn.payload,
        CombineStage::CombineGroupedValues.as_str(),
    )
    .expect("combine_grouped_values");
    let grouped = (k1.clone(), BeamIterable::from_vec(vec![4i64, 5]))
        .encode()
        .expect("encode");
    let combined = run_bundle(combine.as_ref(), &[grouped])
        .iter()
        .map(|bytes| <(RowKey, i64)>::decode(bytes).expect("decode"))
        .collect::<Vec<_>>();
    assert_eq!(combined, vec![(k1, 90)]);
}

#[tokio::test]
async fn row_key_runs_on_prism_without_registered_handlers() {
    let k1 = RowKey {
        group: "a".to_string(),
        bucket: 1,
    };
    let k2 = RowKey {
        group: "b".to_string(),
        bucket: 2,
    };
    let p = TestPipeline::new();
    let out = p
        .apply(
            "Create",
            Create::new(vec![(k1.clone(), 1i64), (k2.clone(), 2), (k1.clone(), 3)]),
        )
        .apply("Sum", CombinePerKey::new(ScaledSum { factor: 10 }));
    passert::that("Totals", &out).contains_in_any_order(vec![(k1, 40), (k2, 20)]);
    assert_eq!(
        combine_fn_spec(p.pipeline(), "Sum").urn,
        URN_RUST_COMBINE_FN_REGISTERED
    );
    assert!(
        p.pipeline()
            .transform_handlers()
            .keys()
            .all(|key| !key.starts_with("Sum"))
    );
    p.run().await.expect("pipeline and assertions pass");
}

#[test]
fn a_lifted_stage_comes_from_the_runner_and_overrides_the_payload() {
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(kvs(&[("a", 1)])))
        .apply("Sum", CombinePerKey::new(ScaledSum { factor: 3 }));
    let combine_fn = combine_fn_spec(&p, "Sum");

    assert!(registered_combine_handler(&combine_fn.payload, "unknown").is_err());
    let precombine =
        registered_combine_handler(&combine_fn.payload, CombineStage::Precombine.as_str())
            .expect("precombine");
    let emitted = run_bundle(
        precombine.as_ref(),
        &[("k".to_string(), 2i64).encode().expect("encode")],
    );
    assert_eq!(
        emitted
            .iter()
            .map(|bytes| <(String, i64)>::decode(bytes).expect("decode"))
            .collect::<Vec<_>>(),
        kvs(&[("k", 6)])
    );
    let convert = registered_combine_handler(
        &combine_fn.payload,
        CombineStage::ConvertToAccumulators.as_str(),
    )
    .expect("convert_to_accumulators");
    let emitted = run_bundle(
        convert.as_ref(),
        &[("k".to_string(), 2i64).encode().expect("encode")],
    );
    assert_eq!(
        emitted
            .iter()
            .map(|bytes| <(String, i64)>::decode(bytes).expect("decode"))
            .collect::<Vec<_>>(),
        kvs(&[("k", 6)])
    );
}

/// Passes elements through. Registered as a `DoFn`.
#[derive(Clone, Serialize, Deserialize, Register)]
struct PassThrough;

impl DoFn for PassThrough {
    type In = (String, i64);
    type Out = (String, i64);

    fn process_element(
        &mut self,
        element: (String, i64),
        ctx: &mut ProcessContext<'_, (String, i64)>,
    ) -> Result {
        ctx.emit(element)
    }
}

#[test]
fn a_spec_that_runs_a_type_as_the_wrong_kind_does_not_build() {
    let p = Pipeline::new();
    let input = p.apply("Create", Create::new(kvs(&[("a", 1)])));
    let _ = input.apply("PassThrough", ParDo::new(PassThrough));
    let _ = input.apply("Sum", CombinePerKey::new(ScaledSum { factor: 1 }));

    let combine_fn = combine_fn_spec(&p, "Sum");
    let error = beam::internals::registered_handler(&combine_fn.payload)
        .err()
        .expect("a CombineFn is not a DoFn");
    assert!(error.contains("registered as a CombineFn"), "{error}");

    let dofn = do_fn_spec(&p, "PassThrough");
    let error = registered_combine_handler(&dofn.payload, CombineStage::Precombine.as_str())
        .err()
        .expect("a DoFn is not a CombineFn");
    assert!(error.contains("registered as a DoFn"), "{error}");
}

#[test]
fn a_tuple_key_takes_the_graph_path() {
    let p = Pipeline::new();
    let pairs = vec![(("a".to_string(), "x".to_string()), 1i64)];
    let _ = p
        .apply("Create", Create::new(pairs))
        .apply("Sum", CombinePerKey::new(ScaledSum { factor: 1 }));
    assert_eq!(
        combine_fn_spec(&p, "Sum").urn,
        URN_RUST_COMBINE_FN_REGISTERED
    );
}

#[test]
fn test_combine_stage_conversions() {
    use beam::internals::{CombineStage, UnknownCombineStage};
    use std::str::FromStr;

    let cases = [
        (CombineStage::Precombine, "precombine"),
        (CombineStage::Merge, "merge"),
        (CombineStage::Extract, "extract"),
        (
            CombineStage::ConvertToAccumulators,
            "convert_to_accumulators",
        ),
        (CombineStage::CombineGroupedValues, "combine_grouped_values"),
    ];

    for (stage, name) in cases {
        assert_eq!(stage.as_ref(), name);
        assert_eq!(stage.to_string(), name);
        assert_eq!(CombineStage::from_str(name).unwrap(), stage);
        assert_eq!(CombineStage::try_from(name).unwrap(), stage);
    }

    assert_eq!(
        CombineStage::from_str("unknown"),
        Err(UnknownCombineStage("unknown".to_string()))
    );
    assert_eq!(
        CombineStage::try_from("invalid"),
        Err(UnknownCombineStage("invalid".to_string()))
    );
}

#[test]
fn test_sdf_stage_conversions() {
    use beam::pipeline::{CombineStage, SdfStage, UnknownSdfStage, stage_key};
    use std::str::FromStr;

    let cases = [
        (SdfStage::Pair, "sdf_pair"),
        (SdfStage::SplitAndSize, "sdf_split_and_size"),
        (SdfStage::Process, "sdf_process"),
    ];

    for (stage, name) in cases {
        assert_eq!(stage.as_ref(), name);
        assert_eq!(stage.to_string(), name);
        assert_eq!(SdfStage::from_str(name).unwrap(), stage);
        assert_eq!(SdfStage::try_from(name).unwrap(), stage);
        assert_eq!(stage_key("foo", stage), format!("foo/{name}"));
    }

    assert_eq!(
        SdfStage::from_str("unknown"),
        Err(UnknownSdfStage("unknown".to_string()))
    );
    assert_eq!(
        SdfStage::try_from("invalid"),
        Err(UnknownSdfStage("invalid".to_string()))
    );

    assert_eq!(stage_key("bar", CombineStage::Precombine), "bar/precombine");
    assert_eq!(stage_key("baz", "custom"), "baz/custom");
}

#[test]
fn combine_globally_is_a_composite_with_the_payload_of_its_combine() {
    let p = Pipeline::new();
    let _ = p
        .apply("Create", Create::new(vec![1i64, 2, 3]))
        .apply("Total", CombineGlobally::new(ScaledSum { factor: 2 }));

    let total = transform(&p, "Total");
    let spec = total.spec.expect("CombineGlobally has a spec");
    assert_eq!(spec.urn, URN_COMBINE_GLOBALLY);
    let inner = transform(&p, "Total/Combine")
        .spec
        .expect("CombinePerKey spec");
    assert_eq!(inner.urn, URN_COMBINE_PER_KEY);
    assert_eq!(spec.payload, inner.payload);
    // The key, combine and extract steps, and the default branch, are inside the composite.
    let names: Vec<String> = total
        .subtransforms
        .iter()
        .map(|id| transform(&p, id).unique_name)
        .collect();
    for step in ["Total/KeyWithVoid", "Total/Combine", "Total/ExtractValues"] {
        assert!(names.iter().any(|n| n == step), "{step} in {names:?}");
    }
}
