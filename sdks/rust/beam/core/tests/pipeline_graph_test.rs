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

//! Integration tests for Apache Beam Pipeline DAG construction and validation.
//!
//! Validates graph construction, coder deduplication, WindowingStrategy attachment,
//! Runner API protobuf serialization, and strict topological error detection.

mod support;

use std::collections::{BTreeMap, HashMap};

use beam::coders;
use beam::pipeline::{Pipeline, URN_FLATTEN, URN_GROUP_BY_KEY, URN_IMPULSE, URN_PAR_DO};
use beam::transforms::PTransform;
use beam::transforms::{Filter, FlatMap, GroupByKey, Inspect, Map};
use beam::values::{IsBounded, PCollection};
use testing::{TestPipeline, passert};

struct MapTransform<F> {
    name: &'static str,
    _func: F,
}

impl<F> MapTransform<F> {
    fn new(name: &'static str, func: F) -> Self {
        Self { name, _func: func }
    }
}

impl<In: 'static, Out: 'static, F> PTransform<PCollection<In>> for MapTransform<F>
where
    F: Fn(In) -> Out + Send + Sync + 'static,
{
    type Output = PCollection<Out>;

    fn expand(&self, input: &PCollection<In>) -> PCollection<Out> {
        let pipeline = input.pipeline();
        let out_coder_id = pipeline.register_coder(coders::URN_STRING_UTF8, Vec::new());
        let out_pcoll =
            pipeline.add_pcollection::<Out>("map_out", &out_coder_id, IsBounded::Bounded);

        let mut inputs = HashMap::new();
        inputs.insert("in".to_string(), input.id().to_string());

        let mut outputs = HashMap::new();
        outputs.insert("out".to_string(), out_pcoll.id().to_string());

        pipeline.add_transform(self.name, URN_PAR_DO, Vec::new(), inputs, outputs);

        out_pcoll
    }
}

#[test]
fn test_pipeline_graph_construction_and_proto() {
    let p = Pipeline::new();

    let impulse_out = p.impulse();
    assert_eq!(p.root_transform_ids().len(), 1);

    let mapped = impulse_out.apply(MapTransform::new("MapToWord", |_b: Vec<u8>| {
        "hello".to_string()
    }));

    p.validate().expect("Pipeline should be valid");

    let pipeline_proto = p.to_proto();
    let components = pipeline_proto.components.expect("Components must exist");

    // Transforms: Impulse + Map.
    assert_eq!(components.transforms.len(), 2);
    // PCollections: impulse_out + map_out.
    assert_eq!(components.pcollections.len(), 2);
    // Coders: GlobalWindow + Bytes + String.
    assert_eq!(components.coders.len(), 3);
    assert_eq!(components.environments.len(), 1);
    assert_eq!(components.windowing_strategies.len(), 1);
    // Root transforms are the top-level transforms in shallow topological order: Impulse, Map.
    assert_eq!(pipeline_proto.root_transform_ids.len(), 2);

    // Wiring: Impulse -> MapToWord, with the coders and windowing each side declares.
    let proto = &p.to_proto();
    let impulse_name = support::producer(proto, impulse_out.id());
    assert_eq!(
        support::urn(support::transform(proto, &impulse_name)),
        URN_IMPULSE
    );
    assert_eq!(
        support::input_producers(proto, "MapToWord"),
        BTreeMap::from([("in".to_string(), impulse_name)])
    );
    assert_eq!(support::single_output(proto, "MapToWord"), mapped.id());
    assert_eq!(support::pcoll_coder(proto, impulse_out.id()), "bytes");
    assert_eq!(support::pcoll_coder(proto, mapped.id()), "string_utf8");
    for pcoll in [impulse_out.id(), mapped.id()] {
        assert_eq!(
            support::window_fn_urn(proto, pcoll),
            "beam:window_fn:global_windows:v1"
        );
    }
}

#[test]
fn test_coder_deduplication() {
    let p = Pipeline::new();

    let id1 = p.register_coder(coders::URN_BYTES, Vec::new());
    let id2 = p.register_coder(coders::URN_BYTES, Vec::new());
    assert_eq!(
        id1, id2,
        "Registering identical coder must return the same ID"
    );

    let id3 = p.register_coder(coders::URN_STRING_UTF8, Vec::new());
    assert_ne!(id1, id3, "Different coders must have distinct IDs");
}

#[test]
fn test_higher_order_collection_combinators() {
    let p = Pipeline::new();

    // Chain: impulse -> flat_map -> filter -> inspect -> key_by -> group_by_key -> map
    let impulse = p.impulse();
    let words = impulse.apply(FlatMap::new("ExtractWords", |_bytes: Vec<u8>| {
        vec![
            "to".to_string(),
            "be".to_string(),
            "or".to_string(),
            "not".to_string(),
            "to".to_string(),
            "be".to_string(),
        ]
    }));
    let filtered = words.apply(Filter::new("FilterWords", |word: &String| word.len() > 1));
    let inspected = filtered.apply(Inspect::new("LogWord", |_word: &String| {}));
    let keyed = inspected.apply(Map::new("KeyByWord", |word: String| (word.clone(), word)));
    let paired = keyed.apply(Map::new("PairWithOne", |(word, _): (String, String)| {
        (word, 1i64)
    }));
    let grouped = paired.apply(GroupByKey::new("GroupWords"));
    let word_counts = grouped.apply(Map::new(
        "FormatCount",
        |(word, counts): (String, coders::BeamIterable<i64>)| {
            let total: i64 = counts.into_iter().sum();
            format!("{word}: {total}")
        },
    ));

    // Validate complete graph structure
    p.validate()
        .expect("Higher-order DAG must be topologically valid");

    let proto = p.to_proto();
    let components = proto.components.expect("Components must exist");

    // Transforms: Impulse + ExtractWords + FilterWords + LogWord + KeyByWord + PairWithOne + GroupWords + FormatCount = 8
    assert_eq!(components.transforms.len(), 8);
    // Root transforms: all 8 top-level transforms in shallow topological order
    assert_eq!(proto.root_transform_ids.len(), 8);

    // Each step consumes exactly the previous step's output.
    let proto = p.to_proto();
    let chain = [
        "ExtractWords",
        "FilterWords",
        "LogWord",
        "KeyByWord",
        "PairWithOne",
        "GroupWords",
        "FormatCount",
    ];
    assert_eq!(
        support::input_producer_names(&proto, chain[0]),
        [support::producer(&proto, impulse.id())]
    );
    for pair in chain.windows(2) {
        assert_eq!(
            support::input_producer_names(&proto, pair[1]),
            [pair[0]],
            "input of {}",
            pair[1]
        );
    }
    assert_eq!(
        support::urn(support::transform(&proto, "GroupWords")),
        URN_GROUP_BY_KEY
    );
    assert_eq!(
        support::output_coder(&proto, "PairWithOne"),
        "kv(string_utf8,varint)"
    );
    assert_eq!(
        support::output_coder(&proto, "GroupWords"),
        "kv(string_utf8,iterable(varint))"
    );
    assert_eq!(
        support::single_output(&proto, "FormatCount"),
        word_counts.id()
    );
    assert_eq!(
        support::pcoll_coder(&proto, word_counts.id()),
        "string_utf8"
    );
}

#[tokio::test]
async fn higher_order_combinators_compute_word_counts() {
    use beam::prelude::Create;

    let p = TestPipeline::new();
    let words = p
        .apply(Create::new(
            "Lines",
            vec!["to be or not to be".to_string(), "a b".to_string()],
        ))
        .apply(FlatMap::new("ExtractWords", |line: String| {
            line.split_whitespace()
                .map(str::to_string)
                .collect::<Vec<_>>()
        }));
    let filtered = words.apply(Filter::new("FilterWords", |word: &String| word.len() > 1));
    let counts = filtered
        .apply(Map::new("PairWithOne", |word: String| (word, 1i64)))
        .apply(GroupByKey::new("GroupWords"))
        .apply(Map::new(
            "FormatCount",
            |(word, counts): (String, coders::BeamIterable<i64>)| {
                let total: i64 = counts.into_iter().sum();
                format!("{word}: {total}")
            },
        ));

    passert::that("Filtered", &filtered).has_count(6);
    passert::that("Counts", &counts)
        .contains_in_any_order(["to: 2", "be: 2", "or: 1", "not: 1"].map(String::from));
    p.run().await.expect("pipeline and assertions");
}

#[test]
fn test_ext_par_do_reshuffle_and_list_flatten() {
    use beam::prelude::*;
    use beam::transforms::ProcessContext;

    #[derive(Clone)]
    struct DoubleFn;
    impl DoFn for DoubleFn {
        type In = i64;
        type Out = i64;
        fn process_element(
            &mut self,
            element: Self::In,
            output: &mut ProcessContext<Self::Out>,
        ) -> Result {
            output.emit(element * 2)?;
            Ok(())
        }
    }

    let p = Pipeline::new();
    let numbers = p.apply(Create::new("CreateNums", vec![1i64, 2, 3]));
    let doubled = numbers.apply(ParDo::new("Double", DoubleFn));
    let reshuffled = doubled.apply(Reshuffle::new("Reshuffle"));
    let partitions = reshuffled.apply(Partition::new("SplitEvenOdd", 2, |n: &i64| {
        (*n % 2) as usize
    }));
    let partitions_ids: Vec<String> = partitions
        .collections()
        .iter()
        .map(|c| c.id().to_string())
        .collect();
    let rejoined = partitions.apply(Flatten::new("MergeBack"));

    p.validate().expect("Pipeline must be topologically valid");

    let proto = p.to_proto();
    assert_eq!(
        support::input_producer_names(&proto, "Double"),
        ["CreateNums/Process"]
    );
    // Reshuffle is a composite whose input is Double's output and whose output feeds
    // the partition.
    let reshuffle = support::transform(&proto, "Reshuffle");
    assert!(!reshuffle.subtransforms.is_empty());
    assert_eq!(
        reshuffle.inputs.values().collect::<Vec<_>>(),
        [&doubled.id().to_string()]
    );
    assert_eq!(
        reshuffle.outputs.values().collect::<Vec<_>>(),
        [&reshuffled.id().to_string()]
    );
    assert_eq!(
        support::transform(&proto, "SplitEvenOdd")
            .inputs
            .values()
            .collect::<Vec<_>>(),
        [&reshuffled.id().to_string()]
    );
    // Flatten consumes exactly the two partitions.
    let merge = support::transform(&proto, "MergeBack");
    assert_eq!(support::urn(merge), URN_FLATTEN);
    let mut inputs: Vec<_> = merge.inputs.values().cloned().collect();
    inputs.sort();
    let mut expected = vec![partitions_ids[0].clone(), partitions_ids[1].clone()];
    expected.sort();
    assert_eq!(inputs, expected);
    assert_eq!(support::single_output(&proto, "MergeBack"), rejoined.id());
}

#[tokio::test]
async fn par_do_reshuffle_partition_flatten_preserves_elements() {
    use beam::prelude::*;

    let p = TestPipeline::new();
    let rejoined = p
        .apply(Create::new("CreateNums", vec![1i64, 2, 3]))
        .apply(Map::new("Double", |n: i64| n * 2))
        .apply(Reshuffle::new("Reshuffle"))
        .apply(Partition::new("SplitByThree", 2, |n: &i64| {
            usize::from(*n > 3)
        }))
        .apply(Flatten::new("MergeBack"));

    passert::that("AssertRejoined", &rejoined).contains_in_any_order([2i64, 4, 6]);
    p.run().await.expect("pipeline and assertions");
}

#[test]
fn test_create_root_transform() {
    use beam::transforms::Create;

    let p = Pipeline::new();

    // Create is rooted at PBegin, so it is applied to the pipeline itself.
    let lines = p.apply(Create::new(
        "CreateLines",
        vec!["alpha".to_string(), "beta".to_string()],
    ));
    let upper = lines.apply(Map::new("Upper", |s: String| s.to_uppercase()));
    p.validate()
        .expect("Create DAG must be topologically valid");

    let proto = p.to_proto();
    let components = proto.components.as_ref().expect("Components must exist");
    // Transforms: CreateLines (composite) + CreateLines/Impulse + CreateLines/Process + Upper = 4
    assert_eq!(components.transforms.len(), 4);
    // Root transforms: only top-level transforms in shallow topological order (CreateLines, Upper) = 2
    assert_eq!(proto.root_transform_ids.len(), 2);

    // Verify CreateLines is a composite containing both subtransforms
    let create_composite = components
        .transforms
        .get(&proto.root_transform_ids[0])
        .expect("Create composite must exist");
    assert_eq!(create_composite.subtransforms.len(), 2);
    assert_eq!(
        support::input_producer_names(&proto, "Upper"),
        ["CreateLines/Process"]
    );
    assert_eq!(support::single_output(&proto, "Upper"), upper.id());
}

#[test]
fn test_default_coder_registration_and_roundtrip() {
    use beam::coders::DefaultCoder;

    let p = Pipeline::new();
    let str_cid = String::register_coder(&p);
    let kv_cid = <(String, i64)>::register_coder(&p);
    let grouped_cid = <(String, Vec<i64>)>::register_coder(&p);
    // Registration is idempotent.
    assert_eq!(<(String, i64)>::register_coder(&p), kv_cid);

    let proto = p.to_proto();
    assert_eq!(support::coder_shape(&proto, &str_cid), "string_utf8");
    assert_eq!(
        support::coder_shape(&proto, &kv_cid),
        "kv(string_utf8,varint)"
    );
    assert_eq!(
        support::coder_shape(&proto, &grouped_cid),
        "kv(string_utf8,iterable(varint))"
    );
    // The KV coder references the very same component coder id.
    assert_eq!(
        support::components(&proto).coders[&kv_cid].component_coder_ids[0],
        str_cid
    );
}

#[test]
fn default_coders_register_under_their_standard_urns() {
    use beam::coders::{DefaultCoder, LengthPrefixed};
    use beam::schema::Row;

    let p = Pipeline::new();
    let cases = [
        (bool::register_coder(&p), "bool"),
        (f64::register_coder(&p), "double"),
        (Row::register_coder(&p), "row"),
        (
            LengthPrefixed::<i64>::register_coder(&p),
            "length_prefix(varint)",
        ),
    ];
    let proto = p.to_proto();
    for (id, shape) in &cases {
        assert_eq!(support::coder_shape(&proto, id), *shape);
    }
    // Registering again hands back the same ids.
    assert_eq!(bool::register_coder(&p), cases[0].0);
    assert_eq!(LengthPrefixed::<i64>::register_coder(&p), cases[3].0);
}

/// A registry that only implements `register_coder`, numbering what it is given.
#[derive(Default)]
struct RecordingRegistry {
    registered: std::cell::RefCell<Vec<(String, Vec<String>)>>,
}

impl coders::CoderRegistry for RecordingRegistry {
    fn register_coder(&self, urn: &str, component_coder_ids: Vec<String>) -> String {
        let mut registered = self.registered.borrow_mut();
        registered.push((urn.to_string(), component_coder_ids));
        format!("c{}", registered.len())
    }
}

#[test]
fn registries_hand_back_the_id_of_what_they_registered() {
    use coders::CoderRegistry;

    // The default `register_coder_with_payload` falls back to `register_coder`.
    let registry = RecordingRegistry::default();
    assert_eq!(
        registry.register_coder_with_payload("urn:a", vec!["x".to_string()], vec![1]),
        "c1"
    );

    // A reference to a registry forwards both methods to it.
    let by_ref = &registry;
    assert_eq!(
        CoderRegistry::register_coder(&by_ref, "urn:b", Vec::new()),
        "c2"
    );
    assert_eq!(
        CoderRegistry::register_coder_with_payload(&by_ref, "urn:c", Vec::new(), vec![2]),
        "c3"
    );
    assert_eq!(
        *registry.registered.borrow(),
        [
            ("urn:a".to_string(), vec!["x".to_string()]),
            ("urn:b".to_string(), Vec::new()),
            ("urn:c".to_string(), Vec::new()),
        ]
    );
}

#[test]
fn test_set_docker_environment() {
    use beam::pipeline::{URN_ENV_DEFAULT, URN_ENV_DOCKER};
    use model::pipeline as proto;
    use prost::Message;

    let p = Pipeline::new();
    let initial_env = p.default_environment();
    assert_eq!(initial_env.urn, URN_ENV_DEFAULT);

    p.set_docker_environment("apache/beam_rust_sdk:latest");

    let docker_env = p.default_environment();
    assert_eq!(docker_env.urn, URN_ENV_DOCKER);

    let payload = proto::DockerPayload::decode(docker_env.payload.as_slice())
        .expect("payload must decode to DockerPayload");
    assert_eq!(payload.container_image, "apache/beam_rust_sdk:latest");

    // Verify in exported proto pipeline
    let proto_pipeline = p.to_proto();
    let components = proto_pipeline.components.unwrap();
    let exported_env = components
        .environments
        .values()
        .find(|e| e.urn == URN_ENV_DOCKER)
        .expect("Docker environment must be exported in pipeline proto");
    let decoded = proto::DockerPayload::decode(exported_env.payload.as_slice()).unwrap();
    assert_eq!(decoded.container_image, "apache/beam_rust_sdk:latest");
}

#[test]
fn test_create_leaves_environment_choice_to_the_runner() {
    use beam::options::PipelineOptions;
    use beam::pipeline::URN_ENV_DEFAULT;

    let options = PipelineOptions::parse_from([
        "app",
        "--sdk_container_image=custom.registry.io/rust-worker:v2",
    ]);

    // Whether the image applies depends on the runner and environment type, so the
    // runner resolves it at run time rather than at construction.
    let p = Pipeline::create(&options);
    let env = p.default_environment();
    assert_eq!(env.urn, URN_ENV_DEFAULT);
    assert!(env.payload.is_empty());
}

#[test]
fn test_create_applies_expansion_mode_and_resource_hints() {
    use beam::options::PipelineOptions;
    use beam::pipeline::ExpansionMode;

    let options = PipelineOptions::parse_from(["app", "--resource_hints=min_ram_bytes=4GB"]);
    let p_create = Pipeline::create(&options);

    assert_eq!(p_create.expansion_mode(), ExpansionMode::Remote);
    assert_eq!(
        p_create.resource_hints().min_ram_bytes(),
        Some(4_000_000_000)
    );
}

#[test]
fn test_set_docker_environment_preserves_foreign_environments() {
    use beam::pipeline::URN_ENV_DOCKER;
    use model::pipeline as proto;
    use prost::Message;

    let p = Pipeline::new();

    // Insert a foreign Java environment, as a cross-language expansion does.
    let java_payload = proto::DockerPayload {
        container_image: "apache/beam_java21_sdk:latest".to_string(),
    }
    .encode_to_vec();

    let java_env = proto::Environment {
        urn: URN_ENV_DOCKER.to_string(),
        payload: java_payload,
        capabilities: vec![
            "beam:protocol:progress_reporting:v1".to_string(),
            "beam:coder:javasdk:v1".to_string(),
        ],
        display_data: Vec::new(),
        resource_hints: HashMap::new(),
        dependencies: Vec::new(),
    };

    let java_env_id = "env_java_bigquery".to_string();
    p.lock()
        .components
        .environments
        .insert(java_env_id.clone(), java_env);

    // Override the default environment with a custom image.
    p.set_docker_environment("custom.registry.io/my-rust-worker:v1");

    let proto_pipeline = p.to_proto();
    let components = proto_pipeline.components.unwrap();

    // Verify default environment was updated.
    let rust_env = &components.environments[&p.default_environment_id()];
    assert_eq!(rust_env.urn, URN_ENV_DOCKER);
    let rust_payload = proto::DockerPayload::decode(rust_env.payload.as_slice()).unwrap();
    assert_eq!(
        rust_payload.container_image,
        "custom.registry.io/my-rust-worker:v1"
    );

    // Verify foreign environment was preserved untouched.
    let preserved_java_env = &components.environments[&java_env_id];
    assert_eq!(preserved_java_env.urn, URN_ENV_DOCKER);
    let java_decoded = proto::DockerPayload::decode(preserved_java_env.payload.as_slice()).unwrap();
    assert_eq!(
        java_decoded.container_image,
        "apache/beam_java21_sdk:latest"
    );
    assert_eq!(
        preserved_java_env.capabilities,
        vec![
            "beam:protocol:progress_reporting:v1".to_string(),
            "beam:coder:javasdk:v1".to_string(),
        ]
    );
}

#[test]
fn test_shallow_topological_root_ordering_with_composites() {
    use beam::pipeline::is_runner_primitive_urn;

    assert!(is_runner_primitive_urn(URN_IMPULSE));
    assert!(is_runner_primitive_urn(URN_GROUP_BY_KEY));
    assert!(!is_runner_primitive_urn(URN_FLATTEN));
    assert!(!is_runner_primitive_urn(URN_PAR_DO));

    let p = Pipeline::new();

    // Runner primitive Impulse has empty environment_id.
    let impulse_out = p.impulse();

    // Map is a primitive user transform, so its environment_id is the default.
    let mapped = impulse_out.apply(MapTransform::new("MapStep", |bytes: Vec<u8>| bytes));

    let bytes_coder_id = p.register_coder(coders::URN_BYTES, Vec::new());

    // Composite transform containing two subtransforms.
    let sub1_pcoll = p.add_pcollection::<Vec<u8>>("sub1_out", &bytes_coder_id, IsBounded::Bounded);
    let sub1_id = p.add_transform(
        "CompositeSub1",
        URN_PAR_DO,
        Vec::new(),
        HashMap::from([("in".to_string(), mapped.id().to_string())]),
        HashMap::from([("out".to_string(), sub1_pcoll.id().to_string())]),
    );

    let sub2_pcoll = p.add_pcollection::<Vec<u8>>("sub2_out", &bytes_coder_id, IsBounded::Bounded);
    let sub2_id = p.add_transform(
        "CompositeSub2",
        URN_PAR_DO,
        Vec::new(),
        HashMap::from([("in".to_string(), sub1_pcoll.id().to_string())]),
        HashMap::from([("out".to_string(), sub2_pcoll.id().to_string())]),
    );

    let composite_id = p.add_composite_transform(
        "CompositeStep",
        None,
        Vec::new(),
        HashMap::from([("in".to_string(), mapped.id().to_string())]),
        HashMap::from([("out".to_string(), sub2_pcoll.id().to_string())]),
        vec![sub1_id.clone(), sub2_id.clone()],
    );

    let downstream_pcoll =
        p.add_pcollection::<Vec<u8>>("downstream_out", &bytes_coder_id, IsBounded::Bounded);
    let downstream_id = p.add_transform(
        "DownstreamStep",
        URN_PAR_DO,
        Vec::new(),
        HashMap::from([("in".to_string(), sub2_pcoll.id().to_string())]),
        HashMap::from([("out".to_string(), downstream_pcoll.id().to_string())]),
    );

    p.validate().expect("Pipeline DAG must be valid");

    let proto = p.to_proto();
    let components = proto.components.expect("Components must exist");

    assert_eq!(components.transforms.len(), 6);

    // Runner primitive Impulse has empty environment_id
    let impulse_t = components
        .transforms
        .values()
        .find(|t| t.unique_name.starts_with("Impulse"))
        .expect("Impulse transform must exist");
    assert!(
        impulse_t.environment_id.is_empty(),
        "Impulse environment_id must be empty"
    );

    // User ParDo has default environment_id
    let map_t = components
        .transforms
        .values()
        .find(|t| t.unique_name.starts_with("MapStep"))
        .expect("MapStep transform must exist");
    assert_eq!(map_t.environment_id, "env_default");

    // Composite transform has empty environment_id
    let comp_t = components
        .transforms
        .get(&composite_id)
        .expect("Composite transform must exist");
    assert!(
        comp_t.environment_id.is_empty(),
        "Composite environment_id must be empty"
    );

    // root_transform_ids must contain exactly the 4 top-level transforms:
    // [Impulse, MapStep, CompositeStep, DownstreamStep]
    // Sub1 and Sub2 MUST NOT be present in root_transform_ids.
    let roots = proto.root_transform_ids;
    assert_eq!(roots.len(), 4);
    assert!(!roots.contains(&sub1_id));
    assert!(!roots.contains(&sub2_id));

    // Must be in shallow topological order:
    // Impulse -> MapStep -> CompositeStep -> DownstreamStep
    assert_eq!(roots[0], impulse_t.unique_name);
    assert_eq!(roots[1], map_t.unique_name);
    assert_eq!(roots[2], composite_id);
    assert_eq!(roots[3], downstream_id);
}
