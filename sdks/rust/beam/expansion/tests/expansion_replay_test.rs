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

//! Tests for expanded specs: the server puts a replay entry in each Rust spec that an
//! expansion creates, and a worker resolves every such spec through the replay.

use std::collections::HashMap;
use std::sync::Arc;

use beam::pipeline::{
    Pipeline, URN_COMBINE_PER_KEY, URN_COMBINE_PER_KEY_EXTRACT_OUTPUTS,
    URN_COMBINE_PER_KEY_MERGE_ACCUMULATORS, URN_COMBINE_PER_KEY_PRECOMBINE, URN_PAR_DO,
    URN_SDF_PAIR_WITH_RESTRICTION, URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS,
    URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS,
};
use beam::schema::{FieldType, FieldValue, Row, Schema};
use beam::transforms::{CombinePerKey, Create, Sum};
use expansion::{ExpansionServiceServer, PCollectionId, SchemaTransformProvider};
use harness::bundle_processor::lookup_handler;
use harness::replay::{ExpandedSpec, ReplayEntry, ReplayRegistration, URN_RUST_DOFN_EXPANDED};
use model::expansion::expansion_service_server::ExpansionService;
use model::expansion::{ExpansionRequest, ExpansionResponse};
use model::pipeline as proto;
use prost::Message;
use tonic::Request;

const GENERATE_SEQUENCE_URN: &str = "beam:schematransform:org.apache.beam:generate_sequence:v1";
const SUM_URN: &str = "beam:schematransform:test:replay_sum:v1";

/// Provider that sums one keyed value, so its expansion has a `CombinePayload`. It reads
/// no input, but declares one, so requests can send input components.
#[derive(Default)]
struct SumProvider;

impl SchemaTransformProvider for SumProvider {
    fn identifier(&self) -> &'static str {
        SUM_URN
    }
    fn description(&self) -> &'static str {
        "sums one keyed value"
    }
    fn config_schema(&self) -> Schema {
        Schema::builder().field("value", FieldType::int64()).build()
    }
    fn build_transform(
        &self,
        config: Row,
        _inputs: HashMap<String, PCollectionId>,
        pipeline: &mut Pipeline,
    ) -> Result<HashMap<String, PCollectionId>, String> {
        let value = config.get_i64("value").ok().flatten().unwrap_or(0);
        let sums = pipeline
            .apply(Create::new("Create", vec![("k".to_string(), value)]))
            .apply(CombinePerKey::new("Sum", Sum));
        Ok(HashMap::from([(
            "output".to_string(),
            sums.id().to_string(),
        )]))
    }
}
expansion::register_schema_transform!(SumProvider);

fn config_payload(schema: Schema, values: Vec<i64>) -> Vec<u8> {
    let schema = Arc::new(schema);
    let row = Row::new(
        Arc::clone(&schema),
        values
            .into_iter()
            .map(|v| Some(FieldValue::Int64(v)))
            .collect(),
    )
    .expect("row matches schema");
    proto::ExternalConfigurationPayload {
        schema: Some((*schema).clone().into()),
        payload: row.to_row_bytes().expect("row encodes"),
    }
    .encode_to_vec()
}

fn generate_sequence(start: i64, stop: i64) -> proto::PTransform {
    let schema = Schema::builder()
        .field("start", FieldType::int64())
        .field("stop", FieldType::int64())
        .build();
    transform(
        GENERATE_SEQUENCE_URN,
        config_payload(schema, vec![start, stop]),
    )
}

fn sum(value: i64) -> proto::PTransform {
    let schema = Schema::builder().field("value", FieldType::int64()).build();
    transform(SUM_URN, config_payload(schema, vec![value]))
}

fn transform(urn: &str, payload: Vec<u8>) -> proto::PTransform {
    proto::PTransform {
        unique_name: "Caller".to_string(),
        spec: Some(proto::FunctionSpec {
            urn: urn.to_string(),
            payload,
        }),
        ..Default::default()
    }
}

async fn expand(
    transform: proto::PTransform,
    components: Option<proto::Components>,
    namespace: &str,
) -> proto::Components {
    let resp: ExpansionResponse = ExpansionServiceServer::new()
        .expand(Request::new(ExpansionRequest {
            transform: Some(transform),
            components,
            namespace: namespace.to_string(),
            ..Default::default()
        }))
        .await
        .expect("expand returns errors in the response")
        .into_inner();
    assert_eq!(resp.error, "");
    resp.components.expect("components")
}

/// Every Rust handler-key spec in `components`, as `(transform URN, expanded spec)`.
fn expanded_specs(components: &proto::Components) -> Vec<(String, ExpandedSpec)> {
    components
        .transforms
        .values()
        .filter_map(|t| t.spec.as_ref())
        .filter_map(|spec| {
            if spec.urn != URN_PAR_DO {
                return None;
            }
            let key_spec = proto::ParDoPayload::decode(spec.payload.as_slice())
                .ok()?
                .do_fn?;
            assert_eq!(key_spec.urn, URN_RUST_DOFN_EXPANDED);
            let expanded = ExpandedSpec::decode(key_spec.payload.as_slice()).expect("decodes");
            Some((spec.urn.clone(), expanded))
        })
        .collect()
}

fn replay_entry(expanded: &ExpandedSpec) -> &ReplayEntry {
    expanded.replay.as_ref().expect("replay entry")
}

/// Decodes the config row of a replay entry with the schema that the entry holds.
fn config_value(entry: &ReplayEntry, field: &str) -> Option<i64> {
    let schema = Schema::try_from(entry.config_schema.clone().expect("config schema"))
        .expect("schema converts");
    Row::from_row_bytes(&Arc::new(schema), &entry.config_row)
        .expect("row decodes")
        .get_i64(field)
        .expect("field exists")
}

#[tokio::test]
async fn expanded_specs_carry_the_handler_key_and_replay_entry() {
    let components = expand(generate_sequence(3, 7), None, "ns_gs/").await;
    let specs = expanded_specs(&components);
    assert!(!specs.is_empty(), "no Rust spec in the expansion");
    for (_, expanded) in &specs {
        assert!(!expanded.handler_key.is_empty());
        let entry = replay_entry(expanded);
        assert_eq!(entry.provider, GENERATE_SEQUENCE_URN);
        assert_eq!(entry.namespace, "ns_gs/");
        assert!(entry.inputs.is_empty());
        assert_eq!(config_value(entry, "start"), Some(3));
        assert_eq!(config_value(entry, "stop"), Some(7));
    }
}

#[tokio::test]
async fn combine_stages_get_expanded_specs() {
    let components = expand(sum(5), None, "ns_sum/").await;
    let stages = expanded_specs(&components);
    assert!(stages.len() >= 2, "{stages:?}");
    for (_, expanded) in &stages {
        let entry = replay_entry(expanded);
        assert_eq!(entry.provider, SUM_URN);
        assert_eq!(config_value(entry, "value"), Some(5));
    }
}

#[tokio::test]
async fn two_expansions_of_one_provider_have_different_replay_entries() {
    let first = expand(generate_sequence(0, 5), None, "ns_a/").await;
    let second = expand(generate_sequence(10, 20), None, "ns_b/").await;
    let entries = |c: &proto::Components| -> Vec<(String, Option<i64>)> {
        expanded_specs(c)
            .iter()
            .map(|(_, e)| replay_entry(e))
            .map(|entry| (entry.namespace.clone(), config_value(entry, "start")))
            .collect()
    };
    let first = entries(&first);
    let second = entries(&second);
    assert!(first.iter().all(|e| *e == ("ns_a/".to_string(), Some(0))));
    assert!(second.iter().all(|e| *e == ("ns_b/".to_string(), Some(10))));
}

#[tokio::test]
async fn each_expansion_gets_its_own_id_even_with_the_same_namespace() {
    let ids = |c: &proto::Components| -> Vec<String> {
        expanded_specs(c)
            .iter()
            .map(|(_, e)| replay_entry(e).expansion_id.clone())
            .collect()
    };
    let mut seen = Vec::new();
    for namespace in ["", "", "External_0", "External_0"] {
        let got = ids(&expand(generate_sequence(0, 5), None, namespace).await);
        assert!(!got.is_empty());
        assert!(got.iter().all(|id| !id.is_empty() && *id == got[0]));
        assert!(!seen.contains(&got[0]), "expansion id {} repeats", got[0]);
        seen.push(got[0].clone());
    }
}

#[tokio::test]
async fn only_sdk_transforms_get_the_rust_environment() {
    let components = expand(generate_sequence(0, 5), None, "ns_env/").await;
    let env_of = |urn: &str| -> Vec<String> {
        components
            .transforms
            .values()
            .filter(|t| t.spec.as_ref().is_some_and(|s| s.urn == urn))
            .map(|t| t.environment_id.clone())
            .collect()
    };
    let impulses = env_of("beam:transform:impulse:v1");
    assert!(!impulses.is_empty());
    assert!(impulses.iter().all(String::is_empty), "{impulses:?}");
    let pardos = env_of(URN_PAR_DO);
    assert!(!pardos.is_empty());
    assert!(
        pardos
            .iter()
            .all(|env| components.environments.contains_key(env)),
        "{pardos:?}"
    );
}

#[tokio::test]
async fn combine_composites_carry_no_spec() {
    // Dataflow rejects a combine composite whose subtransforms are not `GroupByKey` and
    // `CombineValues`.
    let components = expand(sum(1), None, "ns_spec_combine/").await;
    assert!(
        components
            .transforms
            .values()
            .all(|t| t.spec.as_ref().is_none_or(|s| s.urn != URN_COMBINE_PER_KEY)),
        "a combine composite kept its spec"
    );
    let composites: Vec<_> = components
        .transforms
        .values()
        .filter(|t| t.spec.is_none() && !t.subtransforms.is_empty())
        .collect();
    assert!(!composites.is_empty());
}

#[tokio::test]
async fn coder_ids_in_payloads_name_returned_coders() {
    let namespace = "ns_coders_gs/";
    let components = expand(generate_sequence(0, 5), None, namespace).await;
    let named: Vec<String> = components
        .transforms
        .values()
        .filter_map(|t| t.spec.as_ref())
        .filter(|spec| spec.urn == URN_PAR_DO)
        .map(|spec| {
            proto::ParDoPayload::decode(spec.payload.as_slice())
                .expect("decodes")
                .restriction_coder_id
        })
        .filter(|id| !id.is_empty())
        .collect();
    assert!(!named.is_empty(), "no payload coder id in {namespace}");
    for id in named {
        assert!(id.starts_with(namespace), "{id} has no namespace");
        assert!(
            components.coders.contains_key(&id),
            "{id} is not a returned coder"
        );
    }
}

#[tokio::test]
async fn replay_entry_holds_the_input_components_only() {
    let caller = proto::Components {
        pcollections: HashMap::from([
            (
                "in".to_string(),
                proto::PCollection {
                    coder_id: "kv".to_string(),
                    windowing_strategy_id: "ws".to_string(),
                    ..Default::default()
                },
            ),
            ("other".to_string(), proto::PCollection::default()),
        ]),
        coders: HashMap::from([
            ("kv".to_string(), coder(&["bytes", "bytes"])),
            ("bytes".to_string(), coder(&[])),
            ("window".to_string(), coder(&[])),
            ("unused".to_string(), coder(&[])),
        ]),
        windowing_strategies: HashMap::from([(
            "ws".to_string(),
            proto::WindowingStrategy {
                window_coder_id: "window".to_string(),
                environment_id: "caller_env".to_string(),
                ..Default::default()
            },
        )]),
        environments: HashMap::from([("caller_env".to_string(), proto::Environment::default())]),
        ..Default::default()
    };
    let mut request = sum(1);
    request.inputs.insert("input".to_string(), "in".to_string());

    let components = expand(request, Some(caller), "ns_in/").await;
    let specs = expanded_specs(&components);
    let entry = replay_entry(&specs[0].1);
    assert_eq!(
        entry.inputs,
        HashMap::from([("input".to_string(), "in".to_string())])
    );
    let seed = entry.components.as_ref().expect("components");
    let mut coders: Vec<_> = seed.coders.keys().map(String::as_str).collect();
    coders.sort_unstable();
    assert_eq!(coders, ["bytes", "kv", "window"]);
    assert_eq!(seed.pcollections.keys().collect::<Vec<_>>(), ["in"]);
    assert_eq!(seed.windowing_strategies.keys().collect::<Vec<_>>(), ["ws"]);
    assert!(seed.environments.is_empty());
    // Caller components keep their ids and their references.
    assert_eq!(
        components.windowing_strategies["ws"].environment_id,
        "caller_env"
    );
}

fn coder(components: &[&str]) -> proto::Coder {
    proto::Coder {
        spec: Some(proto::FunctionSpec {
            urn: "beam:coder:bytes:v1".to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: components.iter().map(|c| c.to_string()).collect(),
    }
}

#[tokio::test]
async fn replay_entry_rebuilds_every_handler_key() {
    let replay = inventory::iter::<ReplayRegistration>
        .into_iter()
        .next()
        .expect("the expansion crate registers a replay")
        .replay;
    for components in [
        expand(generate_sequence(1, 4), None, "ns_rt_gs/").await,
        expand(sum(2), None, "ns_rt_sum/").await,
    ] {
        for (urn, expanded) in expanded_specs(&components) {
            let handlers = replay(replay_entry(&expanded)).expect("replay builds");
            let key = match urn.as_str() {
                URN_COMBINE_PER_KEY => beam::pipeline::combine_stage_key(
                    &expanded.handler_key,
                    beam::pipeline::COMBINE_STAGE_MERGE,
                ),
                _ => expanded.handler_key.clone(),
            };
            assert!(handlers.contains_key(&key), "replay has no handler '{key}'");
        }
    }
}

/// Copies `transform` under a runner stage URN, with the same payload.
fn stage(transform: &proto::PTransform, urn: &str) -> proto::PTransform {
    let mut stage = transform.clone();
    if let Some(spec) = stage.spec.as_mut() {
        spec.urn = urn.to_string();
    }
    stage
}

#[tokio::test]
async fn worker_resolves_every_sdk_transform_of_an_expansion() {
    // A worker has no pipeline handlers. Each ParDo, splittable stage and lifted combine
    // stage that a runner sends must resolve through the replay entry alone.
    let no_handlers = HashMap::new();
    for components in [
        expand(generate_sequence(0, 3), None, "ns_e2e_gs/").await,
        expand(sum(4), None, "ns_e2e_sum/").await,
    ] {
        let mut checked = 0;
        for (id, t) in &components.transforms {
            let Some(spec) = t.spec.as_ref() else {
                continue;
            };
            let urns: Vec<&str> = match spec.urn.as_str() {
                URN_PAR_DO if t.subtransforms.is_empty() => {
                    let payload = proto::ParDoPayload::decode(spec.payload.as_slice()).unwrap();
                    if payload.restriction_coder_id.is_empty() {
                        vec![URN_PAR_DO]
                    } else {
                        vec![
                            URN_SDF_PAIR_WITH_RESTRICTION,
                            URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS,
                            URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS,
                        ]
                    }
                }
                URN_COMBINE_PER_KEY => vec![
                    URN_COMBINE_PER_KEY_PRECOMBINE,
                    URN_COMBINE_PER_KEY_MERGE_ACCUMULATORS,
                    URN_COMBINE_PER_KEY_EXTRACT_OUTPUTS,
                ],
                _ => continue,
            };
            for urn in urns {
                assert!(
                    lookup_handler(&no_handlers, &stage(t, urn)).is_some(),
                    "{id} does not resolve as {urn}"
                );
                checked += 1;
            }
        }
        assert!(checked > 0, "no SDK transform in the expansion");
    }
}
