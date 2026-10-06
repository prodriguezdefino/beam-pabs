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

//! Each test transform expands from the requests that the suites send, and a worker
//! resolves each of its SDK transforms through the replay alone.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use beam::harness::bundle_processor::lookup_handler;
use beam::pipeline::{Pipeline, URN_PAR_DO};
use beam::schema::{FieldType, FieldValue, Row, Schema};
use beam::transforms::Create;
use expansion::ExpansionServiceServer;
use model::expansion::ExpansionRequest;
use model::expansion::expansion_service_server::ExpansionService;
use model::pipeline as proto;
use prost::Message;
use tests::xlang_transforms::{
    URN_CGBK, URN_COMGL, URN_COMPK, URN_FLATTEN, URN_GBK, URN_MULTI, URN_PARTITION, URN_PREFIX,
};
use tonic::Request;

/// The payload that Python `ImplicitSchemaPayloadBuilder({'data': '0'})` sends.
fn prefix_payload() -> Vec<u8> {
    let schema = Arc::new(Schema::builder().field("data", FieldType::string()).build());
    let row = Row::new(
        Arc::clone(&schema),
        vec![Some(FieldValue::String("0".to_string()))],
    )
    .expect("row matches schema");
    proto::ExternalConfigurationPayload {
        schema: Some((*schema).clone().into()),
        payload: row.to_row_bytes().expect("row encodes"),
    }
    .encode_to_vec()
}

fn strings(p: &Pipeline, name: &str) -> String {
    let values = vec!["a".to_string(), "b".to_string()];
    p.apply(Create::new(name, values)).id().to_string()
}

fn ints(p: &Pipeline, name: &str) -> String {
    p.apply(Create::new(name, vec![1_i64, 2, 3]))
        .id()
        .to_string()
}

fn int_keyed(p: &Pipeline, name: &str) -> String {
    let values = vec![(0_i64, "1".to_string()), (1, "2".to_string())];
    p.apply(Create::new(name, values)).id().to_string()
}

fn string_keyed(p: &Pipeline, name: &str) -> String {
    let values = vec![("a".to_string(), 1_i64), ("b".to_string(), 2)];
    p.apply(Create::new(name, values)).id().to_string()
}

/// Adds an input PCollection with the given transform name and returns its id.
type MakeInput = fn(&Pipeline, &str) -> String;

struct Case {
    urn: &'static str,
    payload: Vec<u8>,
    inputs: Vec<(&'static str, MakeInput)>,
    outputs: &'static [&'static str],
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            urn: URN_PREFIX,
            payload: prefix_payload(),
            inputs: vec![("input", strings)],
            outputs: &["output"],
        },
        Case {
            urn: URN_MULTI,
            payload: Vec::new(),
            inputs: vec![("main1", strings), ("main2", strings), ("side", strings)],
            outputs: &["main", "side"],
        },
        Case {
            urn: URN_GBK,
            payload: Vec::new(),
            inputs: vec![("input", int_keyed)],
            outputs: &["output"],
        },
        Case {
            urn: URN_CGBK,
            payload: Vec::new(),
            inputs: vec![("col1", int_keyed), ("col2", int_keyed)],
            outputs: &["output"],
        },
        Case {
            urn: URN_COMGL,
            payload: Vec::new(),
            inputs: vec![("input", ints)],
            outputs: &["output"],
        },
        Case {
            urn: URN_COMPK,
            payload: Vec::new(),
            inputs: vec![("input", string_keyed)],
            outputs: &["output"],
        },
        Case {
            urn: URN_FLATTEN,
            payload: Vec::new(),
            inputs: vec![("0", ints), ("1", ints)],
            outputs: &["output"],
        },
        Case {
            urn: URN_PARTITION,
            payload: Vec::new(),
            inputs: vec![("input", ints)],
            outputs: &["0", "1"],
        },
    ]
}

#[tokio::test]
async fn every_test_transform_expands_and_resolves_through_the_replay() {
    let no_handlers = HashMap::new();
    for case in cases() {
        let caller = Pipeline::new();
        let inputs: HashMap<String, String> = case
            .inputs
            .iter()
            .map(|(tag, make)| (tag.to_string(), make(&caller, &format!("Create_{tag}"))))
            .collect();
        let components = caller.lock().components.clone();
        let resp = ExpansionServiceServer::new()
            .expand(Request::new(ExpansionRequest {
                transform: Some(proto::PTransform {
                    unique_name: "Caller".to_string(),
                    spec: Some(proto::FunctionSpec {
                        urn: case.urn.to_string(),
                        payload: case.payload,
                    }),
                    inputs,
                    ..Default::default()
                }),
                components: Some(components),
                namespace: "external_1".to_string(),
                ..Default::default()
            }))
            .await
            .expect("expand returns errors in the response")
            .into_inner();
        assert_eq!(resp.error, "", "{}", case.urn);

        let transform = resp.transform.expect("transform");
        let tags: BTreeSet<&str> = transform.outputs.keys().map(String::as_str).collect();
        assert_eq!(tags, case.outputs.iter().copied().collect(), "{}", case.urn);

        let components = resp.components.expect("components");
        for (id, t) in &components.transforms {
            if t.spec.as_ref().is_some_and(|s| s.urn == URN_PAR_DO) && t.subtransforms.is_empty() {
                assert!(
                    lookup_handler(&no_handlers, t).is_some(),
                    "{}: {id} does not resolve",
                    case.urn
                );
            }
        }
    }
}
