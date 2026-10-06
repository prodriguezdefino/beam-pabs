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

//! Tests for the `beam:transform:to_string:v1` handler, which runners (Dataflow's data
//! sampling) use to render sampled elements. The formatter is tested in core.

use std::collections::HashMap;

use beam::coders::{URN_BYTES, URN_KV, URN_STRING_UTF8, URN_VARINT, VarIntCoder};
use beam::internals::{ElementSink, HandlerContext};
use beam::pipeline::URN_TO_STRING;
use harness::bundle_processor::resolve_handler;
use model::fn_execution::ProcessBundleDescriptor;
use model::pipeline as proto;

fn coder(urn: &str, components: &[&str]) -> proto::Coder {
    proto::Coder {
        spec: Some(proto::FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: components.iter().map(|c| (*c).to_string()).collect(),
    }
}

/// The to_string input and output: KV<nonce, element> and KV<nonce, string>.
fn coders() -> HashMap<String, proto::Coder> {
    [
        ("str", coder(URN_STRING_UTF8, &[])),
        ("bytes", coder(URN_BYTES, &[])),
        ("varint", coder(URN_VARINT, &[])),
        ("kv_str_varint", coder(URN_KV, &["str", "varint"])),
        ("in", coder(URN_KV, &["bytes", "kv_str_varint"])),
        ("out", coder(URN_KV, &["bytes", "str"])),
    ]
    .into_iter()
    .map(|(id, c)| (id.to_string(), c))
    .collect()
}

fn nested_str(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    VarIntCoder::encode_varint(s.len() as i64, &mut out).expect("writing to a Vec succeeds");
    out.extend_from_slice(s.as_bytes());
    out
}

fn varint(v: i64) -> Vec<u8> {
    let mut out = Vec::new();
    VarIntCoder::encode_varint(v, &mut out).expect("writing to a Vec succeeds");
    out
}

fn to_string_descriptor() -> (ProcessBundleDescriptor, proto::PTransform) {
    let pcoll = |coder_id: &str| proto::PCollection {
        coder_id: coder_id.to_string(),
        ..Default::default()
    };
    let transform = proto::PTransform {
        unique_name: "ToString".to_string(),
        spec: Some(proto::FunctionSpec {
            urn: URN_TO_STRING.to_string(),
            payload: Vec::new(),
        }),
        inputs: [("in".to_string(), "pc_in".to_string())].into(),
        outputs: [("out".to_string(), "pc_out".to_string())].into(),
        ..Default::default()
    };
    let descriptor = ProcessBundleDescriptor {
        id: "sampling".to_string(),
        transforms: [("to_string".to_string(), transform.clone())].into(),
        pcollections: [
            ("pc_in".to_string(), pcoll("in")),
            ("pc_out".to_string(), pcoll("out")),
        ]
        .into(),
        coders: coders(),
        ..Default::default()
    };
    (descriptor, transform)
}

fn run(
    descriptor: &ProcessBundleDescriptor,
    transform: &proto::PTransform,
    input: &[u8],
) -> Vec<Vec<u8>> {
    let handler =
        resolve_handler(&HashMap::new(), transform, descriptor).expect("to_string is built in");
    let mut out: Vec<Vec<u8>> = Vec::new();
    let mut instance = handler.instantiate();
    {
        let sink: &mut dyn ElementSink = &mut out;
        let mut ctx = HandlerContext::new(sink);
        instance
            .process(input, &mut ctx)
            .expect("to_string succeeds");
    }
    out
}

#[test]
fn to_string_keeps_the_nonce_and_renders_the_element() {
    let (descriptor, transform) = to_string_descriptor();
    let nonce = [2u8, 0xde, 0xad];
    let element = [nested_str("word"), varint(3)].concat();
    let out = run(
        &descriptor,
        &transform,
        &[nonce.as_slice(), &element].concat(),
    );

    let expected = [nonce.to_vec(), nested_str("(\"word\", 3)")].concat();
    assert_eq!(out, vec![expected]);
}
