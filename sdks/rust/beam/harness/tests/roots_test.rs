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

//! Roots carry the input and output names of the runner's descriptor, so the runner sees the
//! watermark of a residual.

use std::collections::HashMap;
use std::sync::Arc;

use beam::Instant;
use beam::internals::{DynamicSplitResult, ResidualApplication};
use beam::pipeline::URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS;
use harness::bundle_processor::{PortFraming, RootEncoder};
use model::fn_execution::{BundleApplication, ProcessBundleDescriptor};
use model::pipeline::{FunctionSpec, PTransform, ParDoPayload, SideInput};
use prost::Message;

mod common;
use common::{CODER_RAW, bytes_coder};

/// A descriptor whose SDF `read` reads `i0`, has the side input `side`, and writes `out0`
/// and `out1`: names that a runner such as Dataflow gives.
fn descriptor() -> ProcessBundleDescriptor {
    let payload = ParDoPayload {
        side_inputs: HashMap::from([("side".to_string(), SideInput::default())]),
        ..Default::default()
    };
    let read = PTransform {
        spec: Some(FunctionSpec {
            urn: URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS.to_string(),
            payload: payload.encode_to_vec(),
        }),
        inputs: HashMap::from([
            ("i0".to_string(), "pc_in".to_string()),
            ("side".to_string(), "pc_side".to_string()),
        ]),
        outputs: HashMap::from([
            ("out0".to_string(), "pc_a".to_string()),
            ("out1".to_string(), "pc_b".to_string()),
        ]),
        ..Default::default()
    };
    ProcessBundleDescriptor {
        id: "d".to_string(),
        transforms: HashMap::from([("read".to_string(), read)]),
        ..Default::default()
    }
}

fn encoder() -> RootEncoder {
    let framing =
        PortFraming::resolve(CODER_RAW, Arc::new(bytes_coder())).expect("the port coder resolves");
    RootEncoder::new(&descriptor(), Arc::new(framing)).expect("root encoder builds")
}

fn residual(transform_id: &str, output_watermark: Option<Instant>) -> ResidualApplication {
    ResidualApplication {
        transform_id: transform_id.to_string(),
        input_id: "in".to_string(),
        element: b"rest".to_vec(),
        output_watermark,
        delay: None,
    }
}

#[test]
fn a_residual_takes_the_main_input_and_puts_its_watermark_on_every_output() {
    let root = encoder()
        .residual(residual("read", Some(Instant::from_millis(100_000))))
        .expect("the residual encodes");
    let application = root.application.expect("an application");
    let watermark = prost_types::Timestamp::from(Instant::from_millis(100_000));
    assert_eq!(application.input_id, "i0");
    assert_eq!(
        application.output_watermarks,
        HashMap::from([
            ("out0".to_string(), watermark),
            ("out1".to_string(), watermark)
        ])
    );
}

#[test]
fn a_residual_without_a_watermark_holds_no_output() {
    let root = encoder()
        .residual(residual("read", None))
        .expect("the residual encodes");
    assert!(
        root.application
            .expect("an application")
            .output_watermarks
            .is_empty()
    );
}

#[test]
fn a_residual_of_a_transform_that_the_descriptor_does_not_have_keeps_its_input() {
    let root = encoder()
        .residual(residual("other", Some(Instant::from_millis(5))))
        .expect("the residual encodes");
    let application = root.application.expect("an application");
    assert_eq!(application.input_id, "in");
    assert!(application.output_watermarks.is_empty());
}

#[test]
fn a_split_renames_the_input_of_its_primary_and_its_residuals() {
    let split = DynamicSplitResult {
        primary: BundleApplication {
            transform_id: "read".to_string(),
            input_id: "in".to_string(),
            element: b"primary".to_vec(),
            ..BundleApplication::default()
        },
        residuals: vec![residual("read", Some(Instant::from_millis(7)))],
    };
    let roots = encoder().split(split).expect("the split encodes");
    assert_eq!(roots.primary_roots[0].input_id, "i0");
    assert!(roots.primary_roots[0].output_watermarks.is_empty());
    let residual = roots.residual_roots[0]
        .application
        .as_ref()
        .expect("an application");
    assert_eq!(residual.input_id, "i0");
    assert_eq!(residual.output_watermarks.len(), 2);
}

#[test]
fn root_encoder_rejects_corrupted_pardo_payload() {
    let framing =
        PortFraming::resolve(CODER_RAW, Arc::new(bytes_coder())).expect("the port coder resolves");
    let corrupt = PTransform {
        unique_name: "corrupt".to_string(),
        spec: Some(FunctionSpec {
            urn: URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS.to_string(),
            payload: vec![0xff],
        }),
        inputs: HashMap::from([
            ("i0".to_string(), "pc_in".to_string()),
            ("side".to_string(), "pc_side".to_string()),
        ]),
        outputs: HashMap::from([("out".to_string(), "pc_out".to_string())]),
        ..Default::default()
    };
    let descriptor = ProcessBundleDescriptor {
        id: "d".to_string(),
        transforms: HashMap::from([("corrupt".to_string(), corrupt)]),
        ..Default::default()
    };
    assert!(RootEncoder::new(&descriptor, Arc::new(framing)).is_err());
}
