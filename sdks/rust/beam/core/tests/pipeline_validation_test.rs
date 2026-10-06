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

//! Integration tests for Apache Beam pipeline structural and semantic validation.
//!
//! Verifies detection of invalid DAG states, missing coders, windowing strategies,
//! unconnected inputs, missing sources, missing subtransforms, and corrupted combine payloads.

use std::collections::HashMap;

use beam::pipeline::{
    Pipeline, PipelineError, UNEXPANDED_PLACEHOLDER_ID, URN_COMBINE_PER_KEY, URN_PAR_DO,
};
use model::pipeline as proto;
use prost::Message;

#[test]
fn test_validation_fails_on_missing_coder() {
    let p = Pipeline::new();
    p.impulse();

    // Tamper with coder_id of pcollection.
    for pcoll in p.lock().components.pcollections.values_mut() {
        pcoll.coder_id = "non_existent_coder".to_string();
    }

    let err = p.validate().unwrap_err();
    match err {
        PipelineError::MissingCoder { .. } => (),
        other => panic!("Expected MissingCoder error, got {other:?}"),
    }
}

#[test]
fn test_validation_fails_on_missing_windowing_strategy() {
    let p = Pipeline::new();
    p.impulse();

    for pcoll in p.lock().components.pcollections.values_mut() {
        pcoll.windowing_strategy_id = "non_existent_ws".to_string();
    }

    let err = p.validate().unwrap_err();
    match err {
        PipelineError::MissingWindowingStrategy { .. } => (),
        other => panic!("Expected MissingWindowingStrategy error, got {other:?}"),
    }
}

#[test]
fn test_validation_fails_on_unconnected_input() {
    let p = Pipeline::new();
    p.impulse();

    // Add transform referencing non-existent input.
    let mut inputs = HashMap::new();
    inputs.insert("in".to_string(), "bogus_pcoll".to_string());
    p.add_transform(
        "BadTransform",
        URN_PAR_DO,
        Vec::new(),
        inputs,
        HashMap::new(),
    );

    let err = p.validate().unwrap_err();
    match err {
        PipelineError::MissingInputPCollection {
            transform_id,
            pcollection_id,
            ..
        } => {
            assert!(transform_id.contains("BadTransform"));
            assert_eq!(pcollection_id, "bogus_pcoll");
        }
        other => panic!("Expected MissingInputPCollection error, got {other:?}"),
    }
}

#[test]
fn test_validation_fails_on_pipeline_without_source() {
    let p = Pipeline::new();

    // The transform has an input, so it is not a source. The referenced PCollection does not
    // exist, because root validation runs first and must reject the pipeline for having no
    // source.
    let mut inputs = HashMap::new();
    inputs.insert("in".to_string(), "pcoll_dummy".to_string());
    p.add_transform(
        "TransformWithoutSource",
        URN_PAR_DO,
        Vec::new(),
        inputs,
        HashMap::new(),
    );

    match p.validate().unwrap_err() {
        PipelineError::InvalidRootTransform {
            transform_id,
            reason,
        } => {
            assert_eq!(transform_id, "TransformWithoutSource");
            assert!(
                reason.contains("at least one source transform"),
                "expected the missing-source rule to fire, got reason: {reason}"
            );
        }
        other => panic!("Expected InvalidRootTransform, got {other:?}"),
    }
}

#[test]
fn test_validation_fails_on_missing_subtransform() {
    let p = Pipeline::new();
    let impulse_out = p.impulse();

    let inputs = HashMap::from([("in".to_string(), impulse_out.id().to_string())]);
    let outputs = HashMap::from([("out".to_string(), impulse_out.id().to_string())]);
    p.add_composite_transform(
        "BadComposite",
        None,
        Vec::new(),
        inputs,
        outputs,
        vec!["non_existent_subtransform".to_string()],
    );

    match p.validate() {
        Err(PipelineError::MissingSubtransform {
            transform_id,
            subtransform_id,
        }) => {
            assert_eq!(transform_id, "BadComposite");
            assert_eq!(subtransform_id, "non_existent_subtransform");
        }
        other => panic!("Expected MissingSubtransform, got {other:?}"),
    }
}

#[test]
fn test_validation_fails_on_invalid_combine_payload() {
    let p = Pipeline::new();
    let impulse_out = p.impulse();

    let inputs = HashMap::from([("in".to_string(), impulse_out.id().to_string())]);
    let outputs = HashMap::from([("out".to_string(), impulse_out.id().to_string())]);
    p.add_composite_transform(
        "CorruptedCombine",
        Some(URN_COMBINE_PER_KEY),
        vec![0xFF, 0xFF],
        inputs,
        outputs,
        Vec::new(),
    );

    match p.validate() {
        Err(PipelineError::InvalidCombinePayload { transform_id, .. }) => {
            assert_eq!(transform_id, "CorruptedCombine");
        }
        other => panic!("Expected InvalidCombinePayload, got {other:?}"),
    }
}

#[test]
fn test_validation_fails_on_missing_accumulator_coder() {
    let p = Pipeline::new();
    let impulse_out = p.impulse();

    let payload = proto::CombinePayload {
        combine_fn: Some(proto::FunctionSpec {
            urn: String::new(),
            payload: Vec::new(),
        }),
        accumulator_coder_id: "non_existent_coder".to_string(),
    };
    let mut payload_bytes = Vec::new();
    payload.encode(&mut payload_bytes).unwrap();

    let inputs = HashMap::from([("in".to_string(), impulse_out.id().to_string())]);
    let outputs = HashMap::from([("out".to_string(), impulse_out.id().to_string())]);
    p.add_composite_transform(
        "MissingCoderCombine",
        Some(URN_COMBINE_PER_KEY),
        payload_bytes,
        inputs,
        outputs,
        Vec::new(),
    );

    match p.validate() {
        Err(PipelineError::MissingAccumulatorCoder {
            transform_id,
            coder_id,
        }) => {
            assert_eq!(transform_id, "MissingCoderCombine");
            assert_eq!(coder_id, "non_existent_coder");
        }
        other => panic!("Expected MissingAccumulatorCoder, got {other:?}"),
    }
}

#[test]
fn test_validation_fails_on_invalid_source_transform() {
    let p = Pipeline::new();

    // Register a leaf transform with no inputs and a non-source URN.
    p.add_transform(
        "InvalidSourceParDo",
        URN_PAR_DO,
        Vec::new(),
        HashMap::new(),
        HashMap::new(),
    );

    match p.validate() {
        Err(PipelineError::InvalidSourceTransform { transform_id, urn }) => {
            assert_eq!(transform_id, "InvalidSourceParDo");
            assert_eq!(urn, URN_PAR_DO);
        }
        other => panic!("Expected InvalidSourceTransform, got {other:?}"),
    }
}

#[test]
fn test_validation_fails_on_orphan_pcollection() {
    use beam::values::IsBounded;

    let p = Pipeline::new();
    p.impulse();

    // Register an orphan PCollection that is never produced by any transform.
    let coder_id = p.register_coder(beam::coders::URN_BYTES, Vec::new());
    let _orphan_id = p.add_pcollection::<Vec<u8>>("orphan_pcoll", &coder_id, IsBounded::Bounded);

    match p.validate() {
        Err(PipelineError::NoProducerPCollection { pcollection_id }) => {
            assert_eq!(pcollection_id, "orphan_pcoll");
        }
        other => panic!("Expected NoProducerPCollection, got {other:?}"),
    }
}

/// An unexpanded cross-language output is reported as an unexpanded transform.
///
/// Its coder and windowing strategy are placeholders until the expansion service replies.
/// So validation finds two dangling references. A missing-coder error would describe the
/// symptom and hide the cause: nothing expanded the transform.
#[test]
fn test_validation_names_an_unexpanded_cross_language_output() {
    let p = Pipeline::new();
    p.impulse();

    for pcoll in p.lock().components.pcollections.values_mut() {
        pcoll.coder_id = UNEXPANDED_PLACEHOLDER_ID.to_string();
        pcoll.windowing_strategy_id = UNEXPANDED_PLACEHOLDER_ID.to_string();
    }

    match p.validate().unwrap_err() {
        PipelineError::UnexpandedCrossLanguageOutput { .. } => (),
        other => panic!("Expected UnexpandedCrossLanguageOutput, got {other:?}"),
    }
}
