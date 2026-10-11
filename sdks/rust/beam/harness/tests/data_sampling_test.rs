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

use std::collections::HashMap;
use std::sync::Arc;

use beam::internals::{ElementSink, TransformFn};
use harness::bundle_processor::{DataSampler, PCollectionSampler, SamplingConfig, SamplingMode};
use model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleRequest, RegisterRequest, SampleDataRequest,
    elements, instruction_request::Request, instruction_response::Response,
};

mod common;
use common::{CODER_RAW, DescriptorBuilder, SINK_ID, SOURCE_ID, TestBundle, bytes_coder};

#[tokio::test]
async fn test_data_sampling_samples_elements_and_exceptions() {
    let handler: TransformFn = Arc::new(|elem: &[u8], sink: &mut dyn ElementSink| {
        if elem == b"fail" {
            return Err("intentional element processing failure".to_string());
        }
        sink.push(elem.to_vec())
    });

    let handlers = HashMap::from([("my_stage".to_string(), handler)]);
    let sampler = Arc::new(DataSampler::new(10, 1000, SamplingMode::All));

    let TestBundle {
        mut data_out_rx,
        data_manager,
        control_client,
        ..
    } = TestBundle::builder()
        .handlers(handlers)
        .data_sampler(Arc::clone(&sampler))
        .build();

    let descriptor = DescriptorBuilder::new("desc_sampling")
        .with_coders(CODER_RAW, bytes_coder())
        .stage("my_stage", "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build();

    let reg_resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_1".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![descriptor],
            })),
        })
        .await;
    assert!(reg_resp.error.is_empty(), "registration must succeed");

    // Run bundle with normal elements
    let bundle_id = "bundle_1".to_string();
    tokio::spawn({
        let dm = data_manager.clone();
        let b_id = bundle_id.clone();
        async move {
            for i in 0..15 {
                let payload = format!("elem_{i}").into_bytes();
                dm.handle_inbound_elements(Elements {
                    data: vec![elements::Data {
                        instruction_id: b_id.clone(),
                        transform_id: SOURCE_ID.to_string(),
                        data: payload,
                        is_last: false,
                    }],
                    timers: Vec::new(),
                })
                .await;
            }
            dm.handle_inbound_elements(Elements {
                data: vec![elements::Data {
                    instruction_id: b_id,
                    transform_id: SOURCE_ID.to_string(),
                    data: Vec::new(),
                    is_last: true,
                }],
                timers: Vec::new(),
            })
            .await;
        }
    });

    let resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: bundle_id,
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_sampling".to_string(),
                ..Default::default()
            })),
        })
        .await;
    assert!(resp.error.is_empty(), "bundle execution must succeed");

    // Drain data_out_rx so channel stays empty
    while data_out_rx.try_recv().is_ok() {}

    // Query samples without filter (all PCollections)
    let sample_req = InstructionRequest {
        instruction_id: "sample_1".to_string(),
        request: Some(Request::SampleData(SampleDataRequest {
            pcollection_ids: Vec::new(),
        })),
    };
    let sample_resp = control_client.handle_instruction(sample_req).await;
    assert!(
        sample_resp.error.is_empty(),
        "sample data request must succeed"
    );

    let sample_data = match sample_resp.response {
        Some(Response::SampleData(data)) => data,
        other => panic!("expected SampleData response, got {other:?}"),
    };

    assert!(
        sample_data.element_samples.contains_key("pcoll_output"),
        "pcoll_output should have samples"
    );
    let out_samples = &sample_data.element_samples["pcoll_output"].elements;
    assert_eq!(
        out_samples.len(),
        10,
        "ring buffer must retain exactly 10 samples"
    );

    // Second sample request drains empty
    let sample_req2 = InstructionRequest {
        instruction_id: "sample_2".to_string(),
        request: Some(Request::SampleData(SampleDataRequest {
            pcollection_ids: vec!["pcoll_output".to_string()],
        })),
    };
    let sample_resp2 = control_client.handle_instruction(sample_req2).await;
    let sample_data2 = match sample_resp2.response {
        Some(Response::SampleData(data)) => data,
        other => panic!("expected SampleData response, got {other:?}"),
    };
    assert!(
        sample_data2
            .element_samples
            .get("pcoll_output")
            .map(|l| l.elements.is_empty())
            .unwrap_or(true),
        "subsequent query should find samples drained"
    );

    // Run bundle that triggers exception
    let fail_bundle_id = "bundle_fail".to_string();
    tokio::spawn({
        let dm = data_manager.clone();
        let b_id = fail_bundle_id.clone();
        async move {
            dm.handle_inbound_elements(Elements {
                data: vec![elements::Data {
                    instruction_id: b_id.clone(),
                    transform_id: SOURCE_ID.to_string(),
                    data: b"fail".to_vec(),
                    is_last: true,
                }],
                timers: Vec::new(),
            })
            .await;
        }
    });

    let fail_resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: fail_bundle_id.clone(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_sampling".to_string(),
                ..Default::default()
            })),
        })
        .await;
    assert!(!fail_resp.error.is_empty(), "bundle execution must fail");

    // Query sample data to verify exception was recorded
    let exc_sample_req = InstructionRequest {
        instruction_id: "sample_exc".to_string(),
        request: Some(Request::SampleData(SampleDataRequest {
            pcollection_ids: vec!["pcoll_input".to_string()],
        })),
    };
    let exc_sample_resp = control_client.handle_instruction(exc_sample_req).await;
    let exc_sample_data = match exc_sample_resp.response {
        Some(Response::SampleData(data)) => data,
        other => panic!("expected SampleData response, got {other:?}"),
    };

    assert!(
        exc_sample_data.element_samples.contains_key("pcoll_input"),
        "exception must be recorded on input pcollection"
    );
    let exc_samples = &exc_sample_data.element_samples["pcoll_input"].elements;
    assert_eq!(
        exc_samples.len(),
        1,
        "exactly one exception sample recorded"
    );
    let exc = exc_samples[0]
        .exception
        .as_ref()
        .expect("exception info must be present");
    assert_eq!(exc.instruction_id, fail_bundle_id);
    assert_eq!(exc.transform_id, "my_stage");
    assert!(exc.error.contains("intentional element processing failure"));
}

/// After the first `max_samples` elements, the schedule takes one element in `sample_every_n`.
#[test]
fn the_schedule_takes_the_first_elements_then_one_in_n() {
    let sampler = PCollectionSampler::new(SamplingConfig {
        max_samples: 3,
        sample_every_n: 5,
        mode: SamplingMode::All,
    });
    (1..=10u8).for_each(|i| sampler.sample(&[i]));
    let taken: Vec<Vec<u8>> = sampler
        .drain_samples()
        .into_iter()
        .map(|sample| sample.element)
        .collect();
    assert_eq!(
        taken,
        [[3], [5], [10]],
        "the newest three of elements 1, 2, 3, 5, 10"
    );
}

/// A PCollection keeps the newest `max_samples` samples.
#[test]
fn a_pcollection_keeps_the_newest_samples() {
    let sampler = PCollectionSampler::new(SamplingConfig {
        max_samples: 2,
        sample_every_n: 1,
        mode: SamplingMode::All,
    });
    [&b"one"[..], b"two", b"three"]
        .into_iter()
        .for_each(|element| sampler.sample(element));
    let taken: Vec<Vec<u8>> = sampler
        .drain_samples()
        .into_iter()
        .map(|sample| sample.element)
        .collect();
    assert_eq!(taken, [b"two".to_vec(), b"three".to_vec()]);
}

/// Only the experiments turn sampling on, and the disable experiment wins.
#[test]
fn experiments_select_the_sampling_mode() {
    let experiments = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
    assert!(DataSampler::from_experiments(&[]).is_none());
    assert!(
        DataSampler::from_experiments(&experiments(&["enable_always_on_exception_sampling"]))
            .unwrap()
            .config()
            .mode
            == SamplingMode::ExceptionsOnly
    );
    assert!(
        DataSampler::from_experiments(&experiments(&[
            "enable_always_on_exception_sampling",
            "disable_always_on_exception_sampling",
        ]))
        .is_none()
    );
    assert_eq!(
        DataSampler::from_experiments(&experiments(&["enable_data_sampling"]))
            .unwrap()
            .config()
            .mode,
        SamplingMode::All
    );
}

/// A request that names PCollections gets only their samples.
#[test]
fn a_sample_request_filters_by_pcollection() {
    let sampler = DataSampler::new(5, 10, SamplingMode::All);
    sampler.sampler_for_pcollection("pcoll_a").sample(b"alpha");
    sampler.sampler_for_pcollection("pcoll_b").sample(b"beta");
    let response = sampler.handle_sample_data(&SampleDataRequest {
        pcollection_ids: vec!["pcoll_a".to_string()],
    });
    assert_eq!(response.element_samples.len(), 1);
    assert_eq!(response.element_samples["pcoll_a"].elements.len(), 1);
}

/// One failed bundle gives one failure sample, from the first transform that reports it.
#[test]
fn a_failed_bundle_gives_one_failure_sample() {
    let sampler = DataSampler::new(5, 10, SamplingMode::ExceptionsOnly);
    sampler.record_exception("inner_pcoll", "inst_1", "inner", "boom", Some(b"x"));
    sampler.record_exception("outer_pcoll", "inst_1", "outer", "boom", None);
    let response = sampler.handle_sample_data(&SampleDataRequest::default());
    let samples: Vec<_> = response
        .element_samples
        .values()
        .flat_map(|list| &list.elements)
        .collect();
    assert_eq!(samples.len(), 1);
    let exception = samples[0].exception.as_ref().unwrap();
    assert_eq!(exception.transform_id, "inner");
    assert_eq!(samples[0].element, b"x");
}
