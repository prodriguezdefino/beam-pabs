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

//! Integration tests for process-wide monitoring infos (`HarnessMonitoringInfosRequest`).

use std::collections::HashMap;
use std::sync::Arc;

use beam::coders::VarIntCoder;
use beam::internals::{BundleHandler, HandlerContext, HandlerInstance, TransformFn};
use beam::metrics::{
    DistributionValue, GaugeValue, LABEL_NAME, LABEL_NAMESPACE, LABEL_PTRANSFORM, Metrics,
    URN_USER_DISTRIBUTION_INT64, URN_USER_LATEST_INT64, URN_USER_SUM_INT64,
};
use harness::bundle_processor::BundleProcessor;
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::{
    Elements, HarnessMonitoringInfosRequest, InstructionRequest, MonitoringInfosMetadataRequest,
    ProcessBundleRequest, RegisterRequest, elements, instruction_request::Request,
    instruction_response::Response,
};

mod common;
use common::{DescriptorBuilder, SINK_ID, SOURCE_ID, STAGE_ID};

fn test_control_client() -> ControlClient {
    let (data_out_tx, _data_out_rx) = tokio::sync::mpsc::channel(256);
    let data_manager = DataManager::new(data_out_tx);
    let bundle_processor = Arc::new(BundleProcessor::with_handlers(data_manager, HashMap::new()));
    ControlClient::new(bundle_processor)
}

/// Checks that HarnessMonitoringInfosRequest returns process-wide user counters keyed by short ID.
#[tokio::test]
async fn test_harness_monitoring_infos_user_counter() {
    Metrics::counter("outside_bundle_ns", "outside_bundle_counter").inc_by(42);

    let control = test_control_client();
    let resp = control
        .handle_instruction(InstructionRequest {
            instruction_id: "inst_hmi_counter".to_string(),
            request: Some(Request::HarnessMonitoringInfos(
                HarnessMonitoringInfosRequest {},
            )),
        })
        .await;

    assert!(resp.error.is_empty(), "unexpected error: {}", resp.error);
    let Some(Response::HarnessMonitoringInfos(hmi_resp)) = resp.response else {
        panic!("expected HarnessMonitoringInfos response");
    };

    assert!(
        !hmi_resp.monitoring_data.is_empty(),
        "monitoring_data must not be empty"
    );

    let short_ids: Vec<String> = hmi_resp.monitoring_data.keys().cloned().collect();
    let meta_resp = control
        .handle_instruction(InstructionRequest {
            instruction_id: "inst_meta_counter".to_string(),
            request: Some(Request::MonitoringInfos(MonitoringInfosMetadataRequest {
                monitoring_info_id: short_ids,
            })),
        })
        .await;

    assert!(
        meta_resp.error.is_empty(),
        "unexpected metadata error: {}",
        meta_resp.error
    );
    let Some(Response::MonitoringInfos(meta_infos)) = meta_resp.response else {
        panic!("expected MonitoringInfosMetadataResponse");
    };

    let counter_match = meta_infos.monitoring_info.iter().find(|(_id, info)| {
        info.urn == URN_USER_SUM_INT64
            && info.labels.get(LABEL_NAMESPACE) == Some(&"outside_bundle_ns".to_string())
            && info.labels.get(LABEL_NAME) == Some(&"outside_bundle_counter".to_string())
    });

    assert!(
        counter_match.is_some(),
        "metadata resolution must find the user counter"
    );

    let (short_id, info) = counter_match.unwrap();
    assert_eq!(
        info.labels.get(LABEL_PTRANSFORM),
        None,
        "process-wide metrics must not have a PTRANSFORM label"
    );

    let payload = hmi_resp
        .monitoring_data
        .get(short_id)
        .expect("short id must exist in monitoring_data");
    let mut reader = &payload[..];
    let val = VarIntCoder::decode_varint(&mut reader).expect("varint payload");
    assert_eq!(val, 42);
}

/// Checks that HarnessMonitoringInfosRequest returns process-wide distributions and gauges.
#[tokio::test]
async fn test_harness_monitoring_infos_distribution_and_gauge() {
    let dist = Metrics::distribution("outside_dist_ns", "outside_dist");
    dist.update(10);
    dist.update(20);

    let gauge = Metrics::gauge("outside_gauge_ns", "outside_gauge");
    gauge.set(12345);

    let control = test_control_client();
    let resp = control
        .handle_instruction(InstructionRequest {
            instruction_id: "inst_hmi_dist_gauge".to_string(),
            request: Some(Request::HarnessMonitoringInfos(
                HarnessMonitoringInfosRequest {},
            )),
        })
        .await;

    assert!(resp.error.is_empty(), "unexpected error: {}", resp.error);
    let Some(Response::HarnessMonitoringInfos(hmi_resp)) = resp.response else {
        panic!("expected HarnessMonitoringInfos response");
    };

    let short_ids: Vec<String> = hmi_resp.monitoring_data.keys().cloned().collect();
    let meta_resp = control
        .handle_instruction(InstructionRequest {
            instruction_id: "inst_meta_dist_gauge".to_string(),
            request: Some(Request::MonitoringInfos(MonitoringInfosMetadataRequest {
                monitoring_info_id: short_ids,
            })),
        })
        .await;

    let Some(Response::MonitoringInfos(meta_infos)) = meta_resp.response else {
        panic!("expected MonitoringInfosMetadataResponse");
    };

    let dist_entry = meta_infos
        .monitoring_info
        .iter()
        .find(|(_id, info)| {
            info.urn == URN_USER_DISTRIBUTION_INT64
                && info.labels.get(LABEL_NAME) == Some(&"outside_dist".to_string())
        })
        .expect("distribution metadata");

    let dist_payload = hmi_resp
        .monitoring_data
        .get(dist_entry.0)
        .expect("distribution payload");
    let dist_val = DistributionValue::decode_payload(dist_payload).expect("decode distribution");
    assert_eq!(
        dist_val,
        DistributionValue {
            count: 2,
            sum: 30,
            min: 10,
            max: 20,
        }
    );

    let gauge_entry = meta_infos
        .monitoring_info
        .iter()
        .find(|(_id, info)| {
            info.urn == URN_USER_LATEST_INT64
                && info.labels.get(LABEL_NAME) == Some(&"outside_gauge".to_string())
        })
        .expect("gauge metadata");

    let gauge_payload = hmi_resp
        .monitoring_data
        .get(gauge_entry.0)
        .expect("gauge payload");
    let gauge_val = GaugeValue::decode_payload(gauge_payload).expect("decode gauge");
    assert_eq!(gauge_val.value, 12345);
    assert!(gauge_val.timestamp_ms > 0);
}

#[derive(Clone)]
struct SetupCounterHandler;

impl BundleHandler for SetupCounterHandler {
    fn setup(&mut self) -> Result<(), String> {
        Metrics::counter("setup_lifecycle_ns", "setup_counter").inc_by(7);
        Ok(())
    }

    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        ctx.sink.push(element.to_vec())
    }

    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }
}

/// Checks that counters recorded during DoFn setup carry the PTRANSFORM label.
#[tokio::test]
async fn test_harness_monitoring_infos_dofn_setup() {
    let (data_out_tx, _data_out_rx) = tokio::sync::mpsc::channel(256);
    let data_manager = DataManager::new(data_out_tx);
    let handler: TransformFn = Arc::new(SetupCounterHandler);
    let handlers = HashMap::from([(STAGE_ID.to_string(), handler)]);
    let bundle_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    ));
    let control = ControlClient::new(bundle_processor);

    let descriptor = DescriptorBuilder::new("desc_setup_test")
        .stage(STAGE_ID, "pcoll_input", "pcoll_stage")
        .sink(SINK_ID, "pcoll_stage")
        .build();

    let reg_resp = control
        .handle_instruction(InstructionRequest {
            instruction_id: "register_setup_desc".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![descriptor],
            })),
        })
        .await;
    assert!(
        reg_resp.error.is_empty(),
        "registration failed: {}",
        reg_resp.error
    );

    // Feed end of stream to complete the bundle.
    tokio::spawn({
        let data_manager = data_manager.clone();
        async move {
            data_manager
                .handle_inbound_elements(Elements {
                    data: vec![elements::Data {
                        instruction_id: "bundle_setup_test".to_string(),
                        transform_id: SOURCE_ID.to_string(),
                        data: Vec::new(),
                        is_last: true,
                    }],
                    timers: Vec::new(),
                })
                .await;
        }
    });

    let pb_resp = control
        .handle_instruction(InstructionRequest {
            instruction_id: "bundle_setup_test".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_setup_test".to_string(),
                ..Default::default()
            })),
        })
        .await;
    assert!(pb_resp.error.is_empty(), "bundle failed: {}", pb_resp.error);

    // Query harness monitoring infos.
    let hmi_resp = control
        .handle_instruction(InstructionRequest {
            instruction_id: "inst_hmi_setup".to_string(),
            request: Some(Request::HarnessMonitoringInfos(
                HarnessMonitoringInfosRequest {},
            )),
        })
        .await;
    assert!(hmi_resp.error.is_empty());
    let Some(Response::HarnessMonitoringInfos(hmi_data)) = hmi_resp.response else {
        panic!("expected HarnessMonitoringInfosResponse");
    };

    let short_ids: Vec<String> = hmi_data.monitoring_data.keys().cloned().collect();
    let meta_resp = control
        .handle_instruction(InstructionRequest {
            instruction_id: "inst_meta_setup".to_string(),
            request: Some(Request::MonitoringInfos(MonitoringInfosMetadataRequest {
                monitoring_info_id: short_ids,
            })),
        })
        .await;
    assert!(meta_resp.error.is_empty());
    let Some(Response::MonitoringInfos(meta_infos)) = meta_resp.response else {
        panic!("expected MonitoringInfosMetadataResponse");
    };

    let setup_match = meta_infos.monitoring_info.iter().find(|(_id, info)| {
        info.urn == URN_USER_SUM_INT64
            && info.labels.get(LABEL_NAMESPACE) == Some(&"setup_lifecycle_ns".to_string())
            && info.labels.get(LABEL_NAME) == Some(&"setup_counter".to_string())
    });

    assert!(
        setup_match.is_some(),
        "must resolve setup counter in metadata"
    );
    let (short_id, info) = setup_match.unwrap();

    assert_eq!(
        info.labels.get(LABEL_PTRANSFORM),
        Some(&STAGE_ID.to_string()),
        "counter from DoFn setup must carry PTRANSFORM label"
    );

    let payload = hmi_data
        .monitoring_data
        .get(short_id)
        .expect("short id must exist in monitoring_data");
    let mut reader = &payload[..];
    let val = VarIntCoder::decode_varint(&mut reader).expect("varint payload");
    assert_eq!(val, 7);
}
