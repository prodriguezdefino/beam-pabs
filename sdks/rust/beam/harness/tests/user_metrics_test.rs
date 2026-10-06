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

//! Integration tests verifying user-defined metrics reporting over the Fn API.

mod common;

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use beam::coders::VarIntCoder;
use beam::metrics::{
    LABEL_NAME, LABEL_NAMESPACE, LABEL_PTRANSFORM, Metrics, URN_USER_DISTRIBUTION_INT64,
    URN_USER_LATEST_INT64, URN_USER_SUM_INT64,
};
use harness::bundle_processor::{BundleProcessor, TransformFn};
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::instruction_request::Request;
use model::fn_execution::instruction_response::Response;
use model::fn_execution::{
    Elements, InstructionRequest, MonitoringInfosMetadataRequest, ProcessBundleRequest,
    RegisterRequest, elements,
};
use tokio::sync::mpsc;

use common::{CODER_RAW, DescriptorBuilder, SOURCE_ID, bytes_coder};

#[tokio::test]
async fn test_user_metrics_reported_in_process_bundle_response() {
    let (data_out_tx, mut _data_out_rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);

    let stage_id = "user_metric_stage";
    let sink_id = "sink_transform";
    let desc_id = "desc_user_metrics";

    use beam::internals::BundleHandler;
    use beam::internals::HandlerContext;

    #[derive(Clone)]
    struct MetricHandler;
    impl BundleHandler for MetricHandler {
        fn instantiate(&self) -> beam::internals::HandlerInstance {
            Box::new(self.clone())
        }

        fn process(&mut self, in_bytes: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
            Metrics::counter("custom_ns", "user_counter").inc_by(10);
            Metrics::distribution("custom_ns", "user_dist").update(in_bytes.len() as i64);
            Metrics::gauge("custom_ns", "user_gauge").set(12345);
            ctx.sink.push(in_bytes.to_vec())
        }
    }

    let mut handlers: HashMap<String, TransformFn> = HashMap::new();
    handlers.insert(stage_id.to_string(), Arc::new(MetricHandler));

    let bundle_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    ));
    let control_client = ControlClient::new(bundle_processor);

    let descriptor = DescriptorBuilder::new(desc_id)
        .with_coders(CODER_RAW, bytes_coder())
        .stage(stage_id, "pcoll_input", "pcoll_output")
        .sink(sink_id, "pcoll_output")
        .build();

    let reg_resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "inst_reg_user_metrics".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![descriptor],
            })),
        })
        .await;
    assert!(reg_resp.error.is_empty());

    // Send two elements and the EOF while the bundle runs.
    let dm = data_manager.clone();
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        dm.handle_inbound_elements(Elements {
            data: vec![
                elements::Data {
                    instruction_id: "inst_user_metrics".to_string(),
                    transform_id: SOURCE_ID.to_string(),
                    data: b"hello".to_vec(),
                    is_last: false,
                },
                elements::Data {
                    instruction_id: "inst_user_metrics".to_string(),
                    transform_id: SOURCE_ID.to_string(),
                    data: b"world!".to_vec(),
                    is_last: false,
                },
                elements::Data {
                    instruction_id: "inst_user_metrics".to_string(),
                    transform_id: SOURCE_ID.to_string(),
                    data: Vec::new(),
                    is_last: true,
                },
            ],
            timers: Vec::new(),
        })
        .await;
    });

    let bundle_req = InstructionRequest {
        instruction_id: "inst_user_metrics".to_string(),
        request: Some(Request::ProcessBundle(ProcessBundleRequest {
            process_bundle_descriptor_id: desc_id.to_string(),
            cache_tokens: Vec::new(),
            elements: None,
            has_no_state: false,
            only_bundle_for_keys: false,
            data_stream_id: String::new(),
        })),
    };

    let bundle_resp = control_client.handle_instruction(bundle_req).await;
    assert!(
        bundle_resp.error.is_empty(),
        "Bundle failed: {}",
        bundle_resp.error
    );

    let Some(Response::ProcessBundle(pb_resp)) = bundle_resp.response else {
        panic!("Expected ProcessBundleResponse");
    };

    // The user counter is in `monitoring_infos`, with its transform and namespace labels.
    let counter_info = pb_resp
        .monitoring_infos
        .iter()
        .find(|info| {
            info.urn == URN_USER_SUM_INT64
                && info.labels.get(LABEL_NAME) == Some(&"user_counter".to_string())
        })
        .expect("user_counter must be present in monitoring_infos");

    assert_eq!(
        counter_info.labels.get(LABEL_PTRANSFORM),
        Some(&stage_id.to_string())
    );
    assert_eq!(
        counter_info.labels.get(LABEL_NAMESPACE),
        Some(&"custom_ns".to_string())
    );
    let counter_val = VarIntCoder::decode_varint(&mut Cursor::new(&counter_info.payload)).unwrap();
    assert_eq!(counter_val, 20); // Two elements, 10 each.

    // The distribution payload encodes count, sum, min and max.
    let dist_info = pb_resp
        .monitoring_infos
        .iter()
        .find(|info| {
            info.urn == URN_USER_DISTRIBUTION_INT64
                && info.labels.get(LABEL_NAME) == Some(&"user_dist".to_string())
        })
        .expect("user_dist must be present in monitoring_infos");

    let mut dist_reader = Cursor::new(&dist_info.payload);
    let count = VarIntCoder::decode_varint(&mut dist_reader).unwrap();
    let sum = VarIntCoder::decode_varint(&mut dist_reader).unwrap();
    let min = VarIntCoder::decode_varint(&mut dist_reader).unwrap();
    let max = VarIntCoder::decode_varint(&mut dist_reader).unwrap();
    assert_eq!(count, 2);
    assert_eq!(sum, 11); // "hello" (5) + "world!" (6)
    assert_eq!(min, 5);
    assert_eq!(max, 6);

    // The gauge payload encodes a timestamp, then the value.
    let gauge_info = pb_resp
        .monitoring_infos
        .iter()
        .find(|info| {
            info.urn == URN_USER_LATEST_INT64
                && info.labels.get(LABEL_NAME) == Some(&"user_gauge".to_string())
        })
        .expect("user_gauge must be present in monitoring_infos");

    let mut gauge_reader = Cursor::new(&gauge_info.payload);
    let ts = VarIntCoder::decode_varint(&mut gauge_reader).unwrap();
    let gauge_val = VarIntCoder::decode_varint(&mut gauge_reader).unwrap();
    assert!(ts > 0);
    assert_eq!(gauge_val, 12345);

    // A metadata request resolves the short ids in `monitoring_data`.
    assert!(!pb_resp.monitoring_data.is_empty());
    let short_ids: Vec<String> = pb_resp.monitoring_data.keys().cloned().collect();

    let meta_resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "inst_meta_user_metrics".to_string(),
            request: Some(Request::MonitoringInfos(MonitoringInfosMetadataRequest {
                monitoring_info_id: short_ids,
            })),
        })
        .await;

    let Some(Response::MonitoringInfos(info_meta)) = meta_resp.response else {
        panic!("Expected MonitoringInfosMetadataResponse");
    };

    let user_counter_resolved = info_meta.monitoring_info.values().any(|m| {
        m.urn == URN_USER_SUM_INT64 && m.labels.get(LABEL_NAME) == Some(&"user_counter".to_string())
    });
    assert!(
        user_counter_resolved,
        "Runner short ID resolution must resolve user counter"
    );
}
