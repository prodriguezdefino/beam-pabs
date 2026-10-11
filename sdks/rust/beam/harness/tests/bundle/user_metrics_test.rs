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

use crate::common;

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use beam::coders::VarIntCoder;
use beam::internals::{ElementSink, TransformFn};
use beam::metrics::{
    BOUNDED_TRIE_DEFAULT_BOUND, BoundedTrieValue, LABEL_NAME, LABEL_NAMESPACE, LABEL_PTRANSFORM,
    Metrics, StringSetValue, TYPE_BOUNDED_TRIE, TYPE_SET_STRING, URN_USER_BOUNDED_TRIE,
    URN_USER_DISTRIBUTION_INT64, URN_USER_LATEST_INT64, URN_USER_SET_STRING, URN_USER_SUM_INT64,
};
use beam_harness::bundle_processor::BundleProcessor;
use beam_harness::control::ControlClient;
use beam_harness::data::DataManager;
use beam_model::fn_execution::instruction_request::Request;
use beam_model::fn_execution::instruction_response::Response;
use beam_model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleRequest, RegisterRequest, elements,
};
use beam_model::pipeline::MonitoringInfo;
use tokio::sync::mpsc;

use common::{
    CODER_RAW, DescriptorBuilder, SOURCE_ID, STAGE_ID, bytes_coder, linear_descriptor, run_bundle,
};

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
    let counter_val = VarIntCoder::decode_varint(&mut &counter_info.payload[..]).unwrap();
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

    let mut dist_reader = &dist_info.payload[..];
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

    let mut gauge_reader = &gauge_info.payload[..];
    let ts = VarIntCoder::decode_varint(&mut gauge_reader).unwrap();
    let gauge_val = VarIntCoder::decode_varint(&mut gauge_reader).unwrap();
    assert!(ts > 0);
    assert_eq!(gauge_val, 12345);

    // A runner without the short-id protocol gets only complete infos.
    assert!(pb_resp.monitoring_data.is_empty());
}

/// Runs one bundle of one element through a `STAGE_ID` handler that calls `record`, and
/// returns the monitoring infos of the response.
async fn monitoring_infos_of(descriptor_id: &str, record: fn()) -> Vec<MonitoringInfo> {
    let handler: TransformFn = Arc::new(move |element: &[u8], sink: &mut dyn ElementSink| {
        record();
        sink.push(element.to_vec())
    });
    let run = run_bundle(
        HashMap::from([(STAGE_ID.to_string(), handler)]),
        linear_descriptor(descriptor_id),
        vec![b"x".to_vec()],
    )
    .await;
    run.bundle_response().monitoring_infos.clone()
}

/// The one info with `urn` and the name `name` in `custom_ns`, reported for `STAGE_ID`.
fn user_info<'a>(infos: &'a [MonitoringInfo], urn: &str, name: &str) -> &'a MonitoringInfo {
    let matching: Vec<&MonitoringInfo> = infos
        .iter()
        .filter(|info| {
            info.urn == urn && info.labels.get(LABEL_NAME).map(String::as_str) == Some(name)
        })
        .collect();
    let [info] = matching[..] else {
        panic!("expected one {urn} info named {name}, got {matching:?}");
    };
    assert_eq!(
        info.labels.get(LABEL_NAMESPACE).map(String::as_str),
        Some("custom_ns")
    );
    assert_eq!(
        info.labels.get(LABEL_PTRANSFORM).map(String::as_str),
        Some(STAGE_ID)
    );
    info
}

/// The bounded trie that the `user_trie` info of `infos` carries, after a type check.
fn reported_trie(infos: &[MonitoringInfo]) -> BoundedTrieValue {
    let info = user_info(infos, URN_USER_BOUNDED_TRIE, "user_trie");
    assert_eq!(info.r#type, TYPE_BOUNDED_TRIE);
    BoundedTrieValue::decode_payload(&info.payload).expect("the payload is a bounded trie")
}

fn paths(expected: &[(&[&str], bool)]) -> BTreeSet<(Vec<String>, bool)> {
    expected
        .iter()
        .map(|(segments, truncated)| (segments.iter().map(|s| s.to_string()).collect(), *truncated))
        .collect()
}

/// No runner returns bounded tries, so the test checks the bundle response.
#[tokio::test]
async fn bounded_trie_metric_is_reported_in_process_bundle_response() {
    let infos = monitoring_infos_of("desc_bounded_trie", || {
        let trie = Metrics::bounded_trie("custom_ns", "user_trie");
        trie.add(&["ab", "cd"]);
        trie.add(&["ab", "ef"]);
        trie.add(&["gh"]);
        trie.add(&["ab", "cd"]);
    })
    .await;

    let trie = reported_trie(&infos);
    assert_eq!(trie.bound(), BOUNDED_TRIE_DEFAULT_BOUND);
    assert_eq!(
        trie.paths(),
        paths(&[
            (&["ab", "cd"], false),
            (&["ab", "ef"], false),
            (&["gh"], false)
        ])
    );
}

/// More paths than the bound truncate the largest subtree and set its truncated flag.
#[tokio::test]
async fn bounded_trie_metric_past_its_bound_reports_a_truncated_path() {
    let infos = monitoring_infos_of("desc_bounded_trie_truncated", || {
        let trie = Metrics::bounded_trie("custom_ns", "user_trie");
        trie.add(&["narrow", "leaf"]);
        (0..=BOUNDED_TRIE_DEFAULT_BOUND)
            .map(|i| i.to_string())
            .for_each(|leaf| trie.add(&["wide", &leaf]));
    })
    .await;

    assert_eq!(
        reported_trie(&infos).paths(),
        paths(&[(&["narrow", "leaf"], false), (&["wide"], true)])
    );
}

/// The bundle response reports a repeated string set value once.
#[tokio::test]
async fn string_set_metric_is_reported_in_process_bundle_response() {
    let infos = monitoring_infos_of("desc_string_set", || {
        let set = Metrics::string_set("custom_ns", "user_set");
        ["ab", "cd", "ab"]
            .into_iter()
            .for_each(|value| set.add(value));
    })
    .await;

    let info = user_info(&infos, URN_USER_SET_STRING, "user_set");
    assert_eq!(info.r#type, TYPE_SET_STRING);
    let set = StringSetValue::decode_payload(&info.payload).unwrap();
    let values: Vec<&str> = set.values().iter().map(String::as_str).collect();
    assert_eq!(values, ["ab", "cd"]);
}
