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

//! Integration tests for element coders and windows at a bundle's data-plane boundary.
//!
//! Covers windowed, param-windowed and length-prefixed source and sink coders, and the
//! window and pane each element carries.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use tokio::sync::mpsc;

use beam::coders::{
    Coder, Context, GlobalWindow, StringUtf8Coder, URN_LENGTH_PREFIX, URN_NULLABLE,
    URN_PARAM_WINDOWED_VALUE, URN_WINDOWED_VALUE, VarIntCoder, WindowedValue, WindowedValueCoder,
};
use beam::internals::ElementSink;
use harness::bundle_processor::{BundleProcessor, TransformFn};
use harness::control::ControlClient;
use harness::data::DataManager;
use model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleDescriptor, ProcessBundleRequest, RegisterRequest,
    RemoteGrpcPort, elements, instruction_request::Request,
};
use model::pipeline as proto_pipeline;
use prost::Message;

mod common;
use common::{
    CODER_RAW, DescriptorBuilder, Observer, SINK_ID, SOURCE_ID, STAGE_ID, bytes_coder,
    identity_handlers, run_bundle, windowed_linear_descriptor, within,
};

/// A source -> stage -> sink descriptor whose sink port coder wraps the element in `element_urn`.
fn descriptor_with_sink_element_coder(
    descriptor_id: &str,
    element_urn: &str,
) -> ProcessBundleDescriptor {
    DescriptorBuilder::new(descriptor_id)
        .with_coder("coder_sink_element", element_urn, &["coder_string"])
        .with_coder(
            "coder_windowed_sink",
            URN_WINDOWED_VALUE,
            &["coder_sink_element", "coder_global_window"],
        )
        .stage(STAGE_ID, "pcoll_input", "pcoll_out")
        .sink_with_coder(SINK_ID, "pcoll_out", "coder_windowed_sink")
        .build()
}

/// Registers `descriptor`, processes one bundle and returns the response error. An accepted
/// graph blocks until its input ends, so an empty final chunk is pushed in the background.
async fn bundle_error_for(descriptor: ProcessBundleDescriptor) -> String {
    let descriptor_id = descriptor.id.clone();
    let instruction_id = format!("inst_{descriptor_id}");

    let (data_out_tx, _data_out_rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);
    let control_client = ControlClient::new(Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        identity_handlers(&[STAGE_ID]),
    )));

    control_client
        .handle_instruction(InstructionRequest {
            instruction_id: format!("reg_{descriptor_id}"),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![descriptor],
            })),
        })
        .await;

    tokio::spawn({
        let dm = data_manager.clone();
        let instruction_id = instruction_id.clone();
        async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            dm.handle_inbound_elements(Elements {
                data: vec![elements::Data {
                    instruction_id,
                    transform_id: SOURCE_ID.to_string(),
                    data: Vec::new(),
                    is_last: true,
                }],
                timers: Vec::new(),
            })
            .await;
        }
    });

    within(
        "the bundle response",
        control_client.handle_instruction(InstructionRequest {
            instruction_id,
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: descriptor_id,
                cache_tokens: Vec::new(),
                elements: None,
                has_no_state: false,
                only_bundle_for_keys: false,
                data_stream_id: String::new(),
            })),
        }),
    )
    .await
    .error
}

/// The sink emits raw or length-prefixed bytes, so a length-prefix element coder is
/// accepted while a self-framing one (`nullable` needs a presence byte first) is rejected.
#[tokio::test]
async fn a_sink_accepts_a_length_prefixed_element_coder_and_rejects_a_self_framing_one() {
    let accepted = bundle_error_for(descriptor_with_sink_element_coder(
        "desc_length_prefixed_sink",
        URN_LENGTH_PREFIX,
    ))
    .await;
    assert!(
        accepted.is_empty(),
        "a length-prefixed sink coder is supported, but got: {accepted}"
    );

    let error = bundle_error_for(descriptor_with_sink_element_coder(
        "desc_nullable_sink",
        URN_NULLABLE,
    ))
    .await;
    assert!(
        error.contains(URN_NULLABLE),
        "error must name the offending coder URN, got: {error}"
    );
    assert!(
        error.contains("coder_windowed_sink"),
        "error must name the sink coder, got: {error}"
    );
}

#[tokio::test]
async fn test_windowed_value_coder_bundle_execution() {
    let (data_out_tx, mut data_out_rx) = mpsc::channel(64);
    let data_manager = DataManager::new(data_out_tx);

    let coder = WindowedValueCoder::new(StringUtf8Coder);

    // The harness strips the windowed-value header, dispatches the bare element (Nested
    // context) and re-attaches the header to what the handler emits.
    let mut handlers: HashMap<String, TransformFn> = HashMap::new();
    handlers.insert(
        STAGE_ID.to_string(),
        Arc::new(move |in_bytes: &[u8], sink: &mut dyn ElementSink| {
            let value = StringUtf8Coder
                .decode(&mut Cursor::new(in_bytes), Context::Nested)
                .map_err(|e| beam::Error::from(e).context("Decode failed"))?;

            let mut out_bytes = Vec::new();
            StringUtf8Coder
                .encode(
                    &format!("{value}_transformed"),
                    &mut out_bytes,
                    Context::Nested,
                )
                .map_err(|e| beam::Error::from(e).context("Encode failed"))?;

            sink.push(out_bytes)
        }),
    );

    let bundle_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    ));
    let control_client = ControlClient::new(bundle_processor);

    let desc = windowed_linear_descriptor("desc_windowed");
    control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_windowed".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![desc],
            })),
        })
        .await;

    let original_element = WindowedValue::global("apache_beam_rust".to_string(), 1700000000);
    let mut encoded_input = Vec::new();
    coder
        .encode(&original_element, &mut encoded_input, Context::Nested)
        .unwrap();

    tokio::spawn({
        let dm = data_manager.clone();
        let payload = encoded_input.clone();
        async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            dm.handle_inbound_elements(Elements {
                data: vec![elements::Data {
                    instruction_id: "inst_windowed_1".to_string(),
                    transform_id: "source_transform".to_string(),
                    data: payload,
                    is_last: true,
                }],
                timers: Vec::new(),
            })
            .await;
        }
    });

    let resp = within(
        "the bundle response",
        control_client.handle_instruction(InstructionRequest {
            instruction_id: "inst_windowed_1".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_windowed".to_string(),
                cache_tokens: Vec::new(),
                elements: None,
                has_no_state: false,
                only_bundle_for_keys: false,
                data_stream_id: String::new(),
            })),
        }),
    )
    .await;

    assert!(resp.error.is_empty(), "Bundle failed: {}", resp.error);

    let mut sink_bytes = Vec::new();
    while let Ok(elems) = data_out_rx.try_recv() {
        for d in elems.data {
            if !d.data.is_empty() {
                sink_bytes.extend_from_slice(&d.data);
            }
        }
    }

    assert!(!sink_bytes.is_empty(), "Must receive encoded output");

    let mut cursor = Cursor::new(sink_bytes);
    let decoded_output = coder.decode(&mut cursor, Context::Nested).unwrap();

    assert_eq!(decoded_output.value, "apache_beam_rust_transformed");
    assert_eq!(decoded_output.timestamp_millis, 1700000000);
    assert_eq!(decoded_output.windows, vec![GlobalWindow]);
    assert_eq!(decoded_output.pane, beam::coders::PaneInfo::NO_FIRING);
}

#[tokio::test]
async fn test_param_windowed_value_input_bundle_execution() {
    let (data_out_tx, mut data_out_rx) = mpsc::channel(64);
    let data_manager = DataManager::new(data_out_tx);

    let mut handlers: HashMap<String, TransformFn> = HashMap::new();
    handlers.insert(
        STAGE_ID.to_string(),
        Arc::new(move |in_bytes: &[u8], sink: &mut dyn ElementSink| {
            let value = StringUtf8Coder
                .decode(&mut Cursor::new(in_bytes), Context::Nested)
                .map_err(|e| beam::Error::from(e).context("Decode failed"))?;

            let mut out_bytes = Vec::new();
            StringUtf8Coder
                .encode(
                    &format!("{value}_from_param"),
                    &mut out_bytes,
                    Context::Nested,
                )
                .map_err(|e| beam::Error::from(e).context("Encode failed"))?;

            sink.push(out_bytes)
        }),
    );

    let bundle_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    ));
    let control_client = ControlClient::new(bundle_processor);

    // The param_windowed_value payload holds timestamp 1700000000, GlobalWindow and NO_FIRING.
    let mut payload = Vec::new();
    let shifted = (1700000000u64 ^ (1u64 << 63)).to_be_bytes();
    payload.extend_from_slice(&shifted);
    payload.extend_from_slice(&1i32.to_be_bytes()); // 1 window
    // GlobalWindow encodes to 0 bytes
    beam::coders::PaneInfo::NO_FIRING
        .encode(false, &mut payload)
        .unwrap();
    payload.push(0); // placeholder bytes

    let mut coders = common::windowed_string_coders();
    coders.insert(
        "coder_param_windowed_string".to_string(),
        proto_pipeline::Coder {
            spec: Some(proto_pipeline::FunctionSpec {
                urn: URN_PARAM_WINDOWED_VALUE.to_string(),
                payload,
            }),
            component_coder_ids: vec![
                "coder_string".to_string(),
                "coder_global_window".to_string(),
            ],
        },
    );

    let desc = DescriptorBuilder::new("desc_param_windowed")
        .with_coders("coder_param_windowed_string", coders)
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink_with_coder(SINK_ID, "pcoll_output", common::CODER_WINDOWED_STRING)
        .build();

    control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_param_windowed".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![desc],
            })),
        })
        .await;

    // With param_windowed_value, the data stream carries only the bare string, in Nested context.
    let mut encoded_input = Vec::new();
    StringUtf8Coder
        .encode(
            &"test_word".to_string(),
            &mut encoded_input,
            Context::Nested,
        )
        .unwrap();

    tokio::spawn({
        let dm = data_manager.clone();
        async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            dm.handle_inbound_elements(Elements {
                data: vec![elements::Data {
                    instruction_id: "inst_param_windowed_1".to_string(),
                    transform_id: "source_transform".to_string(),
                    data: encoded_input,
                    is_last: true,
                }],
                timers: Vec::new(),
            })
            .await;
        }
    });

    let resp = within(
        "the bundle response",
        control_client.handle_instruction(InstructionRequest {
            instruction_id: "inst_param_windowed_1".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_param_windowed".to_string(),
                cache_tokens: Vec::new(),
                elements: None,
                has_no_state: false,
                only_bundle_for_keys: false,
                data_stream_id: String::new(),
            })),
        }),
    )
    .await;

    assert!(resp.error.is_empty(), "Bundle failed: {}", resp.error);

    let mut sink_bytes = Vec::new();
    while let Ok(elems) = data_out_rx.try_recv() {
        for d in elems.data {
            if !d.data.is_empty() {
                sink_bytes.extend_from_slice(&d.data);
            }
        }
    }

    assert!(!sink_bytes.is_empty(), "Must receive encoded output");

    let sink_coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut cursor = Cursor::new(sink_bytes);
    let decoded_output = sink_coder.decode(&mut cursor, Context::Nested).unwrap();

    assert_eq!(decoded_output.value, "test_word_from_param");
    assert_eq!(decoded_output.timestamp_millis, 1700000000);
    assert_eq!(decoded_output.windows, vec![GlobalWindow]);
    assert_eq!(decoded_output.pane, beam::coders::PaneInfo::NO_FIRING);
}

#[tokio::test]
async fn test_unwindowed_element_emitted_to_windowed_sink() {
    let (data_out_tx, mut data_out_rx) = mpsc::channel(64);
    let data_manager = DataManager::new(data_out_tx);

    let mut handlers: HashMap<String, TransformFn> = HashMap::new();
    handlers.insert(
        STAGE_ID.to_string(),
        Arc::new(move |_in_bytes: &[u8], sink: &mut dyn ElementSink| {
            let mut out_bytes = Vec::new();
            StringUtf8Coder
                .encode(
                    &"bare_to_windowed".to_string(),
                    &mut out_bytes,
                    Context::Nested,
                )
                .map_err(|e| beam::Error::from(e).context("Encode failed"))?;
            sink.push(out_bytes)
        }),
    );

    let bundle_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        handlers,
    ));
    let control_client = ControlClient::new(bundle_processor);

    let mut coders = common::windowed_string_coders();
    coders.extend(bytes_coder());

    let desc = DescriptorBuilder::new("desc_unwindowed_to_windowed")
        .with_coders(CODER_RAW, coders)
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink_with_coder(SINK_ID, "pcoll_output", common::CODER_WINDOWED_STRING)
        .build();

    control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_unwindowed_to_windowed".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![desc],
            })),
        })
        .await;

    tokio::spawn({
        let dm = data_manager.clone();
        async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            dm.handle_inbound_elements(Elements {
                data: vec![elements::Data {
                    instruction_id: "inst_u2w_1".to_string(),
                    transform_id: "source_transform".to_string(),
                    data: b"any_raw_bytes".to_vec(),
                    is_last: true,
                }],
                timers: Vec::new(),
            })
            .await;
        }
    });

    let resp = within(
        "the bundle response",
        control_client.handle_instruction(InstructionRequest {
            instruction_id: "inst_u2w_1".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_unwindowed_to_windowed".to_string(),
                cache_tokens: Vec::new(),
                elements: None,
                has_no_state: false,
                only_bundle_for_keys: false,
                data_stream_id: String::new(),
            })),
        }),
    )
    .await;

    assert!(resp.error.is_empty(), "Bundle failed: {}", resp.error);

    let mut sink_bytes = Vec::new();
    while let Ok(elems) = data_out_rx.try_recv() {
        for d in elems.data {
            if !d.data.is_empty() {
                sink_bytes.extend_from_slice(&d.data);
            }
        }
    }

    assert!(!sink_bytes.is_empty(), "Must receive encoded output");

    let sink_coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut cursor = Cursor::new(sink_bytes);
    let decoded_output = sink_coder.decode(&mut cursor, Context::Nested).unwrap();

    assert_eq!(decoded_output.value, "bare_to_windowed");
    assert_eq!(decoded_output.timestamp_millis, 0);
    assert_eq!(decoded_output.windows, vec![GlobalWindow]);
    assert_eq!(decoded_output.pane, beam::coders::PaneInfo::NO_FIRING);
}

#[tokio::test]
async fn test_sink_with_length_prefix_coder_wire_format() {
    let (data_out_tx, mut data_out_rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(data_out_tx);

    let bundle_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        identity_handlers(&[STAGE_ID]),
    ));
    let control_client = ControlClient::new(bundle_processor);

    let descriptor_id = "desc_sink_lp";
    let mut coders = common::windowed_string_coders();
    coders.insert(
        "coder_lp".to_string(),
        proto_pipeline::Coder {
            spec: Some(proto_pipeline::FunctionSpec {
                urn: URN_LENGTH_PREFIX.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: vec!["coder_string".to_string()],
        },
    );
    coders.insert(
        "coder_sink_wv".to_string(),
        proto_pipeline::Coder {
            spec: Some(proto_pipeline::FunctionSpec {
                urn: URN_WINDOWED_VALUE.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: vec!["coder_lp".to_string(), "coder_global_window".to_string()],
        },
    );

    let mut pbd = DescriptorBuilder::new(descriptor_id)
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build();
    pbd.coders = coders;
    if let Some(sink_t) = pbd.transforms.get_mut(SINK_ID) {
        let mut port_payload = Vec::new();
        RemoteGrpcPort {
            api_service_descriptor: None,
            coder_id: "coder_sink_wv".to_string(),
        }
        .encode(&mut port_payload)
        .unwrap();
        sink_t.spec = Some(proto_pipeline::FunctionSpec {
            urn: harness::bundle_processor::URN_DATA_SINK.to_string(),
            payload: port_payload,
        });
    }

    let reg_req = InstructionRequest {
        instruction_id: "inst_reg_lp".to_string(),
        request: Some(Request::Register(RegisterRequest {
            process_bundle_descriptor: vec![pbd],
        })),
    };
    let reg_resp = control_client.handle_instruction(reg_req).await;
    assert!(reg_resp.error.is_empty());

    let wv = WindowedValue::global("hello beam".to_string(), 12345);
    let mut input_data = Vec::new();
    let wv_coder = WindowedValueCoder::new(StringUtf8Coder);
    wv_coder
        .encode(&wv, &mut input_data, Context::Nested)
        .unwrap();

    tokio::spawn({
        let dm = data_manager.clone();
        let chunk = input_data.clone();
        async move {
            dm.handle_inbound_elements(Elements {
                data: vec![elements::Data {
                    instruction_id: "inst_pb_lp".to_string(),
                    transform_id: "source_transform".to_string(),
                    data: chunk,
                    is_last: false,
                }],
                timers: Vec::new(),
            })
            .await;
            dm.handle_inbound_elements(Elements {
                data: vec![elements::Data {
                    instruction_id: "inst_pb_lp".to_string(),
                    transform_id: "source_transform".to_string(),
                    data: Vec::new(),
                    is_last: true,
                }],
                timers: Vec::new(),
            })
            .await;
        }
    });

    let pb_req = InstructionRequest {
        instruction_id: "inst_pb_lp".to_string(),
        request: Some(Request::ProcessBundle(ProcessBundleRequest {
            process_bundle_descriptor_id: descriptor_id.to_string(),
            cache_tokens: Vec::new(),
            elements: None,
            has_no_state: false,
            only_bundle_for_keys: false,
            data_stream_id: String::new(),
        })),
    };

    let pb_resp = within(
        "the bundle response",
        control_client.handle_instruction(pb_req),
    )
    .await;
    assert!(pb_resp.error.is_empty(), "Bundle failed: {}", pb_resp.error);

    let mut sink_bytes = Vec::new();
    while let Ok(elements) = data_out_rx.try_recv() {
        for d in elements.data {
            if d.transform_id == SINK_ID && !d.data.is_empty() {
                sink_bytes.extend_from_slice(&d.data);
            }
        }
    }

    let mut cursor = Cursor::new(sink_bytes.as_slice());
    let mut ts = [0u8; 8];
    std::io::Read::read_exact(&mut cursor, &mut ts).unwrap();
    let mut wc = [0u8; 4];
    std::io::Read::read_exact(&mut cursor, &mut wc).unwrap();
    let mut pane = [0u8; 1];
    std::io::Read::read_exact(&mut cursor, &mut pane).unwrap();

    let elem_len = VarIntCoder::decode_varint(&mut cursor).unwrap();
    assert_eq!(
        elem_len, 11,
        "Element length prefix must match string encoding length"
    );
    let str_len = VarIntCoder::decode_varint(&mut cursor).unwrap();
    assert_eq!(str_len, 10);
    let mut str_bytes = vec![0u8; str_len as usize];
    std::io::Read::read_exact(&mut cursor, &mut str_bytes).unwrap();
    assert_eq!(str_bytes, b"hello beam");
}

/// A raw-bytes source -> identity -> sink descriptor whose output windows use `window_coder_id`.
fn declared_window_descriptor(id: &str, window_coder_id: &str) -> ProcessBundleDescriptor {
    DescriptorBuilder::new(id)
        .with_coders(CODER_RAW, bytes_coder())
        .with_coder(
            "coder_interval_window",
            beam::coders::URN_INTERVAL_WINDOW,
            &[],
        )
        .with_coder(
            "coder_prefixed_interval_window",
            URN_LENGTH_PREFIX,
            &["coder_interval_window"],
        )
        .with_coder("coder_global_window", beam::coders::URN_GLOBAL_WINDOW, &[])
        .windowing_strategy("windowing", window_coder_id)
        .pcollection("pcoll_output", CODER_RAW, "windowing")
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build()
}

/// The error names the collection, including when the window coder is length-prefixed.
#[tokio::test]
async fn an_element_without_window_bytes_into_an_interval_windowed_pcollection_fails_the_bundle() {
    for window_coder in ["coder_interval_window", "coder_prefixed_interval_window"] {
        let run = run_bundle(
            identity_handlers(&[STAGE_ID]),
            declared_window_descriptor("desc_interval_windowed", window_coder),
            vec![b"payload".to_vec()],
        )
        .await;

        let error = run.response.error;
        assert!(
            error.contains("pcoll_output") && error.contains(beam::coders::URN_INTERVAL_WINDOW),
            "{window_coder}: {error}"
        );
    }
}

/// The global window encodes to nothing, so missing window bytes are valid here.
#[tokio::test]
async fn a_globally_windowed_declared_pcollection_accepts_elements() {
    let run = run_bundle(
        identity_handlers(&[STAGE_ID]),
        declared_window_descriptor("desc_globally_windowed", "coder_global_window"),
        vec![b"payload".to_vec()],
    )
    .await;
    run.bundle_response();

    assert_eq!(run.sink_bytes.get(SINK_ID), Some(&b"payload".to_vec()));
}

/// Source -> `STAGE_ID` -> sink over `WindowedValue<length_prefix<string>, GlobalWindow>`.
fn length_prefixed_string_descriptor(id: &str) -> ProcessBundleDescriptor {
    DescriptorBuilder::new(id)
        .with_coder(
            "coder_prefixed_string",
            URN_LENGTH_PREFIX,
            &["coder_string"],
        )
        .with_coder(
            "coder_windowed_prefixed_string",
            URN_WINDOWED_VALUE,
            &["coder_prefixed_string", "coder_global_window"],
        )
        .with_source_coder("coder_windowed_prefixed_string")
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build()
}

/// Covers every element of the chunk, not only the first.
#[tokio::test]
async fn length_prefixed_inbound_elements_reach_the_handler_without_their_prefix() {
    let observer = Observer::default();
    let header = beam::coders::WindowedHeader::global(0, beam::coders::PaneInfo::NO_FIRING);
    let mut chunk = Vec::new();
    for value in ["hello", "world!"] {
        chunk.extend_from_slice(header.as_bytes());
        VarIntCoder::encode_varint(value.len() as i64, &mut chunk).unwrap();
        chunk.extend_from_slice(value.as_bytes());
    }

    let run = run_bundle(
        HashMap::from([(STAGE_ID.to_string(), observer.handler())]),
        length_prefixed_string_descriptor("desc_prefixed_inbound"),
        vec![chunk],
    )
    .await;
    run.bundle_response();

    let delivered: Vec<Vec<u8>> = observer.seen().into_iter().map(|o| o.element).collect();
    assert_eq!(delivered, vec![b"hello".to_vec(), b"world!".to_vec()]);
}

/// An encoded interval window: end instant (sign bit flipped, big-endian), then span as VarInt.
fn interval_window(end_millis: i64, span_millis: i64) -> Vec<u8> {
    let mut window = ((end_millis as u64) ^ (1 << 63)).to_be_bytes().to_vec();
    VarIntCoder::encode_varint(span_millis, &mut window).expect("encoding into a Vec cannot fail");
    window
}

/// Later headers start part-way into the chunk, so pane offsets are relative to each header.
#[tokio::test]
async fn a_later_element_in_a_chunk_keeps_its_own_window_and_pane() {
    use beam::coders::{PaneInfo, Timing, WindowedHeader};

    let observer = Observer::default();
    let descriptor = DescriptorBuilder::new("desc_later_element_header")
        .with_coder(
            "coder_interval_window",
            beam::coders::URN_INTERVAL_WINDOW,
            &[],
        )
        .with_coder(
            "coder_windowed_interval_string",
            URN_WINDOWED_VALUE,
            &["coder_string", "coder_interval_window"],
        )
        .with_source_coder("coder_windowed_interval_string")
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build();
    let headers = [
        WindowedHeader::new(
            1_000,
            &[interval_window(10_000, 10_000)],
            PaneInfo::NO_FIRING,
        ),
        WindowedHeader::new(
            25_000,
            &[interval_window(30_000, 10_000)],
            PaneInfo::new(false, true, Timing::Late, 3, 2),
        ),
    ];
    let mut chunk = Vec::new();
    for (header, value) in headers.iter().zip(["first", "second"]) {
        chunk.extend_from_slice(header.as_bytes());
        StringUtf8Coder
            .encode(&value.to_string(), &mut chunk, Context::Nested)
            .unwrap();
    }

    let run = run_bundle(
        HashMap::from([(STAGE_ID.to_string(), observer.handler())]),
        descriptor,
        vec![chunk],
    )
    .await;
    run.bundle_response();

    let seen = observer.seen();
    let panes: Vec<PaneInfo> = seen.iter().map(common::Observed::pane).collect();
    assert_eq!(
        panes,
        headers.iter().map(WindowedHeader::pane).collect::<Vec<_>>()
    );
    let windows: Vec<&[u8]> = seen.iter().map(|o| o.header.window_bytes()).collect();
    assert_eq!(
        windows,
        headers
            .iter()
            .map(WindowedHeader::window_bytes)
            .collect::<Vec<_>>()
    );
    let delivered: Vec<WindowedHeader> = seen.into_iter().map(|o| o.header).collect();
    assert_eq!(delivered, headers);
}
