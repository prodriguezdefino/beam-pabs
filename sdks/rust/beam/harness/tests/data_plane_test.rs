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

//! Integration tests for the `BeamFnData` transport.
//!
//! Covers multiplexing, buffering of data that arrives before its channel registers,
//! multi-chunk ordering, both EOF styles and named data streams.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

use beam::coders::{Coder, Context, StringUtf8Coder, WindowedValue, WindowedValueCoder};
use harness::bundle_processor::BundleProcessor;
use harness::control::ControlClient;
use harness::data::{DataError, DataManager, DataStreamConnector, INBOUND_QUEUE_CHUNKS};
use model::fn_execution::{
    Elements, InstructionRequest, ProcessBundleRequest, RegisterRequest, elements,
    instruction_request::Request,
};

mod common;
use common::{SINK_ID, STAGE_ID, identity_handlers, linear_descriptor, within};

#[tokio::test]
async fn test_concurrent_bundle_multiplexing() {
    let (data_out_tx, mut data_out_rx) = mpsc::channel(64);
    let data_manager = DataManager::new(data_out_tx);
    let bundle_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        identity_handlers(&[STAGE_ID]),
    ));
    let control_client = ControlClient::new(bundle_processor);

    let desc = linear_descriptor("desc_concurrent");
    control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_concurrent".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![desc],
            })),
        })
        .await;

    // Send interleaved chunks for both instructions while the two bundles run.
    tokio::spawn({
        let dm = data_manager.clone();
        async move {
            // Let the bundles start before the data arrives.
            tokio::time::sleep(Duration::from_millis(15)).await;

            // Each frame carries one chunk for each instruction, in a different order.
            dm.handle_inbound_elements(Elements {
                data: vec![
                    elements::Data {
                        instruction_id: "inst_1".to_string(),
                        transform_id: "source_transform".to_string(),
                        data: b"hello_bundle_1_part1-".to_vec(),
                        is_last: false,
                    },
                    elements::Data {
                        instruction_id: "inst_2".to_string(),
                        transform_id: "source_transform".to_string(),
                        data: b"alpha_bundle_2_part1-".to_vec(),
                        is_last: false,
                    },
                ],
                timers: Vec::new(),
            })
            .await;

            dm.handle_inbound_elements(Elements {
                data: vec![
                    elements::Data {
                        instruction_id: "inst_2".to_string(),
                        transform_id: "source_transform".to_string(),
                        data: b"alpha_bundle_2_part2".to_vec(),
                        is_last: false,
                    },
                    elements::Data {
                        instruction_id: "inst_1".to_string(),
                        transform_id: "source_transform".to_string(),
                        data: b"hello_bundle_1_part2".to_vec(),
                        is_last: false,
                    },
                ],
                timers: Vec::new(),
            })
            .await;

            // The last frame carries only the EOF markers.
            dm.handle_inbound_elements(Elements {
                data: vec![
                    elements::Data {
                        instruction_id: "inst_1".to_string(),
                        transform_id: "source_transform".to_string(),
                        data: Vec::new(),
                        is_last: true,
                    },
                    elements::Data {
                        instruction_id: "inst_2".to_string(),
                        transform_id: "source_transform".to_string(),
                        data: Vec::new(),
                        is_last: true,
                    },
                ],
                timers: Vec::new(),
            })
            .await;
        }
    });

    let client1 = control_client.clone();
    let task1 = tokio::spawn(async move {
        client1
            .handle_instruction(InstructionRequest {
                instruction_id: "inst_1".to_string(),
                request: Some(Request::ProcessBundle(ProcessBundleRequest {
                    process_bundle_descriptor_id: "desc_concurrent".to_string(),
                    cache_tokens: Vec::new(),
                    elements: None,
                    has_no_state: false,
                    only_bundle_for_keys: false,
                    data_stream_id: String::new(),
                })),
            })
            .await
    });

    let client2 = control_client.clone();
    let task2 = tokio::spawn(async move {
        client2
            .handle_instruction(InstructionRequest {
                instruction_id: "inst_2".to_string(),
                request: Some(Request::ProcessBundle(ProcessBundleRequest {
                    process_bundle_descriptor_id: "desc_concurrent".to_string(),
                    cache_tokens: Vec::new(),
                    elements: None,
                    has_no_state: false,
                    only_bundle_for_keys: false,
                    data_stream_id: String::new(),
                })),
            })
            .await
    });

    let (res1, res2) = within("both bundles", async { tokio::join!(task1, task2) }).await;
    let resp1 = res1.unwrap();
    let resp2 = res2.unwrap();

    assert!(resp1.error.is_empty(), "Bundle 1 failed: {}", resp1.error);
    assert!(resp2.error.is_empty(), "Bundle 2 failed: {}", resp2.error);

    // The output of each instruction contains only its own chunks, in order.
    let mut data1 = Vec::new();
    let mut data2 = Vec::new();
    let mut is_last_1 = false;
    let mut is_last_2 = false;

    while let Ok(elems) = data_out_rx.try_recv() {
        for d in elems.data {
            if d.instruction_id == "inst_1" {
                if !d.data.is_empty() {
                    data1.extend_from_slice(&d.data);
                }
                if d.is_last {
                    is_last_1 = true;
                }
            } else if d.instruction_id == "inst_2" {
                if !d.data.is_empty() {
                    data2.extend_from_slice(&d.data);
                }
                if d.is_last {
                    is_last_2 = true;
                }
            }
        }
    }

    assert_eq!(data1, b"hello_bundle_1_part1-hello_bundle_1_part2");
    assert_eq!(data2, b"alpha_bundle_2_part1-alpha_bundle_2_part2");
    assert!(is_last_1, "Bundle 1 must emit EOF");
    assert!(is_last_2, "Bundle 2 must emit EOF");
}
#[tokio::test]
async fn test_empty_vs_non_empty_is_last_eof() {
    let (data_out_tx, mut data_out_rx) = mpsc::channel(64);
    let data_manager = DataManager::new(data_out_tx);
    let bundle_processor = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        identity_handlers(&[STAGE_ID]),
    ));
    let control_client = ControlClient::new(bundle_processor);

    let desc = linear_descriptor("desc_eof_styles");
    control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_eof".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![desc],
            })),
        })
        .await;

    // A frame with a payload and is_last set ends the input.
    tokio::spawn({
        let dm = data_manager.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            dm.handle_inbound_elements(Elements {
                data: vec![elements::Data {
                    instruction_id: "inst_eof_direct".to_string(),
                    transform_id: "source_transform".to_string(),
                    data: b"direct_eof_payload".to_vec(),
                    is_last: true,
                }],
                timers: Vec::new(),
            })
            .await;
        }
    });

    let resp = within(
        "the bundle",
        control_client.handle_instruction(InstructionRequest {
            instruction_id: "inst_eof_direct".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_eof_styles".to_string(),
                cache_tokens: Vec::new(),
                elements: None,
                has_no_state: false,
                only_bundle_for_keys: false,
                data_stream_id: String::new(),
            })),
        }),
    )
    .await;
    assert!(resp.error.is_empty());

    let mut output = Vec::new();
    let mut saw_eof = false;
    while let Ok(elems) = data_out_rx.try_recv() {
        for d in elems.data {
            if !d.data.is_empty() {
                output.extend_from_slice(&d.data);
            }
            if d.is_last {
                saw_eof = true;
            }
        }
    }
    assert_eq!(output, b"direct_eof_payload");
    assert!(saw_eof, "Direct EOF must emit outbound EOF marker");

    // A payload frame followed by an empty frame with is_last set also ends the input.
    data_manager
        .handle_inbound_elements(Elements {
            data: vec![
                elements::Data {
                    instruction_id: "inst_eof_separate".to_string(),
                    transform_id: "source_transform".to_string(),
                    data: b"separate_eof_payload".to_vec(),
                    is_last: false,
                },
                elements::Data {
                    instruction_id: "inst_eof_separate".to_string(),
                    transform_id: "source_transform".to_string(),
                    data: Vec::new(),
                    is_last: true,
                },
            ],
            timers: Vec::new(),
        })
        .await;
    let resp = within(
        "the bundle",
        control_client.handle_instruction(InstructionRequest {
            instruction_id: "inst_eof_separate".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_eof_styles".to_string(),
                ..Default::default()
            })),
        }),
    )
    .await;
    assert!(resp.error.is_empty(), "{}", resp.error);

    let chunks: Vec<_> = std::iter::from_fn(|| data_out_rx.try_recv().ok())
        .flat_map(|elems| elems.data)
        .map(|d| (d.data, d.is_last))
        .collect();
    // The empty EOF frame is not an element. Expect one payload chunk, then one EOF.
    assert_eq!(
        chunks,
        [
            (b"separate_eof_payload".to_vec(), false),
            (Vec::new(), true)
        ]
    );
}
#[tokio::test]
async fn test_named_data_stream_routing() {
    let (default_tx, mut default_rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(default_tx);

    let opened_streams = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let outbound_rxs = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    struct MockConnector {
        streams: Arc<std::sync::Mutex<HashMap<String, mpsc::Sender<Elements>>>>,
        rxs: Arc<tokio::sync::Mutex<HashMap<String, mpsc::Receiver<Elements>>>>,
    }

    #[async_trait::async_trait]
    impl DataStreamConnector for MockConnector {
        async fn connect(&self, data_stream_id: &str) -> Result<mpsc::Sender<Elements>, DataError> {
            let (tx, rx) = mpsc::channel(64);
            self.streams
                .lock()
                .unwrap()
                .insert(data_stream_id.to_string(), tx.clone());
            self.rxs.lock().await.insert(data_stream_id.to_string(), rx);
            Ok(tx)
        }
    }

    data_manager.set_connector(Arc::new(MockConnector {
        streams: opened_streams.clone(),
        rxs: outbound_rxs.clone(),
    }));

    data_manager
        .ensure_stream("named-stream-1")
        .await
        .expect("Stream connection should succeed");
    assert!(
        opened_streams
            .lock()
            .unwrap()
            .contains_key("named-stream-1")
    );

    // Outbound data for a named stream goes only to that stream.
    data_manager
        .send_data("named-stream-1", "inst-1", "transform-1", vec![1, 2, 3])
        .await
        .expect("send_data should succeed");

    assert!(
        default_rx.try_recv().is_err(),
        "Default channel should not receive named stream data"
    );

    let mut guard = outbound_rxs.lock().await;
    let rx = guard
        .get_mut("named-stream-1")
        .expect("Stream receiver should exist");
    let elem = within("the named stream element", rx.recv())
        .await
        .expect("Expected element on named stream");
    assert_eq!(elem.data.len(), 1);
    assert_eq!(elem.data[0].instruction_id, "inst-1");
    assert_eq!(elem.data[0].data, vec![1, 2, 3]);
    assert!(!elem.data[0].is_last);

    // Outbound data with an empty stream id goes to the default channel.
    data_manager
        .send_data("", "inst-2", "transform-1", vec![4, 5, 6])
        .await
        .expect("send_data should succeed");

    let default_elem = within("the default stream element", default_rx.recv())
        .await
        .expect("Expected element on default stream");
    assert_eq!(default_elem.data.len(), 1);
    assert_eq!(default_elem.data[0].instruction_id, "inst-2");
    assert_eq!(default_elem.data[0].data, vec![4, 5, 6]);
}
#[tokio::test]
async fn test_bundle_execution_with_named_data_stream() {
    let descriptor_id = "desc-named-stream";
    let descriptor = linear_descriptor(descriptor_id);

    let (default_tx, mut default_rx) = mpsc::channel::<Elements>(64);
    let data_manager = DataManager::new(default_tx);

    let opened_streams = Arc::new(std::sync::Mutex::new(HashMap::new()));
    let outbound_rxs = Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    struct MockConnector {
        streams: Arc<std::sync::Mutex<HashMap<String, mpsc::Sender<Elements>>>>,
        rxs: Arc<tokio::sync::Mutex<HashMap<String, mpsc::Receiver<Elements>>>>,
    }

    #[async_trait::async_trait]
    impl DataStreamConnector for MockConnector {
        async fn connect(&self, data_stream_id: &str) -> Result<mpsc::Sender<Elements>, DataError> {
            let (tx, rx) = mpsc::channel(64);
            self.streams
                .lock()
                .unwrap()
                .insert(data_stream_id.to_string(), tx.clone());
            self.rxs.lock().await.insert(data_stream_id.to_string(), rx);
            Ok(tx)
        }
    }

    data_manager.set_connector(Arc::new(MockConnector {
        streams: opened_streams.clone(),
        rxs: outbound_rxs.clone(),
    }));

    let bp = Arc::new(BundleProcessor::with_handlers(
        data_manager.clone(),
        identity_handlers(&[STAGE_ID]),
    ));
    let ctrl_client = ControlClient::new(bp.clone());

    let reg_req = InstructionRequest {
        instruction_id: "reg-named".to_string(),
        request: Some(Request::Register(RegisterRequest {
            process_bundle_descriptor: vec![descriptor],
        })),
    };
    let reg_resp = ctrl_client.handle_instruction(reg_req).await;
    assert!(reg_resp.error.is_empty());

    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let original_element = WindowedValue::global("named_stream_payload".to_string(), 1700000000);
    let mut encoded_input = Vec::new();
    coder
        .encode(&original_element, &mut encoded_input, Context::Nested)
        .unwrap();

    // Send the element and the EOF from another task while the bundle runs.
    let dm_for_inbound = data_manager.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        dm_for_inbound
            .handle_inbound_elements(Elements {
                data: vec![elements::Data {
                    instruction_id: "proc-named-1".to_string(),
                    transform_id: "source_transform".to_string(),
                    data: encoded_input,
                    is_last: false,
                }],
                timers: Vec::new(),
            })
            .await;
        dm_for_inbound
            .handle_inbound_elements(Elements {
                data: vec![elements::Data {
                    instruction_id: "proc-named-1".to_string(),
                    transform_id: "source_transform".to_string(),
                    data: Vec::new(),
                    is_last: true,
                }],
                timers: Vec::new(),
            })
            .await;
    });

    // The bundle request names its own data stream.
    let proc_req = InstructionRequest {
        instruction_id: "proc-named-1".to_string(),
        request: Some(Request::ProcessBundle(ProcessBundleRequest {
            process_bundle_descriptor_id: descriptor_id.to_string(),
            data_stream_id: "bundle-stream-99".to_string(),
            ..Default::default()
        })),
    };

    let proc_resp = within(
        "the named-stream bundle",
        ctrl_client.handle_instruction(proc_req),
    )
    .await;
    assert!(proc_resp.error.is_empty());

    // The bundle output goes to its named stream, not to the default channel.
    assert!(default_rx.try_recv().is_err());

    let mut guard = outbound_rxs.lock().await;
    let named_rx = guard
        .get_mut("bundle-stream-99")
        .expect("Named stream receiver should exist");

    let received_chunks: Vec<_> = std::iter::from_fn(|| named_rx.try_recv().ok())
        .flat_map(|elem| elem.data)
        .collect();

    // The bytes coder reads the chunk as one element; identity forwards it, then the sink closes.
    let expected_payload = {
        let mut bytes = Vec::new();
        coder
            .encode(&original_element, &mut bytes, Context::Nested)
            .unwrap();
        bytes
    };
    assert_eq!(
        received_chunks,
        [
            elements::Data {
                instruction_id: "proc-named-1".to_string(),
                transform_id: SINK_ID.to_string(),
                data: expected_payload,
                is_last: false,
            },
            elements::Data {
                instruction_id: "proc-named-1".to_string(),
                transform_id: SINK_ID.to_string(),
                data: Vec::new(),
                is_last: true,
            },
        ]
    );
    assert_eq!(
        opened_streams.lock().unwrap().keys().collect::<Vec<_>>(),
        ["bundle-stream-99"],
        "the bundle's stream is opened once, on demand"
    );
}

fn chunk(instruction_id: &str, data: &[u8], is_last: bool) -> Elements {
    Elements {
        data: vec![elements::Data {
            instruction_id: instruction_id.to_string(),
            transform_id: "source_transform".to_string(),
            data: data.to_vec(),
            is_last,
        }],
        timers: Vec::new(),
    }
}

#[tokio::test]
async fn data_for_an_ended_instruction_is_dropped() {
    let (tx, _rx) = mpsc::channel(4);
    let dm = DataManager::new(tx);
    drop(dm.register_inbound("inst_done").await);
    dm.unregister_inbound("inst_done").await;

    dm.handle_inbound_elements(chunk("inst_done", b"late", true))
        .await;

    // Had the late chunk been buffered, a new registration would replay it.
    let mut rx = dm.register_inbound("inst_done").await;
    assert!(rx.try_recv().is_err(), "late data must not be buffered");
    // An ended instruction gets no more data, so its new receiver is closed.
    assert_eq!(
        rx.try_recv(),
        Err(TryRecvError::Disconnected),
        "an ended instruction registers closed"
    );
}

#[tokio::test]
async fn a_closed_data_stream_fails_waiting_and_later_bundles() {
    let (tx, _rx) = mpsc::channel(4);
    let dm = DataManager::new(tx);
    let mut waiting = dm.register_inbound("inst_waiting").await;

    dm.channel_state()
        .lock()
        .await
        .close("stream gone".to_string());

    assert_eq!(
        within("the waiting receiver", waiting.recv()).await,
        None,
        "closed before is_last"
    );
    let mut later = dm.register_inbound("inst_later").await;
    assert_eq!(
        within("the later receiver", later.recv()).await,
        None,
        "nothing can arrive any more"
    );
}

#[tokio::test]
async fn a_full_inbound_queue_pushes_back_on_the_stream() {
    let (tx, _rx) = mpsc::channel(4);
    let dm = DataManager::new(tx);
    let mut rx = dm.register_inbound("inst_slow").await;

    let reader = dm.clone();
    let stream = tokio::spawn(async move {
        for _ in 0..=INBOUND_QUEUE_CHUNKS {
            reader
                .handle_inbound_elements(chunk("inst_slow", b"x", false))
                .await;
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !stream.is_finished(),
        "the stream waits for the bundle to read"
    );

    assert_eq!(
        within("the first chunk", rx.recv()).await,
        Some(Some(b"x".to_vec()))
    );
    within("the stream", stream).await.expect("stream task");
}

#[tokio::test]
async fn a_finished_bundle_does_not_hold_up_the_stream() {
    let (tx, _rx) = mpsc::channel(4);
    let dm = DataManager::new(tx);
    let rx = dm.register_inbound("inst_gone").await;
    drop(rx);

    let sends = (0..=INBOUND_QUEUE_CHUNKS)
        .map(|_| dm.handle_inbound_elements(chunk("inst_gone", b"x", false)));
    tokio::time::timeout(Duration::from_secs(5), async {
        for send in sends {
            send.await;
        }
    })
    .await
    .expect("sends to a dropped receiver return at once");
}

#[tokio::test]
async fn early_data_is_bounded_like_registered_data_and_never_dropped() {
    let (tx, _rx) = mpsc::channel(4);
    let dm = DataManager::new(tx);

    let reader = dm.clone();
    let stream = tokio::spawn(async move {
        for _ in 0..=INBOUND_QUEUE_CHUNKS {
            reader
                .handle_inbound_elements(chunk("inst_early_full", b"x", false))
                .await;
        }
        reader
            .handle_inbound_elements(chunk("inst_early_full", b"", true))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !stream.is_finished(),
        "a full early queue makes the stream wait"
    );

    let mut rx = dm.register_inbound("inst_early_full").await;
    let mut chunks = 0;
    while let Some(Some(_)) = within("the next early chunk", rx.recv()).await {
        chunks += 1;
    }
    assert_eq!(
        chunks,
        INBOUND_QUEUE_CHUNKS + 1,
        "every early chunk is delivered"
    );
    within("the stream", stream).await.expect("stream task");
}

#[tokio::test(start_paused = true)]
async fn an_instruction_that_never_registers_is_abandoned() {
    let (tx, _rx) = mpsc::channel(4);
    let dm = DataManager::new(tx);

    // With time paused, the stream's wait on the full, unclaimed queue runs out at once.
    for _ in 0..=INBOUND_QUEUE_CHUNKS {
        dm.handle_inbound_elements(chunk("inst_never", b"x", false))
            .await;
    }

    let mut rx = dm.register_inbound("inst_never").await;
    assert_eq!(
        within("the abandoned receiver", rx.recv()).await,
        None,
        "an abandoned instruction fails"
    );
}

/// Must equal `UNCLAIMED_DATA_TIMEOUT` in `src/data/inbound.rs`.
const UNCLAIMED_DATA_TIMEOUT: Duration = Duration::from_secs(3 * 60 * 60);

#[tokio::test(start_paused = true)]
async fn an_unregistered_instruction_keeps_its_data_until_the_three_hour_timeout() {
    let (tx, _rx) = mpsc::channel(4);
    let dm = DataManager::new(tx);
    let stream = tokio::spawn({
        let dm = dm.clone();
        async move {
            for _ in 0..=INBOUND_QUEUE_CHUNKS {
                dm.handle_inbound_elements(chunk("inst_late", b"x", false))
                    .await;
            }
            dm.handle_inbound_elements(chunk("inst_late", b"", true))
                .await;
        }
    });

    tokio::time::sleep(UNCLAIMED_DATA_TIMEOUT - Duration::from_secs(1)).await;
    let mut rx = dm.register_inbound("inst_late").await;
    let mut chunks = 0;
    while let Some(Some(_)) = within("the next chunk", rx.recv()).await {
        chunks += 1;
    }

    assert_eq!(chunks, INBOUND_QUEUE_CHUNKS + 1, "no chunk was dropped");
    within("the stream", stream).await.unwrap();
}

/// The data plane remembers the last 10,000 ended instructions; older ones buffer data again.
#[tokio::test]
async fn the_last_ten_thousand_ended_instructions_are_remembered() {
    const REMEMBERED: usize = 10_000;
    let (tx, _rx) = mpsc::channel(4);
    let dm = DataManager::new(tx);
    let id = |i: usize| format!("inst_ended_{i}");
    for i in 0..=REMEMBERED {
        drop(dm.register_inbound(&id(i)).await);
        dm.unregister_inbound(&id(i)).await;
    }

    let late = Ok(Some(b"late".to_vec()));
    let dropped = Err(TryRecvError::Disconnected);
    for (i, expected) in [(0, late), (1, dropped.clone()), (REMEMBERED, dropped)] {
        dm.handle_inbound_elements(chunk(&id(i), b"late", false))
            .await;
        let mut rx = dm.register_inbound(&id(i)).await;
        assert_eq!(rx.try_recv(), expected, "ended instruction {i}");
    }
}

/// Which of an instruction's inbound queues a case registers and unregisters.
#[derive(Debug, Clone, Copy)]
enum Inbound {
    Data,
    Timers,
}

#[tokio::test]
async fn unregistering_inbound_closes_the_bundles_receiver() {
    let (tx, _rx) = mpsc::channel(4);
    let dm = DataManager::new(tx);
    for kind in [Inbound::Data, Inbound::Timers] {
        let id = format!("inst_unregistered_{kind:?}");
        let received = match kind {
            Inbound::Data => {
                let mut rx = dm.register_inbound(&id).await;
                dm.unregister_inbound(&id).await;
                rx.try_recv().map(drop)
            }
            Inbound::Timers => {
                let mut rx = dm.register_inbound_timers(&id).await;
                dm.unregister_inbound_timers(&id).await;
                rx.try_recv().map(drop)
            }
        };
        assert_eq!(received, Err(TryRecvError::Disconnected), "{kind:?}");
    }
}

#[tokio::test]
async fn a_closed_data_stream_fails_waiting_timer_receivers() {
    let (tx, _rx) = mpsc::channel(4);
    let dm = DataManager::new(tx);
    let mut timers = dm.register_inbound_timers("inst_timers_only").await;

    dm.channel_state()
        .lock()
        .await
        .close("stream gone".to_string());

    assert_eq!(
        within("the timer receiver", timers.recv()).await,
        None,
        "closed before is_last"
    );
}

#[tokio::test]
async fn ensure_stream_connects_each_named_stream_once_and_never_the_default() {
    struct RecordingConnector {
        connected: Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl DataStreamConnector for RecordingConnector {
        async fn connect(&self, data_stream_id: &str) -> Result<mpsc::Sender<Elements>, DataError> {
            self.connected
                .lock()
                .expect("connected lock")
                .push(data_stream_id.to_string());
            Ok(mpsc::channel(4).0)
        }
    }

    let (tx, _rx) = mpsc::channel(4);
    let dm = DataManager::new(tx);
    let connected = Arc::new(std::sync::Mutex::new(Vec::new()));
    dm.set_connector(Arc::new(RecordingConnector {
        connected: connected.clone(),
    }));

    for id in ["", "named", "named", ""] {
        dm.ensure_stream(id).await.unwrap();
    }

    assert_eq!(*connected.lock().unwrap(), ["named"]);
}

#[tokio::test(start_paused = true)]
async fn an_instruction_that_registers_while_its_queue_is_full_is_not_abandoned() {
    let (tx, _rx) = mpsc::channel(4);
    let dm = DataManager::new(tx);
    for _ in 0..INBOUND_QUEUE_CHUNKS {
        dm.handle_inbound_elements(chunk("inst_racing", b"x", false))
            .await;
    }
    let stream = tokio::spawn({
        let dm = dm.clone();
        async move {
            dm.handle_inbound_elements(chunk("inst_racing", b"x", false))
                .await;
            dm.handle_inbound_elements(chunk("inst_racing", b"", true))
                .await;
        }
    });
    // Let the stream block sending the extra chunk to the unclaimed queue.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(!stream.is_finished(), "the stream waits on the full queue");

    let mut rx = dm.register_inbound("inst_racing").await;
    tokio::time::sleep(UNCLAIMED_DATA_TIMEOUT + Duration::from_secs(60)).await;
    let mut chunks = 0;
    while let Some(Some(_)) = within("the next chunk", rx.recv()).await {
        chunks += 1;
    }

    assert_eq!(
        chunks,
        INBOUND_QUEUE_CHUNKS + 1,
        "the chunk in flight when the instruction registered is delivered"
    );
    within("the stream", stream).await.unwrap();
}
