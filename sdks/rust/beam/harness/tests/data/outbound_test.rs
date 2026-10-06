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

//! Integration tests for a bundle's outbound data: megabyte batching and backpressure.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;

use beam_harness::data::{
    DataManager, FlushLimits, INBOUND_QUEUE_CHUNKS, OUTBOUND_FLUSH_BYTES, Outbound,
};
use beam_model::fn_execution::Elements;

use crate::common::within;

/// Must equal `OUTBOUND_PENDING_MESSAGES` in `src/data/outbound.rs`.
const PENDING_MESSAGES: usize = 64;

const INSTRUCTION: &str = "inst_out";

/// One data chunk, summarised: transform, length, distinct bytes, `is_last`.
type Summary = (String, usize, Vec<u8>, bool);

/// Appends `len` copies of `byte` to `sink`.
fn write(out: &mut Outbound, sink: usize, len: usize, byte: u8) {
    out.write(sink, |buf| {
        buf.resize(buf.len() + len, byte);
        Ok(())
    })
    .expect("write to a known sink");
}

/// Every message already queued, without waiting.
fn queued(rx: &mut mpsc::Receiver<Elements>) -> Vec<Elements> {
    std::iter::from_fn(|| rx.try_recv().ok()).collect()
}

/// Summarises each message's chunks so a failure does not print megabytes.
/// Every chunk must be for [`INSTRUCTION`].
fn summarise(messages: &[Elements]) -> Vec<Vec<Summary>> {
    messages
        .iter()
        .map(|elements| {
            elements
                .data
                .iter()
                .map(|data| {
                    assert_eq!(data.instruction_id, INSTRUCTION);
                    let mut bytes = data.data.clone();
                    bytes.dedup();
                    (
                        data.transform_id.clone(),
                        data.data.len(),
                        bytes,
                        data.is_last,
                    )
                })
                .collect()
        })
        .collect()
}

#[tokio::test]
async fn outbound_flushes_at_a_megabyte_and_not_before() {
    /// Writes as (sink, bytes).
    type Writes = &'static [(usize, usize)];
    const MB: usize = OUTBOUND_FLUSH_BYTES;
    // (case, writes, bytes per message sent)
    let cases: [(&str, Writes, &[usize]); 6] = [
        ("one byte short", &[(0, MB - 1)], &[]),
        ("exactly a megabyte", &[(0, MB)], &[MB]),
        (
            "two halves one byte short",
            &[(0, MB / 2), (0, MB / 2 - 1)],
            &[],
        ),
        ("two halves", &[(0, MB / 2), (0, MB / 2)], &[MB]),
        (
            "across sinks one byte short",
            &[(0, MB / 2), (1, MB / 2 - 1)],
            &[],
        ),
        (
            "a byte past a megabyte",
            &[(0, 1), (0, MB - 1), (0, 1)],
            &[MB],
        ),
    ];
    for (case, writes, expected) in cases {
        let (tx, mut rx) = mpsc::channel(16);
        let dm = DataManager::new(tx);
        let mut out = dm.outbound(
            "",
            INSTRUCTION,
            ["sink_a", "sink_b"],
            std::iter::empty::<(&str, &str)>(),
        );
        for &(sink, len) in writes {
            write(&mut out, sink, len, 1);
        }
        let sent: Vec<usize> = queued(&mut rx)
            .iter()
            .map(|elements| elements.data.iter().map(|data| data.data.len()).sum())
            .collect();
        assert_eq!(sent, expected, "{case}");
    }

    // A full megabyte goes out as one chunk, for the written sink only.
    let (tx, mut rx) = mpsc::channel(16);
    let dm = DataManager::new(tx);
    let mut out = dm.outbound(
        "",
        INSTRUCTION,
        ["sink_a", "sink_b", "sink_c"],
        std::iter::empty::<(&str, &str)>(),
    );
    write(&mut out, 1, OUTBOUND_FLUSH_BYTES, 7);
    assert_eq!(
        summarise(&queued(&mut rx)),
        [[("sink_b".to_string(), OUTBOUND_FLUSH_BYTES, vec![7], false)]]
    );
    assert!(!out.has_pending());
}

/// A drain sends the held messages in order.
#[tokio::test]
async fn drain_sends_held_messages_in_order() {
    let (tx, mut rx) = mpsc::channel(1);
    let dm = DataManager::new(tx);
    let mut out = dm.outbound(
        "",
        INSTRUCTION,
        ["sink"],
        std::iter::empty::<(&str, &str)>(),
    );
    // One message fills the queue and the others stay within the limit of held messages.
    let messages = 1 + PENDING_MESSAGES;

    write(&mut out, 0, OUTBOUND_FLUSH_BYTES, 0);
    assert!(!out.has_pending(), "the first message fits in the queue");
    for i in 1..messages {
        write(&mut out, 0, OUTBOUND_FLUSH_BYTES, i as u8);
        assert!(out.has_pending(), "message {i} is held");
    }

    let reader = tokio::spawn(async move {
        let mut received = Vec::new();
        while let Some(elements) = rx.recv().await {
            let done = elements.data.iter().any(|data| data.is_last);
            received.push(elements);
            if done {
                break;
            }
        }
        received
    });
    within("drain", out.drain()).await.unwrap();
    assert!(!out.has_pending(), "drain sends everything held");
    within("finish", out.finish()).await.unwrap();

    let received = summarise(&within("the reader", reader).await.unwrap());
    let expected: Vec<Vec<Summary>> = (0..messages)
        .map(|i| {
            vec![(
                "sink".to_string(),
                OUTBOUND_FLUSH_BYTES,
                vec![i as u8],
                false,
            )]
        })
        .chain([vec![("sink".to_string(), 0, Vec::new(), true)]])
        .collect();
    assert_eq!(received, expected);
}

/// A flush first queues the held messages while the queue has room, then the new message.
#[tokio::test]
async fn a_flush_queues_held_messages_first_when_the_queue_has_room() {
    let (tx, mut rx) = mpsc::channel(2);
    let dm = DataManager::new(tx);
    let mut out = dm.outbound(
        "",
        INSTRUCTION,
        ["sink"],
        std::iter::empty::<(&str, &str)>(),
    );
    let message = |i: u8| vec![("sink".to_string(), OUTBOUND_FLUSH_BYTES, vec![i], false)];
    let next = |rx: &mut mpsc::Receiver<Elements>| {
        summarise(&[rx.try_recv().expect("a queued message")]).remove(0)
    };

    // Messages 0 and 1 fill the queue, and messages 2 and 3 are held.
    (0..4).for_each(|i| write(&mut out, 0, OUTBOUND_FLUSH_BYTES, i));
    assert!(out.has_pending());
    assert_eq!(next(&mut rx), message(0));

    // One free slot: message 2 goes out, and messages 3 and 4 are held.
    write(&mut out, 0, OUTBOUND_FLUSH_BYTES, 4);
    assert_eq!(summarise(&queued(&mut rx)), [message(1), message(2)]);
    assert!(out.has_pending());

    // Two free slots: messages 3 and 4 go out, and message 5 is held.
    write(&mut out, 0, OUTBOUND_FLUSH_BYTES, 5);
    assert_eq!(summarise(&queued(&mut rx)), [message(3), message(4)]);
    assert!(out.has_pending());

    // Two free slots and one held message: messages 5 and 6 go out and nothing is held.
    write(&mut out, 0, OUTBOUND_FLUSH_BYTES, 6);
    assert!(!out.has_pending());
    assert_eq!(summarise(&queued(&mut rx)), [message(5), message(6)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_outbound_queue_holds_64_messages_then_blocks_the_writer() {
    let (tx, mut rx) = mpsc::channel(1);
    let dm = DataManager::new(tx);
    // One message fills the queue, then 64 are held, then one more blocks.
    let messages = 1 + PENDING_MESSAGES + 1;
    let written = Arc::new(AtomicUsize::new(0));

    // The writer blocks, as on a bundle thread.
    let runtime = tokio::runtime::Handle::current();
    let writer = tokio::task::spawn_blocking({
        let written = Arc::clone(&written);
        move || {
            let mut out = dm.outbound(
                "",
                INSTRUCTION,
                ["sink"],
                std::iter::empty::<(&str, &str)>(),
            );
            let mut pending = Vec::new();
            for i in 0..messages {
                write(&mut out, 0, OUTBOUND_FLUSH_BYTES, i as u8);
                pending.push(out.has_pending());
                written.fetch_add(1, Ordering::SeqCst);
            }
            runtime.block_on(out.finish()).expect("finish");
            pending
        }
    });

    within("the writer to hold 64 messages", async {
        while written.load(Ordering::SeqCst) < messages - 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        written.load(Ordering::SeqCst),
        messages - 1,
        "the writer blocks on the 66th message until the queue is read"
    );

    let mut received = Vec::new();
    loop {
        let elements = within("the next message", rx.recv())
            .await
            .expect("the writer finishes before closing");
        let done = elements.data.iter().any(|data| data.is_last);
        received.push(elements);
        if done {
            break;
        }
    }
    let pending = within("the writer", writer).await.unwrap();

    assert_eq!(
        pending,
        (0..messages)
            .map(|i| (1..messages - 1).contains(&i))
            .collect::<Vec<_>>(),
        "messages are held from the second; the last write sends them all"
    );
    let expected: Vec<Vec<Summary>> = (0..messages)
        .map(|i| {
            vec![(
                "sink".to_string(),
                OUTBOUND_FLUSH_BYTES,
                vec![i as u8],
                false,
            )]
        })
        .chain([vec![("sink".to_string(), 0, Vec::new(), true)]])
        .collect();
    assert_eq!(summarise(&received), expected);
}

/// A bundle thread is a plain thread that holds only an enter guard of the runtime. On a full
/// queue with 64 held messages, its next write blocks and does not panic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_thread_with_only_an_enter_guard_blocks_on_a_full_outbound_queue() {
    let (tx, mut rx) = mpsc::channel(1);
    let dm = DataManager::new(tx);
    let messages = 1 + PENDING_MESSAGES + 1;
    let written = Arc::new(AtomicUsize::new(0));

    let runtime = tokio::runtime::Handle::current();
    let writer = std::thread::spawn({
        let written = Arc::clone(&written);
        move || {
            let _runtime = runtime.enter();
            let mut out = dm.outbound(
                "",
                INSTRUCTION,
                ["sink"],
                std::iter::empty::<(&str, &str)>(),
            );
            for i in 0..messages {
                write(&mut out, 0, OUTBOUND_FLUSH_BYTES, i as u8);
                written.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    within("the writer to hold 64 messages", async {
        while written.load(Ordering::SeqCst) < messages - 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(written.load(Ordering::SeqCst), messages - 1);
    assert!(
        !writer.is_finished(),
        "the writer waits in the 66th write; a panic ends the thread"
    );

    for _ in 0..messages {
        within("the next message", rx.recv())
            .await
            .expect("the writer keeps the channel open");
    }
    within("the writer to end", async {
        while !writer.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    writer.join().expect("the writer does not panic");
    assert_eq!(written.load(Ordering::SeqCst), messages);
}

/// While a write waits on a full outbound queue, the inbound limit grows: the stream takes
/// more chunks for a bundle that does not read than the base limit holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_that_waits_on_a_full_outbound_queue_raises_the_inbound_limit() {
    let (tx, mut rx) = mpsc::channel(1);
    let dm = DataManager::new(tx);
    let _unread = dm.register_inbound(INSTRUCTION);
    let messages = 1 + PENDING_MESSAGES + 1;

    let runtime = tokio::runtime::Handle::current();
    let writer = std::thread::spawn({
        let dm = dm.clone();
        move || {
            let _runtime = runtime.enter();
            let mut out = dm.outbound(
                "",
                INSTRUCTION,
                ["sink"],
                std::iter::empty::<(&str, &str)>(),
            );
            for i in 0..messages {
                write(&mut out, 0, OUTBOUND_FLUSH_BYTES, i as u8);
            }
        }
    });

    // At the base limit, the stream waits before the last of these chunks.
    within("the input above the base limit", async {
        for _ in 0..=2 * INBOUND_QUEUE_CHUNKS {
            dm.handle_inbound_elements(Elements {
                data: vec![beam_model::fn_execution::elements::Data {
                    instruction_id: INSTRUCTION.to_string(),
                    transform_id: "source".to_string(),
                    data: vec![0],
                    is_last: false,
                }],
                timers: Vec::new(),
            })
            .await;
        }
    })
    .await;

    for _ in 0..messages {
        within("the next message", rx.recv())
            .await
            .expect("the writer keeps the channel open");
    }
    within("the writer to end", async {
        while !writer.is_finished() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    writer.join().expect("the writer does not panic");
}

#[tokio::test]
async fn outbound_streams_timers_and_flushes_at_a_megabyte() {
    let (tx, mut rx) = mpsc::channel(100);
    let dm = DataManager::new(tx);
    let mut out = dm.outbound("", "inst", std::iter::empty(), [("t_id", "f_id")]);

    let mut peak_buffered = 0;
    for _ in 0..2_000_000 {
        out.write_timer(0, |buf| {
            buf.extend_from_slice(b"t"); // 1 byte timer for test
            Ok(())
        })
        .unwrap();
        if out.buffered_bytes() > peak_buffered {
            peak_buffered = out.buffered_bytes();
        }
    }
    assert!(peak_buffered <= OUTBOUND_FLUSH_BYTES + 1); // Peak buffered never goes over limit + timer size
    out.finish().await.unwrap();
    let messages = queued(&mut rx);
    assert!(messages.len() > 1);

    let mut total_timers = 0;
    let mut last_marker_received = false;
    for m in messages {
        for t in m.timers {
            total_timers += t.timers.len();
            if t.is_last {
                last_marker_received = true;
                assert!(t.timers.is_empty());
            }
        }
    }
    assert_eq!(total_timers, 2_000_000);
    assert!(last_marker_received);
}

#[tokio::test]
async fn outbound_time_based_flush() {
    let (tx, mut rx) = mpsc::channel(100);
    let dm = DataManager::new(tx).with_flush_limits(FlushLimits {
        time: Some(Duration::from_millis(50)),
        ..FlushLimits::default()
    });
    let mut out = dm.outbound("", "inst", std::iter::empty(), [("t_id", "f_id")]);

    out.write_timer(0, |buf| {
        buf.push(1);
        Ok(())
    })
    .unwrap();

    // Wait for the timeout
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(out.time_until_flush(), Some(Duration::ZERO));
    out.flush().unwrap();

    let messages = queued(&mut rx);
    assert_eq!(messages.len(), 1);
}

/// Writes during one long element call (such as a streaming read) send their output once the
/// oldest buffered byte is older than the time limit, without waiting for a megabyte.
#[tokio::test]
async fn a_write_after_the_time_limit_flushes_the_buffer() {
    let (tx, mut rx) = mpsc::channel(100);
    let dm = DataManager::new(tx).with_flush_limits(FlushLimits {
        time: Some(Duration::from_millis(50)),
        ..FlushLimits::default()
    });
    let mut out = dm.outbound(
        "",
        INSTRUCTION,
        ["sink"],
        std::iter::empty::<(&str, &str)>(),
    );

    write(&mut out, 0, 100, 1);
    assert!(queued(&mut rx).is_empty(), "the first write stays buffered");

    tokio::time::sleep(Duration::from_millis(60)).await;
    write(&mut out, 0, 100, 2);

    let messages = queued(&mut rx);
    assert_eq!(
        summarise(&messages),
        vec![vec![("sink".to_string(), 200, vec![1, 2], false)]]
    );
    assert_eq!(out.buffered_bytes(), 0);
}

/// Without a time limit, writes flush only at a megabyte.
#[tokio::test]
async fn without_a_time_limit_a_late_write_stays_buffered() {
    let (tx, mut rx) = mpsc::channel(100);
    let dm = DataManager::new(tx);
    let mut out = dm.outbound(
        "",
        INSTRUCTION,
        ["sink"],
        std::iter::empty::<(&str, &str)>(),
    );

    write(&mut out, 0, 100, 1);
    tokio::time::sleep(Duration::from_millis(60)).await;
    write(&mut out, 0, 100, 2);

    assert!(queued(&mut rx).is_empty());
    assert_eq!(out.buffered_bytes(), 200);
}

/// A worker can lower the size at which a bundle sends its buffer.
#[tokio::test]
async fn a_smaller_byte_limit_flushes_earlier() {
    let (tx, mut rx) = mpsc::channel(100);
    let dm = DataManager::new(tx).with_flush_limits(FlushLimits {
        bytes: 10,
        time: None,
    });
    let mut out = dm.outbound(
        "",
        INSTRUCTION,
        ["sink"],
        std::iter::empty::<(&str, &str)>(),
    );
    write(&mut out, 0, 9, 1);
    assert!(queued(&mut rx).is_empty(), "9 bytes stay buffered");
    write(&mut out, 0, 1, 2);
    assert_eq!(queued(&mut rx).len(), 1, "the 10th byte sends the buffer");
}

/// Emitting one 64-byte element and calling `flush_output` delivers it before a 50 ms sleep
/// finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_output_sends_before_blocking_wait() {
    use std::sync::OnceLock;
    use std::time::Instant;

    use beam::coders::{Coder, Context, DefaultCoder, WindowedValue, WindowedValueCoder};
    use beam::internals::DoFnHandler;
    use beam::transforms::{DoFn, ProcessContext};
    use beam_model::fn_execution::{
        InstructionRequest, ProcessBundleRequest, RegisterRequest, elements,
        instruction_request::Request,
    };

    use crate::common::{DescriptorBuilder, SINK_ID, SOURCE_ID, STAGE_ID, TestBundle};

    #[derive(Clone)]
    struct FlushThenSleep {
        flushed_at: Arc<OnceLock<Instant>>,
        sleep: Duration,
    }

    impl DoFn for FlushThenSleep {
        type In = Vec<u8>;
        type Out = Vec<u8>;

        fn process_element(
            &mut self,
            _: Vec<u8>,
            ctx: &mut ProcessContext<Vec<u8>>,
        ) -> beam::Result {
            ctx.emit(vec![7u8; 64])?;
            let _ = self.flushed_at.set(Instant::now());
            ctx.flush_output()?;
            std::thread::sleep(self.sleep);
            Ok(())
        }
    }

    let flushed_at = Arc::new(OnceLock::new());
    let TestBundle {
        mut data_out_rx,
        data_manager,
        control_client,
        ..
    } = TestBundle::builder()
        .handler(
            STAGE_ID,
            Arc::new(DoFnHandler::new(FlushThenSleep {
                flushed_at: Arc::clone(&flushed_at),
                sleep: Duration::from_millis(50),
            })),
        )
        .build();

    let descriptor = DescriptorBuilder::new("desc_flush_before_wait")
        .with_coder("bytes", "beam:coder:bytes:v1", &[])
        .with_coder("window", "beam:coder:global_window:v1", &[])
        .with_coder(
            "w_bytes",
            "beam:coder:windowed_value:v1",
            &["bytes", "window"],
        )
        .with_source_coder("w_bytes")
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink_with_coder(SINK_ID, "pcoll_output", "w_bytes")
        .build();

    let reg = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_flush_before_wait".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![descriptor],
            })),
        })
        .await;
    assert!(reg.error.is_empty(), "register: {}", reg.error);

    let coder = WindowedValueCoder::new(Vec::<u8>::coder());
    let mut encoded_in = Vec::new();
    coder
        .encode(
            &WindowedValue::global(Vec::new(), 0),
            &mut encoded_in,
            Context::Nested,
        )
        .expect("encode input");
    data_manager
        .handle_inbound_elements(Elements {
            data: vec![elements::Data {
                instruction_id: "inst_flush_before_wait".to_string(),
                transform_id: SOURCE_ID.to_string(),
                data: encoded_in,
                is_last: true,
            }],
            timers: Vec::new(),
        })
        .await;

    let bundle = tokio::spawn(async move {
        control_client
            .handle_instruction(InstructionRequest {
                instruction_id: "inst_flush_before_wait".to_string(),
                request: Some(Request::ProcessBundle(ProcessBundleRequest {
                    process_bundle_descriptor_id: "desc_flush_before_wait".to_string(),
                    ..Default::default()
                })),
            })
            .await
    });

    let first = within("the flushed element", data_out_rx.recv())
        .await
        .expect("flushed chunk");
    let received_at = Instant::now();
    let flushed_at = *flushed_at.get().expect("handler recorded flush time");
    let elapsed = received_at.saturating_duration_since(flushed_at);
    assert!(
        elapsed < Duration::from_millis(20),
        "flushed element arrived in {elapsed:?}, expected < 20 ms"
    );
    assert_eq!(first.data.len(), 1);
    assert!(!first.data[0].is_last);
    let mut reader = first.data[0].data.as_slice();
    let decoded = coder
        .decode(&mut reader, Context::Nested)
        .expect("decode output");
    assert_eq!(decoded.value, vec![7u8; 64]);
    assert!(reader.is_empty());

    let resp = within("bundle completion", bundle)
        .await
        .expect("bundle task");
    assert!(resp.error.is_empty(), "bundle failed: {}", resp.error);

    let eof = within("outbound EOF", data_out_rx.recv())
        .await
        .expect("EOF chunk");
    assert_eq!(eof.data.len(), 1);
    assert!(eof.data[0].data.is_empty());
    assert!(eof.data[0].is_last);
}

/// Flushing an empty outbound buffer leaves `has_flushed` false, sends no chunks, and keeps
/// response element embedding available.
#[tokio::test]
async fn empty_flush_does_not_mark_flushed_or_send_chunks() {
    use beam::coders::{Coder, Context, DefaultCoder, WindowedValue, WindowedValueCoder};
    use beam::internals::DoFnHandler;
    use beam::pipeline::PROTOCOL_CONTROL_RESPONSE_ELEMENTS_EMBEDDING;
    use beam::transforms::{DoFn, ProcessContext};
    use beam_model::fn_execution::{
        InstructionRequest, ProcessBundleRequest, RegisterRequest, elements,
        instruction_request::Request, instruction_response::Response,
    };

    use crate::common::{DescriptorBuilder, SINK_ID, SOURCE_ID, STAGE_ID, TestBundle};

    let (tx, mut rx) = mpsc::channel(16);
    let dm = DataManager::new(tx);
    let mut out = dm.outbound("", INSTRUCTION, ["sink"], [("t_id", "f_id")]);

    out.flush().expect("empty flush succeeds");
    assert!(!out.has_flushed());
    assert!(queued(&mut rx).is_empty());

    // After a real flush, a second flush on the now-empty buffer sends no empty chunk.
    write(&mut out, 0, 64, 5);
    out.flush().expect("non-empty flush succeeds");
    assert!(out.has_flushed());
    assert_eq!(
        summarise(&queued(&mut rx)),
        [[("sink".to_string(), 64, vec![5], false)]]
    );
    out.flush().expect("second empty flush succeeds");
    assert!(queued(&mut rx).is_empty());

    #[derive(Clone)]
    struct EmptyFlushThenEmit;

    impl DoFn for EmptyFlushThenEmit {
        type In = Vec<u8>;
        type Out = Vec<u8>;

        fn process_element(
            &mut self,
            _: Vec<u8>,
            ctx: &mut ProcessContext<Vec<u8>>,
        ) -> beam::Result {
            ctx.flush_output()?;
            ctx.emit(vec![9u8; 64])
        }
    }

    let TestBundle {
        mut data_out_rx,
        control_client,
        ..
    } = TestBundle::builder()
        .capabilities(PROTOCOL_CONTROL_RESPONSE_ELEMENTS_EMBEDDING)
        .handler(STAGE_ID, Arc::new(DoFnHandler::new(EmptyFlushThenEmit)))
        .build();

    let descriptor = DescriptorBuilder::new("desc_empty_flush")
        .with_coder("bytes", "beam:coder:bytes:v1", &[])
        .with_coder("window", "beam:coder:global_window:v1", &[])
        .with_coder(
            "w_bytes",
            "beam:coder:windowed_value:v1",
            &["bytes", "window"],
        )
        .with_source_coder("w_bytes")
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink_with_coder(SINK_ID, "pcoll_output", "w_bytes")
        .build();

    let reg = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "reg_empty_flush".to_string(),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![descriptor],
            })),
        })
        .await;
    assert!(reg.error.is_empty(), "register: {}", reg.error);

    let coder = WindowedValueCoder::new(Vec::<u8>::coder());
    let mut encoded_in = Vec::new();
    coder
        .encode(
            &WindowedValue::global(Vec::new(), 0),
            &mut encoded_in,
            Context::Nested,
        )
        .expect("encode input");
    let resp = control_client
        .handle_instruction(InstructionRequest {
            instruction_id: "inst_empty_flush".to_string(),
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: "desc_empty_flush".to_string(),
                elements: Some(Elements {
                    data: vec![elements::Data {
                        instruction_id: "inst_empty_flush".to_string(),
                        transform_id: SOURCE_ID.to_string(),
                        data: encoded_in,
                        is_last: true,
                    }],
                    timers: Vec::new(),
                }),
                ..Default::default()
            })),
        })
        .await;
    assert!(resp.error.is_empty(), "bundle failed: {}", resp.error);
    assert!(
        queued(&mut data_out_rx).is_empty(),
        "empty flush must not send anything on the data stream"
    );
    let pb_resp = match resp.response {
        Some(Response::ProcessBundle(pb_resp)) => pb_resp,
        other => panic!("expected ProcessBundle response, got {other:?}"),
    };
    let embedded = pb_resp
        .elements
        .expect("output stays embedded because empty flush did not set has_flushed");
    assert_eq!(embedded.data.len(), 2);
    assert!(!embedded.data[0].is_last);
    assert!(embedded.data[1].is_last);
}
