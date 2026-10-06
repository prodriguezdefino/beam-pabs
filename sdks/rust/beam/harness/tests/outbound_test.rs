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

use harness::data::{DataManager, OUTBOUND_FLUSH_BYTES, Outbound};
use model::fn_execution::Elements;

mod common;
use common::within;

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
        let mut out = dm.outbound("", INSTRUCTION, ["sink_a", "sink_b"]);
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
    let mut out = dm.outbound("", INSTRUCTION, ["sink_a", "sink_b", "sink_c"]);
    write(&mut out, 1, OUTBOUND_FLUSH_BYTES, 7);
    assert_eq!(
        summarise(&queued(&mut rx)),
        [[("sink_b".to_string(), OUTBOUND_FLUSH_BYTES, vec![7], false)]]
    );
    assert!(!out.has_pending());
}

/// Off the multi-threaded runtime held messages may exceed the limit.
#[tokio::test]
async fn drain_sends_held_messages_in_order_and_a_current_thread_writer_never_blocks() {
    let (tx, mut rx) = mpsc::channel(1);
    let dm = DataManager::new(tx);
    let mut out = dm.outbound("", INSTRUCTION, ["sink"]);
    let messages = PENDING_MESSAGES + 2;

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
    within("finish", out.finish(Vec::new())).await.unwrap();

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_outbound_queue_holds_64_messages_then_blocks_the_writer() {
    let (tx, mut rx) = mpsc::channel(1);
    let dm = DataManager::new(tx);
    // One message fills the queue, then 64 are held, then one more blocks.
    let messages = 1 + PENDING_MESSAGES + 1;
    let written = Arc::new(AtomicUsize::new(0));

    let writer = tokio::spawn({
        let written = written.clone();
        async move {
            let mut out = dm.outbound("", INSTRUCTION, ["sink"]);
            let mut pending = Vec::new();
            for i in 0..messages {
                write(&mut out, 0, OUTBOUND_FLUSH_BYTES, i as u8);
                pending.push(out.has_pending());
                written.fetch_add(1, Ordering::SeqCst);
            }
            out.finish(Vec::new()).await.expect("finish");
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
