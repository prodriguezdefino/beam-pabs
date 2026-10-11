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

//! The logs of a finalization callback keep the id of its instruction.
//!
//! This binary has one test, so no other thread reaches the instruction span. `tracing`
//! caches the interest of each callsite for the process. While only one subscriber is
//! registered, the thread that first reaches a callsite decides that interest with its own
//! default subscriber. A test with no subscriber in the same process can then disable the
//! instruction span for the subscriber of this test.

use beam::internals::PendingFinalizer;
use harness::logging::{BeamFnLoggingHandle, BeamFnLoggingLayer, LOG_QUEUE_BATCHES, LoggingClient};
use model::fn_execution::{
    FinalizeBundleRequest, InstructionRequest, LogEntry, instruction_request::Request,
};
use tokio::sync::mpsc;
use tracing_subscriber::layer::SubscriberExt;

mod common;
use common::{TestBundle, within};

/// A finalization callback that expires in one minute.
fn pending(callback: impl FnOnce() -> beam::Result + Send + 'static) -> PendingFinalizer {
    PendingFinalizer {
        deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
        callback: Box::new(callback),
    }
}

fn finalize(id: &str, target: &str) -> InstructionRequest {
    InstructionRequest {
        instruction_id: id.to_string(),
        request: Some(Request::FinalizeBundle(FinalizeBundleRequest {
            instruction_id: target.to_string(),
        })),
    }
}

#[tokio::test]
async fn a_finalization_callback_logs_with_the_id_of_its_instruction() {
    let TestBundle {
        processor,
        control_client,
        ..
    } = TestBundle::empty();
    let handle = BeamFnLoggingHandle::new();
    let (entries_tx, mut entries_rx) = mpsc::channel(LOG_QUEUE_BATCHES);
    let client = LoggingClient::new(entries_tx);
    handle.set_client(client.clone());
    let _subscriber = tracing::subscriber::set_default(
        tracing_subscriber::registry().with(BeamFnLoggingLayer::new(handle)),
    );
    processor.store_finalization(
        "bundle",
        vec![pending(|| {
            tracing::warn!("the callback commits");
            Ok(())
        })],
    );

    let response = within(
        "the finalization response",
        control_client.handle_instruction(finalize("finalize-7", "bundle")),
    )
    .await;
    assert!(response.error.is_empty(), "{}", response.error);

    client.flush().await;
    // The client sends entries in batches, so the entry of the callback can be in any batch.
    let logged: LogEntry = within("the entry of the callback", async {
        loop {
            let batch = entries_rx.recv().await.expect("the log stream is open");
            if let Some(entry) = batch
                .log_entries
                .into_iter()
                .find(|entry| entry.message == "the callback commits")
            {
                break entry;
            }
        }
    })
    .await;
    assert_eq!(logged.instruction_id, "finalize-7");
}
