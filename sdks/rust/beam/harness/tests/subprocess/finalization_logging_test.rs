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
//! `tracing` caches the interest of each callsite for the process. While only one subscriber
//! is registered, the first thread that reaches a callsite sets that interest from its own
//! default subscriber. Running the check in a fresh child process keeps other tests from
//! disabling the instruction span before this subscriber attaches.

use std::collections::HashMap;
use std::future::IntoFuture;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use beam::internals::PendingFinalizer;
use beam_harness::bundle_processor::BundleProcessor;
use beam_harness::control::ControlClient;
use beam_harness::data::DataManager;
use beam_harness::logging::{
    BeamFnLoggingHandle, BeamFnLoggingLayer, LOG_QUEUE_BATCHES, LoggingClient,
};
use beam_model::fn_execution::{
    FinalizeBundleRequest, InstructionRequest, LogEntry, instruction_request::Request,
};
use tokio::sync::mpsc;
use tracing_subscriber::layer::SubscriberExt;

const CHILD_ENV: &str = "BEAM_FINALIZATION_LOGGING_TEST_CHILD";
const TEST: &str =
    "finalization_logging_test::a_finalization_callback_logs_with_the_id_of_its_instruction";
const WAIT: Duration = Duration::from_secs(5);

async fn within<F: IntoFuture>(what: &str, fut: F) -> F::Output {
    tokio::time::timeout(WAIT, fut)
        .await
        .unwrap_or_else(|_| panic!("timed out after {WAIT:?} waiting for {what}"))
}

/// A finalization callback that expires in one minute.
fn pending(callback: impl FnOnce() -> beam::Result + Send + 'static) -> PendingFinalizer {
    PendingFinalizer {
        deadline: Instant::now() + Duration::from_secs(60),
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

async fn act() {
    let (data_out_tx, _data_out_rx) = mpsc::channel(16);
    let processor = Arc::new(BundleProcessor::with_handlers(
        DataManager::new(data_out_tx),
        HashMap::new(),
    ));
    let control_client = ControlClient::new(Arc::clone(&processor));

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

#[tokio::test]
async fn a_finalization_callback_logs_with_the_id_of_its_instruction() {
    if std::env::var_os(CHILD_ENV).is_some() {
        act().await;
        return;
    }

    let exe = std::env::current_exe().expect("path of the running test binary");
    let out = Command::new(exe)
        .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, "1")
        .env_remove("RUST_LOG")
        .output()
        .expect("run the child test process");
    assert!(
        out.status.success(),
        "child failed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
