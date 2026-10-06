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

//! Tests for the per-test wall-clock deadline helpers.

use std::sync::mpsc;
use std::time::Duration;

use testutils::{with_custom_timeout, with_timeout};

/// Extracts the message from a caught panic payload.
///
/// `panic!("literal")` gives a `&'static str` payload and `panic!("{x}")` gives a
/// `String`. This function handles both.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .expect("panic payload should be a string")
}

#[test]
fn returns_the_body_result() {
    assert_eq!(with_timeout("returns_the_body_result", || 21 * 2), 42);
}

#[test]
fn propagates_body_panics() {
    let panic = std::panic::catch_unwind(|| {
        with_timeout("propagates_body_panics", || panic!("boom from body"));
    })
    .expect_err("the body panic should reach the caller");

    assert_eq!(panic_message(panic.as_ref()), "boom from body");
}

#[test]
fn fails_instead_of_hanging() {
    let (unblock_tx, unblock_rx) = mpsc::channel::<()>();
    // `Receiver` is not `UnwindSafe`, but nothing observes it after the unwind.
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        with_custom_timeout(
            "fails_instead_of_hanging",
            Duration::from_millis(50),
            move || {
                // Blocks until the test drops `unblock_tx`, which is long after the timeout.
                let _ = unblock_rx.recv();
            },
        );
    }))
    .expect_err("an over-running body should fail the test");

    let message = panic_message(panic.as_ref());
    assert!(
        message.contains("did not complete within"),
        "unexpected timeout message: {message}"
    );
    drop(unblock_tx);
}
