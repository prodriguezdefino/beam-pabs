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

//! ValidatesRunner test suite execution on DataflowRunner (Google Cloud Dataflow).
//!
//! Runs the same ValidatesRunner tests as `validates_runner_prism.rs` on Dataflow Runner V2
//! with the configured worker container. Both suites and the worker binary are generated
//! from [`tests::validates_runner_suite!`]. Each test passes its registry entry to
//! [`tests::run_test`]; the worker calls the same entry's builder.
//!
//! Every test is `#[ignore]`d: it submits real Dataflow jobs. Run the suite with
//! `./gradlew :sdks:rust:validatesRunnerDataflow` (or `...DataflowStreaming`), which
//! configures the `BEAM_DATAFLOW_*` environment and passes `--ignored`. A missing
//! setting then fails the test instead of skipping it.
//!
//! Non-ValidatesRunner suites are isolated into dedicated test binaries:
//! - Negative assertion & driver panic tests: `passert_test.rs`
//! - `TestStream` & event-time triggers (`UsesTestStream`): `test_stream_test.rs`
//! - Cross-language expansion (`UsesExternalService`): `xlang_test.rs`
//! - Local filesystem I/O (`UsesLocalFilesystem`): `textio_test.rs`

use tests::*;

macro_rules! dataflow_suite {
    ($($(#[$doc:meta])* $suite:ident {
        $($id:ident => $build:ident $([$expect:expr])?),* $(,)?
    })*) => {
        $(
            $(#[$doc])*
            mod $suite {
                use super::*;

                $(
                    #[ignore = "submits a Dataflow job; run with ./gradlew :sdks:rust:validatesRunnerDataflow"]
                    #[tokio::test]
                    async fn $id() {
                        run_test(
                            find_validates_runner_test(stringify!($id)).expect("registered test"),
                            &dataflow_runner_from_env(stringify!($id)),
                        )
                        .await;
                    }
                )*
            }
        )*
    };
}

validates_runner_suite!(dataflow_suite);
