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

//! ValidatesRunner test suite execution on PrismRunner (portable runner).
//!
//! Organizes tests into capability modules:
//! - `core_transforms`: Element-wise, GroupByKey, Combine, Flatten, Partition, SDF
//! - `joins`: CoGroupByKey and Relational Joins
//! - `schemas`: BeamRow Schema encoding/decoding
//! - `side_inputs`: Iterable and Singleton side inputs (State API)
//! - `state_and_timers`: Stateful ParDo, Map/Set state, bundle lifecycle
//! - `windowing`: Fixed windowing and windowed aggregation
//! - `assertions`: In-graph PAssert assertions
//!
//! The tests are generated from the one suite list,
//! [`tests::validates_runner_suite!`], shared with the Dataflow suite and worker. Each
//! one hands its registry entry, a pipeline builder and the expected outcome, to
//! [`tests::run_test`] together with a Prism runner.
//!
//! Non-ValidatesRunner suites are isolated into dedicated test binaries:
//! - Negative assertion & driver panic tests: `passert_test.rs`
//! - `TestStream` & event-time triggers (`UsesTestStream`): `test_stream_test.rs`
//! - Cross-language expansion (`UsesExternalService`): `xlang_test.rs`
//! - Local filesystem I/O (`UsesLocalFilesystem`): `textio_test.rs`

use tests::*;

/// Runs the registered test `$id` on Prism.
macro_rules! run_on_prism {
    ($id:ident) => {
        run_test(
            find_validates_runner_test(stringify!($id)).expect("registered test"),
            &prism_runner_from_env(),
        )
        .await
    };
}

macro_rules! prism_vr_test {
    // Prism drops multimap state mutations issued by a bundle that did not also
    // write that state (observed Prism behavior).
    (map_and_set_state_clear) => {
        #[ignore = "Prism does not honour a state clear issued by a bundle that did not also write the state"]
        #[tokio::test]
        async fn map_and_set_state_clear() {
            run_on_prism!(map_and_set_state_clear);
        }
    };
    ($id:ident) => {
        #[tokio::test]
        async fn $id() {
            run_on_prism!($id);
        }
    };
}

macro_rules! prism_suite {
    ($($(#[$doc:meta])* $suite:ident {
        $($id:ident => $build:ident $([$expect:expr])?),* $(,)?
    })*) => {
        $(
            $(#[$doc])*
            mod $suite {
                use super::*;

                $(prism_vr_test!($id);)*
            }
        )*
    };
}

validates_runner_suite!(prism_suite);
