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

//! The bounds of event time on the runner: the `Impulse` timestamp and the end of the
//! global window.

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};

use crate::dofns::EndOfGlobalWindowDoFn;

/// Validates that the elements of `Create` carry the `Impulse` timestamp, [`Instant::MIN`].
pub fn build_impulse_timestamp_is_min(p: &TestPipeline) {
    let timestamps = p
        .apply("Create", Create::new(vec![1i64]))
        .par_do_fn("Timestamp", |_value: i64, ctx| {
            ctx.emit(ctx.timestamp().as_millis())
        });
    passert::that("AssertMin", &timestamps).contains_in_any_order([Instant::MIN.as_millis()]);
}

/// Validates the firing times at the end of the global window. A timer at `max_timestamp`
/// fires at [`Instant::END_OF_GLOBAL_WINDOW`], and `on_window_expiration` runs after it with
/// hold timestamp [`Instant::END_OF_GLOBAL_WINDOW`] and fire timestamp below [`Instant::MAX`].
pub fn build_end_of_global_window_firing_times(p: &TestPipeline) {
    let out = p
        .apply("Create", Create::new(vec![("k".to_string(), 1i64)]))
        .apply("EndOfGlobalWindow", ParDo::new(EndOfGlobalWindowDoFn));
    passert::that("AssertFiringTimes", &out)
        .contains_in_any_order(["timer", "expiration"].map(|label| (label.to_string(), true)));
}
