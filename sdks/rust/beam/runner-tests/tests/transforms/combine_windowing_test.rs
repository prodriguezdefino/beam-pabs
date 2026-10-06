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

//! Combine transforms crossed with every window type.
//!
//! Combining aggregates *within a window*. The pre-combine step buffers accumulators
//! across a bundle, so it has to keep one accumulator per key per window assignment and
//! put each one back into the windows it came from. Getting that wrong does not fail the
//! job: the accumulators land in the wrong window, or in no window at all, and the runner
//! reports success having produced nothing. Each test here pins a different way for that
//! to go wrong. The checks that run a pipeline are registry builders, so they run on every
//! runner.

use std::time::Duration;

use beam::prelude::*;
use beam::transforms::Sum;

#[test]
fn test_combine_globally_with_defaults_rejects_non_global_windows() {
    let p = Pipeline::new();
    p.apply("Create", Create::new(vec![1i64, 2, 3]))
        .apply(
            "WindowInto",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        )
        .combine_globally("SumAll", Sum::new());
    let err = p.validate().unwrap_err();
    assert!(err.to_string().contains("needs the global window"), "{err}");
}
