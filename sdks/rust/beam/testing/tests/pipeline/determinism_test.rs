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

//! Tests for deterministic pipeline graph construction and `assert_deterministic_pipeline`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use beam::pipeline::ResourceHints;
use beam::prelude::*;
use beam::values::AnySideInput;
use beam_testing::assert_deterministic_pipeline;

#[derive(State)]
#[expect(dead_code, reason = "the test checks graph determinism, not execution")]
struct MultiState {
    count: ValueState<i64>,
    seen: BagState<String>,
    scores: MapState<String, i64>,
}

#[derive(Timers)]
enum MultiTimers {
    #[timer(event_time)]
    Flush,
    #[timer(processing_time)]
    Heartbeat,
    #[timer(event_time)]
    Dynamic(String),
}

#[derive(Clone)]
struct StatefulWithSideInputs {
    limit: SingletonView<i64>,
    tags: IterView<String>,
}

impl StatefulDoFn for StatefulWithSideInputs {
    type Key = String;
    type Value = i64;
    type Out = (String, i64);
    type State = MultiState;
    type Timer = MultiTimers;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.limit, &self.tags]
    }

    fn process_element(
        &mut self,
        key: &String,
        value: i64,
        _state: &mut MultiState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        ctx.emit((key.clone(), value))
    }
}

#[test]
fn identical_pipelines_with_state_timers_side_inputs_and_hints_are_deterministic() {
    // Build an unrelated pipeline first to confirm component IDs do not share process state.
    let prior = Pipeline::new();
    let _ = prior
        .apply("Other", Create::new(vec!["x".to_string()]))
        .apply(
            "OtherWindows",
            WindowInto::new(FixedWindows::of(Duration::from_secs(30))),
        );

    assert_deterministic_pipeline(|| {
        let p = Pipeline::new();
        let limit = p.apply("Limit", Create::new(vec![10i64])).as_singleton();
        let tags = p
            .apply("Tags", Create::new(vec!["a".to_string(), "b".to_string()]))
            .as_iter();
        let input = p
            .apply("Input", Create::new(vec![("k".to_string(), 1i64)]))
            .apply(
                "Windows",
                WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
            );
        let stateful = input.apply(
            "Stateful",
            ParDo::new(StatefulWithSideInputs { limit, tags })
                .with_resource_hints(ResourceHints::new().with_cpu_count(2)),
        );
        let _ = stateful.apply("Sum", CombinePerKey::new(Sum::new()));
        p
    });
}

#[test]
#[should_panic(expected = "differ")]
fn a_pipeline_with_non_deterministic_transform_names_fails_determinism_check() {
    let counter = AtomicUsize::new(0);
    assert_deterministic_pipeline(|| {
        let n = counter.fetch_add(1, Ordering::Relaxed);
        let p = Pipeline::new();
        let _ = p
            .apply("Input", Create::new(vec![1i64, 2]))
            .apply(format!("Step_{n}"), Map::new(|x: i64| x + 1));
        p
    });
}

#[test]
#[should_panic(expected = "differ")]
fn a_pipeline_with_non_deterministic_windowing_fails_determinism_check() {
    let counter = AtomicUsize::new(1);
    assert_deterministic_pipeline(|| {
        let secs = counter.fetch_add(1, Ordering::Relaxed) as u64;
        let p = Pipeline::new();
        let _ = p.apply("Input", Create::new(vec![1i64])).apply(
            "Windows",
            WindowInto::new(FixedWindows::of(Duration::from_secs(secs))),
        );
        p
    });
}

#[test]
#[should_panic(expected = "differ")]
fn a_pipeline_with_non_deterministic_create_payload_fails_determinism_check() {
    let counter = AtomicUsize::new(0);
    assert_deterministic_pipeline(|| {
        let value = counter.fetch_add(1, Ordering::Relaxed) as i64;
        let p = Pipeline::new();
        let _ = p.apply("Input", Create::new(vec![value]));
        p
    });
}
