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

//! The transforms of `WriteFiles` do not depend on the windowing strategy of its input.

use std::collections::BTreeMap;
use std::time::Duration;

use beam::internals::PipelineInternals;
use beam::pipeline::{Pipeline, UNEXPANDED_PLACEHOLDER_ID};
use beam::prelude::*;
use beam::windowing::{FixedWindows, GlobalWindows, Trigger, WindowInto};
use beam_io_file::textio;

/// The windowing of the input to the write.
#[derive(Debug, Clone, Copy)]
enum Input {
    Global,
    GlobalWithTrigger,
    Fixed,
    /// A fixed-windowed input whose strategy the pipeline does not know, as on a worker for
    /// the output of a cross-language transform.
    Unresolved,
}

/// Each transform of the pipeline as `id -> (urn, inputs, outputs, subtransforms)`.
type Shape = BTreeMap<String, (String, Vec<String>, Vec<String>, Vec<String>)>;

/// The transforms of a pipeline that writes an input with the windowing of `input`.
fn write_shape(input: Input) -> Shape {
    let pipeline = Pipeline::new();
    let lines = pipeline.apply("Create", Create::new(vec!["a".to_string()]));
    let windowed = match input {
        Input::Global => lines.apply("Window", WindowInto::new(GlobalWindows)),
        Input::GlobalWithTrigger => lines.apply(
            "Window",
            WindowInto::new(GlobalWindows).with_trigger(Trigger::after_count(1)),
        ),
        Input::Fixed | Input::Unresolved => lines.apply(
            "Window",
            WindowInto::new(FixedWindows::of(Duration::from_secs(60))),
        ),
    };
    if let Input::Unresolved = input {
        pipeline
            .lock()
            .components
            .pcollections
            .get_mut(windowed.id())
            .expect("the window step registers its output")
            .windowing_strategy_id = UNEXPANDED_PLACEHOLDER_ID.to_string();
    }
    let _ = windowed.apply("Write", textio::Write::new("/tmp/beam-graph-shape/out"));

    let lock = pipeline.lock();
    lock.components
        .transforms
        .iter()
        .map(|(id, t)| {
            let sorted = |mut values: Vec<String>| {
                values.sort();
                values
            };
            let urn = t.spec.as_ref().map(|s| s.urn.clone()).unwrap_or_default();
            let inputs = sorted(t.inputs.values().cloned().collect());
            let outputs = sorted(t.outputs.values().cloned().collect());
            let subtransforms = sorted(t.subtransforms.clone());
            (id.clone(), (urn, inputs, outputs, subtransforms))
        })
        .collect()
}

#[test]
fn write_files_builds_the_same_transforms_for_every_input_windowing() {
    let fixed = write_shape(Input::Fixed);
    assert!(
        fixed.keys().any(|id| id.contains("RewindowIntoGlobal")),
        "{:?}",
        fixed.keys()
    );
    [Input::Global, Input::GlobalWithTrigger, Input::Unresolved]
        .into_iter()
        .for_each(|input| assert_eq!(write_shape(input), fixed, "{input:?}"));
}
