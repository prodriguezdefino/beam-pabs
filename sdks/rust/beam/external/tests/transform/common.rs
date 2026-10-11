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

//! Helpers for the tests that compare a launcher graph with a worker graph.

use std::collections::BTreeMap;

use beam::internals::PipelineInternals;
use beam::options::{HarnessOptions, OptionsSnapshot, PipelineOptions};
use beam::pipeline::{ExpansionMode, Pipeline};

/// A worker pipeline with the options that the launcher sends to its workers.
pub(crate) fn worker_pipeline(launcher: &Pipeline) -> Pipeline {
    let encoded = launcher.options().snapshot().unwrap().encode();
    let harness = HarnessOptions {
        worker: true,
        ..HarnessOptions::default()
    };
    let options =
        PipelineOptions::from_snapshot(OptionsSnapshot::decode(&encoded).unwrap(), harness)
            .unwrap();
    let worker = Pipeline::create(&options);
    assert_eq!(worker.expansion_mode(), ExpansionMode::Placeholder);
    worker
}

/// Each transform as `id -> (urn, outputs, subtransforms)`.
pub(crate) type Shape = BTreeMap<String, (String, Vec<String>, Vec<String>)>;

/// The shape of the transforms of `pipeline` whose id starts with `prefix`.
pub(crate) fn shape(pipeline: &Pipeline, prefix: &str) -> Shape {
    let lock = pipeline.lock();
    lock.components
        .transforms
        .iter()
        .filter(|(id, _)| id.starts_with(prefix))
        .map(|(id, t)| {
            let urn = t.spec.as_ref().map(|s| s.urn.clone()).unwrap_or_default();
            let mut outputs: Vec<String> = t.outputs.values().cloned().collect();
            outputs.sort();
            let mut subtransforms = t.subtransforms.clone();
            subtransforms.sort();
            (id.clone(), (urn, outputs, subtransforms))
        })
        .collect()
}
