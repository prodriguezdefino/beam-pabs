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

//! Composite nodes for transforms that expand into several steps. A composite gives the
//! steps the name of the transform in the graph, in runner UIs and in metric step names.

use std::collections::HashMap;

use crate::pipeline::Pipeline;
use crate::values::PCollection;

/// Applies `build` under a unique name based on `name`, and groups the transforms that it
/// adds in a composite of that name. The composite reads `inputs` and writes the output.
pub(crate) fn composite<T: 'static>(
    pipeline: &Pipeline,
    name: &str,
    inputs: HashMap<String, String>,
    build: impl FnOnce(&str) -> PCollection<T>,
) -> PCollection<T> {
    let name = pipeline.unique_transform_name(name);
    let before = pipeline.lock().transform_ids();
    let output = build(&name);
    let subtransforms = pipeline.lock().outermost_transforms_since(&before);
    let outputs = HashMap::from([("out".to_string(), output.id().to_string())]);
    pipeline.add_composite_transform(&name, None, Vec::new(), inputs, outputs, subtransforms);
    output
}

/// The inputs of a composite with one main input.
pub(crate) fn main_input<T: 'static>(input: &PCollection<T>) -> HashMap<String, String> {
    HashMap::from([("in".to_string(), input.id().to_string())])
}
