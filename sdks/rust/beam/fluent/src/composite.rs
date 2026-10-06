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

//! The composite node of a fluent method that applies more than one transform.

use std::collections::HashMap;

use beam::internals::PipelineInternals;
use beam::pipeline::Pipeline;
use beam::values::POutput;

/// Applies `build` as the transform `name` that reads `inputs`. The steps that `build` adds
/// get names in the scope of the transform.
pub(crate) fn composite<O: POutput>(
    pipeline: &Pipeline,
    name: &str,
    inputs: HashMap<String, String>,
    build: impl FnOnce() -> O,
) -> O {
    pipeline.apply_scoped(name, inputs, build)
}
