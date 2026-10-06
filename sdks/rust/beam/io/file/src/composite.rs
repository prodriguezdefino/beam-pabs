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

use beam::internals::PipelineInternals;
use beam::pipeline::Pipeline;
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use model::pipeline::FunctionSpec;

/// Runs `build`, the steps of the transform that is expanding, then gives its node the
/// display data of `transform` and the composite spec `spec`, if any.
pub(crate) fn expand<T: HasDisplayData, Out>(
    pipeline: &Pipeline,
    transform: &T,
    spec: Option<FunctionSpec>,
    build: impl FnOnce(&Pipeline) -> Out,
) -> Out {
    let out = build(pipeline);
    if let Some(spec) = spec {
        pipeline.set_scope_spec(spec);
    }
    let mut builder = DisplayDataBuilder::with_namespace(pipeline.scope_name().unwrap_or_default());
    transform.populate_display_data(&mut builder);
    pipeline.set_scope_display_data(builder.into_proto());
    out
}
