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

//! Default components of a new pipeline graph.

use std::collections::HashMap;

use beam_model::pipeline as proto;

use crate::pipeline::constants::{
    URN_ENV_DEFAULT, URN_WINDOW_FN_GLOBAL_WINDOWS, standard_capabilities,
};

/// The environment that a transform gets when no other environment applies.
pub(super) fn default_environment() -> proto::Environment {
    proto::Environment {
        urn: URN_ENV_DEFAULT.to_string(),
        payload: Vec::new(),
        display_data: Vec::new(),
        capabilities: standard_capabilities(),
        resource_hints: HashMap::new(),
        dependencies: Vec::new(),
    }
}

/// The coder for the single window that spans all time.
pub(super) fn global_window_coder() -> proto::Coder {
    proto::Coder {
        spec: Some(proto::FunctionSpec {
            urn: crate::coders::URN_GLOBAL_WINDOW.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: Vec::new(),
    }
}

/// The default windowing strategy: one global, non-merging window.
pub(super) fn global_windows_strategy(
    window_coder_id: String,
    environment_id: String,
) -> proto::WindowingStrategy {
    proto::WindowingStrategy {
        window_fn: Some(proto::FunctionSpec {
            urn: URN_WINDOW_FN_GLOBAL_WINDOWS.to_string(),
            payload: Vec::new(),
        }),
        merge_status: proto::merge_status::Enum::NonMerging as i32,
        window_coder_id,
        trigger: Some(proto::Trigger {
            trigger: Some(proto::trigger::Trigger::Default(proto::trigger::Default {})),
        }),
        accumulation_mode: proto::accumulation_mode::Enum::Discarding as i32,
        output_time: proto::output_time::Enum::EndOfWindow as i32,
        closing_behavior: proto::closing_behavior::Enum::EmitIfNonempty as i32,
        allowed_lateness: 0,
        on_time_behavior: proto::on_time_behavior::Enum::FireAlways as i32,
        assigns_to_one_window: true,
        environment_id,
    }
}
