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

//! Reference model for [`Query0`](crate::queries::Query0).

use beam::Instant;

use super::standard_event_stream;
use crate::config::NexmarkConfig;
use crate::model::Event;

/// Computes expected `(Event, Instant)` outputs for Query 0 (`PASSTHROUGH`).
pub fn query0(config: &NexmarkConfig) -> Vec<(Event, Instant)> {
    standard_event_stream(config)
}
