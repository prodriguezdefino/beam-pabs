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

//! Row types for the schema pipelines.

use beam::schema::BeamRow;

#[derive(Debug, Clone, PartialEq, Eq, BeamRow)]
#[beam(crate = "::beam", id = "beam.runner_tests.AuditEvent")]
pub struct AuditEvent {
    pub account_id: String,
    pub amount: i64,
    pub tags: Vec<String>,
    pub is_verified: bool,
}
