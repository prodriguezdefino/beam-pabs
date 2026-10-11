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

//! Pipeline graph, validation, and element metadata propagation tests.

#[path = "../support/mod.rs"]
mod support;

mod coder_propagation_test;
mod element_metadata_propagation_test;
mod graph_combinators_test;
mod job_state_test;
mod label_scope_test;
mod pcollection_boundedness_test;
mod pipeline_graph_test;
mod pipeline_validation_test;
