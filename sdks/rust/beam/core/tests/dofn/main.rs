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

//! DoFn, StatefulDoFn, state, timer, side input, and failure routing tests.

mod batched_dofn_test;
mod bundle_finalizer_test;
mod common;
mod failure_coder_test;
mod finish_bundle_context_test;
mod registered_dofn_test;
mod schema_mapping_test;
mod side_input_test;
mod state_row_schema_test;
mod state_test;
mod stateful_dofn_test;
mod timer_builder_test;
mod timer_test;
mod typed_element_test;
mod typed_outputs_test;
