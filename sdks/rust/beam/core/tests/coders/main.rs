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

//! Core coder and wire-framing tests.

mod coder_encoding_test;
mod coder_hostile_input_test;
mod coder_traversal_test;
mod decimal_coder_test;
mod element_metadata_test;
mod instant_test;
mod iterable_framing_test;
mod length_prefix_helpers_test;
mod pane_info_test;
mod param_windowed_value_test;
mod row_coder_test;
mod runner_framing_test;
mod tuple_coder_test;
