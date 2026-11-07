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

//! CombineFn, CombineGlobally, CombinePerKey, CombiningState, and built-in combiner tests.

mod common;

mod approximate_combine_test;
mod combine_fn_test;
mod combine_table_test;
mod combining_state_test;
mod compose_test;
mod fanout_test;
mod registered_combine_test;
mod registered_library_combine_test;
mod statistics_combine_test;
