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

//! Runner and harness plumbing: byte-level handlers, readers and collectors.
//!
//! The only public path for these items. Pipeline code does not need them.

use std::collections::HashMap;

pub use crate::values::SideInputKind;

/// A foldhash `HashMap` for tables hashed per element. Crate-private, so public APIs
/// keep the standard `HashMap` and foldhash stays out of the SDK's semver surface.
pub(crate) type FastHashMap<K, V> = HashMap<K, V, foldhash::fast::RandomState>;
