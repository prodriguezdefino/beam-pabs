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

//! Ids of expansions.

use std::sync::LazyLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

/// Returns a new id for an expansion.
///
/// The caller sets the namespace, so it can be empty and two callers can send the same one.
/// The id holds the process id, the time of the first id in this process and a counter, so
/// it is unique across requests and across expansion service processes.
pub(super) fn new_expansion_id() -> String {
    static PROCESS: LazyLock<String> = LazyLock::new(|| {
        let start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        format!("{}-{start}", std::process::id())
    });
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("{}-{}", *PROCESS, NEXT.fetch_add(1, Ordering::Relaxed))
}
