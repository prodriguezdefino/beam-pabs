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

//! Event-time and processing-time timer accessors on [`ProcessContext`].
//!
//! Timers are scoped to a family, a key and a window.

use std::sync::Arc;

use super::ProcessContext;
use crate::transforms::dofn::timer::{Timer, TimerFamilySpec};

impl<T> ProcessContext<'_, T> {
    /// Returns a handle that sets or clears timers in `spec`, bound to the key (if any) and
    /// window of the current element. Use [`Timer::tag`] for a dynamic tag and [`Timer::key`]
    /// for an explicit key, for example when a timer callback sets its own timer again.
    /// Returns an error if no timer collector is active.
    pub fn timer(&self, spec: &TimerFamilySpec) -> crate::Result<Timer> {
        let collector = self.timer_collector.map(Arc::clone).ok_or_else(|| {
            format!(
                "No active TimerCollector registered for timer family '{}'",
                spec.family_name
            )
        })?;
        Ok(Timer::with_context(
            &spec.family_name,
            "",
            self.key_bytes
                .as_deref()
                .map(<[u8]>::to_vec)
                .unwrap_or_default(),
            self.window().to_vec(),
            spec.time_domain,
            collector,
        ))
    }
}
