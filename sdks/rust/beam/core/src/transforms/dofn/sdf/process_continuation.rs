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

//! Signals whether a [`SplittableDoFn`](super::SplittableDoFn) is done or must be rescheduled.

use std::time::Duration;

/// The continuation status that
/// [`SplittableDoFn::process_element`](super::SplittableDoFn::process_element) returns.
///
/// Return `Stop` when all work for the current restriction is done. Return `Resume` or
/// `ResumeAfter` to self-checkpoint; the runner resumes the remaining work after the delay.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ProcessContinuation {
    /// The restriction is done. Do not reschedule.
    #[default]
    Stop,
    /// Resume as soon as possible after the self-checkpoint.
    Resume,
    ResumeAfter(Duration),
}

impl ProcessContinuation {
    pub fn stop() -> Self {
        Self::Stop
    }

    pub fn resume() -> Self {
        Self::Resume
    }

    pub fn resume_after(delay: Duration) -> Self {
        Self::ResumeAfter(delay)
    }

    /// Returns `ResumeAfter(delay)`, also when `self` is `Stop`.
    pub fn with_delay(self, delay: Duration) -> Self {
        Self::ResumeAfter(delay)
    }

    pub fn is_stop(&self) -> bool {
        matches!(self, Self::Stop)
    }

    /// Returns `true` for `Resume` or `ResumeAfter`.
    pub fn is_resume(&self) -> bool {
        !self.is_stop()
    }

    pub fn delay(&self) -> Option<Duration> {
        match self {
            Self::ResumeAfter(delay) => Some(*delay),
            _ => None,
        }
    }
}
