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

//! Synthetic load before a query: CPU work and state writes for each element.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::time::{Duration, Instant as WallInstant};

use beam::coders::DefaultCoder;
use beam::prelude::*;
use serde::{Deserialize, Serialize};

use crate::model::Event;

/// Low bits of a hash that one search round must match.
const MASK: u64 = (1 << 16) - 1;
const TARGET: u64 = 0x243F_6A88_85A3_08D3;
const INIT_PLAINTEXT: u64 = 50_000;
/// Largest state value that one write holds.
const MAX_BUFFER_SIZE: u64 = 1 << 24;

/// Keeps one core busy for a fixed wall time on each element, then passes it on.
#[derive(Clone, Copy, Debug)]
pub struct CpuDelay {
    delay: Duration,
}

impl CpuDelay {
    /// Builds the transform for `delay_ms` milliseconds of work per element.
    pub fn new(delay_ms: i64) -> Self {
        Self {
            delay: Duration::from_millis(u64::try_from(delay_ms).unwrap_or(0)),
        }
    }
}

impl<T: DefaultCoder + Send + Sync + 'static> PTransform<PCollection<T>> for CpuDelay {
    type Output = PCollection<T>;

    fn expand(self, input: &PCollection<T>) -> PCollection<T> {
        let delay = self.delay;
        input.map("CpuDelay", move |element| {
            burn_cpu(delay);
            element
        })
    }
}

/// Runs hash searches until `delay` of wall time passes. Gives the number of searches.
pub fn burn_cpu(delay: Duration) -> usize {
    let start = WallInstant::now();
    std::iter::from_fn(|| (start.elapsed() < delay).then(find_plaintext))
        .map(std::hint::black_box)
        .count()
}

/// The first plaintext from [`INIT_PLAINTEXT`] whose hash matches [`TARGET`] in its low bits.
fn find_plaintext() -> u64 {
    let mut plaintext = INIT_PLAINTEXT;
    loop {
        let mut hasher = DefaultHasher::new();
        plaintext.hash(&mut hasher);
        if hasher.finish() & MASK == TARGET & MASK {
            return plaintext;
        }
        plaintext += 1;
    }
}

/// Writes a fixed number of bytes to keyed state on each event, then passes it on.
///
/// All elements share one key, so one worker at a time runs the writes.
#[derive(Clone, Copy, Debug)]
pub struct DiskBusy {
    bytes: u64,
}

impl DiskBusy {
    /// Builds the transform for `bytes` bytes of state writes per event.
    pub fn new(bytes: i64) -> Self {
        Self {
            bytes: u64::try_from(bytes).unwrap_or(0),
        }
    }
}

impl PTransform<PCollection<Event>> for DiskBusy {
    type Output = PCollection<Event>;

    fn expand(self, input: &PCollection<Event>) -> PCollection<Event> {
        input.key_by("DiskBusyKey", |_| 0_i32).apply(
            "DiskBusyWrite",
            ParDo::new(DiskBusyFn { bytes: self.bytes }),
        )
    }
}

/// Sizes of the state writes for `bytes` bytes, each at most [`MAX_BUFFER_SIZE`].
pub fn write_sizes(bytes: u64) -> impl Iterator<Item = u64> {
    std::iter::successors(Some(bytes), |remain| {
        remain.checked_sub(MAX_BUFFER_SIZE).filter(|&left| left > 0)
    })
    .filter(|&remain| remain > 0)
    .map(|remain| remain.min(MAX_BUFFER_SIZE))
}

#[derive(State)]
struct DiskBusyState {
    buffer: ValueState<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Register)]
struct DiskBusyFn {
    bytes: u64,
}

impl StatefulDoFn for DiskBusyFn {
    type Key = i32;
    type Value = Event;
    type Out = Event;
    type State = DiskBusyState;
    type Timer = NoTimers;

    fn process_element(
        &mut self,
        _key: &i32,
        value: Event,
        state: &mut DiskBusyState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let mut buf = Vec::new();
        write_sizes(self.bytes).try_for_each(|size| {
            // Java fills each buffer with the low byte of the current time.
            let fill = Instant::now().as_millis().to_le_bytes()[0];
            #[expect(
                clippy::expect_used,
                reason = "MAX_BUFFER_SIZE fits usize on 32-bit and 64-bit targets"
            )]
            let len = usize::try_from(size).expect("a write is at most MAX_BUFFER_SIZE bytes");
            buf.clear();
            buf.resize(len, fill);
            state.buffer.write(&buf)
        })?;
        ctx.output(value).emit()
    }
}
