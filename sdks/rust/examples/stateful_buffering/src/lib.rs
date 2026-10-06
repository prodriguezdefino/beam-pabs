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

//! A stateful DoFn that buffers the values of each key and flushes them as one batch.
//!
//! [`BufferAndFlush`] shows user state and timers:
//!
//! - [`Buffer`], declared with `#[derive(State)]`: a `BagState` with the buffered values and
//!   a `CombiningState` with their running sum;
//! - [`Flush`], declared with `#[derive(Timers)]`: an event-time timer at the end of the
//!   window, and a processing-time timer that flushes a batch that waits too long.
//!
//! The first value of a batch sets both timers. The timer that fires first emits the
//! [`Batch`] and clears the other timer.
//!
//! Run on Prism, the default runner:
//! ```text
//! cargo run -p stateful_buffering -- --output=/tmp/batches.txt
//! ```

use std::fmt;
use std::time::Duration;

use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// The options of the example.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(
    name = "stateful_buffering",
    about = "Apache Beam stateful buffering example",
    version
)]
pub struct Args {
    /// Output file path for the batches.
    #[arg(long, default_value = "/tmp/stateful_buffering_output.txt")]
    pub output: String,

    /// Number of generated readings.
    #[arg(long, default_value_t = 100)]
    pub readings: i64,

    /// Generated readings per second.
    #[arg(long, default_value_t = 20, value_parser = clap::value_parser!(i64).range(1..))]
    pub per_second: i64,

    /// Number of sensor keys.
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(i64).range(1..))]
    pub sensors: i64,

    /// Fixed window size in seconds.
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u64).range(1..))]
    pub window_secs: u64,

    /// Processing-time limit in milliseconds for a batch to wait.
    #[arg(long, default_value_t = 500, value_parser = clap::value_parser!(u64).range(1..))]
    pub timeout_millis: u64,
}

impl PipelineOptionGroup for Args {}

/// The state of one key and window.
#[derive(State)]
pub struct Buffer {
    values: BagState<i64>,
    sum: CombiningState<Sum<i64>>,
}

/// The timers that flush a batch. Each one is also the reason of the flush in a [`Batch`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Timers, BeamEnum)]
pub enum Flush {
    #[timer(event_time)]
    WindowEnd,
    #[timer(processing_time)]
    Timeout,
}

/// The buffered values of one key, in ascending order, with their sum.
#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct Batch {
    pub key: String,
    pub values: Vec<i64>,
    pub sum: i64,
    pub reason: Flush,
}

impl fmt::Display for Batch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            key,
            values,
            sum,
            reason,
        } = self;
        write!(f, "{key} {reason:?}: sum={sum} values={values:?}")
    }
}

/// Buffers the values of each key and window, and emits them as one [`Batch`].
#[derive(Clone, Serialize, Deserialize, Register)]
pub struct BufferAndFlush {
    timeout: Duration,
}

impl BufferAndFlush {
    /// Flushes a batch at the end of its window, or `timeout` after its first value.
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }
}

impl StatefulDoFn for BufferAndFlush {
    type Key = String;
    type Value = i64;
    type Out = Batch;
    type State = Buffer;
    type Timer = Flush;

    fn process_element(
        &mut self,
        _key: &String,
        value: i64,
        state: &mut Buffer,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        if state.sum.is_empty()? {
            ctx.timer(Flush::WindowEnd)
                .at(ctx.window()?.max_timestamp())?;
            ctx.timer(Flush::Timeout).after(self.timeout)?;
        }
        state.values.append(value)?;
        state.sum.add(value)
    }

    fn on_timer(
        &mut self,
        timer: Flush,
        key: &String,
        state: &mut Buffer,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        // A timer can fire after the other timer flushed the batch and cleared it.
        if state.sum.is_empty()? {
            return Ok(());
        }
        let other = match timer {
            Flush::WindowEnd => Flush::Timeout,
            Flush::Timeout => Flush::WindowEnd,
        };
        ctx.timer(other).clear()?;
        // The order of a bag is not defined, so sort it for a stable output.
        let mut values = state.values.read()?;
        values.sort_unstable();
        let batch = Batch {
            key: key.clone(),
            values,
            sum: state.sum.read()?,
            reason: timer,
        };
        state.values.clear()?;
        state.sum.clear()?;
        ctx.emit(batch)
    }
}

/// Windows keyed values into fixed windows and buffers them with [`BufferAndFlush`].
pub struct BufferPerKey {
    window: Duration,
    timeout: Duration,
}

impl BufferPerKey {
    pub fn new(window: Duration, timeout: Duration) -> Self {
        Self { window, timeout }
    }
}

impl PTransform<PCollection<(String, i64)>> for BufferPerKey {
    type Output = PCollection<Batch>;

    fn expand(&self, input: &PCollection<(String, i64)>) -> PCollection<Batch> {
        input
            .window_into("FixedWindows", FixedWindows::of(self.window))
            .apply(
                "BufferAndFlush",
                ParDo::new(BufferAndFlush::new(self.timeout)),
            )
    }
}

/// Generates sensor readings at a fixed rate, buffers them per sensor and writes the
/// batches.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);
    let sensors = args.sensors;
    p.apply(
        "GenerateReadings",
        GenerateSequence::new(0)
            .with_end(args.readings)
            .with_rate(args.per_second, Duration::from_secs(1)),
    )
    .map("KeyBySensor", move |n: i64| {
        (format!("sensor-{}", n % sensors), n)
    })
    .apply(
        "BufferPerKey",
        BufferPerKey::new(
            Duration::from_secs(args.window_secs),
            Duration::from_millis(args.timeout_millis),
        ),
    )
    .map("FormatBatches", |batch: Batch| batch.to_string())
    .apply("WriteBatches", textio::Write::new(&args.output));
    p
}
