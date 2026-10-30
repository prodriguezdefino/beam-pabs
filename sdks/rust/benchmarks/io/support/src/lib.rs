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

//! Shared launcher, `Touch` steps, flag parsers and metric reporting for the I/O benchmarks.

use std::error::Error;
use std::iter;
use std::marker::PhantomData;

use beam::coders::DefaultCoder;
use beam::logging::init_logging;
use beam::metrics::MetricResults;
use beam::options::{PipelineOptionGroup, parse};
use beam::prelude::*;
use clap::{Args as ClapArgs, ValueEnum};
use serde::{Deserialize, Serialize};

/// Metric namespace of the benchmark `Touch` steps.
pub const NAMESPACE: &str = "e2e";

/// Maximum consumer pool size accepted by the Kafka read.
const MAX_CONSUMER_POOL_SIZE: usize = 16;

/// Upper bounds in milliseconds of the end-to-end latency buckets.
///
/// Keep in sync with `LATENCY_BOUNDS_MS` in `KafkaBenchmark.java`, `kafka_benchmark.py` and
/// `benchmarks/tools/io_report.py`.
pub const LATENCY_BOUNDS_MS: [i64; 12] = [
    50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 60_000, 120_000, 300_000,
];

/// Connector selector flag accepted by the Rust benchmark pipelines.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Connector {
    /// Connector of the Rust SDK.
    Rust,
}

/// Start position of a Kafka partition that has no committed offset (`auto.offset.reset`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum AutoOffsetReset {
    /// First record of the partition.
    Earliest,
    /// Next record written to the partition.
    Latest,
}

/// Splits one `key=value` pair at the first `=`.
///
/// # Errors
///
/// Fails if `pair` has no `=`, or if the key is empty or contains whitespace.
pub fn parse_property(pair: &str) -> std::result::Result<(String, String), String> {
    match pair.split_once('=') {
        Some((key, _)) if key.is_empty() || key.contains(char::is_whitespace) => Err(format!(
            "The key of '{pair}' must not be empty or have a space"
        )),
        Some((key, value)) => Ok((key.to_string(), value.to_string())),
        None => Err(format!("'{pair}' is not a key=value pair")),
    }
}

/// Parses `--consumer_pool_size` in `1..=16`.
///
/// # Errors
///
/// Fails if `value` is not an integer in `1..=16`.
pub fn parse_consumer_pool_size(value: &str) -> std::result::Result<usize, String> {
    let size = value
        .parse::<usize>()
        .map_err(|err| format!("'{value}' is not a valid consumer_pool_size: {err}"))?;
    if (1..=MAX_CONSUMER_POOL_SIZE).contains(&size) {
        Ok(size)
    } else {
        Err(format!(
            "consumer_pool_size must be in 1..={MAX_CONSUMER_POOL_SIZE}, got {size}"
        ))
    }
}

/// Counter names of the latency buckets, ending with `latency_le_inf`.
pub fn bucket_names() -> impl Iterator<Item = String> {
    LATENCY_BOUNDS_MS
        .iter()
        .map(|bound| format!("latency_le_{bound}"))
        .chain(iter::once("latency_le_inf".to_string()))
}

/// Bucket index of `latency`: the first bound not smaller than `latency`, or the `inf` index.
pub fn bucket(latency: i64) -> usize {
    LATENCY_BOUNDS_MS.partition_point(|&bound| latency > bound)
}

/// Counts `e2e/rows` and emits each element with no change.
pub struct TouchFn<T = Row> {
    rows: Counter,
    element: PhantomData<fn(T) -> T>,
}

impl<T> TouchFn<T> {
    /// Creates a `TouchFn` that counts `e2e/rows`.
    pub fn new() -> Self {
        Self {
            rows: Metrics::counter(NAMESPACE, "rows"),
            element: PhantomData,
        }
    }
}

impl<T> Clone for TouchFn<T> {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
            element: PhantomData,
        }
    }
}

impl<T> Default for TouchFn<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DefaultCoder> DoFn for TouchFn<T> {
    type In = T;
    type Out = T;

    fn process_element(&mut self, element: T, ctx: &mut ProcessContext<'_, T>) -> Result {
        self.rows.inc();
        ctx.emit(element)
    }
}

/// Counts `e2e/rows`, records `e2e/latency_ms` and bucket counters, and emits each element.
pub struct TouchCopyFn<T = Row> {
    rows: Counter,
    latency_ms: Distribution,
    latency_buckets: Vec<Counter>,
    element: PhantomData<fn(T) -> T>,
}

impl<T> TouchCopyFn<T> {
    /// Creates a `TouchCopyFn` that records row count and element-timestamp latency.
    pub fn new() -> Self {
        Self {
            rows: Metrics::counter(NAMESPACE, "rows"),
            latency_ms: Metrics::distribution(NAMESPACE, "latency_ms"),
            latency_buckets: bucket_names()
                .map(|name| Metrics::counter(NAMESPACE, name))
                .collect(),
            element: PhantomData,
        }
    }
}

impl<T> Clone for TouchCopyFn<T> {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
            latency_ms: self.latency_ms.clone(),
            latency_buckets: self.latency_buckets.clone(),
            element: PhantomData,
        }
    }
}

impl<T> Default for TouchCopyFn<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DefaultCoder> DoFn for TouchCopyFn<T> {
    type In = T;
    type Out = T;

    fn process_element(&mut self, element: T, ctx: &mut ProcessContext<'_, T>) -> Result {
        self.rows.inc();
        let timestamp_ms = ctx.timestamp().as_millis();
        let now_ms = Instant::now().as_millis();
        if timestamp_ms > 0 && now_ms >= timestamp_ms {
            let latency = now_ms - timestamp_ms;
            self.latency_ms.update(latency);
            self.latency_buckets[bucket(latency)].inc();
        }
        ctx.emit(element)
    }
}

/// Adds a global `i64` count to `e2e/rows` and emits the count.
#[derive(Clone)]
pub struct RecordRowsFn {
    rows: Counter,
}

impl RecordRowsFn {
    /// Creates a `RecordRowsFn` that increments `e2e/rows` by the input total.
    pub fn new() -> Self {
        Self {
            rows: Metrics::counter(NAMESPACE, "rows"),
        }
    }
}

impl Default for RecordRowsFn {
    fn default() -> Self {
        Self::new()
    }
}

impl DoFn for RecordRowsFn {
    type In = i64;
    type Out = i64;

    fn process_element(&mut self, total: i64, ctx: &mut ProcessContext<'_, i64>) -> Result {
        self.rows.inc_by(total);
        ctx.emit(total)
    }
}

/// Prints the `e2e` row count and, when present, latency metrics of a finished run.
pub fn print_metrics(metrics: &MetricResults) {
    let counter = |name: &str| metrics.counter(NAMESPACE, name).unwrap_or(0);
    println!("e2e/rows={}", counter("rows"));
    if let Some(latency) = metrics.distribution(NAMESPACE, "latency_ms") {
        let mean = if latency.count > 0 {
            latency.sum / latency.count
        } else {
            0
        };
        println!(
            "e2e/latency_ms count={} mean={mean} min={} max={}",
            latency.count, latency.min, latency.max
        );
        let buckets = bucket_names()
            .map(|name| {
                let label = name.strip_prefix("latency_").unwrap_or(&name);
                format!("{label}={}", counter(&name))
            })
            .collect::<Vec<_>>();
        println!("e2e/latency buckets: {}", buckets.join(" "));
    }
}

/// Parses CLI options, builds the benchmark pipeline, runs it and prints `JOB_ID` and metrics.
///
/// # Errors
///
/// Fails if pipeline construction or execution fails.
pub async fn run<A, F>(build: F) -> std::result::Result<(), Box<dyn Error + Send + Sync>>
where
    A: ClapArgs + PipelineOptionGroup,
    F: FnOnce(&PipelineOptions, &A) -> Result<Pipeline>,
{
    run_with_options(|_, _| {}, build).await
}

/// Parses CLI options, updates `PipelineOptions`, builds and runs the benchmark pipeline.
///
/// # Errors
///
/// Fails if pipeline construction or execution fails.
pub async fn run_with_options<A, C, F>(
    configure: C,
    build: F,
) -> std::result::Result<(), Box<dyn Error + Send + Sync>>
where
    A: ClapArgs + PipelineOptionGroup,
    C: FnOnce(&mut PipelineOptions, &A),
    F: FnOnce(&PipelineOptions, &A) -> Result<Pipeline>,
{
    init_logging();
    let (mut options, args) = parse::<A>();
    configure(&mut options, &args);
    let p = build(&options, &args).map_err(|e| e.to_string())?;
    let result = p.run().await?;
    // The benchmark script finds the job by its name; this line is for the log.
    println!("JOB_ID={}", result.job_id);
    if let Some(metrics) = result.metrics() {
        print_metrics(metrics);
    }
    Ok(())
}
