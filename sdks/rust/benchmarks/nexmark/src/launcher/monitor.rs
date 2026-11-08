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

//! Pass-through transform that records throughput and timing metrics.

use std::fmt::Debug;

use beam::coders::DefaultCoder;
use beam::metrics::Metrics;
use beam::prelude::*;

use crate::model::KnownSize;

/// Metric namespace of the [`Monitor`] metrics.
pub const METRIC_NAMESPACE: &str = "nexmark";

/// Prefix of the metrics that the monitor after the source records.
pub const EVENTS: &str = "events";

/// Prefix of the metrics that the monitor before the sink records.
pub const RESULTS: &str = "results";

/// Counter of the elements that pass the monitor.
pub const ELEMENTS: &str = "elements";

/// Counter of the [`KnownSize`] bytes of the elements.
pub const BYTES: &str = "bytes";

/// Distribution of the wall-clock milliseconds at which the monitor sees each element.
pub const PROCESSING_TIME_MS: &str = "processing_time_ms";

/// Distribution of the element timestamps, in milliseconds.
pub const EVENT_TIMESTAMP_MS: &str = "event_timestamp_ms";

/// Full metric name, for example `events.elements`.
pub fn metric_name(prefix: &str, metric: &str) -> String {
    format!("{prefix}.{metric}")
}

/// Records [`ELEMENTS`], [`BYTES`], [`PROCESSING_TIME_MS`] and [`EVENT_TIMESTAMP_MS`] for
/// each element under one prefix, and outputs the element without change.
///
/// The minimum and maximum of the processing time give the first and last time that the
/// monitor saw an element.
#[derive(Clone, Copy, Debug)]
pub struct Monitor {
    prefix: &'static str,
    log_elements: bool,
}

impl Monitor {
    /// Creates a monitor that records its metrics under `prefix`.
    pub fn new(prefix: &'static str) -> Self {
        Self {
            prefix,
            log_elements: false,
        }
    }

    /// Logs each element at the `info` level when `log_elements` is true.
    pub fn with_logging(self, log_elements: bool) -> Self {
        Self {
            log_elements,
            ..self
        }
    }
}

impl<T> PTransform<PCollection<T>> for Monitor
where
    T: DefaultCoder + KnownSize + Debug + Send + Sync + 'static,
{
    type Output = PCollection<T>;

    fn expand(self, input: &PCollection<T>) -> PCollection<T> {
        let Self {
            prefix,
            log_elements,
        } = self;
        let elements = Metrics::counter(METRIC_NAMESPACE, metric_name(prefix, ELEMENTS));
        let bytes = Metrics::counter(METRIC_NAMESPACE, metric_name(prefix, BYTES));
        let processing_time =
            Metrics::distribution(METRIC_NAMESPACE, metric_name(prefix, PROCESSING_TIME_MS));
        let event_timestamp =
            Metrics::distribution(METRIC_NAMESPACE, metric_name(prefix, EVENT_TIMESTAMP_MS));
        input.par_do_fn("Record", move |element: T, ctx| {
            elements.inc();
            bytes.inc_by(i64::try_from(element.estimated_size_bytes()).unwrap_or(i64::MAX));
            processing_time.update(Instant::now().as_millis());
            event_timestamp.update(ctx.timestamp().as_millis());
            if log_elements {
                tracing::info!(prefix, element = ?element, "nexmark element");
            }
            ctx.emit(element)
        })
    }
}
