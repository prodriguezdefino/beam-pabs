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

//! Metrics API for pipeline authors.
//!
//! User code can create a metric anywhere it runs:
//!
//! ```ignore
//! Metrics::counter("wordcount", "empty_lines").inc();
//! ```
//!
//! An unbound metric records into the [`MetricsScope`](crate::internals::MetricsScope) of the active
//! transform. When no scope is active, it records into the process-wide container. To bind a
//! metric to a [`MetricsContainer`](crate::internals::MetricsContainer), use `bind`.

use std::sync::Arc;
use std::time::SystemTime;

use super::context::MetricsContainer;
use super::scope;

#[derive(Clone, Debug, PartialEq, Eq)]
struct MetricName {
    namespace: Arc<str>,
    name: Arc<str>,
}

impl MetricName {
    fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: Arc::from(namespace.into()),
            name: Arc::from(name.into()),
        }
    }
}

#[derive(Clone, Debug)]
enum Binding {
    Scoped,
    Bound {
        container: Arc<MetricsContainer>,
        transform_id: Arc<str>,
    },
}

#[derive(Clone, Debug)]
struct MetricHandle {
    name: MetricName,
    binding: Binding,
}

impl MetricHandle {
    fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            name: MetricName::new(namespace, name),
            binding: Binding::Scoped,
        }
    }

    fn bind(&self, container: Arc<MetricsContainer>, transform_id: impl Into<Arc<str>>) -> Self {
        Self {
            name: self.name.clone(),
            binding: Binding::Bound {
                container,
                transform_id: transform_id.into(),
            },
        }
    }

    fn namespace(&self) -> &str {
        &self.name.namespace
    }

    fn name(&self) -> &str {
        &self.name.name
    }

    fn record(&self, f: impl FnOnce(&MetricsContainer, &str, &str, &str)) {
        match &self.binding {
            Binding::Bound {
                container,
                transform_id,
            } => {
                f(
                    container,
                    transform_id,
                    &self.name.namespace,
                    &self.name.name,
                );
            }
            Binding::Scoped => {
                scope::with_current(|container, transform_id| {
                    f(
                        container,
                        transform_id,
                        &self.name.namespace,
                        &self.name.name,
                    );
                });
            }
        }
    }
}

/// A user-defined 64-bit integer counter. Runners aggregate the totals across bundles.
/// Equality compares only the namespace and the name.
#[derive(Clone, Debug)]
pub struct Counter {
    handle: MetricHandle,
}

impl PartialEq for Counter {
    fn eq(&self, other: &Self) -> bool {
        self.handle.name == other.handle.name
    }
}

impl Eq for Counter {}

impl Counter {
    /// Creates an unbound counter with this namespace and name.
    pub(crate) fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            handle: MetricHandle::new(namespace, name),
        }
    }

    /// Returns a copy of this counter bound to `container` and `transform_id`.
    pub fn bind(
        &self,
        container: Arc<MetricsContainer>,
        transform_id: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            handle: self.handle.bind(container, transform_id),
        }
    }

    pub fn namespace(&self) -> &str {
        self.handle.namespace()
    }

    pub fn name(&self) -> &str {
        self.handle.name()
    }

    /// Increments this counter by 1.
    pub fn inc(&self) {
        self.inc_by(1);
    }

    /// Increments this counter by `n`.
    pub fn inc_by(&self, n: i64) {
        self.handle
            .record(|container, transform_id, namespace, name| {
                container.inc_counter(transform_id, namespace, name, n);
            });
    }

    /// Decrements this counter by 1.
    pub fn dec(&self) {
        self.inc_by(-1);
    }

    /// Decrements this counter by `n`.
    pub fn dec_by(&self, n: i64) {
        self.inc_by(-n);
    }
}

/// A user-defined distribution of 64-bit integers: count, sum, minimum and maximum. Equality
/// compares only the namespace and the name.
#[derive(Clone, Debug)]
pub struct Distribution {
    handle: MetricHandle,
}

impl PartialEq for Distribution {
    fn eq(&self, other: &Self) -> bool {
        self.handle.name == other.handle.name
    }
}

impl Eq for Distribution {}

impl Distribution {
    /// Creates an unbound distribution with this namespace and name.
    pub(crate) fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            handle: MetricHandle::new(namespace, name),
        }
    }

    /// Returns a copy of this distribution bound to `container` and `transform_id`.
    pub fn bind(
        &self,
        container: Arc<MetricsContainer>,
        transform_id: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            handle: self.handle.bind(container, transform_id),
        }
    }

    pub fn namespace(&self) -> &str {
        self.handle.namespace()
    }

    pub fn name(&self) -> &str {
        self.handle.name()
    }

    /// Adds the sample `value` to the distribution.
    pub fn update(&self, value: i64) {
        self.handle
            .record(|container, transform_id, namespace, name| {
                container.update_distribution(transform_id, namespace, name, value);
            });
    }
}

/// A user-defined gauge that reports the latest value. Equality compares only the namespace
/// and the name.
#[derive(Clone, Debug)]
pub struct Gauge {
    handle: MetricHandle,
}

impl PartialEq for Gauge {
    fn eq(&self, other: &Self) -> bool {
        self.handle.name == other.handle.name
    }
}

impl Eq for Gauge {}

impl Gauge {
    /// Creates an unbound gauge with this namespace and name.
    pub(crate) fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            handle: MetricHandle::new(namespace, name),
        }
    }

    /// Returns a copy of this gauge bound to `container` and `transform_id`.
    pub fn bind(
        &self,
        container: Arc<MetricsContainer>,
        transform_id: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            handle: self.handle.bind(container, transform_id),
        }
    }

    pub fn namespace(&self) -> &str {
        self.handle.namespace()
    }

    pub fn name(&self) -> &str {
        self.handle.name()
    }

    /// Sets the gauge to `value` at the current system time.
    pub fn set(&self, value: i64) {
        let timestamp_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        self.handle
            .record(|container, transform_id, namespace, name| {
                container.set_gauge(transform_id, namespace, name, value, timestamp_ms);
            });
    }
}

/// Entry point to create pipeline metrics.
pub struct Metrics;

impl Metrics {
    /// Returns an unbound [`Counter`] that records against the running transform, from any
    /// code that a transform calls.
    pub fn counter(namespace: impl Into<String>, name: impl Into<String>) -> Counter {
        Counter::new(namespace, name)
    }

    /// Returns an unbound [`Distribution`] that records against the running transform.
    pub fn distribution(namespace: impl Into<String>, name: impl Into<String>) -> Distribution {
        Distribution::new(namespace, name)
    }

    /// Returns an unbound [`Gauge`] that records against the running transform.
    pub fn gauge(namespace: impl Into<String>, name: impl Into<String>) -> Gauge {
        Gauge::new(namespace, name)
    }
}
