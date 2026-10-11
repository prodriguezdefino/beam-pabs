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
//! An unbound metric records into the [`MetricsScope`](super::MetricsScope) that the worker
//! harness entered for the running transform, and does nothing outside a scope, for example in
//! a plain unit test. To bind a metric to a [`MetricsContainer`], use `with_container` or `bind`.

use std::sync::Arc;
use std::time::SystemTime;

use super::context::MetricsContainer;
use super::scope;

/// A user-defined 64-bit integer counter. Runners aggregate the totals across bundles.
/// Equality compares only the namespace and the name.
#[derive(Clone, Debug)]
pub struct Counter {
    namespace: String,
    name: String,
    container: Option<Arc<MetricsContainer>>,
    transform_id: String,
}

impl PartialEq for Counter {
    fn eq(&self, other: &Self) -> bool {
        self.namespace == other.namespace && self.name == other.name
    }
}

impl Eq for Counter {}

impl Counter {
    /// Creates an unbound counter with this namespace and name.
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            container: None,
            transform_id: String::new(),
        }
    }

    /// Creates a counter bound to `container` and `transform_id`.
    pub fn with_container(
        namespace: impl Into<String>,
        name: impl Into<String>,
        container: Arc<MetricsContainer>,
        transform_id: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            container: Some(container),
            transform_id: transform_id.into(),
        }
    }

    /// Returns a copy of this counter bound to `container` and `transform_id`.
    pub fn bind(&self, container: Arc<MetricsContainer>, transform_id: impl Into<String>) -> Self {
        Self {
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            container: Some(container),
            transform_id: transform_id.into(),
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Increments this counter by 1.
    pub fn inc(&self) {
        self.inc_by(1);
    }

    /// Increments this counter by `n`.
    pub fn inc_by(&self, n: i64) {
        match &self.container {
            Some(container) => container.inc_counter(
                bound_transform(&self.transform_id),
                &self.namespace,
                &self.name,
                n,
            ),
            None => scope::with_current(|container, transform_id| {
                container.inc_counter(transform_id, &self.namespace, &self.name, n);
            }),
        }
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
    namespace: String,
    name: String,
    container: Option<Arc<MetricsContainer>>,
    transform_id: String,
}

impl PartialEq for Distribution {
    fn eq(&self, other: &Self) -> bool {
        self.namespace == other.namespace && self.name == other.name
    }
}

impl Eq for Distribution {}

impl Distribution {
    /// Creates an unbound distribution with this namespace and name.
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            container: None,
            transform_id: String::new(),
        }
    }

    /// Creates a distribution bound to `container` and `transform_id`.
    pub fn with_container(
        namespace: impl Into<String>,
        name: impl Into<String>,
        container: Arc<MetricsContainer>,
        transform_id: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            container: Some(container),
            transform_id: transform_id.into(),
        }
    }

    /// Returns a copy of this distribution bound to `container` and `transform_id`.
    pub fn bind(&self, container: Arc<MetricsContainer>, transform_id: impl Into<String>) -> Self {
        Self {
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            container: Some(container),
            transform_id: transform_id.into(),
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Adds the sample `value` to the distribution.
    pub fn update(&self, value: i64) {
        match &self.container {
            Some(container) => container.update_distribution(
                bound_transform(&self.transform_id),
                &self.namespace,
                &self.name,
                value,
            ),
            None => scope::with_current(|container, transform_id| {
                container.update_distribution(transform_id, &self.namespace, &self.name, value);
            }),
        }
    }
}

/// A user-defined gauge that reports the latest value. Equality compares only the namespace
/// and the name.
#[derive(Clone, Debug)]
pub struct Gauge {
    namespace: String,
    name: String,
    container: Option<Arc<MetricsContainer>>,
    transform_id: String,
}

impl PartialEq for Gauge {
    fn eq(&self, other: &Self) -> bool {
        self.namespace == other.namespace && self.name == other.name
    }
}

impl Eq for Gauge {}

impl Gauge {
    /// Creates an unbound gauge with this namespace and name.
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            container: None,
            transform_id: String::new(),
        }
    }

    /// Creates a gauge bound to `container` and `transform_id`.
    pub fn with_container(
        namespace: impl Into<String>,
        name: impl Into<String>,
        container: Arc<MetricsContainer>,
        transform_id: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            container: Some(container),
            transform_id: transform_id.into(),
        }
    }

    /// Returns a copy of this gauge bound to `container` and `transform_id`.
    pub fn bind(&self, container: Arc<MetricsContainer>, transform_id: impl Into<String>) -> Self {
        Self {
            namespace: self.namespace.clone(),
            name: self.name.clone(),
            container: Some(container),
            transform_id: transform_id.into(),
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Sets the gauge to `value` at the current system time.
    pub fn set(&self, value: i64) {
        let timestamp_ms = || {
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        };
        match &self.container {
            Some(container) => container.set_gauge(
                bound_transform(&self.transform_id),
                &self.namespace,
                &self.name,
                value,
                timestamp_ms(),
            ),
            None => scope::with_current(|container, transform_id| {
                container.set_gauge(
                    transform_id,
                    &self.namespace,
                    &self.name,
                    value,
                    timestamp_ms(),
                );
            }),
        }
    }
}

/// Returns the transform ID for a bound metric. An empty ID becomes `"user"`.
fn bound_transform(transform_id: &str) -> &str {
    if transform_id.is_empty() {
        "user"
    } else {
        transform_id
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
