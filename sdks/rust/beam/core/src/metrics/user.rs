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

use std::borrow::Cow;
use std::sync::Arc;
use std::time::SystemTime;

use super::context::MetricsContainer;
use super::scope;
use super::values::HistogramBuckets;

/// A metric's namespace and name. A `&'static str` name does not allocate, so a handle made
/// for each element costs no heap work.
#[derive(Clone, Debug, PartialEq, Eq)]
struct MetricName {
    namespace: Cow<'static, str>,
    name: Cow<'static, str>,
}

impl MetricName {
    fn new(namespace: impl Into<Cow<'static, str>>, name: impl Into<Cow<'static, str>>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
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
    fn new(namespace: impl Into<Cow<'static, str>>, name: impl Into<Cow<'static, str>>) -> Self {
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

/// Declares a user metric handle: a [`MetricHandle`] with its constructor, `bind`, `namespace`
/// and `name`. Equality compares only the namespace and the name.
macro_rules! metric_handle {
    ($(#[$doc:meta])* $handle:ident) => {
        $(#[$doc])*
        #[derive(Clone, Debug)]
        pub struct $handle {
            handle: MetricHandle,
        }

        impl PartialEq for $handle {
            fn eq(&self, other: &Self) -> bool {
                self.handle.name == other.handle.name
            }
        }

        impl Eq for $handle {}

        impl $handle {
            pub(crate) fn new(namespace: impl Into<Cow<'static, str>>, name: impl Into<Cow<'static, str>>) -> Self {
                Self {
                    handle: MetricHandle::new(namespace, name),
                }
            }

            /// Returns a copy bound to `container` and `transform_id`.
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
        }
    };
}

metric_handle! {
    /// A user-defined 64-bit integer counter. Runners aggregate the totals across bundles.
    Counter
}

impl Counter {
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

metric_handle! {
    /// A user-defined distribution of 64-bit integers: count, sum, minimum and maximum.
    Distribution
}

impl Distribution {
    /// Adds the sample `value` to the distribution.
    pub fn update(&self, value: i64) {
        self.handle
            .record(|container, transform_id, namespace, name| {
                container.update_distribution(transform_id, namespace, name, value);
            });
    }
}

metric_handle! {
    /// A user-defined gauge that reports the latest value.
    Gauge
}

impl Gauge {
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

metric_handle! {
    /// A user-defined set of distinct strings, for example the sources that a transform reads.
    /// Runners take the union across bundles.
    StringSet
}

impl StringSet {
    /// Adds `value` to the set.
    pub fn add(&self, value: &str) {
        self.handle
            .record(|container, transform_id, namespace, name| {
                container.add_to_string_set(transform_id, namespace, name, value);
            });
    }
}

metric_handle! {
    /// A user-defined set of paths that keeps at most
    /// [`BOUNDED_TRIE_DEFAULT_BOUND`](super::BOUNDED_TRIE_DEFAULT_BOUND) paths and truncates
    /// the rest.
    BoundedTrie
}

impl BoundedTrie {
    /// Adds the path `segments`, for example `&["bigquery", "project", "dataset.table"]`.
    pub fn add(&self, segments: &[&str]) {
        self.handle
            .record(|container, transform_id, namespace, name| {
                container.add_to_bounded_trie(transform_id, namespace, name, segments);
            });
    }
}

/// A user-defined histogram: counts of values per bucket. Equality compares only the namespace
/// and the name.
#[derive(Clone, Debug)]
pub struct Histogram {
    handle: MetricHandle,
    buckets: HistogramBuckets,
}

impl PartialEq for Histogram {
    fn eq(&self, other: &Self) -> bool {
        self.handle.name == other.handle.name
    }
}

impl Eq for Histogram {}

impl Histogram {
    pub(crate) fn new(
        namespace: impl Into<Cow<'static, str>>,
        name: impl Into<Cow<'static, str>>,
        buckets: HistogramBuckets,
    ) -> Self {
        Self {
            handle: MetricHandle::new(namespace, name),
            buckets,
        }
    }

    /// Returns a copy of this histogram bound to `container` and `transform_id`.
    pub fn bind(
        &self,
        container: Arc<MetricsContainer>,
        transform_id: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            handle: self.handle.bind(container, transform_id),
            buckets: self.buckets,
        }
    }

    pub fn namespace(&self) -> &str {
        self.handle.namespace()
    }

    pub fn name(&self) -> &str {
        self.handle.name()
    }

    /// Counts `value` in its bucket. A value outside the buckets counts only in the total.
    pub fn update(&self, value: f64) {
        let buckets = self.buckets;
        self.handle
            .record(|container, transform_id, namespace, name| {
                container.update_histogram(transform_id, namespace, name, buckets, value);
            });
    }
}

/// Entry point to create pipeline metrics.
///
/// A metric records against the transform that runs on the current thread. A thread that user
/// code starts has no transform, so its metrics go to the process-wide container. To keep the
/// transform, `bind` the metric to a container and transform ID.
pub struct Metrics;

impl Metrics {
    /// Returns an unbound [`Counter`] that records against the running transform, from any
    /// code that a transform calls.
    pub fn counter(
        namespace: impl Into<Cow<'static, str>>,
        name: impl Into<Cow<'static, str>>,
    ) -> Counter {
        Counter::new(namespace, name)
    }

    /// Returns an unbound [`Distribution`] that records against the running transform.
    pub fn distribution(
        namespace: impl Into<Cow<'static, str>>,
        name: impl Into<Cow<'static, str>>,
    ) -> Distribution {
        Distribution::new(namespace, name)
    }

    /// Returns an unbound [`Gauge`] that records against the running transform.
    pub fn gauge(
        namespace: impl Into<Cow<'static, str>>,
        name: impl Into<Cow<'static, str>>,
    ) -> Gauge {
        Gauge::new(namespace, name)
    }

    /// Returns an unbound [`StringSet`] that records against the running transform.
    pub fn string_set(
        namespace: impl Into<Cow<'static, str>>,
        name: impl Into<Cow<'static, str>>,
    ) -> StringSet {
        StringSet::new(namespace, name)
    }

    /// Returns an unbound [`BoundedTrie`] that records against the running transform.
    pub fn bounded_trie(
        namespace: impl Into<Cow<'static, str>>,
        name: impl Into<Cow<'static, str>>,
    ) -> BoundedTrie {
        BoundedTrie::new(namespace, name)
    }

    /// Returns an unbound [`Histogram`] with `buckets` that records against the running
    /// transform.
    pub fn histogram(
        namespace: impl Into<Cow<'static, str>>,
        name: impl Into<Cow<'static, str>>,
        buckets: HistogramBuckets,
    ) -> Histogram {
        Histogram::new(namespace, name, buckets)
    }
}
