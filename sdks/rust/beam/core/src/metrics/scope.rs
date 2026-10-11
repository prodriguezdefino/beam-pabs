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

//! The metrics container and the transform that the running user code belongs to.
//!
//! The worker harness enters a [`MetricsScope`] around each call into a transform, so a
//! metric created anywhere in that call, even `Metrics::counter("ns", "name").inc()` in a
//! helper, records against the running transform. Scopes nest: a consumer that runs inside
//! the producer's call has its own scope until that call returns.

use std::cell::RefCell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use super::context::MetricsContainer;

enum FrameContainer {
    Bundle(Arc<MetricsContainer>),
    ProcessWide,
}

/// The metrics of one bundle: its container, its transforms, and the running transform.
///
/// The worker selects the running transform for each call into a fused operator. The
/// selection is an atomic store into this value, so it does not touch thread-local storage.
pub struct TransformMetrics {
    container: Arc<MetricsContainer>,
    transform_ids: Arc<[Arc<str>]>,
    /// Index into `transform_ids`. It is out of range when no transform runs.
    current: AtomicUsize,
}

impl TransformMetrics {
    pub fn new(container: Arc<MetricsContainer>, transform_ids: Arc<[Arc<str>]>) -> Self {
        let none = transform_ids.len();
        Self {
            container,
            transform_ids,
            current: AtomicUsize::new(none),
        }
    }

    /// Makes transform `index` the running transform until the returned guard drops.
    pub fn select(&self, index: usize) -> TransformSelection<'_> {
        // One thread selects and records, so a relaxed load and store are enough. A `swap`
        // would be a locked instruction on x86.
        let previous = self.current.load(Ordering::Relaxed);
        self.current.store(index, Ordering::Relaxed);
        TransformSelection {
            metrics: self,
            previous,
        }
    }

    fn running(&self) -> Option<&str> {
        let index = self.current.load(Ordering::Relaxed);
        self.transform_ids.get(index).map(AsRef::as_ref)
    }
}

/// One entered scope.
enum Frame {
    /// One transform, in a bundle container or in the process-wide container.
    Single {
        container: FrameContainer,
        transform_id: Arc<str>,
    },
    /// The fused transforms of a bundle. [`TransformMetrics::select`] sets the running one.
    Transforms(Arc<TransformMetrics>),
}

thread_local! {
    static CURRENT: RefCell<Vec<Frame>> = const { RefCell::new(Vec::new()) };
}

/// Makes a container and transform current on this thread until dropped. Unbound metrics, such
/// as [`Metrics::counter`](super::Metrics::counter), record into the innermost active scope, or
/// into the process-wide container when no scope is active.
#[must_use = "the scope ends as soon as it is dropped"]
pub struct MetricsScope {
    depth: usize,
    // Scopes are a per-thread stack. A drop on another thread would pop the wrong entry.
    _not_send: PhantomData<*const ()>,
}

impl MetricsScope {
    /// Enters a scope that records unbound metrics into `container` under `transform_id`.
    pub fn enter(container: Arc<MetricsContainer>, transform_id: Arc<str>) -> Self {
        Self::push(Frame::Single {
            container: FrameContainer::Bundle(container),
            transform_id,
        })
    }

    /// Enters a scope that records metrics into the process-wide container under `transform_id`.
    pub fn enter_process_wide(transform_id: Arc<str>) -> Self {
        Self::push(Frame::Single {
            container: FrameContainer::ProcessWide,
            transform_id,
        })
    }

    /// Enters a scope for a sequence of calls into the transforms of `metrics`. Metrics record
    /// against the transform that [`TransformMetrics::select`] makes current, or into the
    /// process-wide container when none is selected. Entering clones one `Arc`.
    pub fn enter_transforms(metrics: &Arc<TransformMetrics>) -> Self {
        Self::push(Frame::Transforms(Arc::clone(metrics)))
    }

    fn push(frame: Frame) -> Self {
        let depth = CURRENT.with(|stack| {
            let mut stack = stack.borrow_mut();
            stack.push(frame);
            stack.len()
        });
        Self {
            depth,
            _not_send: PhantomData,
        }
    }
}

impl Drop for MetricsScope {
    fn drop(&mut self) {
        CURRENT.with(|stack| {
            let mut stack = stack.borrow_mut();
            debug_assert_eq!(stack.len(), self.depth, "MetricsScope dropped out of order");
            stack.pop();
        });
    }
}

/// Guard for the transform that [`TransformMetrics::select`] made current. Drop restores the
/// previous one.
#[must_use = "the selection ends as soon as it is dropped"]
pub struct TransformSelection<'a> {
    metrics: &'a TransformMetrics,
    previous: usize,
}

impl Drop for TransformSelection<'_> {
    fn drop(&mut self) {
        self.metrics.current.store(self.previous, Ordering::Relaxed);
    }
}

static PROCESS_WIDE: LazyLock<MetricsContainer> = LazyLock::new(MetricsContainer::new);

/// Returns the process-wide metrics container.
///
/// Collects metrics reported outside a bundle, such as in DoFn setup and teardown.
/// Runners read these metrics with `HarnessMonitoringInfosRequest`.
pub fn process_wide_container() -> &'static MetricsContainer {
    &PROCESS_WIDE
}

/// Runs `f` with the active container and transform, or the process-wide container with an empty
/// transform name when no scope is active.
pub(super) fn with_current(f: impl FnOnce(&MetricsContainer, &str)) {
    CURRENT.with(|stack| {
        let stack = stack.borrow();
        match stack.last() {
            Some(Frame::Single {
                container: FrameContainer::Bundle(container),
                transform_id,
            }) => f(container, transform_id),
            Some(Frame::Single {
                container: FrameContainer::ProcessWide,
                transform_id,
            }) => f(process_wide_container(), transform_id),
            Some(Frame::Transforms(metrics)) => match metrics.running() {
                Some(transform_id) => f(&metrics.container, transform_id),
                None => f(process_wide_container(), ""),
            },
            None => f(process_wide_container(), ""),
        }
    });
}
