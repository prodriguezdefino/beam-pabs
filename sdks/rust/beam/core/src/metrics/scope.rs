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
use std::sync::Arc;

use super::context::MetricsContainer;

/// One entered scope: a container, the transforms that can record into it, and the running one.
struct Frame {
    container: Arc<MetricsContainer>,
    transform_ids: Arc<[Arc<str>]>,
    /// Index into `transform_ids`. It is out of range when no transform runs.
    current: usize,
}

thread_local! {
    static CURRENT: RefCell<Vec<Frame>> = const { RefCell::new(Vec::new()) };
}

/// Makes a container and transform current on this thread until dropped. Unbound metrics, such
/// as [`Metrics::counter`](super::Metrics::counter), record into the innermost active scope, or
/// do nothing when no scope is active.
#[must_use = "the scope ends as soon as it is dropped"]
pub struct MetricsScope {
    // Scopes are a per-thread stack. A drop on another thread would pop the wrong entry.
    _not_send: PhantomData<*const ()>,
}

impl MetricsScope {
    /// Enters a scope that records unbound metrics into `container` under `transform_id`.
    pub fn enter(container: Arc<MetricsContainer>, transform_id: Arc<str>) -> Self {
        Self::push(Frame {
            container,
            transform_ids: Arc::from([transform_id]),
            current: 0,
        })
    }

    /// Enters a scope for a sequence of calls into several transforms. No transform runs at
    /// first; [`select`](Self::select) sets the running one. Entering clones two `Arc`s, too
    /// slow for each call into a fused operator, so a worker enters one scope per element.
    pub fn enter_transforms(
        container: Arc<MetricsContainer>,
        transform_ids: Arc<[Arc<str>]>,
    ) -> Self {
        Self::push(Frame {
            container,
            current: transform_ids.len(),
            transform_ids,
        })
    }

    /// Makes transform `index` of the innermost scope the running transform until the returned
    /// guard drops.
    pub fn select(index: usize) -> TransformSelection {
        let previous = CURRENT.with(|stack| {
            stack
                .borrow_mut()
                .last_mut()
                .map(|frame| std::mem::replace(&mut frame.current, index))
        });
        TransformSelection {
            previous,
            _not_send: PhantomData,
        }
    }

    fn push(frame: Frame) -> Self {
        CURRENT.with(|stack| stack.borrow_mut().push(frame));
        Self {
            _not_send: PhantomData,
        }
    }
}

impl Drop for MetricsScope {
    fn drop(&mut self) {
        CURRENT.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}

/// Guard for the transform that [`MetricsScope::select`] made current. Drop restores the
/// previous one.
#[must_use = "the selection ends as soon as it is dropped"]
pub struct TransformSelection {
    previous: Option<usize>,
    _not_send: PhantomData<*const ()>,
}

impl Drop for TransformSelection {
    fn drop(&mut self) {
        if let Some(previous) = self.previous {
            CURRENT.with(|stack| {
                if let Some(frame) = stack.borrow_mut().last_mut() {
                    frame.current = previous;
                }
            });
        }
    }
}

/// Runs `f` with the innermost active container and running transform, if they exist.
pub(super) fn with_current(f: impl FnOnce(&MetricsContainer, &str)) {
    CURRENT.with(|stack| {
        if let Some(frame) = stack.borrow().last()
            && let Some(transform_id) = frame.transform_ids.get(frame.current)
        {
            f(&frame.container, transform_id);
        }
    });
}
