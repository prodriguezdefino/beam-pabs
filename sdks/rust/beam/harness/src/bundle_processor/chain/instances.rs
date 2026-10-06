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

//! The handler instances of a bundle processor, and how the fused chain borrows them.
//!
//! An operator output calls the `process` of its consumers in one call stack, so each call
//! needs a mutable handler while upstream operators are still in their calls. The handlers
//! are in a slice in topological order, and an operator pushes only to later operators (the
//! graph builder checks this). A call to operator `i` splits the slice at `i`: the operator
//! gets `&mut` to its handler and its outputs get `&mut` to the handlers after it. The
//! compiler checks each borrow, without a lock, reference counting or a runtime flag.

use beam::internals::{BundleHandler, HandlerInstance};
use tracing::warn;

use super::super::BundleError;

/// The handler instances of one bundle processor, indexed by topological position.
pub struct Instances(Vec<HandlerInstance>);

impl Instances {
    pub fn new(handlers: Vec<HandlerInstance>) -> Self {
        Self(handlers)
    }

    /// Returns every instance, for a call that enters the chain from outside an operator.
    pub fn all(&mut self) -> Downstream<'_> {
        Downstream {
            handlers: &mut self.0,
            base: 0,
        }
    }

    /// Returns the instances in topological order, each with the id for its failures.
    pub fn iter_mut<'a>(
        &'a mut self,
        ids: &'a [impl AsRef<str>],
    ) -> impl Iterator<Item = (&'a str, &'a mut dyn BundleHandler)> {
        ids.iter().map(AsRef::as_ref).zip(
            self.0
                .iter_mut()
                .map(|handler| -> &'a mut dyn BundleHandler { handler.as_mut() }),
        )
    }

    /// Sets up every instance in topological order. If one setup fails, tears down the
    /// instances already set up and returns [`BundleError::Setup`] with the transform id.
    pub fn setup(&mut self, ids: &[String]) -> Result<(), BundleError> {
        let failure = self
            .iter_mut(ids)
            .enumerate()
            .find_map(|(position, (t_id, handler))| {
                handler
                    .setup()
                    .err()
                    .map(|e| (position, format!("transform '{t_id}': {e}")))
            });
        match failure {
            None => Ok(()),
            Some((set_up, message)) => {
                self.iter_mut(ids).take(set_up).for_each(teardown);
                Err(BundleError::Setup(message))
            }
        }
    }

    /// Tears down every instance. Failures are only logged, because the processor is
    /// discarded in all cases.
    pub fn teardown(&mut self, ids: &[String]) {
        self.iter_mut(ids).for_each(teardown);
    }
}

fn teardown((t_id, handler): (&str, &mut dyn BundleHandler)) {
    if let Err(e) = handler.teardown() {
        warn!("Teardown failed for transform '{t_id}': {e}");
    }
}

/// The handler instances after an operator in topological order, which its outputs reach.
pub struct Downstream<'a> {
    handlers: &'a mut [HandlerInstance],
    /// The operator position of `handlers[0]`.
    base: usize,
}

impl Downstream<'_> {
    /// Borrows these instances again for one call. `self` stays usable after the call.
    pub fn reborrow(&mut self) -> Downstream<'_> {
        Downstream {
            handlers: &mut *self.handlers,
            base: self.base,
        }
    }

    /// Splits off the instance at operator position `index` and the instances after it.
    ///
    /// Returns [`BundleError::InvalidGraph`] when `index` is not after this point: an
    /// operator pushes to itself or to an earlier one, a cycle the graph builder must reject.
    pub fn split(
        &mut self,
        index: usize,
    ) -> Result<(&mut dyn BundleHandler, Downstream<'_>), BundleError> {
        let local = index
            .checked_sub(self.base)
            .filter(|&local| local < self.handlers.len())
            .ok_or_else(|| {
                BundleError::InvalidGraph(format!(
                    "operator {index} is not downstream of the operator invoking it (operators \
                     {} onwards are)",
                    self.base
                ))
            })?;
        let (upto, after) = self.handlers.split_at_mut(local + 1);
        Ok((
            &mut *upto[local],
            Downstream {
                handlers: after,
                base: index + 1,
            },
        ))
    }
}
