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

//! Byte-level execution interfaces. Public through [`crate::internals`].

use std::sync::Arc;

use crate::coders::{DefaultCoder, WindowedHeader};
use crate::transforms::dofn::HandlerContext;

/// Receives encoded output elements as a handler produces them, so a runner can stream outputs
/// to the next operator without buffering the stage. `Vec<Vec<u8>>` collects all outputs.
pub trait ElementSink {
    /// Accepts one encoded element for the main output.
    fn push(&mut self, element: Vec<u8>) -> Result<(), String>;

    /// Accepts one encoded element for output `tag`.
    fn push_tagged(&mut self, tag: &str, element: Vec<u8>) -> Result<(), String> {
        let _ = tag;
        self.push(element)
    }

    /// Accepts one encoded element with an explicit windowed-value header.
    fn push_windowed(&mut self, header: &WindowedHeader, element: Vec<u8>) -> Result<(), String> {
        let _ = header;
        self.push(element)
    }

    /// Accepts one encoded element for output `tag` with an explicit header.
    fn push_tagged_windowed(
        &mut self,
        tag: &str,
        header: &WindowedHeader,
        element: Vec<u8>,
    ) -> Result<(), String> {
        let _ = header;
        self.push_tagged(tag, element)
    }

    /// Accepts one element by value; `tag` and `header` are as in the `push_*` methods. A sink
    /// that feeds a fused operator of the same type can [`TypedElement::take`] the value and skip
    /// the encode and decode. The default implementation encodes the value.
    fn push_value(
        &mut self,
        tag: Option<&str>,
        header: Option<&WindowedHeader>,
        element: TypedElement<'_>,
    ) -> Result<(), String> {
        let encoded = element.encode()?;
        match (tag, header) {
            (None, None) => self.push(encoded),
            (Some(tag), None) => self.push_tagged(tag, encoded),
            (None, Some(header)) => self.push_windowed(header, encoded),
            (Some(tag), Some(header)) => self.push_tagged_windowed(tag, header, encoded),
        }
    }
}

/// A type-erased output element passed by value through [`ElementSink::push_value`]. A consumer
/// that expects exactly `T` can [`take`](Self::take) it; others [`encode`](Self::encode) it.
pub struct TypedElement<'v> {
    slot: &'v mut dyn std::any::Any,
    encode: fn(&dyn std::any::Any) -> Result<Vec<u8>, String>,
}

impl<'v> TypedElement<'v> {
    /// Wraps a filled slot.
    pub fn new<T: DefaultCoder>(slot: &'v mut Option<T>) -> Self {
        Self {
            slot,
            encode: encode_slot::<T>,
        }
    }

    /// Takes the value if it is a `T`; `None` if the type differs or the value was already taken.
    pub fn take<T: 'static>(&mut self) -> Option<T> {
        self.slot.downcast_mut::<Option<T>>()?.take()
    }

    /// Encodes the value with its default coder. Returns an error if the value was already taken.
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        (self.encode)(&*self.slot)
    }
}

impl std::fmt::Debug for TypedElement<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedElement").finish_non_exhaustive()
    }
}

fn encode_slot<T: DefaultCoder>(slot: &dyn std::any::Any) -> Result<Vec<u8>, String> {
    slot.downcast_ref::<Option<T>>()
        .and_then(Option::as_ref)
        .ok_or_else(|| "Emitted element was already consumed".to_string())?
        .encode()
        .map_err(|e| format!("Failed to encode emitted element: {e}"))
}

impl ElementSink for Vec<Vec<u8>> {
    fn push(&mut self, element: Vec<u8>) -> Result<(), String> {
        Vec::push(self, element);
        Ok(())
    }
}

/// Executes a transform over the encoded elements of one bundle.
///
/// The registered handler is a prototype; each bundle processor gets its own copy from
/// [`instantiate`](Self::instantiate). A copy calls `setup` once; per bundle `start_bundle`,
/// then `process` and `on_timer`, then `finish_bundle`; and `teardown` at the end. A copy runs
/// one bundle at a time and no two bundles share a copy, so the lifecycle methods take
/// `&mut self` and need no locks. Pipeline code implements `DoFn`;
/// `ParDo` adapts it to this trait.
pub trait BundleHandler: Send + Sync {
    /// Called once when the owning bundle processor is created.
    fn setup(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Called before the first element of a bundle.
    fn start_bundle(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Processes one encoded element and pushes the outputs to `ctx`.
    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String>;

    /// Processes one element that a fused upstream operator gives by value. The default
    /// implementation encodes it and calls [`process`](Self::process).
    fn process_value(
        &mut self,
        element: TypedElement<'_>,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        let encoded = element.encode()?;
        self.process(&encoded, ctx)
    }

    /// Called when a timer in `timer_family` fires.
    fn on_timer(
        &mut self,
        timer_family: &str,
        record: &crate::coders::TimerRecord,
        ctx: &mut HandlerContext<'_>,
    ) -> Result<(), String> {
        let _ = (timer_family, record, ctx);
        Ok(())
    }

    /// Called after the last element of a bundle. Outputs pushed to `ctx` are emitted.
    fn finish_bundle(&mut self, ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        let _ = ctx;
        Ok(())
    }

    /// Called once when the bundle processor is discarded: after a failed bundle or at shutdown.
    fn teardown(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Returns the handler for an expanded stage (for example an SDF stage) by its Beam URN.
    fn stage_handler(&self, stage_urn: &str) -> Option<Arc<dyn BundleHandler>> {
        let _ = stage_urn;
        None
    }

    /// Returns a new copy for one bundle processor. It must not share mutable state with `self`.
    fn instantiate(&self) -> HandlerInstance;
}

/// A cloneable closure over encoded elements is a stateless [`BundleHandler`].
impl<F> BundleHandler for F
where
    F: Fn(&[u8], &mut dyn ElementSink) -> Result<(), String> + Clone + Send + Sync + 'static,
{
    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        self(element, ctx.sink)
    }

    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }
}

/// The registered prototype handler of a transform.
pub type TransformFn = Arc<dyn BundleHandler>;

/// One bundle processor's own copy of a [`BundleHandler`].
pub type HandlerInstance = Box<dyn BundleHandler>;
