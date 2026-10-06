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

//! DoFns for the core transform pipelines: lifecycle order, tagged outputs and
//! bundle-scoped batching.

use beam::prelude::*;
use beam::transforms::{FinishBundleContext, ProcessContext};
use beam::windowing::GlobalWindow;

/// A custom DoFn testing bundle lifecycle execution order.
///
/// Fails the bundle if a lifecycle method is called out of order: `start_bundle`
/// before `setup` or inside a bundle, `process_element` or `finish_bundle` outside
/// one. Emits `process:<element>` per element and `finish:<n>` per bundle, `n` being
/// the number of elements that bundle processed.
#[derive(Clone, Default)]
pub struct LifecycleDoFn {
    set_up: bool,
    in_bundle: bool,
    processed: usize,
}

impl DoFn for LifecycleDoFn {
    type In = String;
    type Out = String;

    fn setup(&mut self) -> Result {
        self.set_up = true;
        Ok(())
    }

    fn start_bundle(&mut self) -> Result {
        if !self.set_up {
            return Err("start_bundle called before setup".into());
        }
        if self.in_bundle {
            return Err("start_bundle called inside a bundle".into());
        }
        self.in_bundle = true;
        self.processed = 0;
        Ok(())
    }

    fn process_element(&mut self, element: String, out: &mut ProcessContext<String>) -> Result {
        if !self.in_bundle {
            return Err(format!("process_element({element}) called outside a bundle").into());
        }
        self.processed += 1;
        out.emit(format!("process:{element}"))
    }

    fn finish_bundle(&mut self, out: &mut FinishBundleContext<'_, String>) -> Result {
        if !self.in_bundle {
            return Err("finish_bundle called outside a bundle".into());
        }
        self.in_bundle = false;
        // The pipeline is globally windowed.
        out.emit(
            format!("finish:{}", self.processed),
            BoundedWindow::Global(GlobalWindow),
            Instant::END_OF_GLOBAL_WINDOW,
        )
    }
}

/// The outputs of [`SplitTagsDoFn`]: one PCollection per variant.
#[derive(Outputs)]
pub enum Parity {
    Evens(String),
    Odds(String),
}

/// Splits elements across multiple tagged outputs.
#[derive(Clone)]
pub struct SplitTagsDoFn;

impl DoFn for SplitTagsDoFn {
    type In = i64;
    type Out = Parity;

    fn process_element(
        &mut self,
        element: Self::In,
        out: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        if element % 2 == 0 {
            out.emit(Parity::Evens(format!("even:{element}")))?;
        } else {
            out.emit(Parity::Odds(format!("odd:{element}")))?;
        }
        Ok(())
    }
}

pub struct TransactionalBatchSink {
    pub batch_size: usize,
    pub buffer: Vec<String>,
    pub active_tx: Option<u64>,
}

/// A copy starts with an empty buffer and no transaction: both belong to one bundle.
impl Clone for TransactionalBatchSink {
    fn clone(&self) -> Self {
        Self {
            batch_size: self.batch_size,
            buffer: Vec::new(),
            active_tx: None,
        }
    }
}

impl DoFn for TransactionalBatchSink {
    type In = String;
    type Out = String;

    fn start_bundle(&mut self) -> Result {
        self.active_tx = Some(101);
        Ok(())
    }

    fn process_element(
        &mut self,
        element: Self::In,
        out: &mut ProcessContext<Self::Out>,
    ) -> Result {
        self.buffer.push(element);
        if self.buffer.len() >= self.batch_size {
            let chunk = std::mem::take(&mut self.buffer);
            out.emit(format!("batch_flush:{}", chunk.join("+")))?;
        }
        Ok(())
    }

    fn finish_bundle(&mut self, out: &mut FinishBundleContext<'_, Self::Out>) -> Result {
        // The pipeline is globally windowed.
        let window = BoundedWindow::Global(GlobalWindow);
        let ts = Instant::END_OF_GLOBAL_WINDOW;
        let tx = self.active_tx.take().unwrap_or(0);
        if !self.buffer.is_empty() {
            let chunk = std::mem::take(&mut self.buffer);
            out.emit(format!("batch_flush:{}", chunk.join("+")), window, ts)?;
        }
        out.emit(format!("tx_commit:{tx}"), window, ts)
    }
}

/// The lifecycle position of one function copy. Each method returns an error for a call
/// out of order.
#[derive(Clone, Copy, Debug, Default)]
pub struct CallOrder {
    set_up: bool,
    in_bundle: bool,
    processed: usize,
}

impl CallOrder {
    /// Records `setup`. A second call is an error.
    pub fn setup(&mut self) -> Result {
        if self.set_up {
            return Err("setup called twice".into());
        }
        self.set_up = true;
        Ok(())
    }

    /// Starts a bundle. Fails before `setup` or inside a bundle.
    pub fn start_bundle(&mut self) -> Result {
        match (self.set_up, self.in_bundle) {
            (false, _) => Err("start_bundle called before setup".into()),
            (true, true) => Err("start_bundle called inside a bundle".into()),
            (true, false) => {
                self.in_bundle = true;
                self.processed = 0;
                Ok(())
            }
        }
    }

    /// Counts one element of the bundle. `call` names the method in the error.
    pub fn process(&mut self, call: &str) -> Result {
        if !self.in_bundle {
            return Err(format!("{call} called outside a bundle").into());
        }
        self.processed += 1;
        Ok(())
    }

    /// Ends the bundle and returns the number of elements that it processed.
    pub fn finish_bundle(&mut self) -> Result<usize> {
        if !self.in_bundle {
            return Err("finish_bundle called outside a bundle".into());
        }
        self.in_bundle = false;
        Ok(self.processed)
    }
}

/// The outputs of [`MultiOutputLifecycleDoFn`].
#[derive(Outputs)]
pub enum LifecycleCalls {
    /// `process:<element>` for each element.
    Processed(String),
    /// `finish:<n>` for each bundle that processed `n` elements.
    Finished(String),
}

/// A multi-output DoFn that fails on a lifecycle call out of order. It emits each element
/// to [`LifecycleCalls::Processed`] and the count of each bundle to
/// [`LifecycleCalls::Finished`].
#[derive(Clone, Default)]
pub struct MultiOutputLifecycleDoFn {
    calls: CallOrder,
}

impl DoFn for MultiOutputLifecycleDoFn {
    type In = String;
    type Out = LifecycleCalls;

    fn setup(&mut self) -> Result {
        self.calls.setup()
    }

    fn start_bundle(&mut self) -> Result {
        self.calls.start_bundle()
    }

    fn process_element(
        &mut self,
        element: String,
        out: &mut ProcessContext<LifecycleCalls>,
    ) -> Result {
        self.calls.process("process_element")?;
        out.emit(LifecycleCalls::Processed(format!("process:{element}")))
    }

    fn finish_bundle(&mut self, out: &mut FinishBundleContext<'_, LifecycleCalls>) -> Result {
        let processed = self.calls.finish_bundle()?;
        // The pipeline is globally windowed.
        out.emit(
            LifecycleCalls::Finished(format!("finish:{processed}")),
            BoundedWindow::Global(GlobalWindow),
            Instant::END_OF_GLOBAL_WINDOW,
        )
    }
}

/// A stateful DoFn that fails on a lifecycle call out of order. It emits
/// `process:<key>=<value>` for each element and `finish:<n>` for each bundle.
#[derive(Clone, Default)]
pub struct StatefulLifecycleDoFn {
    calls: CallOrder,
}

impl StatefulDoFn for StatefulLifecycleDoFn {
    type Key = String;
    type Value = String;
    type Out = String;
    type State = ();
    type Timer = NoTimers;

    fn setup(&mut self) -> Result {
        self.calls.setup()
    }

    fn start_bundle(&mut self) -> Result {
        self.calls.start_bundle()
    }

    fn process_element(
        &mut self,
        key: &String,
        value: String,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        self.calls.process("process_element")?;
        ctx.emit(format!("process:{key}={value}"))
    }

    fn finish_bundle(&mut self, out: &mut FinishBundleContext<'_, String>) -> Result {
        let processed = self.calls.finish_bundle()?;
        // The pipeline is globally windowed.
        out.emit(
            format!("finish:{processed}"),
            BoundedWindow::Global(GlobalWindow),
            Instant::END_OF_GLOBAL_WINDOW,
        )
    }
}

/// A bundle method of a DoFn.
#[derive(Clone, Copy, Debug)]
pub enum BundleMethod {
    StartBundle,
    FinishBundle,
}

/// A DoFn that returns an error from `method`, with the message `<method> failure`.
#[derive(Clone)]
pub struct FailingBundleDoFn {
    pub method: BundleMethod,
}

impl DoFn for FailingBundleDoFn {
    type In = i64;
    type Out = i64;

    fn start_bundle(&mut self) -> Result {
        match self.method {
            BundleMethod::StartBundle => Err("start_bundle failure".into()),
            BundleMethod::FinishBundle => Ok(()),
        }
    }

    fn process_element(&mut self, element: i64, out: &mut ProcessContext<i64>) -> Result {
        out.emit(element)
    }

    fn finish_bundle(&mut self, _out: &mut FinishBundleContext<'_, i64>) -> Result {
        match self.method {
            BundleMethod::StartBundle => Ok(()),
            BundleMethod::FinishBundle => Err("finish_bundle failure".into()),
        }
    }
}
