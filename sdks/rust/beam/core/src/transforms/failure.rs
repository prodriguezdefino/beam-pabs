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

//! Dead-letter routing: transforms that send failed elements to a second output.
//!
//! A transform that can fail for each element returns [`WithFailures`]: the regular `output`
//! and a `failures` collection, by default of [`Failure`] records. No element is dropped and
//! neither the bundle nor the job fails. The caller decides what to do with the failures, for
//! example with [`WithFailures::failures_to`]. [`TryMap`](crate::transforms::TryMap) is the
//! element-wise transform. A custom `DoFn` declares `type Out = Result<T, F>` and emits
//! `Err(failure)`; see [`Outputs`](crate::transforms::Outputs).

use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

use crate::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder, URN_KV};
use crate::pipeline::Pipeline;
use crate::transforms::PTransform;
use crate::values::{PCollection, POutput};

/// An input element that a transform could not process, with the reason.
///
/// `E` is `String` by default, as produced by transforms that accept any
/// [`Display`](std::fmt::Display) error. For a structured error, use `Failure<T, MyError>` with
/// `MyError: DefaultCoder`. The wire format is `beam:coder:kv:v1` over `(input, error)`, so
/// other SDKs read a failure collection as `KV<T, E>`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Failure<T, E = String> {
    /// The element that failed.
    pub input: T,
    /// The reason for the failure.
    pub error: E,
}

impl<T, E> Failure<T, E> {
    /// Creates a failure record for `input`.
    pub fn new(input: T, error: impl Into<E>) -> Self {
        Self {
            input,
            error: error.into(),
        }
    }
}

/// Coder for [`Failure<T, E>`]: `KV<T, E>`.
#[derive(Clone, Debug)]
pub struct FailureCoder<IC, EC> {
    input_coder: IC,
    error_coder: EC,
}

impl<IC, EC> FailureCoder<IC, EC> {
    /// Creates a coder from the input and error component coders.
    pub fn new(input_coder: IC, error_coder: EC) -> Self {
        Self {
            input_coder,
            error_coder,
        }
    }
}

impl<T, E, IC, EC> Coder<Failure<T, E>> for FailureCoder<IC, EC>
where
    T: Send + Sync + 'static,
    E: Send + Sync + 'static,
    IC: Coder<T>,
    EC: Coder<E>,
{
    fn urn(&self) -> &'static str {
        URN_KV
    }

    fn encode(
        &self,
        value: &Failure<T, E>,
        writer: &mut dyn Write,
        context: Context,
    ) -> Result<(), CoderError> {
        self.input_coder
            .encode(&value.input, writer, Context::Nested)?;
        self.error_coder.encode(&value.error, writer, context)
    }

    fn decode(&self, reader: &mut dyn Read, context: Context) -> Result<Failure<T, E>, CoderError> {
        let input = self.input_coder.decode(reader, Context::Nested)?;
        let error = self.error_coder.decode(reader, context)?;
        Ok(Failure { input, error })
    }
}

impl<T: DefaultCoder, E: DefaultCoder> DefaultCoder for Failure<T, E> {
    type Coder = FailureCoder<T::Coder, E::Coder>;

    fn coder() -> Self::Coder {
        FailureCoder::new(T::coder(), E::coder())
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        self.input.encode_element(writer)?;
        self.error.encode_element(writer)
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        let input = T::decode_element(reader)?;
        let error = E::decode_element(reader)?;
        Ok(Failure { input, error })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let input_id = T::register_coder(registry);
        let error_id = E::register_coder(registry);
        registry.register_coder(URN_KV, vec![input_id, error_id])
    }
}

/// The input of a [`TryMap`](crate::transforms::TryMap) exception handler: the failed element
/// and its error. It is not encoded; the handler converts it into the failure type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExceptionElement<T, E> {
    /// The element that failed.
    pub element: T,
    /// The error returned for it.
    pub exception: E,
}

/// The two outputs of a transform with dead-letter routing. `F` is the failure element type:
/// [`Failure<T>`] unless the transform has a custom exception handler.
pub struct WithFailures<U, F> {
    /// Elements that were processed successfully.
    pub output: PCollection<U>,
    /// One element per failure.
    pub failures: PCollection<F>,
}

impl<U, F> Clone for WithFailures<U, F> {
    fn clone(&self) -> Self {
        Self {
            output: self.output.clone(),
            failures: self.failures.clone(),
        }
    }
}

impl<U, F> std::fmt::Debug for WithFailures<U, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WithFailures")
            .field("output", &self.output)
            .field("failures", &self.failures)
            .finish()
    }
}

impl<U: 'static, F: 'static> WithFailures<U, F> {
    /// Applies `sink` to [`failures`](Self::failures) and returns [`output`](Self::output), so the
    /// main chain continues:
    ///
    /// ```ignore
    /// lines
    ///     .try_map("Parse", |s: &String| s.parse::<i64>())
    ///     .failures_to(dead_letter_sink) // any PTransform<PCollection<F>>
    ///     .map("Double", |n| n * 2);
    /// ```
    pub fn failures_to<S>(self, sink: S) -> PCollection<U>
    where
        S: PTransform<PCollection<F>>,
    {
        self.failures.apply(sink);
        self.output
    }
}

impl<U: 'static, F: 'static> POutput for WithFailures<U, F> {
    fn pipeline(&self) -> &Pipeline {
        self.output.pipeline()
    }
}
