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

//! The outputs of a `DoFn`, declared by its `Out` type.

use std::collections::HashMap;

use crate::coders::{DefaultCoder, WindowedHeader};
use crate::pipeline::Pipeline;
use crate::transforms::failure::WithFailures;
use crate::transforms::{ElementSink, TypedElement};
use crate::values::{IsBounded, PCollection, PCollectionList, POutput};

/// What a `DoFn` emits, which sets what applying it returns.
///
/// - Any element type `T` is a single output; the transform returns `PCollection<T>`.
/// - `Result<T, F>` sends `Ok` to `output` and `Err` to `errors`; the transform returns
///   [`WithFailures<T, F>`](crate::transforms::WithFailures). Use it for dead-letter outputs.
///   `F` is any element type: a [`Failure`](crate::transforms::Failure), or an error `Row`
///   with the schema that a Java schema transform expects.
/// - An enum with `#[derive(Outputs)]` has one output per variant; the transform returns the
///   generated `<Enum>PCollections` struct, with one typed field per variant.
/// - [`WithFinalizer<O>`] has the outputs of `O`, and lets the function register bundle
///   finalizers. The function emits `WithFinalizer(value)`, or `value.into()`.
///
/// ```
/// use beam::prelude::*;
///
/// #[derive(Outputs)]
/// enum Parity {
///     Even(i64),
///     Odd(String),
/// }
///
/// #[derive(Clone)]
/// struct Split;
///
/// impl DoFn for Split {
///     type In = i64;
///     type Out = Parity;
///
///     fn process_element(&mut self, n: i64, ctx: &mut ProcessContext<'_, Parity>) -> Result {
///         ctx.emit(if n % 2 == 0 { Parity::Even(n) } else { Parity::Odd(n.to_string()) })
///     }
/// }
///
/// # let p = Pipeline::default();
/// # let numbers = p.apply(Create::new("Numbers", vec![1i64, 2, 3]));
/// let ParityPCollections { even, odd } = numbers.apply(ParDo::new("Split", Split));
/// ```
pub trait Outputs: Send + Sized + 'static {
    /// What applying the transform returns.
    type Collections: POutput;

    /// Whether the function registers bundle finalizers, so the runner must finalize its
    /// bundles.
    const FINALIZES: bool = false;

    /// Adds one PCollection per output.
    #[doc(hidden)]
    fn declare(outputs: &mut OutputDeclarer<'_>) -> Self::Collections;

    /// Sends the value to its output.
    #[doc(hidden)]
    fn emit_to(self, emitter: Emitter<'_>) -> Result<(), String>;
}

impl<T: DefaultCoder> Outputs for T {
    type Collections = PCollection<T>;

    fn declare(outputs: &mut OutputDeclarer<'_>) -> PCollection<T> {
        outputs.declare("out")
    }

    fn emit_to(self, emitter: Emitter<'_>) -> Result<(), String> {
        emitter.push(None, self)
    }
}

/// Output tag of `Ok` values, as in Java schema transforms.
pub(crate) const OUTPUT_TAG: &str = "output";
/// Output tag of `Err` values, as the `errors` output of Java schema transforms.
pub(crate) const ERRORS_TAG: &str = "errors";

impl<T: DefaultCoder, F: DefaultCoder> Outputs for Result<T, F> {
    type Collections = WithFailures<T, F>;

    fn declare(outputs: &mut OutputDeclarer<'_>) -> WithFailures<T, F> {
        WithFailures {
            output: outputs.declare(OUTPUT_TAG),
            failures: outputs.declare(ERRORS_TAG),
        }
    }

    fn emit_to(self, emitter: Emitter<'_>) -> Result<(), String> {
        match self {
            Ok(value) => emitter.push(Some(OUTPUT_TAG), value),
            Err(failure) => emitter.push(Some(ERRORS_TAG), failure),
        }
    }
}

/// An output of a function that registers bundle finalizers: a value of `O`.
///
/// A function with `type Out = WithFinalizer<O>` has the outputs of `O`: applying it returns
/// the collections of `O`, with the same tags. It emits each value of `O` wrapped, which
/// `.into()` does. Its [`ProcessContext`](crate::transforms::ProcessContext) and
/// [`FinishBundleContext`](crate::transforms::FinishBundleContext) have `finalizer`, and the
/// runner finalizes its bundles. Other functions have no `finalizer`:
///
/// ```compile_fail
/// use beam::prelude::*;
///
/// #[derive(Clone)]
/// struct Ack;
///
/// impl DoFn for Ack {
///     type In = String;
///     type Out = String;
///
///     fn process_element(&mut self, id: String, ctx: &mut ProcessContext<'_, String>) -> Result {
///         ctx.finalizer(move || Ok(())).register()
///     }
/// }
/// ```
///
/// With `WithFinalizer`, the function compiles:
///
/// ```
/// use beam::prelude::*;
/// use beam::transforms::WithFinalizer;
///
/// #[derive(Clone)]
/// struct Ack;
///
/// impl DoFn for Ack {
///     type In = String;
///     type Out = WithFinalizer<String>;
///
///     fn process_element(&mut self, id: String, ctx: &mut ProcessContext<'_, Self::Out>) -> Result {
///         ctx.emit(id.clone().into())?;
///         ctx.finalizer(move || {
///             println!("acknowledge {id}");
///             Ok(())
///         })
///         .register()
///     }
/// }
/// ```
pub struct WithFinalizer<O>(pub O);

impl<O> From<O> for WithFinalizer<O> {
    fn from(value: O) -> Self {
        Self(value)
    }
}

impl<O: Outputs> Outputs for WithFinalizer<O> {
    type Collections = O::Collections;
    const FINALIZES: bool = true;

    fn declare(outputs: &mut OutputDeclarer<'_>) -> O::Collections {
        O::declare(outputs)
    }

    fn emit_to(self, emitter: Emitter<'_>) -> Result<(), String> {
        let Self(value) = self;
        value.emit_to(emitter)
    }
}

/// One output of a transform with a number of outputs known only when it is built, such as
/// [`Partition`](crate::transforms::Partition). The index is the position of the output.
#[doc(hidden)]
pub struct Indexed<T>(pub usize, pub T);

impl<T: DefaultCoder> Outputs for Indexed<T> {
    type Collections = PCollectionList<T>;

    fn declare(outputs: &mut OutputDeclarer<'_>) -> PCollectionList<T> {
        let collections = (0..outputs.count)
            .map(|i| outputs.declare(&i.to_string()))
            .collect();
        PCollectionList::from_vec(outputs.pipeline.clone(), collections)
    }

    fn emit_to(self, emitter: Emitter<'_>) -> Result<(), String> {
        let Self(index, value) = self;
        emitter.push(Some(&index.to_string()), value)
    }
}

/// Adds the output PCollections of a transform. `#[derive(Outputs)]` calls it.
#[doc(hidden)]
pub struct OutputDeclarer<'p> {
    pipeline: &'p Pipeline,
    transform: &'p str,
    windowing_strategy_id: &'p str,
    is_bounded: IsBounded,
    count: usize,
    tags: HashMap<String, String>,
}

impl<'p> OutputDeclarer<'p> {
    pub(crate) fn new(
        pipeline: &'p Pipeline,
        transform: &'p str,
        windowing_strategy_id: &'p str,
        is_bounded: IsBounded,
        count: usize,
    ) -> Self {
        Self {
            pipeline,
            transform,
            windowing_strategy_id,
            is_bounded,
            count,
            tags: HashMap::new(),
        }
    }

    /// Adds the PCollection of output `tag`, with the coder of `T`.
    pub fn declare<T: DefaultCoder>(&mut self, tag: &str) -> PCollection<T> {
        let coder_id = T::register_coder(self.pipeline);
        let pcoll = self.pipeline.add_pcollection_with_windowing::<T>(
            &format!("{}_{}", self.transform, tag),
            &coder_id,
            self.is_bounded,
            self.windowing_strategy_id,
        );
        self.tags.insert(tag.to_string(), pcoll.id().to_string());
        pcoll
    }

    /// The PCollection id of each output tag.
    pub(crate) fn into_tags(self) -> HashMap<String, String> {
        self.tags
    }
}

/// Sends one value to an output. `#[derive(Outputs)]` calls it.
#[doc(hidden)]
pub struct Emitter<'s> {
    sink: &'s mut dyn ElementSink,
    header: Option<&'s WindowedHeader>,
}

impl<'s> Emitter<'s> {
    pub(crate) fn new(sink: &'s mut dyn ElementSink, header: Option<&'s WindowedHeader>) -> Self {
        Self { sink, header }
    }

    /// Sends `value` to output `tag`, or to the main output for `None`.
    pub fn push<V: DefaultCoder>(self, tag: Option<&str>, value: V) -> Result<(), String> {
        self.sink
            .push_value(tag, self.header, TypedElement::new(&mut Some(value)))
    }
}
