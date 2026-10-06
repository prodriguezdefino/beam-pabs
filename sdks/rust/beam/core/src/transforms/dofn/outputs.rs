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
use crate::transforms::{ElementSink, TypedElement};
use crate::values::{IsBounded, PCollection, POutput};

/// What a `DoFn` emits, which sets what applying it returns.
///
/// Any element type `T` is a single output; the transform returns `PCollection<T>`.
pub trait Outputs: Send + Sized + 'static {
    /// What applying the transform returns.
    type Collections: POutput;

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

/// Adds the output PCollections of a transform. `#[derive(Outputs)]` calls it.
#[doc(hidden)]
pub struct OutputDeclarer<'p> {
    pipeline: &'p Pipeline,
    transform: &'p str,
    windowing_strategy_id: &'p str,
    is_bounded: IsBounded,
    tags: HashMap<String, String>,
}

impl<'p> OutputDeclarer<'p> {
    #[expect(dead_code, reason = "the DoFn handlers of the next commit use it")]
    pub(crate) fn new(
        pipeline: &'p Pipeline,
        transform: &'p str,
        windowing_strategy_id: &'p str,
        is_bounded: IsBounded,
    ) -> Self {
        Self {
            pipeline,
            transform,
            windowing_strategy_id,
            is_bounded,
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
    #[expect(dead_code, reason = "the DoFn handlers of the next commit use it")]
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
