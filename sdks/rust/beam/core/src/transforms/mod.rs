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

//! Transforms: the operations that build a pipeline's graph.
//!
//! A [`PTransform`] expands a [`PInput`] into a [`POutput`].
//!
//! This crate is `apply`-style: `pcoll.apply(Map::new("Name", f))`. Method syntax
//! (`.map(..)`) comes from extension traits in `apache-beam-fluent`; `beam::prelude`
//! brings both.

use crate::values::{PInput, POutput};

pub mod display_data;

/// A composite or primitive transform converting a [`PInput`] into a [`POutput`].
pub trait PTransform<Input: PInput> {
    /// The value this transform produces.
    type Output: POutput;

    /// Builds the transform's sub-graph and returns its output.
    fn expand(&self, input: &Input) -> Self::Output;
}

/// A transform wrapper that scopes execution environment resource hints during expansion.
pub struct WithResourceHints<T> {
    transform: T,
    hints: crate::pipeline::resources::ResourceHints,
}

impl<T> WithResourceHints<T> {
    pub fn new(transform: T, hints: crate::pipeline::resources::ResourceHints) -> Self {
        Self { transform, hints }
    }

    /// Merges `hints` over the hints already set.
    pub fn with_resource_hints(mut self, hints: crate::pipeline::resources::ResourceHints) -> Self {
        self.hints = hints.merge_with_outer(&self.hints);
        self
    }
}

impl<In: PInput, T: PTransform<In>> PTransform<In> for WithResourceHints<T> {
    type Output = T::Output;

    fn expand(&self, input: &In) -> Self::Output {
        let _guard = input
            .pipeline()
            .enter_resource_hints_scope(self.hints.clone());
        self.transform.expand(input)
    }
}

/// Extension trait providing `.with_resource_hints()` on all [`PTransform`] implementors.
pub trait WithResourceHintsExt<In: PInput>: PTransform<In> + Sized {
    /// Scopes the specified resource hints during the expansion of this transform.
    fn with_resource_hints(
        self,
        hints: crate::pipeline::resources::ResourceHints,
    ) -> WithResourceHints<Self> {
        WithResourceHints::new(self, hints)
    }

    /// Scopes a single resource hint by URN and serialized payload during expansion.
    fn with_resource_hint(
        self,
        urn: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> WithResourceHints<Self> {
        let hints = crate::pipeline::resources::ResourceHints::new().with_hint(urn, payload);
        self.with_resource_hints(hints)
    }
}

impl<In: PInput, T: PTransform<In>> WithResourceHintsExt<In> for T {}
