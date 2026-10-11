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

//! The generic value plumbing shared by every [`PTransform`] input and output.
//!
//! [`PBegin`] is the input of root transforms and [`PDone`] the output of sinks.

use std::collections::HashMap;

use crate::pipeline::Pipeline;
use crate::transforms::{PTransform, TryPTransform};

/// A value that a [`PTransform`] can be applied to.
pub trait PInput {
    fn pipeline(&self) -> &Pipeline;

    /// The PCollection ids of this input, by tag. A composite node lists them as its inputs.
    fn pcollection_ids(&self) -> HashMap<String, String>;
}

/// A value that a [`PTransform`] can produce.
pub trait POutput {
    fn pipeline(&self) -> &Pipeline;

    /// The PCollection ids of this output, by tag. A composite node lists them as its outputs.
    fn pcollection_ids(&self) -> HashMap<String, String>;
}

/// The input to a root [`PTransform`], such as a source. Get it from [`Pipeline::begin`];
/// [`Pipeline::apply`] creates one for most users.
#[derive(Clone, Debug)]
pub struct PBegin {
    pipeline: Pipeline,
}

impl PBegin {
    pub fn new(pipeline: Pipeline) -> Self {
        Self { pipeline }
    }

    pub fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    /// Applies the root transform `transform`, such as a source, under the name `name`.
    pub fn apply<Tform>(&self, name: impl Into<String>, transform: Tform) -> Tform::Output
    where
        Tform: PTransform<Self>,
    {
        self.pipeline
            .apply_scoped(&name.into(), PInput::pcollection_ids(self), || {
                transform.expand(self)
            })
    }

    /// Applies a root transform whose expansion can fail, under the name `name`.
    pub fn try_apply<Tform>(
        &self,
        name: impl Into<String>,
        transform: Tform,
    ) -> Result<Tform::Output, Tform::Error>
    where
        Tform: TryPTransform<Self>,
    {
        self.pipeline
            .try_apply_scoped(&name.into(), PInput::pcollection_ids(self), || {
                transform.try_expand(self)
            })
    }
}

impl PInput for PBegin {
    fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    fn pcollection_ids(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}

/// The output of a terminal [`PTransform`], such as a sink. It carries no data, so no later
/// transform can consume it.
#[derive(Clone, Debug)]
pub struct PDone {
    pipeline: Pipeline,
}

impl PDone {
    /// Marks the end of a branch of `pipeline`.
    pub fn new(pipeline: Pipeline) -> Self {
        Self { pipeline }
    }

    pub fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }
}

impl POutput for PDone {
    fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    fn pcollection_ids(&self) -> HashMap<String, String> {
        HashMap::new()
    }
}
