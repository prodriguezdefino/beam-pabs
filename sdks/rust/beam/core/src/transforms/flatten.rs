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

//! Merging multiple PCollections of the same type into a single PCollection.

use std::collections::HashMap;
use std::marker::PhantomData;

use super::{DisplayDataBuilder, HasDisplayData, PTransform};
use crate::coders::DefaultCoder;
use crate::pipeline::{PipelineError, URN_FLATTEN};
use crate::values::{IsBounded, PCollection, PCollectionList};

/// Merges multiple [`PCollection`]s of the same element type into a single [`PCollection`].
///
/// A runner primitive (`beam:transform:flatten:v1`), so there is no worker handler.
/// `expand` panics if the input list is empty or the inputs have different window functions.
pub struct Flatten<T> {
    _marker: PhantomData<T>,
}

impl<T> Default for Flatten<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Flatten<T> {
    /// Creates a flatten transform.
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }

    /// Flattens a slice of [`PCollection`] references.
    ///
    /// # Panics
    ///
    /// Panics if `collections` is empty: there is no pipeline to record the error in.
    pub fn pcollections(name: impl Into<String>, collections: &[&PCollection<T>]) -> PCollection<T>
    where
        T: DefaultCoder,
    {
        assert!(
            !collections.is_empty(),
            "Cannot flatten empty list of PCollections"
        );
        let pipeline = collections[0].pipeline().clone();
        let list =
            PCollectionList::from_vec(pipeline, collections.iter().map(|c| (*c).clone()).collect());
        list.apply(name, Self::new())
    }
}

impl<T> HasDisplayData for Flatten<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "Flatten");
    }
}

impl<T> PTransform<PCollectionList<T>> for Flatten<T>
where
    T: DefaultCoder,
{
    type Output = PCollection<T>;

    fn expand(&self, input: &PCollectionList<T>) -> PCollection<T> {
        let pipeline = input.pipeline();
        let name = pipeline.primitive_name("Flatten");
        if input.is_empty() {
            // A configuration error: record it and return an output with no producer, so the
            // later transforms still attach to it.
            pipeline.record_construction_error(PipelineError::InvalidConstruction {
                transform_id: name.clone(),
                reason: "Flatten requires at least one input PCollection".to_string(),
            });
            let coder_id = T::register_coder(pipeline);
            return pipeline.add_pcollection(&format!("{name}.out"), &coder_id, IsBounded::Bounded);
        }
        // The output takes the coder of the first input, so a row keeps its schema.
        let coder_id = input.collections()[0]
            .registered_coder_id()
            .map_or_else(|| T::register_coder(pipeline), str::to_string);

        let is_bounded = input
            .collections()
            .iter()
            .map(PCollection::is_bounded)
            .collect();

        // The output must keep the input windowing: with the pipeline default, the runner
        // decodes interval-windowed elements as global-windowed ones and gets bad data
        // without an error. Beam requires the same window function, not the same strategy:
        // a cross-language transform sends its own strategy, so two globally windowed
        // inputs can have different strategy ids. The output takes the first input's strategy.
        let ws_ids: Vec<String> = input
            .collections()
            .iter()
            .map(PCollection::windowing_strategy_id)
            .collect();

        // A window function mismatch is a configuration error, recorded after the lock.
        let mut mismatch = None;
        let out_ws_id = {
            let inner = pipeline.lock();
            let window_fn_of = |ws_id: &str| {
                inner
                    .components
                    .windowing_strategies
                    .get(ws_id)
                    .and_then(|ws| ws.window_fn.as_ref())
                    .map(|spec| (spec.urn.as_str(), spec.payload.as_slice()))
            };

            // Compare only inputs with a registered strategy. A cross-language output is a
            // placeholder with no coder or strategy until the expansion service replies.
            let resolved = ws_ids.iter().find(|id| window_fn_of(id).is_some()).cloned();

            if let Some(first) = &resolved {
                let expected = window_fn_of(first);
                let mismatched: Vec<&str> = ws_ids
                    .iter()
                    .filter_map(|id| window_fn_of(id))
                    .filter(|found| Some(*found) != expected)
                    .map(|(urn, _)| urn)
                    .collect();
                if !mismatched.is_empty() {
                    mismatch = Some(PipelineError::InvalidConstruction {
                        transform_id: name.clone(),
                        reason: format!(
                            "every input must share one window function, but it was given \
                             {mismatched:?} alongside {:?}. Apply the same window to each \
                             input before flattening.",
                            expected.map(|(urn, _)| urn)
                        ),
                    });
                }
            }

            resolved.unwrap_or_else(|| inner.default_windowing_strategy_id.clone())
        };
        if let Some(error) = mismatch {
            pipeline.record_construction_error(error);
        }

        let out_pcoll = pipeline.add_pcollection_with_windowing::<T>(
            &format!("{name}_out"),
            &coder_id,
            is_bounded,
            &out_ws_id,
        );

        let inputs: HashMap<String, String> = input
            .collections()
            .iter()
            .enumerate()
            .map(|(i, p)| (format!("input_{i}"), p.id().to_string()))
            .collect();
        let outputs = HashMap::from([("out".to_string(), out_pcoll.id().to_string())]);

        let mut builder = DisplayDataBuilder::with_namespace(name.clone());
        self.populate_display_data(&mut builder);

        // If a runner sends Flatten to the SDK, the harness runs a built-in pass-through,
        // so there is no handler to register.
        pipeline.add_transform_with_display_data(
            &name,
            URN_FLATTEN,
            Vec::new(),
            inputs,
            outputs,
            builder.into_proto(),
        );

        out_pcoll
    }
}
