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
use crate::pipeline::URN_FLATTEN;
use crate::values::{IsBounded, PCollection, PCollectionList};
use model::pipeline as proto;

/// Merges multiple [`PCollection`]s of the same element type into a single [`PCollection`].
///
/// A runner primitive (`beam:transform:flatten:v1`), so there is no worker handler.
/// `expand` panics if the input list is empty or the inputs have different window functions.
pub struct Flatten<T> {
    name: String,
    _marker: PhantomData<T>,
}

impl<T> Flatten<T> {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _marker: PhantomData,
        }
    }

    /// Flattens a slice of [`PCollection`] references. Panics like [`Flatten`]'s `expand`.
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
        list.apply(Self::new(name))
    }
}

impl<T> HasDisplayData for Flatten<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "Flatten");
        builder.add_text("name", &self.name);
    }
}

impl<T> PTransform<PCollectionList<T>> for Flatten<T>
where
    T: DefaultCoder,
{
    type Output = PCollection<T>;

    fn expand(&self, input: &PCollectionList<T>) -> PCollection<T> {
        assert!(
            !input.is_empty(),
            "Flatten requires at least one input PCollection"
        );
        let pipeline = input.pipeline();
        let coder_id = T::register_coder(pipeline);

        let is_bounded = {
            let inner = pipeline.lock();
            let any_unbounded = input.collections().iter().any(|pcol| {
                inner
                    .components
                    .pcollections
                    .get(pcol.id())
                    .is_some_and(|p| p.is_bounded == proto::is_bounded::Enum::Unbounded as i32)
            });
            if any_unbounded {
                IsBounded::Unbounded
            } else {
                IsBounded::Bounded
            }
        };

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
                assert!(
                    mismatched.is_empty(),
                    "Flatten requires every input to share one window function, but '{}' was \
                     given {:?} alongside {:?}. Apply the same window to each input before \
                     flattening.",
                    self.name,
                    mismatched,
                    expected.map(|(urn, _)| urn)
                );
            }

            resolved.unwrap_or_else(|| inner.default_windowing_strategy_id.clone())
        };

        let out_pcoll = pipeline.add_pcollection_with_windowing::<T>(
            &format!("{}_out", self.name),
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

        let mut builder = DisplayDataBuilder::with_namespace(self.name.clone());
        self.populate_display_data(&mut builder);

        // If a runner sends Flatten to the SDK, the harness runs a built-in pass-through,
        // so there is no handler to register.
        pipeline.add_transform_with_display_data(
            &self.name,
            URN_FLATTEN,
            Vec::new(),
            inputs,
            outputs,
            builder.into_proto(),
        );

        out_pcoll
    }
}
