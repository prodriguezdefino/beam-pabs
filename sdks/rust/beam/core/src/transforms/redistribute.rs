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

//! Redistribute: hints to the runner that it must spread the elements again, arbitrarily or
//! by key. A runner that knows the composite replaces it; other runners run its shuffle.

use std::marker::PhantomData;

use prost::Message;

use super::ProcessContext;
use super::dofn::{DoFn, ParDo};
use super::group_by_key::GroupByKey;
use super::reshuffle::{
    ReifiedHeader, describe_composite, emit_reified, emit_restored, shuffle_on_random_keys,
    shuffle_strategy_id,
};
use crate::coders::{BeamIterable, DefaultCoder};
use crate::pipeline::{URN_REDISTRIBUTE_ARBITRARILY, URN_REDISTRIBUTE_BY_KEY};
use crate::transforms::PTransform;
use crate::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use crate::values::PCollection;
use beam_model::pipeline as proto;

/// Returns the encoded `RedistributePayload` of a redistribute composite.
fn payload(allow_duplicates: bool) -> Vec<u8> {
    proto::RedistributePayload { allow_duplicates }.encode_to_vec()
}

/// Adds the display data of a redistribute transform with the name `transform`.
fn populate(builder: &mut DisplayDataBuilder, transform: &str, allow_duplicates: bool) {
    builder.add_text("transform", transform);
    if allow_duplicates {
        builder.add_boolean("allow_duplicates", true);
    }
}

/// A composite transform that spreads elements across workers with no rule for the placement.
///
/// The output holds the elements of the input with their timestamps, windows and panes.
///
/// ```
/// use beam::prelude::*;
/// use beam::transforms::RedistributeArbitrarily;
///
/// # let p = Pipeline::default();
/// let urls = p.apply("Urls", Create::new(vec!["a".to_string(), "b".to_string()]));
/// let spread = urls.apply("Spread", RedistributeArbitrarily::new());
/// # let _ = spread;
/// ```
#[derive(Clone, Debug)]
pub struct RedistributeArbitrarily<T> {
    allow_duplicates: bool,
    _marker: PhantomData<T>,
}

impl<T> Default for RedistributeArbitrarily<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> RedistributeArbitrarily<T> {
    /// Creates a transform that does not permit duplicates.
    pub fn new() -> Self {
        Self {
            allow_duplicates: false,
            _marker: PhantomData,
        }
    }

    /// Tells the runner whether it can emit an element more than once. A runner can then use
    /// a faster shuffle. Set it only when the downstream transforms accept duplicates.
    pub fn with_allow_duplicates(mut self, allow_duplicates: bool) -> Self {
        self.allow_duplicates = allow_duplicates;
        self
    }
}

impl<T> HasDisplayData for RedistributeArbitrarily<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        populate(builder, "RedistributeArbitrarily", self.allow_duplicates);
    }
}

impl<T: DefaultCoder> PTransform<PCollection<T>> for RedistributeArbitrarily<T> {
    type Output = PCollection<T>;

    fn expand(self, input: &PCollection<T>) -> PCollection<T> {
        let output = shuffle_on_random_keys(input, None);
        describe_composite(
            input.pipeline(),
            URN_REDISTRIBUTE_ARBITRARILY,
            payload(self.allow_duplicates),
            &self,
        );
        output
    }
}

/// A composite transform that spreads key-value elements across workers by their key.
///
/// The elements of one key go to the same worker. The output holds the elements of the input
/// with their timestamps, windows and panes.
///
/// ```
/// use beam::prelude::*;
/// use beam::transforms::RedistributeByKey;
///
/// # let p = Pipeline::default();
/// let scores = p.apply("Scores", Create::new(vec![("a".to_string(), 1i64)]));
/// let spread = scores.apply("Spread", RedistributeByKey::new());
/// # let _ = spread;
/// ```
#[derive(Clone, Debug)]
pub struct RedistributeByKey<K, V> {
    allow_duplicates: bool,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> Default for RedistributeByKey<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> RedistributeByKey<K, V> {
    /// Creates a transform that does not permit duplicates.
    pub fn new() -> Self {
        Self {
            allow_duplicates: false,
            _marker: PhantomData,
        }
    }

    /// Tells the runner whether it can emit an element more than once. A runner can then use
    /// a faster shuffle. Set it only when the downstream transforms accept duplicates.
    pub fn with_allow_duplicates(mut self, allow_duplicates: bool) -> Self {
        self.allow_duplicates = allow_duplicates;
        self
    }
}

impl<K, V> HasDisplayData for RedistributeByKey<K, V> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        populate(builder, "RedistributeByKey", self.allow_duplicates);
    }
}

/// Stores the header of each element in its value, so that the shuffle keeps it.
struct ReifyHeaderFn<K, V>(PhantomData<fn() -> (K, V)>);

impl<K, V> Clone for ReifyHeaderFn<K, V> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<K: DefaultCoder, V: DefaultCoder> DoFn for ReifyHeaderFn<K, V> {
    type In = (K, V);
    type Out = (K, (ReifiedHeader, V));

    fn process_element(
        &mut self,
        (key, value): Self::In,
        out: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result {
        emit_reified(out, |reified| (key, (reified, value)))
    }
}

/// Emits each grouped value with its key and the header that it had before the shuffle.
struct RestoreHeaderFn<K, V>(PhantomData<fn() -> (K, V)>);

impl<K, V> Clone for RestoreHeaderFn<K, V> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<K: DefaultCoder + Clone, V: DefaultCoder> DoFn for RestoreHeaderFn<K, V> {
    type In = (K, BeamIterable<(ReifiedHeader, V)>);
    type Out = (K, V);

    fn process_element(
        &mut self,
        (key, values): Self::In,
        out: &mut ProcessContext<'_, Self::Out>,
    ) -> crate::Result {
        values.try_into_iter().try_for_each(|grouped| {
            let (reified, value) = grouped
                .map_err(|e| format!("RedistributeByKey failed to read a grouped element: {e}"))?;
            // Each output element owns its key, so a group of n values needs n keys.
            emit_restored(out, &reified, (key.clone(), value))
        })
    }
}

impl<K, V> PTransform<PCollection<(K, V)>> for RedistributeByKey<K, V>
where
    K: DefaultCoder + Clone,
    V: DefaultCoder,
{
    type Output = PCollection<(K, V)>;

    fn expand(self, input: &PCollection<(K, V)>) -> PCollection<(K, V)> {
        let pipeline = input.pipeline();
        let shuffle_ws_id = shuffle_strategy_id(pipeline);

        // The key and the value keep their coders through the shuffle, so a row keeps its
        // schema. The Rust types give the coders when the input has no KV coder.
        let reified = input
            .apply("ReifyHeader", ParDo::new(ReifyHeaderFn(PhantomData)))
            .with_windowing_strategy_id(shuffle_ws_id);
        let reified = match pipeline.kv_coder_parts(input.coder_id()) {
            Some((key, value)) => {
                let header = <ReifiedHeader>::register_coder(pipeline);
                let shuffled = pipeline.register_kv_coder(header, value);
                let coder_id = pipeline.register_kv_coder(key, shuffled);
                reified.with_coder_id(coder_id)
            }
            None => reified,
        };

        let output = reified
            .apply("GroupByKey", GroupByKey::new())
            .apply("RestoreHeader", ParDo::new(RestoreHeaderFn(PhantomData)))
            .with_coder_of(input)
            .with_windowing_strategy_of(input);

        describe_composite(
            pipeline,
            URN_REDISTRIBUTE_BY_KEY,
            payload(self.allow_duplicates),
            &self,
        );
        output
    }
}
