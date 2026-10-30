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

//! Reshuffle: a `GroupByKey` on random keys as a shuffle barrier. The runner cannot fuse the
//! upstream and downstream transforms, so it can autoscale and spread the downstream work.
//!
//! The steps move encoded elements and never decode them, so they do not depend on the
//! element type. They are registered functions: a worker builds them from their payload,
//! also for a pipeline that a driver in another language submits.

use std::any::TypeId;
use std::io::{Read, Write};
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use super::ProcessContext;
use super::dofn::registered::{FnRegistration, build_registered, encode_registered};
use super::dofn::{DoFn, ParDo};
use super::group_by_key::GroupByKey;
use crate::coders::{
    BeamIterable, Coder, CoderError, CoderRegistry, Context, DefaultCoder, PaneInfo, URN_BYTES,
    WindowedHeader,
};
use crate::pipeline::URN_RESHUFFLE;
use crate::transforms::PTransform;
use crate::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use crate::values::PCollection;
use crate::windowing::Trigger;
use model::pipeline as proto;

/// A composite transform that breaks runner fusion and rebalances elements across workers.
#[derive(Clone, Debug)]
pub struct Reshuffle<T> {
    _marker: PhantomData<T>,
}

impl<T> Default for Reshuffle<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Reshuffle<T> {
    /// Creates a `Reshuffle` transform.
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> HasDisplayData for Reshuffle<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "Reshuffle");
    }
}

/// The bytes of one whole element, in the coder of its PCollection. Use it only as a whole
/// element: it reads to the end of the input, so it cannot be inside another value.
struct EncodedElement(Vec<u8>);

/// Writes and reads the bytes of an [`EncodedElement`] unchanged.
struct EncodedElementCoder;

impl Coder<EncodedElement> for EncodedElementCoder {
    fn urn(&self) -> &'static str {
        URN_BYTES
    }

    fn encode(
        &self,
        value: &EncodedElement,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        Ok(writer.write_all(&value.0)?)
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<EncodedElement, CoderError> {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Ok(EncodedElement(bytes))
    }
}

impl DefaultCoder for EncodedElement {
    type Coder = EncodedElementCoder;

    fn coder() -> Self::Coder {
        EncodedElementCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        EncodedElementCoder.encode(self, writer, Context::WholeStream)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        EncodedElementCoder.decode(reader, Context::WholeStream)
    }

    /// The expansion replaces this coder with the coder of the element.
    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_BYTES, Vec::new())
    }
}

/// The windowed header of an element as a value: the pane offset and the wire bytes. The
/// header holds the timestamp, the windows, the pane and the metadata.
type ReifiedHeader = (i64, Vec<u8>);

/// An element in the shuffle: its header and its encoded bytes. The bytes coder encodes the
/// element bytes, so the shuffle does not decode the element. Do not use a `length_prefix`
/// coder of the element coder: Dataflow rejects a `length_prefix` around a coder that it
/// knows, because it adds the prefix itself.
type Shuffled = (ReifiedHeader, Vec<u8>);

/// Stores `header` in a value, so that it goes through the global-window shuffle unchanged.
fn reify(header: &WindowedHeader) -> ReifiedHeader {
    let pane_start = match header.is_empty() {
        true => 0,
        false => WindowedHeader::WINDOWS_START + header.window_bytes().len(),
    };
    (pane_start as i64, header.as_bytes().to_vec())
}

/// Rebuilds the header that [`reify`] stored.
fn restore((pane_start, bytes): &ReifiedHeader) -> WindowedHeader {
    WindowedHeader::from_wire(bytes, usize::try_from(*pane_start).unwrap_or_default())
}

/// Assigns a shard key to each element, so that the downstream `GroupByKey` spreads work. Each
/// instance starts from a random key and increments it. With a fixed start, every worker emits
/// the same key range and the `GroupByKey` merges them into a few very large groups.
#[derive(Serialize, Deserialize)]
struct PairWithRandomKeyFn {
    #[serde(skip, default = "random_seed")]
    next_shard: i64,
}

impl Default for PairWithRandomKeyFn {
    fn default() -> Self {
        Self {
            next_shard: random_seed(),
        }
    }
}

/// Each copy gets its own random start key, so bundle processors emit different ranges.
impl Clone for PairWithRandomKeyFn {
    fn clone(&self) -> Self {
        Self::default()
    }
}

/// Returns a random start key without a random number crate. The OS seeds `RandomState` per
/// process and each instance changes the seed. This is enough: keys must be well spread, not
/// unpredictable.
pub(super) fn random_seed() -> i64 {
    use std::hash::{BuildHasher, Hasher};
    std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish() as i64
}

impl DoFn for PairWithRandomKeyFn {
    type In = EncodedElement;
    type Out = (i64, Shuffled);

    fn process_element(
        &mut self,
        EncodedElement(element): Self::In,
        out: &mut ProcessContext<Self::Out>,
    ) -> crate::Result {
        let shard = self.next_shard;
        self.next_shard = shard.wrapping_add(1);
        let reified = reify(out.header());
        // The shuffle runs in the global window, so the output header must encode the global
        // window: its wire coder is the global window coder.
        let global = WindowedHeader::global_at(out.timestamp(), PaneInfo::NO_FIRING);
        out.output((shard, (reified, element)))
            .windowed(&global)
            .emit()
    }
}

/// Emits each grouped element with the header that it had before the shuffle.
#[derive(Clone, Default, Serialize, Deserialize)]
struct ExpandShardFn;

impl DoFn for ExpandShardFn {
    type In = (i64, BeamIterable<Shuffled>);
    type Out = EncodedElement;

    fn process_element(
        &mut self,
        (_shard, elements): Self::In,
        out: &mut ProcessContext<Self::Out>,
    ) -> crate::Result {
        elements.try_into_iter().try_for_each(|elem| {
            let (reified, element) =
                elem.map_err(|e| format!("Reshuffle failed to read a grouped element: {e}"))?;
            let header = restore(&reified);
            match header.is_empty() {
                true => out.emit(EncodedElement(element)),
                false => out.output(EncodedElement(element)).windowed(&header).emit(),
            }
        })
    }
}

inventory::submit! {
    FnRegistration {
        id: "beam:rust:reshuffle:pair_with_random_key:v1",
        type_id: TypeId::of::<PairWithRandomKeyFn>,
        encode: encode_registered::<PairWithRandomKeyFn>,
        build: build_registered::<PairWithRandomKeyFn, _>,
    }
}

inventory::submit! {
    FnRegistration {
        id: "beam:rust:reshuffle:expand_shard:v1",
        type_id: TypeId::of::<ExpandShardFn>,
        encode: encode_registered::<ExpandShardFn>,
        build: build_registered::<ExpandShardFn, _>,
    }
}

/// The strategy of the shuffle: the global window, so that no element is late and no window
/// merges, and a trigger that fires for each pane. Each element keeps its own timestamp, so
/// the output time is the earliest in the pane, not the end of the window.
fn shuffle_strategy(global: &proto::WindowingStrategy) -> proto::WindowingStrategy {
    proto::WindowingStrategy {
        trigger: Some(proto::Trigger::from(&Trigger::always())),
        accumulation_mode: proto::accumulation_mode::Enum::Discarding as i32,
        output_time: proto::output_time::Enum::EarliestInPane as i32,
        ..global.clone()
    }
}

impl<T: DefaultCoder> PTransform<PCollection<T>> for Reshuffle<T> {
    type Output = PCollection<T>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<T> {
        let pipeline = input.pipeline();
        let name = pipeline.scope_name().unwrap_or_default();

        let shuffle_ws_id = {
            let mut lock = pipeline.lock();
            let strategy = lock
                .components
                .windowing_strategies
                .get(&lock.default_windowing_strategy_id)
                .map(shuffle_strategy)
                .expect("the pipeline registers its default strategy when it is created");
            lock.register_windowing_strategy(strategy)
        };

        // Each element keeps its coder through the shuffle, so a row keeps its schema. The
        // output of a cross-language transform has no coder yet: the Rust type gives it.
        let element_coder_id = input
            .registered_coder_id()
            .map_or_else(|| T::register_coder(pipeline), str::to_string);
        let shuffled_coder_id = <(i64, Shuffled)>::register_coder(pipeline);

        let input_bytes = PCollection::<EncodedElement>::new(
            input.id().to_string(),
            element_coder_id.clone(),
            pipeline.clone(),
        );
        let output = {
            let paired = input_bytes
                .apply(
                    "PairWithRandomKey",
                    ParDo::new(PairWithRandomKeyFn::default()),
                )
                .with_windowing_strategy_id(shuffle_ws_id)
                .with_coder_id(shuffled_coder_id);

            let grouped = paired.apply("GroupByKey", GroupByKey::new());

            let expanded = grouped
                .apply("Expand", ParDo::new(ExpandShardFn))
                .with_coder_id(element_coder_id.clone())
                .with_windowing_strategy_of(&input_bytes);
            PCollection::<T>::new(
                expanded.id().to_string(),
                element_coder_id,
                pipeline.clone(),
            )
        };
        pipeline.set_scope_spec(model::pipeline::FunctionSpec {
            urn: URN_RESHUFFLE.to_string(),
            payload: Vec::new(),
        });

        let mut builder = DisplayDataBuilder::with_namespace(name);
        self.populate_display_data(&mut builder);
        pipeline.set_scope_display_data(builder.into_proto());

        output
    }
}
