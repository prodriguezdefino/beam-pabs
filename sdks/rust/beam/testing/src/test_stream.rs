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

//! A scripted, unbounded source for testing streaming pipelines.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use model::pipeline as proto;
use model::pipeline::test_stream_payload::{
    Event, TimestampedElement,
    event::{AddElements, AdvanceProcessingTime, AdvanceWatermark, Event as EventKind},
};
use prost::Message;

use beam::coders::{DefaultCoder, URN_LENGTH_PREFIX, VarIntCoder};
use beam::internals::TransformFn;
use beam::internals::{ElementSink, ParDoRegistration};
use beam::pipeline::URN_TEST_STREAM;
use beam::transforms::PTransform;
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::values::{IsBounded, PBegin, PCollection};
use beam::windowing::BEAM_MIN_TIMESTAMP;

/// Watermark in milliseconds that closes all windows, including the global window.
///
/// This is the runners' end of time (`i64::MAX` microseconds), past the end of the global
/// window. A watermark at [`BEAM_MAX_TIMESTAMP`] never fires a global-window trigger.
///
/// [`BEAM_MAX_TIMESTAMP`]: beam::windowing::BEAM_MAX_TIMESTAMP
pub const WATERMARK_INFINITY_MILLIS: i64 = i64::MAX / 1000;

/// One step of a [`TestStream`] script.
#[derive(Clone, Debug)]
enum Step<T> {
    Elements(Vec<(T, i64)>),
    Watermark(i64),
    ProcessingTime(Duration),
}

/// Unbounded source that replays a fixed script of elements, watermark advances and
/// processing-time advances.
///
/// Use it to stage late data, early and late trigger firings, and processing-time timers
/// exactly. It is a runner primitive (`beam:transform:teststream:v1`), so only runners
/// that support it, such as Prism, can run it.
///
/// Elements are emitted in the global window. Apply a
/// [`WindowInto`](beam::windowing::WindowInto) to window them by their timestamps.
///
/// ```
/// use beam::prelude::*;
/// use testing::{passert, TestStream};
///
/// let p = Pipeline::new();
/// let events = p.apply(
///     TestStream::new("TestStream")
///         .add_timestamped_elements([("a".to_string(), 1_000), ("b".to_string(), 2_000)])
///         .advance_watermark_to(10_000)
///         .add_timestamped_elements([("late".to_string(), 3_000)])
///         .advance_watermark_to_infinity(),
/// );
///
/// passert::that("AssertEvents", &events).has_count(3);
/// ```
///
/// If a script does not finish with
/// [`advance_watermark_to_infinity`](Self::advance_watermark_to_infinity), the runner
/// advances the watermark after the script ends.
#[derive(Clone, Debug)]
pub struct TestStream<T> {
    name: String,
    steps: Vec<Step<T>>,
    watermark: i64,
}

impl<T> TestStream<T> {
    /// Starts an empty script for a transform named `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            steps: Vec::new(),
            watermark: BEAM_MIN_TIMESTAMP,
        }
    }

    /// The watermark the script has advanced to so far, in milliseconds.
    pub fn current_watermark(&self) -> i64 {
        self.watermark
    }

    /// Emits `elements`, timestamped at the current watermark.
    ///
    /// Before any watermark advance, the timestamp is [`BEAM_MIN_TIMESTAMP`].
    #[must_use]
    pub fn add_elements<I: IntoIterator<Item = T>>(self, elements: I) -> Self {
        let timestamp = self.watermark;
        self.add_timestamped_elements(elements.into_iter().map(|e| (e, timestamp)))
    }

    /// Emits `elements`, each paired with its event timestamp in milliseconds.
    ///
    /// A timestamp can be behind the current watermark. Use this to stage late data.
    #[must_use]
    pub fn add_timestamped_elements<I: IntoIterator<Item = (T, i64)>>(
        mut self,
        elements: I,
    ) -> Self {
        let elements: Vec<(T, i64)> = elements.into_iter().collect();
        if !elements.is_empty() {
            self.steps.push(Step::Elements(elements));
        }
        self
    }

    /// Advances the watermark to `timestamp_millis`.
    ///
    /// # Panics
    ///
    /// Panics if `timestamp_millis` is behind the current watermark, because watermarks
    /// never move back.
    #[must_use]
    pub fn advance_watermark_to(mut self, timestamp_millis: i64) -> Self {
        assert!(
            timestamp_millis >= self.watermark,
            "TestStream '{}': the watermark cannot move backwards, from {} to {timestamp_millis}",
            self.name,
            self.watermark
        );
        self.watermark = timestamp_millis;
        self.steps.push(Step::Watermark(timestamp_millis));
        self
    }

    /// Advances the watermark past the end of all windows, which closes them all.
    #[must_use]
    pub fn advance_watermark_to_infinity(self) -> Self {
        self.advance_watermark_to(WATERMARK_INFINITY_MILLIS)
    }

    /// Advances the processing-time clock by `duration` and fires timers and triggers due.
    ///
    /// # Panics
    ///
    /// Panics if `duration` is shorter than one millisecond, the resolution of the protocol.
    #[must_use]
    pub fn advance_processing_time(mut self, duration: Duration) -> Self {
        assert!(
            duration >= Duration::from_millis(1),
            "TestStream '{}': processing time must advance by at least one millisecond",
            self.name
        );
        self.steps.push(Step::ProcessingTime(duration));
        self
    }
}

impl<T: DefaultCoder> TestStream<T> {
    /// Translates the script into Runner API events.
    ///
    /// Frames each element as `beam:coder:length_prefix:v1` over bytes. The payload is the
    /// encoding of the element. Runners handle this framing the same way for all `T`, and
    /// the SDK receives the payload unchanged. See [`PTransform::expand`].
    fn events(&self) -> Result<Vec<Event>, String> {
        self.steps
            .iter()
            .map(|step| {
                let event = match step {
                    Step::Elements(elements) => EventKind::ElementEvent(AddElements {
                        elements: elements
                            .iter()
                            .map(|(element, timestamp)| {
                                Ok(TimestampedElement {
                                    encoded_element: length_prefixed(element)?,
                                    timestamp: *timestamp,
                                })
                            })
                            .collect::<Result<_, String>>()?,
                        tag: String::new(),
                    }),
                    Step::Watermark(watermark) => EventKind::WatermarkEvent(AdvanceWatermark {
                        new_watermark: *watermark,
                        tag: String::new(),
                    }),
                    Step::ProcessingTime(duration) => {
                        EventKind::ProcessingTimeEvent(AdvanceProcessingTime {
                            advance_duration: i64::try_from(duration.as_millis())
                                .unwrap_or(i64::MAX),
                        })
                    }
                };
                Ok(Event { event: Some(event) })
            })
            .collect()
    }
}

/// Encodes `element` and frames it with a VarInt length prefix.
fn length_prefixed<T: DefaultCoder>(element: &T) -> Result<Vec<u8>, String> {
    let payload = element
        .encode()
        .map_err(|e| format!("TestStream element could not be encoded: {e}"))?;
    let mut framed = Vec::with_capacity(payload.len() + 5);
    VarIntCoder::encode_varint(payload.len() as i64, &mut framed)
        .map_err(|e| format!("TestStream element could not be framed: {e}"))?;
    framed.extend(payload);
    Ok(framed)
}

impl<T> HasDisplayData for TestStream<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "TestStream");
        builder.add_text("name", &self.name);
        builder.add_integer("event_count", self.steps.len() as i64);
    }
}

impl<T: DefaultCoder> PTransform<PBegin> for TestStream<T> {
    type Output = PCollection<T>;

    /// Adds the runner-executed TestStream primitive and the SDK step that decodes it.
    ///
    /// The primitive output is declared as length-prefixed bytes, not as `T`. A runner can
    /// rewrite and re-frame a TestStream coder that it thinks is ambiguous between nested
    /// and outer contexts (strings, bytes, KVs, custom coders), but never a length prefix
    /// over bytes. The Fn API worker removes the prefix, so the decoding step gets exactly
    /// the encoding of `T` and forwards it into a collection coded as `T`.
    fn expand(&self, input: &PBegin) -> PCollection<T> {
        let pipeline = input.pipeline();
        let name = pipeline.unique_transform_name(&self.name);

        let bytes_coder_id = <Vec<u8>>::register_coder(pipeline);
        let framed_coder_id = pipeline.register_coder(URN_LENGTH_PREFIX, vec![bytes_coder_id]);
        let raw = pipeline.add_pcollection::<Vec<u8>>(
            &format!("{name}.raw"),
            &framed_coder_id,
            IsBounded::Unbounded,
        );

        let events = self
            .events()
            .unwrap_or_else(|e| panic!("TestStream '{name}': {e}"));
        let payload = proto::TestStreamPayload {
            coder_id: framed_coder_id,
            events,
            endpoint: None,
        }
        .encode_to_vec();

        let primitive_id = pipeline.add_transform(
            &format!("{name}/Replay"),
            URN_TEST_STREAM,
            payload,
            HashMap::new(),
            HashMap::from([("out".to_string(), raw.id().to_string())]),
        );

        let coder_id = T::register_coder(pipeline);
        let out =
            pipeline.add_pcollection::<T>(&format!("{name}.out"), &coder_id, IsBounded::Unbounded);
        let handler: TransformFn =
            Arc::new(|element: &[u8], out: &mut dyn ElementSink| out.push(element.to_vec()));
        let decode_id = ParDoRegistration::new(pipeline, format!("{name}/Decode"), raw.id())
            .output("out", out.id())
            .register(handler);

        let transform_id = pipeline.add_composite_transform(
            &name,
            None,
            Vec::new(),
            HashMap::new(),
            HashMap::from([("out".to_string(), out.id().to_string())]),
            vec![primitive_id, decode_id],
        );

        let mut builder = DisplayDataBuilder::with_namespace(name);
        self.populate_display_data(&mut builder);
        pipeline.set_transform_display_data(&transform_id, builder.into_proto());

        out
    }
}
