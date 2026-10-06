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

//! A root transform producing a PCollection from in-memory data.

use std::collections::HashMap;
use std::sync::Arc;

use model::pipeline as proto;
use prost::Message;

use super::{DisplayDataBuilder, ElementSink, HasDisplayData, PTransform, TransformFn};
use crate::coders::DefaultCoder;
use crate::internals::ParDoRegistration;
use crate::pipeline::{PipelineError, URN_RUST_DOFN_CREATE};
use crate::values::{IsBounded, PBegin, PCollection};

/// A root transform producing a [`PCollection`] from an in-memory collection.
///
/// Every element is stored in the pipeline graph, so use it only for small inputs such as
/// test data. If an element does not encode, [`Pipeline::validate`] fails.
///
/// [`Pipeline::validate`]: crate::pipeline::Pipeline::validate
pub struct Create<T> {
    name: String,
    elements: Vec<T>,
}

impl<T> Create<T> {
    /// Creates a transform that emits the given in-memory elements into the pipeline.
    pub fn new<I: IntoIterator<Item = T>>(name: impl Into<String>, elements: I) -> Self {
        Self {
            name: name.into(),
            elements: elements.into_iter().collect(),
        }
    }
}

impl<T> HasDisplayData for Create<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "Create");
        builder.add_text("name", &self.name);
        builder.add_integer("element_count", self.elements.len() as i64);
    }
}

/// The `do_fn` payload of [`URN_RUST_DOFN_CREATE`].
#[derive(Clone, PartialEq, Message)]
struct CreatePayload {
    #[prost(bytes = "vec", repeated, tag = "1")]
    elements: Vec<Vec<u8>>,
}

impl<T: DefaultCoder> PTransform<PBegin> for Create<T> {
    type Output = PCollection<T>;

    fn expand(&self, input: &PBegin) -> PCollection<T> {
        let pipeline = input.pipeline();
        let name = pipeline.unique_transform_name(&self.name);
        // A value can carry more than its type, such as the schema of a row.
        let coder_id = self.elements.first().map_or_else(
            || T::register_coder(pipeline),
            |first| first.register_value_coder(pipeline),
        );

        let (impulse_id, impulse) = pipeline.add_impulse(&format!("{name}/Impulse"));
        let out_pcoll =
            pipeline.add_pcollection::<T>(&format!("{name}.out"), &coder_id, IsBounded::Bounded);

        // An element that does not encode fails `validate`. The graph keeps an empty
        // `Create`, so the other transforms still attach to its output.
        let elements = self
            .elements
            .iter()
            .map(DefaultCoder::encode)
            .collect::<Result<Vec<_>, _>>()
            .unwrap_or_else(|e| {
                pipeline.record_construction_error(PipelineError::InvalidConstruction {
                    transform_id: name.clone(),
                    reason: format!("an element does not encode: {e}"),
                });
                Vec::new()
            });
        let do_fn = proto::FunctionSpec {
            urn: URN_RUST_DOFN_CREATE.to_string(),
            payload: CreatePayload { elements }.encode_to_vec(),
        };
        let process_id = ParDoRegistration::new(pipeline, format!("{name}/Process"), impulse.id())
            .output("out", out_pcoll.id())
            .register_spec(do_fn);

        let outputs = HashMap::from([("out".to_string(), out_pcoll.id().to_string())]);
        let transform_id = pipeline.add_composite_transform(
            &name,
            None,
            Vec::new(),
            HashMap::new(),
            outputs,
            vec![impulse_id, process_id],
        );

        let mut builder = DisplayDataBuilder::with_namespace(name);
        self.populate_display_data(&mut builder);
        pipeline.set_transform_display_data(&transform_id, builder.into_proto());

        out_pcoll
    }
}

/// Builds the handler of a [`URN_RUST_DOFN_CREATE`] `do_fn` from its payload. The handler
/// emits every element once for each impulse.
pub fn create_handler(payload: &[u8]) -> Result<TransformFn, String> {
    let CreatePayload { elements } = CreatePayload::decode(payload)
        .map_err(|e| format!("Failed to decode the Create payload: {e}"))?;
    let elements: Arc<[Vec<u8>]> = elements.into();
    Ok(Arc::new(
        move |_impulse: &[u8], out: &mut dyn ElementSink| {
            // `push` takes ownership, and the payload must stay for the next impulse.
            elements.iter().try_for_each(|e| out.push(e.clone()))
        },
    ))
}
