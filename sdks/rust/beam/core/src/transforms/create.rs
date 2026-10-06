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

use super::{DisplayDataBuilder, ElementSink, HasDisplayData, PTransform, TransformFn};
use crate::coders::DefaultCoder;
use crate::internals::ParDoRegistration;
use crate::values::{IsBounded, PBegin, PCollection};

/// A root transform producing a [`PCollection`] from an in-memory collection.
///
/// Every element is stored in the pipeline graph, so use it only for small inputs such as
/// test data. `expand` panics if an element does not encode.
pub struct Create<T> {
    name: String,
    elements: Vec<T>,
}

impl<T> Create<T> {
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

impl<T: DefaultCoder> PTransform<PBegin> for Create<T> {
    type Output = PCollection<T>;

    fn expand(&self, input: &PBegin) -> PCollection<T> {
        let pipeline = input.pipeline();
        let name = pipeline.unique_transform_name(&self.name);
        let coder_id = T::register_coder(pipeline);

        let (impulse_id, impulse) = pipeline.add_impulse(&format!("{name}/Impulse"));
        let out_pcoll =
            pipeline.add_pcollection::<T>(&format!("{name}.out"), &coder_id, IsBounded::Bounded);

        // Encode once at expansion time, so the handler is a stateless closure.
        let encoded: Vec<Vec<u8>> = self
            .elements
            .iter()
            .map(|e| e.encode().expect("Create element must be encodable"))
            .collect();

        let handler: TransformFn = Arc::new(move |_bytes: &[u8], out: &mut dyn ElementSink| {
            encoded.iter().try_for_each(|e| out.push(e.clone()))
        });
        let process_id = ParDoRegistration::new(pipeline, format!("{name}/Process"), impulse.id())
            .output("out", out_pcoll.id())
            .register(handler);

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
