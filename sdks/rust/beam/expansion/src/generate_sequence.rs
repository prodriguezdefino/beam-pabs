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

use crate::PCollectionId;
use crate::SchemaTransformProvider;
use beam::pipeline::Pipeline;
use beam::schema::{FieldType, Row, Schema};
use std::collections::HashMap;

/// Built-in test SchemaTransform for GenerateSequence.
#[derive(Default)]
pub struct GenerateSequenceSchemaTransformProvider;

impl SchemaTransformProvider for GenerateSequenceSchemaTransformProvider {
    fn identifier(&self) -> &'static str {
        "beam:schematransform:org.apache.beam:generate_sequence:v1"
    }

    fn description(&self) -> &'static str {
        "Generates a sequence of integers from start to stop."
    }

    fn config_schema(&self) -> Schema {
        Schema::builder()
            .field("start", FieldType::int64())
            .field("stop", FieldType::int64())
            .build()
    }

    fn input_tags(&self) -> Vec<String> {
        Vec::new() // A source has no inputs.
    }

    fn output_tags(&self) -> Vec<String> {
        vec!["output".to_string()]
    }

    fn build_transform(
        &self,
        config: Row,
        _inputs: crate::TransformInputs,
        pipeline: &mut Pipeline,
    ) -> beam::Result<HashMap<String, PCollectionId>> {
        let start = config.get_i64("start")?.unwrap_or(0);
        let stop = config.get_i64("stop")?.unwrap_or(10);

        let pcoll = pipeline.apply(
            "GenerateSequence",
            beam::transforms::GenerateSequence::new(start).with_end(stop),
        );

        Ok(HashMap::from([(
            "output".to_string(),
            PCollectionId::from(pcoll.id()),
        )]))
    }
}

crate::register_schema_transform!(GenerateSequenceSchemaTransformProvider);
