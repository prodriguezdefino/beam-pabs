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

//! Cross-language pipelines: a transform expanded by an external expansion service,
//! executed by the runner in that service's environment.

use std::sync::Arc;

use beam::prelude::*;
use beam::schema::FieldType;
use beam::testing::{TestPipeline, passert};
use external::transform::{ExternalSource, ExternalTransform};

/// Identifier of the `GenerateSequence` SchemaTransform.
pub const GENERATE_SEQUENCE_SCHEMA_TRANSFORM: &str =
    "beam:schematransform:org.apache.beam:generate_sequence:v1";

/// Builds a cross-language source: the `GenerateSequence` SchemaTransform,
/// expanded by the expansion service at `expansion_endpoint`, feeding Rust transforms.
///
/// The runner has to stage the foreign environment's artifacts, run its SDK harness
/// alongside the Rust one, and exchange schema'd rows across the language boundary.
/// Each value must arrive exactly once.
pub fn build_xlang_generate_sequence(p: &TestPipeline, expansion_endpoint: &str) {
    let rate_schema = Schema::builder()
        .field("elements", FieldType::int64())
        .nullable_field("seconds", FieldType::int64())
        .build();
    let schema = Arc::new(
        Schema::builder()
            .field("start", FieldType::int64())
            .nullable_field("end", FieldType::int64())
            .nullable_field("rate", FieldType::row(rate_schema))
            .build(),
    );
    let config = Row::builder(schema)
        .with_value(0i64)
        .with_value(10i64)
        .with_null()
        .build()
        .expect("GenerateSequence config row");
    let transform = ExternalTransform::schema_transform(
        "XlangGenerateSequence",
        GENERATE_SEQUENCE_SCHEMA_TRANSFORM,
        expansion_endpoint,
        &config,
    )
    .expect("GenerateSequence payload")
    .with_output_tags(["output"]);

    let rows = p.apply(ExternalSource::new(transform).with_output_tag("output"));
    let values = rows.par_do_fn("RowValue", |row: Row, ctx| {
        let value = row
            .get_i64("value")
            .map_err(|e| e.to_string())?
            .ok_or("GenerateSequence row without a value")?;
        ctx.emit(value)
    });
    passert::that("AssertValues", &values).contains_in_any_order(0..10);
}
