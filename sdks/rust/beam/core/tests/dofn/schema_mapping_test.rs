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

//! Tests for the field-name mapping of the DoFn handler.

use std::sync::Arc;

use beam::coders::DefaultCoder;
use beam::internals::{BundleHandler, DoFnHandler, HandlerContext};
use beam::schema::{BeamRow, FieldType, Row, Schema};
use beam::transforms::{DoFn, ProcessContext};

#[derive(Clone, Debug, PartialEq, BeamRow)]
#[beam(crate = "::beam")]
struct EvolvedReader {
    id: i64,
    name: String,
    added_optional: Option<String>,
}

#[derive(Clone)]
struct PassthroughEvolved;

impl DoFn for PassthroughEvolved {
    type In = (String, EvolvedReader);
    type Out = (String, EvolvedReader);

    fn process_element(
        &mut self,
        input: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result {
        ctx.emit(input)
    }
}

#[test]
fn dofn_handler_maps_reordered_writer_schema_and_rejects_missing_field() {
    let writer_schema = Arc::new(
        Schema::builder()
            .field("extra", FieldType::string())
            .field("name", FieldType::string())
            .field("id", FieldType::int64())
            .build(),
    );
    let mut handler = DoFnHandler::new(PassthroughEvolved);
    let mut sink: Vec<Vec<u8>> = Vec::new();
    for (k, id, name) in [("a", 10_i64, "ten"), ("b", 20_i64, "twenty")] {
        let writer_row = Row::builder(Arc::clone(&writer_schema))
            .with_value("ignored")
            .with_value(name)
            .with_value(id)
            .build()
            .expect("row");
        let mut elem = k.to_string().encode().expect("key");
        elem.extend_from_slice(&writer_row.to_row_bytes().expect("row bytes"));
        let mut ctx = HandlerContext::new(&mut sink).with_input_schema(Some(&writer_schema));
        handler.process(&elem, &mut ctx).expect("process");
    }
    assert_eq!(sink.len(), 2);
    assert_eq!(
        <(String, EvolvedReader)>::decode(&sink[0]).expect("out 0"),
        (
            "a".to_string(),
            EvolvedReader {
                id: 10,
                name: "ten".to_string(),
                added_optional: None,
            }
        )
    );
    assert_eq!(
        <(String, EvolvedReader)>::decode(&sink[1]).expect("out 1"),
        (
            "b".to_string(),
            EvolvedReader {
                id: 20,
                name: "twenty".to_string(),
                added_optional: None,
            }
        )
    );

    let bad_writer = Arc::new(Schema::builder().field("id", FieldType::int64()).build());
    let bad_row = Row::builder(Arc::clone(&bad_writer))
        .with_value(1_i64)
        .build()
        .expect("bad row");
    let mut bad_elem = "c".to_string().encode().expect("key");
    bad_elem.extend_from_slice(&bad_row.to_row_bytes().expect("row bytes"));
    let mut bad_ctx = HandlerContext::new(&mut sink).with_input_schema(Some(&bad_writer));
    let err = handler
        .process(&bad_elem, &mut bad_ctx)
        .expect_err("missing required field must fail");
    assert!(err.contains("name"), "unexpected error: {err}");
}
