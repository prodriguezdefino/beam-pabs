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

//! Typed outputs: `#[derive(Outputs)]` enums and `Result<T, F>`.

use std::collections::HashMap;

use beam_core::coders::{DefaultCoder, WindowedHeader};
use beam_core::internals::{
    BundleHandler, DoFnHandler, ElementSink, HandlerContext, PipelineInternals, TypedElement,
};
use beam_core::prelude::*;
use beam_core::transforms::display_data::{DisplayDataItem, DisplayDataValue};
use beam_model::pipeline as proto;

/// Records each pushed element with its output tag (`None` for the main output).
#[derive(Default)]
struct TagRecorder(Vec<(Option<String>, Vec<u8>)>);

impl ElementSink for TagRecorder {
    fn push(&mut self, element: Vec<u8>) -> std::result::Result<(), String> {
        self.0.push((None, element));
        Ok(())
    }

    fn push_tagged(&mut self, tag: &str, element: Vec<u8>) -> std::result::Result<(), String> {
        self.0.push((Some(tag.to_string()), element));
        Ok(())
    }

    fn push_tagged_windowed(
        &mut self,
        tag: &str,
        _header: &WindowedHeader,
        element: Vec<u8>,
    ) -> std::result::Result<(), String> {
        self.push_tagged(tag, element)
    }
}

fn transform<'p>(proto: &'p proto::Pipeline, name: &str) -> &'p proto::PTransform {
    proto
        .components
        .as_ref()
        .expect("components")
        .transforms
        .values()
        .find(|t| t.unique_name == name)
        .unwrap_or_else(|| panic!("no transform named {name}"))
}

/// The coder URN of the PCollection `pcoll_id`.
fn coder_urn(proto: &proto::Pipeline, pcoll_id: &str) -> String {
    let components = proto.components.as_ref().expect("components");
    let coder_id = &components.pcollections[pcoll_id].coder_id;
    components.coders[coder_id]
        .spec
        .as_ref()
        .expect("coder spec")
        .urn
        .clone()
}

#[derive(Outputs)]
#[beam(crate = "beam_core")]
enum Parsed {
    Number(i64),
    #[output(id = "not_a_number")]
    Text(String),
}

#[derive(Clone)]
struct ParseFn;

impl DoFn for ParseFn {
    type In = String;
    type Out = Parsed;

    fn process_element(&mut self, s: String, ctx: &mut ProcessContext<'_, Parsed>) -> Result {
        ctx.emit(match s.parse::<i64>() {
            Ok(n) => Parsed::Number(n),
            Err(_) => Parsed::Text(s),
        })
    }
}

#[test]
fn an_outputs_enum_declares_one_typed_pcollection_per_variant() {
    let p = Pipeline::new();
    let input = p.apply("Words", Create::new(vec!["1".to_string(), "a".to_string()]));
    let ParsedPCollections { number, text } = input.apply("Parse", ParDo::new(ParseFn));

    let proto = p.to_proto();
    let outputs = &transform(&proto, "Parse").outputs;
    assert_eq!(outputs.len(), 2, "{outputs:?}");
    assert_eq!(outputs["number"], number.id());
    assert_eq!(outputs["not_a_number"], text.id());
    assert_ne!(number.id(), text.id());

    let (number_coder, text_coder) = (coder_urn(&proto, number.id()), coder_urn(&proto, text.id()));
    assert_eq!(number_coder, "beam:coder:varint:v1");
    assert_eq!(text_coder, "beam:coder:string_utf8:v1");

    // A multi-output ParDo lists its output tags in its display data, in sorted order.
    let display: HashMap<_, _> = transform(&proto, "Parse")
        .display_data
        .iter()
        .map(|item| DisplayDataItem::from_proto(item).expect("display data item"))
        .map(|item| (item.key, item.value))
        .collect();
    assert_eq!(display["num_outputs"], DisplayDataValue::Integer(2));
    assert_eq!(
        display["output_tag_0"],
        DisplayDataValue::Text("not_a_number".to_string())
    );
    assert_eq!(
        display["output_tag_1"],
        DisplayDataValue::Text("number".to_string())
    );
}

#[test]
fn the_handler_routes_each_variant_to_its_tag() {
    let mut handler = DoFnHandler::new(ParseFn);
    let mut sink = TagRecorder::default();
    for word in ["42", "x"] {
        let element = word.to_string().encode().expect("encode");
        handler
            .process(&element, &mut HandlerContext::new(&mut sink))
            .expect("process");
    }
    assert_eq!(
        sink.0,
        vec![
            (Some("number".to_string()), 42_i64.encode().unwrap()),
            (
                Some("not_a_number".to_string()),
                "x".to_string().encode().unwrap()
            ),
        ]
    );
}

#[derive(Clone)]
struct CheckedHalf;

impl DoFn for CheckedHalf {
    type In = i64;
    type Out = std::result::Result<i64, Failure<i64>>;

    fn process_element(&mut self, n: i64, ctx: &mut ProcessContext<'_, Self::Out>) -> Result {
        if n % 2 == 0 {
            ctx.emit(Ok(n / 2))
        } else {
            ctx.emit(Err(Failure::new(n, "odd")))
        }
    }
}

#[test]
fn result_outputs_return_with_failures_tagged_output_and_errors() {
    let p = Pipeline::new();
    let numbers = p.apply("Numbers", Create::new(vec![1_i64, 2]));
    let WithFailures { output, failures } = numbers.apply("Half", ParDo::new(CheckedHalf));

    let proto = p.to_proto();
    let outputs = &transform(&proto, "Half").outputs;
    assert_eq!(outputs.len(), 2, "{outputs:?}");
    assert_eq!(outputs["output"], output.id());
    assert_eq!(outputs["errors"], failures.id());
    assert_ne!(
        coder_urn(&proto, output.id()),
        coder_urn(&proto, failures.id())
    );

    let mut handler = DoFnHandler::new(CheckedHalf);
    let mut sink = TagRecorder::default();
    for n in [4_i64, 3] {
        handler
            .process(&n.encode().unwrap(), &mut HandlerContext::new(&mut sink))
            .expect("process");
    }
    let tags: Vec<_> = sink.0.iter().map(|(tag, _)| tag.as_deref()).collect();
    assert_eq!(tags, [Some("output"), Some("errors")]);
    assert_eq!(i64::decode(&sink.0[0].1).unwrap(), 2);
    let failure = Failure::<i64>::decode(&sink.0[1].1).expect("decode failure");
    assert_eq!(failure, Failure::new(3, "odd"));
}

/// Records each indexed output as `(index, tag, encoded_bytes)`.
#[derive(Default)]
struct IndexedRecorder(Vec<(usize, String, Vec<u8>)>);

impl ElementSink for IndexedRecorder {
    fn push(&mut self, _element: Vec<u8>) -> std::result::Result<(), String> {
        Err("expected push_indexed_value".into())
    }

    fn push_value(
        &mut self,
        _tag: Option<&str>,
        _header: Option<&WindowedHeader>,
        _element: TypedElement<'_>,
    ) -> std::result::Result<(), String> {
        Err("expected push_indexed_value".into())
    }

    fn push_indexed_value(
        &mut self,
        index: usize,
        tag: &str,
        _header: Option<&WindowedHeader>,
        element: TypedElement<'_>,
    ) -> std::result::Result<(), String> {
        self.0.push((index, tag.to_string(), element.encode()?));
        Ok(())
    }
}

#[test]
fn result_indexed_and_derived_outputs_call_push_indexed_value() {
    // Derived `Outputs`: sorted tags are "not_a_number" (0) < "number" (1).
    let mut derived_handler = DoFnHandler::new(ParseFn);
    let mut derived_sink = IndexedRecorder::default();
    for word in ["42", "x"] {
        let encoded = word.to_string().encode().expect("encode");
        derived_handler
            .process(&encoded, &mut HandlerContext::new(&mut derived_sink))
            .expect("process derived");
    }
    let x = "x".to_string().encode().unwrap();
    assert_eq!(
        derived_sink.0,
        vec![
            (1, "number".to_string(), 42_i64.encode().unwrap()),
            (0, "not_a_number".to_string(), x),
        ]
    );

    // `Result<T, F>`: sorted tags are "errors" (0) < "output" (1).
    let mut result_handler = DoFnHandler::new(CheckedHalf);
    let mut result_sink = IndexedRecorder::default();
    for n in [4_i64, 3] {
        let encoded = n.encode().expect("encode");
        result_handler
            .process(&encoded, &mut HandlerContext::new(&mut result_sink))
            .expect("process result");
    }
    let odd = Failure::<i64, String>::new(3_i64, "odd").encode().unwrap();
    assert_eq!(
        result_sink.0,
        vec![
            (1, "output".to_string(), 2_i64.encode().unwrap()),
            (0, "errors".to_string(), odd),
        ]
    );

    // `Indexed<T>` through `Partition`: emits `(index, index.to_string())`.
    let p = Pipeline::new();
    let numbers = p.apply("Numbers", Create::new(vec![0_i64]));
    let _parts = numbers.apply("Split", Partition::new(3, |n: &i64| (*n as usize) % 3));
    let mut partition_handler = p
        .transform_handlers()
        .remove("Split")
        .expect("partition handler")
        .instantiate();
    let mut indexed_sink = IndexedRecorder::default();
    for n in [10_i64, 11, 12] {
        let encoded = n.encode().expect("encode");
        partition_handler
            .process(&encoded, &mut HandlerContext::new(&mut indexed_sink))
            .expect("process partition");
    }
    assert_eq!(
        indexed_sink.0,
        vec![
            (1, "1".to_string(), 10_i64.encode().unwrap()),
            (2, "2".to_string(), 11_i64.encode().unwrap()),
            (0, "0".to_string(), 12_i64.encode().unwrap()),
        ]
    );
}
