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

use beam::coders::{DefaultCoder, WindowedHeader};
use beam::internals::{BundleHandler, DoFnHandler, ElementSink, HandlerContext};
use beam::prelude::*;
use model::pipeline as proto;

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
    let input = p.apply(Create::new("Words", vec!["1".to_string(), "a".to_string()]));
    let ParsedPCollections { number, text } = input.apply(ParDo::new("Parse", ParseFn));

    let proto = p.to_proto();
    let outputs = &transform(&proto, "Parse").outputs;
    assert_eq!(outputs.len(), 2, "{outputs:?}");
    assert_eq!(outputs["number"], number.id());
    assert_eq!(outputs["not_a_number"], text.id());
    assert_ne!(number.id(), text.id());

    let (number_coder, text_coder) = (coder_urn(&proto, number.id()), coder_urn(&proto, text.id()));
    assert_eq!(number_coder, "beam:coder:varint:v1");
    assert_eq!(text_coder, "beam:coder:string_utf8:v1");
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
    let numbers = p.apply(Create::new("Numbers", vec![1_i64, 2]));
    let WithFailures { output, failures } = numbers.apply(ParDo::new("Half", CheckedHalf));

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
