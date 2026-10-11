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

#![expect(
    clippy::unwrap_used,
    reason = "test fixtures unwrap; a failure is a test failure"
)]

//! Side inputs downstream of a cross-language transform that a worker does not expand.

use std::sync::Mutex;

use beam::coders::{DefaultCoder, PaneInfo, WindowedHeader};
use beam::internals::{HandlerContext, SideInputElements, SideInputError, SideInputReader};
use beam::pipeline::{ExpansionMode, Pipeline, URN_EXPANSION_SCHEMA_TRANSFORM};
use beam::schema::Row;
use beam::transforms::Map;
use beam::values::{IterView, SideInputWindowing};
use external::*;

/// A runner reader that serves one element in each window and records the windows it reads.
struct DescriptorReader {
    windowing: SideInputWindowing,
    windows: Mutex<Vec<Vec<u8>>>,
}

impl SideInputReader for DescriptorReader {
    fn get_iterable<'a>(
        &'a self,
        _tag: &str,
        window: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        self.windows.lock().unwrap().push(window.to_vec());
        Ok(Box::new(std::iter::once(Ok(7_i64
            .encode()
            .unwrap()
            .into()))))
    }

    fn get_multimap<'a>(
        &'a self,
        tag: &str,
        _window: &[u8],
        _key: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        Err(SideInputError::MissingReader {
            tag: tag.to_string(),
        })
    }

    fn window_mapping(&self, _tag: &str) -> Result<&SideInputWindowing, SideInputError> {
        Ok(&self.windowing)
    }
}

/// Views of the placeholder output of a cross-language read, and of a Rust transform after it.
fn placeholder_views() -> [IterView<i64>; 2] {
    let pipeline = Pipeline::new().with_expansion_mode(ExpansionMode::Placeholder);
    let read = ExternalTransform::new(
        URN_EXPANSION_SCHEMA_TRANSFORM,
        "http://127.0.0.1:1",
        Vec::new(),
    )
    .with_namespace("ns_read");
    let counts = pipeline
        .begin()
        .apply("Read", ExternalSource::new(read))
        .apply("Count", Map::new(|_: Row| 1_i64));
    let downstream = counts.apply("Double", Map::new(|count: i64| count * 2));
    [counts.as_iter(), downstream.as_iter()]
}

#[test]
fn a_side_input_after_a_placeholder_reads_the_window_mapping_from_the_runner() {
    placeholder_views().iter().for_each(|view| {
        assert_eq!(view.to_proto().window_mapping_fn, None, "{}", view.tag());
        let reader = DescriptorReader {
            windowing: SideInputWindowing::Global,
            windows: Mutex::default(),
        };
        let header = WindowedHeader::global(0, PaneInfo::NO_FIRING);
        let mut sink: Vec<Vec<u8>> = Vec::new();
        let mut hctx = HandlerContext::new(&mut sink)
            .with_side_inputs(Some(&reader))
            .with_header(&header);
        let ctx = hctx.as_process_context::<i64>();

        let values = ctx.side_input(view).unwrap().to_vec().unwrap();

        assert_eq!(values, [7], "{}", view.tag());
        assert_eq!(*reader.windows.lock().unwrap(), [Vec::<u8>::new()]);
    });
}
