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

//! `WindowIntoHandler` on encoded elements and on elements by value.

use std::sync::Arc;
use std::time::Duration;

use beam::coders::{DefaultCoder, IntervalWindow, PaneInfo, WindowedHeader};
use beam::internals::{
    BundleHandler, ElementSink, HandlerContext, TypedElement, WindowIntoHandler,
};
use beam::prelude::*;
use beam::windowing::{BoundedWindow, FixedWindows};

/// Records the header and the bytes of each output.
#[derive(Default)]
struct RecordingSink(Vec<(WindowedHeader, Vec<u8>)>);

impl ElementSink for RecordingSink {
    fn push(&mut self, _element: Vec<u8>) -> std::result::Result<(), String> {
        Err("expected an output with an explicit header".into())
    }

    fn push_windowed(
        &mut self,
        header: &WindowedHeader,
        element: Vec<u8>,
    ) -> std::result::Result<(), String> {
        self.0.push((header.clone(), element));
        Ok(())
    }
}

fn fixed_10s() -> WindowIntoHandler {
    WindowIntoHandler::new(Arc::new(FixedWindows::of(Duration::from_secs(10))))
}

fn elements() -> Vec<(i64, (String, String))> {
    vec![
        (12_345, ("long".to_string(), "x".repeat(300))),
        (25_000, ("short".to_string(), "y".to_string())),
        (25_001, ("empty".to_string(), String::new())),
    ]
}

#[test]
fn process_value_gives_the_same_output_as_process() {
    let mut by_value = RecordingSink::default();
    let mut by_bytes = RecordingSink::default();
    let mut value_handler = fixed_10s();
    let mut bytes_handler = fixed_10s();

    for (timestamp, value) in elements() {
        let header = WindowedHeader::global(timestamp, PaneInfo::NO_FIRING);
        let encoded = value.encode().expect("encode element");
        bytes_handler
            .process(
                &encoded,
                &mut HandlerContext::new(&mut by_bytes).with_header(&header),
            )
            .expect("window encoded element");
        let mut slot = Some(value);
        value_handler
            .process_value(
                TypedElement::new(&mut slot),
                &mut HandlerContext::new(&mut by_value).with_header(&header),
            )
            .expect("window element by value");
    }

    assert_eq!(by_value.0, by_bytes.0);
    let windows: Vec<BoundedWindow> = by_value
        .0
        .iter()
        .map(|(header, _)| header.single_window().expect("one window"))
        .collect();
    let interval = |start, end| {
        BoundedWindow::Interval(IntervalWindow::new(
            Instant::from_millis(start),
            Instant::from_millis(end),
        ))
    };
    assert_eq!(
        windows,
        [
            interval(10_000, 20_000),
            interval(20_000, 30_000),
            interval(20_000, 30_000),
        ]
    );
    let decoded: Vec<(String, String)> = by_value
        .0
        .iter()
        .map(|(_, bytes)| DefaultCoder::decode(bytes).expect("decode element"))
        .collect();
    let expected: Vec<(String, String)> = elements().into_iter().map(|(_, v)| v).collect();
    assert_eq!(decoded, expected);
}

#[test]
fn process_value_fails_for_a_taken_element() {
    let mut sink = RecordingSink::default();
    let header = WindowedHeader::global(0, PaneInfo::NO_FIRING);
    let mut slot = Some(("k".to_string(), "v".to_string()));
    let mut element = TypedElement::new(&mut slot);
    assert!(element.take::<(String, String)>().is_some());

    let result = fixed_10s().process_value(
        element,
        &mut HandlerContext::new(&mut sink).with_header(&header),
    );

    assert!(result.is_err());
    assert!(sink.0.is_empty());
}
