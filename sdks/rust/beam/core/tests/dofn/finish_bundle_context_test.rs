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

//! `FinishBundleContext` outputs.

use beam::coders::{DefaultCoder, PaneInfo, WindowedHeader};
use beam::internals::{BundleHandler, DoFnHandler, ElementSink, HandlerContext};
use beam::prelude::*;

/// Records each element with the header it was pushed under.
#[derive(Default)]
struct HeaderRecorder(Vec<(Option<WindowedHeader>, Vec<u8>)>);

impl ElementSink for HeaderRecorder {
    fn push(&mut self, element: Vec<u8>) -> std::result::Result<(), String> {
        self.0.push((None, element));
        Ok(())
    }

    fn push_windowed(
        &mut self,
        header: &WindowedHeader,
        element: Vec<u8>,
    ) -> std::result::Result<(), String> {
        self.0.push((Some(header.clone()), element));
        Ok(())
    }
}

fn window() -> IntervalWindow {
    IntervalWindow::new(Instant::from_millis(1_000), Instant::from_millis(2_000))
}

/// Emits one element from `finish_bundle`, into `window()` at 1.5s.
#[derive(Clone)]
struct EmitsAtFinish;

impl DoFn for EmitsAtFinish {
    type In = String;
    type Out = String;

    fn process_element(&mut self, _s: String, _ctx: &mut ProcessContext<'_, String>) -> Result {
        Ok(())
    }

    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, String>) -> Result {
        ctx.emit(
            "done".to_string(),
            BoundedWindow::Interval(window()),
            Instant::from_millis(1_500),
        )
    }
}

#[test]
fn finish_bundle_emit_carries_the_given_window_and_timestamp() {
    let mut handler = DoFnHandler::new(EmitsAtFinish);
    let mut sink = HeaderRecorder::default();
    handler
        .finish_bundle(&mut HandlerContext::new(&mut sink))
        .expect("finish_bundle");

    let [(Some(header), bytes)] = sink.0.as_slice() else {
        panic!("expected one windowed output, got {:?}", sink.0.len());
    };
    assert_eq!(String::decode(bytes).unwrap(), "done");
    assert_eq!(header.timestamp_millis(), 1_500);
    assert_eq!(
        header.single_window().expect("one window"),
        BoundedWindow::Interval(window())
    );
    assert_eq!(header.pane(), PaneInfo::NO_FIRING);
}
