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

//! Tests verifying `DoFn` access to element pane info and metadata propagation.

use beam::coders::{
    CausedByDrain, DefaultCoder, ElementMetadata, PaneInfo, Timing, ValueKind, WindowedHeader,
};
use beam::internals::ElementSink;
use beam::transforms::ProcessContext;

/// Test sink that keeps the header of each pushed element, which `Vec<Vec<u8>>` drops.
#[derive(Default)]
struct HeaderSink {
    pushed: Vec<(Option<String>, WindowedHeader, Vec<u8>)>,
}

impl ElementSink for HeaderSink {
    fn push(&mut self, element: Vec<u8>) -> Result<(), String> {
        self.pushed.push((None, WindowedHeader::default(), element));
        Ok(())
    }

    fn push_tagged(&mut self, tag: &str, element: Vec<u8>) -> Result<(), String> {
        self.pushed
            .push((Some(tag.to_string()), WindowedHeader::default(), element));
        Ok(())
    }

    fn push_windowed(&mut self, header: &WindowedHeader, element: Vec<u8>) -> Result<(), String> {
        self.pushed.push((None, header.clone(), element));
        Ok(())
    }

    fn push_tagged_windowed(
        &mut self,
        tag: &str,
        header: &WindowedHeader,
        element: Vec<u8>,
    ) -> Result<(), String> {
        self.pushed
            .push((Some(tag.to_string()), header.clone(), element));
        Ok(())
    }
}

fn draining_metadata() -> ElementMetadata {
    ElementMetadata {
        drain: CausedByDrain::CausedByDrain,
        ..Default::default()
    }
}

/// Pane with indices large enough to need the two-index encoding, so it is wider than a byte.
fn multi_byte_pane() -> PaneInfo {
    PaneInfo {
        is_first: false,
        is_last: false,
        timing: Timing::Late,
        index: 300,
        on_time_index: 7,
    }
}

#[test]
fn a_dofn_sees_the_pane_and_metadata_of_its_element() {
    let header = WindowedHeader::global_with_metadata(
        1_000,
        PaneInfo::ON_TIME_AND_ONLY_FIRING,
        &draining_metadata(),
    );
    let mut sink = HeaderSink::default();
    let ctx = ProcessContext::<i32>::with_context(&mut sink, None, &header);

    assert_eq!(ctx.timestamp(), 1_000);
    assert_eq!(ctx.pane(), PaneInfo::ON_TIME_AND_ONLY_FIRING);
    assert_eq!(ctx.metadata(), draining_metadata());
    assert!(ctx.is_draining());
}

#[test]
fn a_dofn_without_a_header_is_not_draining() {
    let mut sink = HeaderSink::default();
    let ctx = ProcessContext::<i32>::new(&mut sink);

    assert_eq!(ctx.timestamp(), 0);
    assert_eq!(ctx.pane(), PaneInfo::NO_FIRING);
    assert!(!ctx.is_draining());
}

#[test]
fn re_timestamping_an_output_keeps_its_pane_and_metadata() {
    // Re-timestamping must keep the pane info and metadata of the input. A fresh NO_FIRING
    // header would drop both, and the re-timestamped element would lose its drain flag.
    let header =
        WindowedHeader::global_with_metadata(1_000, multi_byte_pane(), &draining_metadata());
    for tag in [None, Some("side")] {
        let mut sink = HeaderSink::default();
        {
            let mut ctx = ProcessContext::<i32>::with_context(&mut sink, None, &header);
            let builder = ctx.output(7);
            let builder = match tag {
                Some(t) => builder.to(t),
                None => builder,
            };
            builder.at(2_000).emit().expect("emit");
        }

        assert_eq!(sink.pushed.len(), 1, "{tag:?}: exactly one output");
        let (pushed_tag, out, _) = sink.pushed.first().expect("one output");
        assert_eq!(pushed_tag.as_deref(), tag);
        assert_eq!(out.timestamp_millis(), 2_000, "{tag:?}");
        assert_eq!(out.pane(), multi_byte_pane(), "{tag:?}");
        assert_eq!(out.metadata(), draining_metadata(), "{tag:?}");
    }
}

#[test]
fn a_plain_built_output_inherits_everything() {
    let header = WindowedHeader::global_with_metadata(
        1_000,
        PaneInfo::ON_TIME_AND_ONLY_FIRING,
        &draining_metadata(),
    );
    let mut sink = HeaderSink::default();
    {
        let mut ctx = ProcessContext::<i32>::with_context(&mut sink, None, &header);
        ctx.output(7).emit().expect("emit");
    }

    assert_eq!(sink.pushed.len(), 1, "exactly one output");
    let (_, out, element) = sink.pushed.first().expect("one output");
    assert_eq!(out.timestamp_millis(), 1_000);
    assert_eq!(out.pane(), PaneInfo::ON_TIME_AND_ONLY_FIRING);
    assert_eq!(out.metadata(), draining_metadata());
    assert_eq!(element, &7i32.encode().expect("encode"));
}

#[test]
fn a_built_output_can_override_each_field() {
    let header = WindowedHeader::global(1_000, PaneInfo::NO_FIRING);
    let mut sink = HeaderSink::default();
    {
        let mut ctx = ProcessContext::<i32>::with_context(&mut sink, None, &header);
        ctx.output(7)
            .to("late")
            .at(3_000)
            .with_pane(multi_byte_pane())
            .with_drain(CausedByDrain::CausedByDrain)
            .with_value_kind(ValueKind::Delete)
            .with_trace("00-trace-span-01", Some("vendor=1".to_string()))
            .emit()
            .expect("emit");
    }

    assert_eq!(sink.pushed.len(), 1, "exactly one output");
    let (tag, out, _) = sink.pushed.first().expect("one output");
    assert_eq!(tag.as_deref(), Some("late"));
    assert_eq!(out.timestamp_millis(), 3_000);
    assert_eq!(out.pane(), multi_byte_pane());
    assert_eq!(
        out.metadata(),
        ElementMetadata {
            drain: CausedByDrain::CausedByDrain,
            value_kind: ValueKind::Delete,
            traceparent: Some("00-trace-span-01".to_string()),
            tracestate: Some("vendor=1".to_string()),
        }
    );
}

#[test]
fn a_built_output_can_clear_inherited_metadata() {
    let header =
        WindowedHeader::global_with_metadata(1_000, PaneInfo::NO_FIRING, &draining_metadata());
    let mut sink = HeaderSink::default();
    {
        let mut ctx = ProcessContext::<i32>::with_context(&mut sink, None, &header);
        ctx.output(7)
            .with_metadata(ElementMetadata::default())
            .emit()
            .expect("emit");
    }

    assert_eq!(sink.pushed.len(), 1, "exactly one output");
    let (_, out, _) = sink.pushed.first().expect("one output");
    assert_eq!(out.metadata(), ElementMetadata::default());
}

#[test]
fn a_multi_byte_pane_does_not_bleed_into_the_window_bytes() {
    // The window bytes are a state and timer key. The trailing VarInts of a multi-byte pane
    // must not attach to them, because this splits the key silently.
    let windows = vec![vec![0xAAu8; 8]];
    let header = WindowedHeader::new(1_000, &windows, multi_byte_pane());

    assert_eq!(header.window_bytes(), &[0xAAu8; 8]);
    assert_eq!(header.pane(), multi_byte_pane());

    let mut sink = HeaderSink::default();
    let ctx = ProcessContext::<i32>::with_context(&mut sink, None, &header);
    assert_eq!(ctx.window(), &[0xAAu8; 8]);
}
