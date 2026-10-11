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

//! Integration tests for bundle execution over the Fn API.
//!
//! Covers transform ordering, fan-out, tagged outputs, the `DoFn` bundle lifecycle across
//! chunks, bundle metrics and residuals.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use beam::coders::{
    Coder, Context, GlobalWindow, StringUtf8Coder, VarIntCoder, WindowedValue, WindowedValueCoder,
};
use beam::internals::{DoFnHandler, ElementSink, TransformFn, duration_to_proto};
use beam::prelude::{BoundedWindow, Instant};
use beam::transforms::{DoFn, FinishBundleContext, ProcessContext};
use model::fn_execution::ProcessBundleDescriptor;

mod common;
use common::{
    CODER_RAW, DescriptorBuilder, SINK_ID, STAGE_ID, SampledSizes, URN_PARDO, bytes_coder,
    element_counts, identity_handlers, linear_descriptor, run_bundle, sampled_byte_sizes,
};

const TRANSFORM_A: &str = "transform_a";
const TRANSFORM_B: &str = "transform_b";
const BATCH_ID: &str = "batch_transform";

/// Two chained user transforms: the second runs only if topological order and routing work.
#[tokio::test]
async fn chained_transforms_apply_in_topological_order() {
    let wrap = |tag: &'static str| -> TransformFn {
        Arc::new(move |element: &[u8], sink: &mut dyn ElementSink| {
            let mut out = tag.as_bytes().to_vec();
            out.extend_from_slice(element);
            sink.push(out)
        })
    };
    let handlers = HashMap::from([
        ("stage_first".to_string(), wrap("first:")),
        ("stage_second".to_string(), wrap("second:")),
    ]);

    let descriptor = DescriptorBuilder::new("desc_chain")
        .with_coders(CODER_RAW, bytes_coder())
        .stage("stage_first", "pcoll_input", "pcoll_mid")
        .stage("stage_second", "pcoll_mid", "pcoll_out")
        .sink(SINK_ID, "pcoll_out")
        .build();

    let run = run_bundle(handlers, descriptor, vec![b"payload".to_vec()]).await;
    let _ = run.bundle_response();

    let received = run.sink_bytes.get(SINK_ID).cloned().unwrap_or_default();
    assert_eq!(received, b"second:first:payload");
}

/// A DATA_SOURCE fanning out through two ParDos into two separate DATA_SINKs.
fn create_multi_sink_descriptor(descriptor_id: &str) -> ProcessBundleDescriptor {
    DescriptorBuilder::new(descriptor_id)
        .with_coders(CODER_RAW, bytes_coder())
        .stage(TRANSFORM_A, "pcoll_input", "pcoll_a")
        .stage(TRANSFORM_B, "pcoll_input", "pcoll_b")
        .sink("sink_a", "pcoll_a")
        .sink("sink_b", "pcoll_b")
        .build()
}
#[tokio::test]
async fn multi_sink_branching_routes_to_all_sinks() {
    let handlers: HashMap<String, TransformFn> = HashMap::from([
        (
            "transform_a".to_string(),
            Arc::new(|in_bytes: &[u8], sink: &mut dyn ElementSink| {
                let mut out = b"A:".to_vec();
                out.extend_from_slice(in_bytes);
                sink.push(out)
            }) as TransformFn,
        ),
        (
            "transform_b".to_string(),
            Arc::new(|in_bytes: &[u8], sink: &mut dyn ElementSink| {
                let mut out = b"B:".to_vec();
                out.extend_from_slice(in_bytes);
                sink.push(out)
            }) as TransformFn,
        ),
    ]);

    let desc_id = "desc_multi_sink";
    let pbd = create_multi_sink_descriptor(desc_id);

    let run = run_bundle(handlers, pbd, vec![b"payload".to_vec()]).await;
    let pb_resp = run.bundle_response();

    assert_eq!(
        run.sink_bytes.get("sink_a").map(Vec::as_slice),
        Some(&b"A:payload"[..])
    );
    assert_eq!(
        run.sink_bytes.get("sink_b").map(Vec::as_slice),
        Some(&b"B:payload"[..])
    );

    let read_index_info = pb_resp
        .monitoring_infos
        .iter()
        .find(|info| info.urn == beam::metrics::URN_DATA_CHANNEL_READ_INDEX)
        .expect("data_channel_read_index metric must be present");
    assert_eq!(
        read_index_info.labels.get(beam::metrics::LABEL_PTRANSFORM),
        Some(&"source_transform".to_string())
    );
    let read_index = VarIntCoder::decode_varint(&mut &read_index_info.payload[..]).unwrap();
    // The final read index is one past the last element read, so it is a count.
    assert_eq!(read_index, 1);

    let counts_by_pcol: HashMap<_, _> = pb_resp
        .monitoring_infos
        .iter()
        .filter(|info| info.urn == beam::metrics::URN_ELEMENT_COUNT)
        .filter_map(|info| {
            let pcol = info.labels.get(beam::metrics::LABEL_PCOLLECTION)?;
            let count = VarIntCoder::decode_varint(&mut &info.payload[..]).ok()?;
            Some((pcol.as_str(), count))
        })
        .collect();

    assert_eq!(counts_by_pcol.get("pcoll_input"), Some(&1));
    assert_eq!(counts_by_pcol.get("pcoll_a"), Some(&1));
    assert_eq!(counts_by_pcol.get("pcoll_b"), Some(&1));

    // A runner without the short-id protocol gets only complete infos.
    assert!(pb_resp.monitoring_data.is_empty());
}

/// A DATA_SOURCE -> ParDo -> DATA_SINK descriptor over `WindowedValue<String, GlobalWindow>`.
fn create_batching_pipeline_descriptor(descriptor_id: &str) -> ProcessBundleDescriptor {
    DescriptorBuilder::new(descriptor_id)
        .stage(BATCH_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build()
}
#[tokio::test]
async fn multi_chunk_state_batching_lifecycle_runs_start_and_finish_once() {
    let start_count = Arc::new(AtomicUsize::new(0));
    let finish_count = Arc::new(AtomicUsize::new(0));

    /// Copies share the counters the test reads; `start_bundle` resets the buffer.
    #[derive(Clone)]
    struct MultiChunkBatchingDoFn {
        start_bundles: Arc<AtomicUsize>,
        finish_bundles: Arc<AtomicUsize>,
        buffer: Vec<String>,
    }

    impl DoFn for MultiChunkBatchingDoFn {
        type In = String;
        type Out = String;

        fn start_bundle(&mut self) -> beam::Result {
            self.start_bundles.fetch_add(1, Ordering::SeqCst);
            self.buffer.clear();
            Ok(())
        }

        fn process_element(
            &mut self,
            element: String,
            _out: &mut ProcessContext<String>,
        ) -> beam::Result {
            self.buffer.push(element);
            Ok(())
        }

        fn finish_bundle(&mut self, out: &mut FinishBundleContext<'_, String>) -> beam::Result {
            self.finish_bundles.fetch_add(1, Ordering::SeqCst);
            out.emit(
                std::mem::take(&mut self.buffer).join("+"),
                BoundedWindow::Global(GlobalWindow),
                Instant::END_OF_GLOBAL_WINDOW,
            )
        }
    }

    let batching_fn = MultiChunkBatchingDoFn {
        start_bundles: start_count.clone(),
        finish_bundles: finish_count.clone(),
        buffer: Vec::new(),
    };

    let handlers = HashMap::from([(
        "batch_transform".to_string(),
        Arc::new(DoFnHandler::new(batching_fn)) as TransformFn,
    )]);

    let descriptor_id = "desc-multi-chunk-batch";
    let descriptor = create_batching_pipeline_descriptor(descriptor_id);

    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut chunk1 = Vec::new();
    coder
        .encode(
            &WindowedValue::global("alpha".to_string(), 1000),
            &mut chunk1,
            Context::Nested,
        )
        .unwrap();
    coder
        .encode(
            &WindowedValue::global("beta".to_string(), 1000),
            &mut chunk1,
            Context::Nested,
        )
        .unwrap();

    let mut chunk2 = Vec::new();
    coder
        .encode(
            &WindowedValue::global("gamma".to_string(), 1000),
            &mut chunk2,
            Context::Nested,
        )
        .unwrap();
    coder
        .encode(
            &WindowedValue::global("delta".to_string(), 1000),
            &mut chunk2,
            Context::Nested,
        )
        .unwrap();

    let run = run_bundle(handlers, descriptor, vec![chunk1, chunk2]).await;
    let _ = run.bundle_response();

    assert_eq!(
        start_count.load(Ordering::SeqCst),
        1,
        "start_bundle must be called exactly once for the bundle, not once per chunk"
    );
    assert_eq!(
        finish_count.load(Ordering::SeqCst),
        1,
        "finish_bundle must be called exactly once for the bundle, not once per chunk"
    );

    let sink_bytes = run.sink_bytes.get(SINK_ID).cloned().unwrap_or_default();
    let mut cursor = sink_bytes.as_slice();
    let mut decoded_values = Vec::new();
    while !cursor.is_empty() {
        let wv: WindowedValue<String, GlobalWindow> =
            coder.decode(&mut cursor, Context::Nested).unwrap();
        decoded_values.push(wv.value);
    }

    assert_eq!(
        decoded_values,
        vec!["alpha+beta+gamma+delta".to_string()],
        "Buffered elements across all chunks must be aggregated and flushed in finish_bundle"
    );
}

/// The error names the transforms in the loop; a transform outside it still orders fine.
#[tokio::test]
async fn a_descriptor_whose_transforms_form_a_cycle_is_rejected() {
    let descriptor = DescriptorBuilder::new("desc_cycle")
        .with_coders(CODER_RAW, bytes_coder())
        .stage("stage_fed", "pcoll_input", "pcoll_out")
        .stage("cycle_a", "pcoll_b", "pcoll_a")
        .stage("cycle_b", "pcoll_a", "pcoll_b")
        .sink(SINK_ID, "pcoll_out")
        .build();

    let run = run_bundle(
        identity_handlers(&["stage_fed", "cycle_a", "cycle_b"]),
        descriptor,
        vec![b"payload".to_vec()],
    )
    .await;

    let error = run.response.error;
    assert!(error.contains("cycle"), "{error}");
    assert!(
        error.contains("'cycle_a'") && error.contains("'cycle_b'"),
        "the error must name both transforms of the loop: {error}"
    );
}

#[tokio::test]
async fn an_unregistered_transform_error_names_its_id_and_urn() {
    let run = run_bundle(
        HashMap::new(),
        linear_descriptor("desc_unregistered_detail"),
        Vec::new(),
    )
    .await;

    let error = run.response.error;
    let expected = format!("'{STAGE_ID}' (id='{STAGE_ID}', urn='{URN_PARDO}')");
    assert!(error.contains(&expected), "expected {expected} in: {error}");
}

const ROUTER_ID: &str = "router";

/// A source -> router -> {evens, odds} descriptor, each tagged output read by its own sink.
fn router_descriptor(builder: DescriptorBuilder) -> ProcessBundleDescriptor {
    builder
        .stage_with_outputs(
            ROUTER_ID,
            "pcoll_input",
            &[("evens", "pcoll_evens"), ("odds", "pcoll_odds")],
        )
        .sink("sink_evens", "pcoll_evens")
        .sink("sink_odds", "pcoll_odds")
        .build()
}

/// The output tag for an element whose last byte is an ASCII digit.
fn parity_tag(element: &[u8]) -> &'static str {
    match element.last() {
        Some(digit) if digit % 2 == 0 => "evens",
        _ => "odds",
    }
}

#[tokio::test]
async fn tagged_outputs_reach_only_the_sink_reading_that_tag() {
    let router: TransformFn = Arc::new(|element: &[u8], sink: &mut dyn ElementSink| {
        sink.push_tagged(parity_tag(element), element.to_vec())
    });
    let descriptor = router_descriptor(
        DescriptorBuilder::new("desc_tagged").with_coders(CODER_RAW, bytes_coder()),
    );

    let run = run_bundle(
        HashMap::from([(ROUTER_ID.to_string(), router)]),
        descriptor,
        ["1", "2", "3", "4"].map(|s| s.as_bytes().to_vec()).to_vec(),
    )
    .await;
    run.bundle_response();

    assert_eq!(run.sink_bytes.get("sink_evens"), Some(&b"24".to_vec()));
    assert_eq!(run.sink_bytes.get("sink_odds"), Some(&b"13".to_vec()));
}

/// Each element keeps the header it was emitted with, not its input's.
#[tokio::test]
async fn tagged_windowed_outputs_reach_only_the_sink_reading_that_tag() {
    const EMITTED_AT: i64 = 42;
    let router: TransformFn = Arc::new(|element: &[u8], sink: &mut dyn ElementSink| {
        let header = beam::coders::WindowedHeader::global(
            EMITTED_AT,
            beam::coders::PaneInfo::ON_TIME_AND_ONLY_FIRING,
        );
        sink.push_tagged_windowed(parity_tag(element), &header, element.to_vec())
    });
    let descriptor = router_descriptor(DescriptorBuilder::new("desc_tagged_windowed"));
    let coder = WindowedValueCoder::new(StringUtf8Coder);
    let mut chunk = Vec::new();
    for value in ["1", "2", "3", "4"] {
        coder
            .encode(
                &WindowedValue::global(value.to_string(), 0),
                &mut chunk,
                Context::Nested,
            )
            .unwrap();
    }

    let run = run_bundle(
        HashMap::from([(ROUTER_ID.to_string(), router)]),
        descriptor,
        vec![chunk],
    )
    .await;
    run.bundle_response();

    let decode_all = |bytes: &[u8]| {
        let mut cursor = bytes;
        let mut out = Vec::new();
        while !cursor.is_empty() {
            let wv: WindowedValue<String, GlobalWindow> =
                coder.decode(&mut cursor, Context::Nested).unwrap();
            out.push((wv.value, wv.timestamp_millis, wv.pane));
        }
        out
    };
    let on_time = beam::coders::PaneInfo::ON_TIME_AND_ONLY_FIRING;
    let expected = |values: [&str; 2]| {
        values
            .map(|v| (v.to_string(), EMITTED_AT, on_time))
            .to_vec()
    };
    assert_eq!(
        decode_all(
            run.sink_bytes
                .get("sink_evens")
                .map_or(&[][..], Vec::as_slice)
        ),
        expected(["2", "4"])
    );
    assert_eq!(
        decode_all(
            run.sink_bytes
                .get("sink_odds")
                .map_or(&[][..], Vec::as_slice)
        ),
        expected(["1", "3"])
    );
}

#[tokio::test]
async fn final_response_reports_sampled_byte_sizes_per_pcollection() {
    let run = run_bundle(
        identity_handlers(&[STAGE_ID]),
        linear_descriptor("desc_sampled_sizes"),
        vec![b"abc".to_vec(), b"hello".to_vec()],
    )
    .await;

    let sizes = SampledSizes {
        count: 2,
        sum: 8,
        min: 3,
        max: 5,
    };
    assert_eq!(
        sampled_byte_sizes(&run.bundle_response().monitoring_infos),
        HashMap::from([
            ("pcoll_input".to_string(), sizes),
            ("pcoll_output".to_string(), sizes),
        ])
    );
}

/// It reports no sizes either: the metrics are absent, not zero.
#[tokio::test]
async fn a_pcollection_that_saw_no_elements_reports_no_element_count() {
    let drop_all: TransformFn = Arc::new(|_element: &[u8], _sink: &mut dyn ElementSink| Ok(()));
    let run = run_bundle(
        HashMap::from([(STAGE_ID.to_string(), drop_all)]),
        linear_descriptor("desc_no_output"),
        vec![b"dropped".to_vec()],
    )
    .await;
    let infos = &run.bundle_response().monitoring_infos;

    assert_eq!(
        element_counts(infos),
        HashMap::from([("pcoll_input".to_string(), 1)])
    );
    assert_eq!(
        sampled_byte_sizes(infos).keys().collect::<Vec<_>>(),
        vec!["pcoll_input"]
    );
}

/// Defers each element it is given back to the runner as a residual.
#[derive(Clone)]
struct DeferEverything;

impl beam::internals::BundleHandler for DeferEverything {
    fn process(
        &mut self,
        element: &[u8],
        ctx: &mut beam::internals::HandlerContext<'_>,
    ) -> Result<(), String> {
        ctx.add_residual(beam::internals::ResidualApplication {
            transform_id: ctx.transform_id().to_string(),
            input_id: "in".to_string(),
            element: element.to_vec(),
            output_watermark: Some(Instant::from_millis(1_000)),
            delay: Some(std::time::Duration::from_secs(2)),
        });
        Ok(())
    }

    fn instantiate(&self) -> beam::internals::HandlerInstance {
        Box::new(self.clone())
    }
}

#[tokio::test]
async fn a_deferred_residual_is_returned_in_the_bundle_response() {
    let run = run_bundle(
        HashMap::from([(
            STAGE_ID.to_string(),
            Arc::new(DeferEverything) as TransformFn,
        )]),
        linear_descriptor("desc_residual"),
        vec![b"rest".to_vec()],
    )
    .await;

    let expected = model::fn_execution::DelayedBundleApplication {
        application: Some(model::fn_execution::BundleApplication {
            transform_id: STAGE_ID.to_string(),
            input_id: "in".to_string(),
            element: b"rest".to_vec(),
            output_watermarks: HashMap::from([(
                "out".to_string(),
                Instant::from_millis(1_000).into(),
            )]),
            ..Default::default()
        }),
        requested_time_delay: Some(duration_to_proto(std::time::Duration::from_secs(2))),
    };
    assert_eq!(run.bundle_response().residual_roots, vec![expected]);
}

/// Outputs live in a hash map, so several bundles with fresh graphs guard against a lucky pick.
#[tokio::test]
async fn untagged_emissions_of_a_multi_output_transform_go_to_its_main_output() {
    const ROUNDS: usize = 8;
    let outputs = [
        ("out", "pcoll_main"),
        ("side_a", "pcoll_side_a"),
        ("side_b", "pcoll_side_b"),
        ("side_c", "pcoll_side_c"),
    ];
    for round in 0..ROUNDS {
        let descriptor = outputs
            .iter()
            .fold(
                DescriptorBuilder::new("desc_main_output")
                    .with_coders(CODER_RAW, bytes_coder())
                    .stage_with_outputs(STAGE_ID, "pcoll_input", &outputs),
                |builder, (_, pcoll)| builder.sink(&format!("sink_{pcoll}"), pcoll),
            )
            .build();

        let run = run_bundle(
            identity_handlers(&[STAGE_ID]),
            descriptor,
            vec![b"payload".to_vec()],
        )
        .await;
        run.bundle_response();

        let written: HashMap<&str, &[u8]> = run
            .sink_bytes
            .iter()
            .filter(|(_, bytes)| !bytes.is_empty())
            .map(|(sink, bytes)| (sink.as_str(), bytes.as_slice()))
            .collect();
        assert_eq!(
            written,
            HashMap::from([("sink_pcoll_main", &b"payload"[..])]),
            "round {round}"
        );
    }
}
