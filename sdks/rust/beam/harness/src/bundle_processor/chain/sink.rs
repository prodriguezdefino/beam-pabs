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

use std::sync::Arc;

use beam::coders::{PaneInfo, URN_INTERVAL_WINDOW, WindowedHeader};
use beam::internals::{BundleHandler, ElementSink, TypedElement};
use model::fn_execution::ProcessBundleDescriptor;

use super::super::BundleError;
use crate::data::Outbound;

use crate::bundle_processor::chain::build::{
    OperatorGraph, OutputRouting, PCollIdx, PCollectionRoute,
};
use crate::bundle_processor::chain::instances::{Downstream, Instances};
use crate::bundle_processor::chain::stats::PCollectionStats;

/// Mutable state that one push through the chain carries.
pub(in crate::bundle_processor) struct ChainCtx<'a> {
    pub(in crate::bundle_processor) descriptor: &'a ProcessBundleDescriptor,
    pub(in crate::bundle_processor) graph: &'a OperatorGraph,
    /// The data sink buffers of the bundle.
    pub(in crate::bundle_processor) outbound: &'a mut Outbound,
    pub(in crate::bundle_processor) side_input_reader:
        Option<&'a Arc<dyn beam::internals::SideInputReader>>,
    /// The view of each operator on the bundle user state, indexed by operator. Empty when
    /// the bundle has no user state.
    pub(in crate::bundle_processor) state_readers:
        &'a [Arc<dyn beam::internals::UserStateReader>],
    pub(in crate::bundle_processor) timer_collector:
        Option<&'a Arc<beam::internals::TimerCollector>>,
    pub(in crate::bundle_processor) residual_collector:
        Option<&'a Arc<beam::internals::ResidualCollector>>,
    pub(in crate::bundle_processor) metrics_container:
        Option<&'a Arc<beam::metrics::MetricsContainer>>,
    pub(in crate::bundle_processor) bundle_finalizer:
        Option<&'a Arc<beam::internals::BundleFinalizerCollector>>,
    pub(in crate::bundle_processor) dynamic_split_registrar:
        Option<&'a Arc<beam::internals::DynamicSplitRegistrar>>,
    pub(in crate::bundle_processor) state_stream_reader:
        Option<&'a Arc<dyn beam::coders::StateStreamReader>>,
    pub(in crate::bundle_processor) stats: &'a mut PCollectionStats,
    /// Charges execution time to the running operator. See `sampler.rs`.
    pub(in crate::bundle_processor) sampler: &'a crate::bundle_processor::sampler::ExecutionSampler,
}

impl ChainCtx<'_> {
    /// Enters the thread-local metrics scope for one synchronous run through the chain. Each
    /// operator call selects its transform in it. Do not hold it across an `.await`.
    pub(in crate::bundle_processor) fn enter_metrics_scope(
        &self,
    ) -> Option<beam::metrics::MetricsScope> {
        self.metrics_container.map(|container| {
            beam::metrics::MetricsScope::enter_transforms(
                Arc::clone(container),
                Arc::clone(&self.graph.transform_ids),
            )
        })
    }
}

/// Routes the outputs of a transform to every PCollection that it produces.
struct ChainSink<'ctx, 'a> {
    ctx: &'ctx mut ChainCtx<'a>,
    /// The handlers of the operators after this one. Its outputs go to these handlers.
    downstream: Downstream<'ctx>,
    routing: &'ctx OutputRouting,
    header: &'ctx WindowedHeader,
}

impl ChainSink<'_, '_> {
    /// Returns the PCollection that an output tag names.
    fn tagged(&self, tag: &str) -> Result<PCollIdx, String> {
        self.routing.resolve(tag).ok_or_else(|| {
            format!(
                "Worker harness: output tag '{tag}' not found in transform outputs: {:?}",
                self.routing.by_tag.keys().collect::<Vec<_>>()
            )
        })
    }

    /// Delivers the encoded `element` to the untagged output: the main output if the transform
    /// has one, otherwise every output.
    fn push_untagged(&mut self, header: &WindowedHeader, element: &[u8]) -> Result<(), String> {
        match self.routing.default_pcoll {
            Some(pcoll) => push_to_pcoll(self.ctx, &mut self.downstream, pcoll, header, element),
            None => self.routing.by_tag.values().try_for_each(|&pcoll| {
                push_to_pcoll(self.ctx, &mut self.downstream, pcoll, header, element)
            }),
        }
        .map_err(|e| e.to_string())
    }
}

impl ElementSink for ChainSink<'_, '_> {
    fn push(&mut self, element: Vec<u8>) -> Result<(), String> {
        self.push_untagged(self.header, &element)
    }

    fn push_tagged(&mut self, tag: &str, element: Vec<u8>) -> Result<(), String> {
        let pcoll = self.tagged(tag)?;
        push_to_pcoll(self.ctx, &mut self.downstream, pcoll, self.header, &element)
            .map_err(|e| e.to_string())
    }

    fn push_windowed(&mut self, header: &WindowedHeader, element: Vec<u8>) -> Result<(), String> {
        self.push_untagged(header, &element)
    }

    fn push_tagged_windowed(
        &mut self,
        tag: &str,
        header: &WindowedHeader,
        element: Vec<u8>,
    ) -> Result<(), String> {
        let pcoll = self.tagged(tag)?;
        push_to_pcoll(self.ctx, &mut self.downstream, pcoll, header, &element)
            .map_err(|e| e.to_string())
    }

    fn push_value(
        &mut self,
        tag: Option<&str>,
        header: Option<&WindowedHeader>,
        element: TypedElement<'_>,
    ) -> Result<(), String> {
        let header = header.unwrap_or(self.header);
        let pcoll = match tag {
            Some(tag) => Some(self.tagged(tag)?),
            None => self.routing.default_pcoll,
        };
        match pcoll {
            Some(pcoll) => {
                push_value_to_pcoll(self.ctx, &mut self.downstream, pcoll, header, element)
                    .map_err(|e| e.to_string())
            }
            // An untagged element of a multi-output transform goes to every output. It is
            // shared, so it must travel as bytes.
            None => self.push_untagged(header, &element.encode()?),
        }
    }
}

/// Rejects an element that reached an interval-windowed collection without its window.
fn check_window(route: &PCollectionRoute, header: &WindowedHeader) -> Result<(), BundleError> {
    // An interval window never encodes to zero bytes. An element without window bytes
    // has lost its window upstream. Do not pass it on: the interval window coder does
    // not fail on an empty encoding. It reads the element bytes as a window instead.
    if route.needs_window_bytes && header.window_bytes().is_empty() {
        return Err(BundleError::Coder(format!(
            "Element written to PCollection '{}' carries no encoded window, but that \
             collection is windowed by '{URN_INTERVAL_WINDOW}', whose encoding is never empty. \
             The window was dropped by whichever transform produced this element. A transform \
             that buffers elements and emits them later has to record the window each one \
             arrived in and emit it back under that window, through \
             ctx.output(value).windowed(&header) rather than ctx.emit(value).",
            route.id
        )));
    }
    Ok(())
}

/// Returns true when the SDK measures the encoded size of the `count`-th element.
///
/// An encode only to measure the size removes much of the gain of passing by value, so sizes
/// are sampled: the first 16 elements of every bundle, then about one in 32. A pseudo-random
/// spread prevents bias from a periodic pattern in the data.
fn sample_byte_size(count: i64) -> bool {
    const ALWAYS_SAMPLED: i64 = 16;
    count <= ALWAYS_SAMPLED || (count as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 59 == 0
}

/// Delivers one element passed by value to the consumers of a PCollection. Without a
/// [`typed_consumer`](PCollectionRoute::typed_consumer), the element is encoded once and
/// continues on the byte path.
fn push_value_to_pcoll(
    ctx: &mut ChainCtx<'_>,
    downstream: &mut Downstream<'_>,
    pcoll: PCollIdx,
    header: &WindowedHeader,
    element: TypedElement<'_>,
) -> Result<(), BundleError> {
    let graph: &OperatorGraph = ctx.graph;
    let route = &graph.pcollections[pcoll];
    let Some(consumer) = route.typed_consumer else {
        let encoded = element.encode().map_err(BundleError::Coder)?;
        return push_to_pcoll(ctx, downstream, pcoll, header, &encoded);
    };

    check_window(route, header)?;
    if sample_byte_size(ctx.stats.count(pcoll)) {
        let encoded = element.encode().map_err(BundleError::Coder)?;
        ctx.stats.record_size(pcoll, encoded.len());
    }

    invoke_operator(
        ctx,
        downstream.reborrow(),
        consumer,
        header,
        None,
        route.schema.as_ref(),
        |handler, h_ctx| handler.process_value(element, h_ctx),
    )
}

/// Delivers a source element to every PCollection that the bundle data source feeds.
pub(in crate::bundle_processor) fn push_source(
    ctx: &mut ChainCtx<'_>,
    instances: &mut Instances,
    header: &WindowedHeader,
    payload: &[u8],
) -> Result<(), BundleError> {
    let mut downstream = instances.all();
    let graph: &OperatorGraph = ctx.graph;
    graph
        .sources
        .iter()
        .try_for_each(|&pcoll| push_to_pcoll(ctx, &mut downstream, pcoll, header, payload))
}

/// Delivers one element to every consumer of a PCollection, recursing downstream
/// synchronously. The depth is the depth of the operator chain, usually fewer than ten.
fn push_to_pcoll(
    ctx: &mut ChainCtx<'_>,
    downstream: &mut Downstream<'_>,
    pcoll: PCollIdx,
    header: &WindowedHeader,
    element: &[u8],
) -> Result<(), BundleError> {
    // The graph outlives this traversal and does not change, so borrow its routes.
    let graph: &OperatorGraph = ctx.graph;
    let route = &graph.pcollections[pcoll];
    check_window(route, header)?;
    ctx.stats.count(pcoll);
    ctx.stats.record_size(pcoll, element.len());

    route.sinks.iter().try_for_each(|sink| {
        let reframed;
        let element: &[u8] = match &sink.nested_row_prefix_coder {
            Some(coder_id) => {
                reframed = beam::coders::add_nested_row_length_prefixes(
                    element,
                    coder_id,
                    &ctx.descriptor.coders,
                )
                .map_err(|e| {
                    std::io::Error::new(
                        e.kind(),
                        format!(
                            "failed to length-prefix nested Rows for sink '{}': {e}",
                            sink.id
                        ),
                    )
                })?;
                &reframed
            }
            None => element,
        };
        let fallback_header;
        let wire_header = if sink.expects_windowed_value {
            if header.is_empty() {
                fallback_header = WindowedHeader::global(0, PaneInfo::NO_FIRING);
                &fallback_header
            } else {
                header
            }
        } else {
            WindowedHeader::EMPTY
        };
        ctx.outbound.write(sink.index, |buf| {
            buf.extend_from_slice(wire_header.as_bytes());
            if sink.expects_length_prefix {
                beam::coders::VarIntCoder::encode_varint(element.len() as i64, buf)?;
            }
            buf.extend_from_slice(element);
            Ok(())
        })
    })?;

    if route.consumers.is_empty() {
        return Ok(());
    }
    let key_bytes = route
        .key_coder_id
        .as_deref()
        .and_then(|key_coder| beam::coders::key_slice(element, key_coder, &ctx.descriptor.coders));

    route.consumers.iter().try_for_each(|&consumer| {
        invoke_operator(
            ctx,
            downstream.reborrow(),
            consumer,
            header,
            key_bytes,
            route.schema.as_ref(),
            |handler, h_ctx| handler.process(element, h_ctx),
        )
    })
}

/// Calls `finish_bundle` on every operator in topological order. Flushed elements go
/// through the rest of the chain, so a downstream operator, not finished yet, still accepts
/// the flush of an upstream one.
pub(in crate::bundle_processor) fn finish_chain(
    ctx: &mut ChainCtx<'_>,
    instances: &mut Instances,
    header: &WindowedHeader,
) -> Result<(), BundleError> {
    // Elements flushed at the end of a bundle need a wire header. Use a global-window,
    // no-firing header when no header is present.
    let effective_header = if header.is_empty() {
        WindowedHeader::global(0, PaneInfo::NO_FIRING)
    } else {
        header.clone()
    };
    (0..ctx.graph.operators.len()).try_for_each(|operator| {
        invoke_operator(
            ctx,
            instances.all(),
            operator,
            &effective_header,
            None,
            None,
            |handler, h_ctx| handler.finish_bundle(h_ctx),
        )
    })
}

/// Runs one operator with a `HandlerContext` of explicit references to the bundle state, not
/// thread-local state. Elements, timers and `finish_bundle` all enter operators here.
///
/// `handlers` holds the handler of the operator and the handlers after it. The operator gets
/// its handler mutably and lends the rest to its outputs, so every operator it reaches also
/// runs on an exclusive borrow. Returns [`BundleError::InvalidGraph`] for an unreachable
/// operator.
pub(in crate::bundle_processor) fn invoke_operator(
    ctx: &mut ChainCtx<'_>,
    mut handlers: Downstream<'_>,
    operator: usize,
    header: &WindowedHeader,
    key_bytes: Option<&[u8]>,
    input_schema: Option<&Arc<beam::schema::Schema>>,
    invoke: impl FnOnce(
        &mut dyn BundleHandler,
        &mut beam::internals::HandlerContext<'_>,
    ) -> Result<(), String>,
) -> Result<(), BundleError> {
    // Borrow the output routing from the graph, which outlives the bundle.
    let graph: &OperatorGraph = ctx.graph;
    let operator = &graph.operators[operator];
    let t_id: &str = &operator.id;
    let (handler, downstream) = handlers.split(operator.index)?;

    let scoped_side_inputs = ctx
        .side_input_reader
        .map(|reader| beam::internals::ScopedSideInputReader::new(reader.as_ref(), t_id));
    let side_inputs: Option<&dyn beam::internals::SideInputReader> = scoped_side_inputs
        .as_ref()
        .map(|s| s as &dyn beam::internals::SideInputReader);
    // Lend the bundle-scoped services to the call. These are copies of references that
    // outlive the bundle, not clones of the services.
    let ChainCtx {
        state_readers,
        timer_collector,
        residual_collector,
        metrics_container,
        bundle_finalizer,
        dynamic_split_registrar,
        state_stream_reader,
        sampler,
        ..
    } = *ctx;
    // Select this operator in the scope from `ChainCtx::enter_metrics_scope`, so metrics
    // calls below record against it. Downstream operators select themselves inside this
    // call, and this operator is selected again when they return.
    let _metrics_selection =
        metrics_container.map(|_| beam::metrics::MetricsScope::select(operator.index));

    let mut sink = ChainSink {
        ctx,
        downstream,
        routing: &operator.routing,
        header,
    };
    let mut h_ctx = beam::internals::HandlerContext::new(&mut sink)
        .with_side_inputs(side_inputs)
        .with_state_reader(state_readers.get(operator.index))
        .with_timer_collector(timer_collector)
        .with_residual_collector(residual_collector)
        .with_metrics_container(metrics_container)
        .with_bundle_finalizer(bundle_finalizer)
        .with_dynamic_split_registrar(dynamic_split_registrar)
        .with_state_stream_reader(state_stream_reader)
        .with_transform_id(t_id)
        .with_header(header)
        .with_key_bytes(key_bytes)
        .with_input_schema(input_schema);

    let previous = sampler.enter(operator.index);
    let result = invoke(handler, &mut h_ctx);
    sampler.exit(previous);
    result.map_err(BundleError::Coder)
}
