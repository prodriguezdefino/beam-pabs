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

//! Shared `ProcessBundleDescriptor` fixtures for the harness integration tests.

#![allow(
    dead_code,
    reason = "shared by several test binaries, each using a different subset"
)]

pub mod state_mock;

use std::collections::HashMap;

use prost::Message;

use beam::coders::{
    PaneInfo, URN_BYTES, URN_GLOBAL_WINDOW, URN_STRING_UTF8, URN_WINDOWED_VALUE, WindowedHeader,
};
use beam::internals::{BundleHandler, ElementSink, HandlerContext, HandlerInstance};
use harness::bundle_processor::{TransformFn, URN_DATA_SINK, URN_DATA_SOURCE};
use model::fn_execution::{ProcessBundleDescriptor, RemoteGrpcPort};
use model::pipeline as proto_pipeline;

/// How long a test waits for an event that should arrive promptly. Correct code takes
/// milliseconds; the deadline makes a lost input fail the test, not hang it.
pub const WAIT: std::time::Duration = std::time::Duration::from_secs(5);

/// Awaits `fut`, panicking with `what` if it does not complete within [`WAIT`].
pub async fn within<F: IntoFuture>(what: &str, fut: F) -> F::Output {
    tokio::time::timeout(WAIT, fut)
        .await
        .unwrap_or_else(|_| panic!("timed out after {WAIT:?} waiting for {what}"))
}

/// A one-shot latch that threads block on until another thread opens it. Waits are bounded
/// by [`WAIT`], so a latch that never opens fails instead of hanging.
#[derive(Default)]
pub struct Gate {
    opened: std::sync::Mutex<bool>,
    changed: std::sync::Condvar,
}

impl Gate {
    pub fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::default()
    }

    /// Opens the gate, releasing every current and future waiter. Idempotent.
    pub fn open(&self) {
        *self.opened.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.changed.notify_all();
    }

    pub fn is_open(&self) -> bool {
        *self.opened.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Blocks until the gate opens; panics if it is still closed after [`WAIT`].
    pub fn wait(&self, what: &str) {
        let guard = self.opened.lock().unwrap_or_else(|e| e.into_inner());
        let (guard, _) = self
            .changed
            .wait_timeout_while(guard, WAIT, |opened| !*opened)
            .unwrap_or_else(|e| e.into_inner());
        assert!(*guard, "timed out after {WAIT:?} waiting for {what}");
    }

    /// A guard that opens this gate when dropped, even during a panic, so a failing
    /// assertion cannot leave a blocked handler parked.
    pub fn open_on_drop(self: &std::sync::Arc<Self>) -> OpenOnDrop {
        OpenOnDrop(std::sync::Arc::clone(self))
    }
}

/// Opens its [`Gate`] on drop. See [`Gate::open_on_drop`].
pub struct OpenOnDrop(std::sync::Arc<Gate>);

impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.0.open();
    }
}

/// URN the harness dispatches on to recognise a user transform.
pub const URN_PARDO: &str = "beam:transform:pardo:v1";

/// Coder id for `WindowedValue<String, GlobalWindow>`, defined by [`windowed_string_coders`].
pub const CODER_WINDOWED_STRING: &str = "coder_windowed_string";
/// Coder id for opaque bytes, defined by [`bytes_coder`].
pub const CODER_RAW: &str = "coder_raw";

/// Default transform ids, so tests and fixtures cannot disagree about the wiring.
pub const SOURCE_ID: &str = "source_transform";
pub const STAGE_ID: &str = "transform";
pub const SINK_ID: &str = "sink_transform";

fn function_spec(urn: &str, payload: Vec<u8>) -> Option<proto_pipeline::FunctionSpec> {
    Some(proto_pipeline::FunctionSpec {
        urn: urn.to_string(),
        payload,
    })
}

/// A `ParDo` spec whose `do_fn` carries `key`, the handler key the harness resolves.
pub fn pardo_spec(key: &str) -> Option<proto_pipeline::FunctionSpec> {
    let payload = proto_pipeline::ParDoPayload {
        do_fn: function_spec("beam:dofn:rust:v1", key.as_bytes().to_vec()),
        ..Default::default()
    }
    .encode_to_vec();
    function_spec(URN_PARDO, payload)
}

/// As [`pardo_spec`], but the payload also declares the user states `state_ids`.
pub fn stateful_pardo_spec(
    key: &str,
    state_ids: &[String],
) -> Option<proto_pipeline::FunctionSpec> {
    let payload = proto_pipeline::ParDoPayload {
        do_fn: function_spec("beam:dofn:rust:v1", key.as_bytes().to_vec()),
        state_specs: state_ids
            .iter()
            .map(|id| (id.clone(), proto_pipeline::StateSpec::default()))
            .collect(),
        ..Default::default()
    }
    .encode_to_vec();
    function_spec(URN_PARDO, payload)
}

fn coder(urn: &str, components: &[&str]) -> proto_pipeline::Coder {
    proto_pipeline::Coder {
        spec: function_spec(urn, Vec::new()),
        component_coder_ids: components.iter().map(|c| c.to_string()).collect(),
    }
}

fn transform(
    unique_name: &str,
    spec: Option<proto_pipeline::FunctionSpec>,
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
) -> proto_pipeline::PTransform {
    proto_pipeline::PTransform {
        unique_name: unique_name.to_string(),
        spec,
        subtransforms: Vec::new(),
        inputs,
        outputs,
        display_data: Vec::new(),
        environment_id: "env_default".to_string(),
        annotations: HashMap::new(),
    }
}

fn port(name: &str, coder_id: &str) -> HashMap<String, String> {
    HashMap::from([(name.to_string(), coder_id.to_string())])
}

/// Serialized `RemoteGrpcPort` payload for a DATA_SOURCE or DATA_SINK spec.
fn port_payload(coder_id: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    RemoteGrpcPort {
        api_service_descriptor: None,
        coder_id: coder_id.to_string(),
    }
    .encode(&mut payload)
    .expect("encoding a RemoteGrpcPort into a fresh Vec cannot fail");
    payload
}

/// The three coders making up `WindowedValue<String, GlobalWindow>`.
pub fn windowed_string_coders() -> HashMap<String, proto_pipeline::Coder> {
    HashMap::from([
        (
            CODER_WINDOWED_STRING.to_string(),
            coder(URN_WINDOWED_VALUE, &["coder_string", "coder_global_window"]),
        ),
        ("coder_string".to_string(), coder(URN_STRING_UTF8, &[])),
        (
            "coder_global_window".to_string(),
            coder(URN_GLOBAL_WINDOW, &[]),
        ),
    ])
}

/// A single opaque-bytes coder. Read in the outer context, so one inbound chunk decodes
/// as exactly one element.
pub fn bytes_coder() -> HashMap<String, proto_pipeline::Coder> {
    HashMap::from([(CODER_RAW.to_string(), coder(URN_BYTES, &[]))])
}

struct Stage {
    id: String,
    input: String,
    /// Output tag -> PCollection id.
    outputs: Vec<(String, String)>,
    /// User state ids its `ParDoPayload` declares; none for a stateless stage.
    state_ids: Vec<String>,
}

struct Sink {
    id: String,
    input: String,
    coder_id: String,
}

/// Builds `ProcessBundleDescriptor`s for harness tests. Starts as a DATA_SOURCE alone;
/// [`Self::stage`] and [`Self::sink`] extend it. Default: `WindowedValue<String, GlobalWindow>`.
pub struct DescriptorBuilder {
    id: String,
    source_coder_id: String,
    source_output: String,
    stages: Vec<Stage>,
    sinks: Vec<Sink>,
    coders: HashMap<String, proto_pipeline::Coder>,
    pcollections: HashMap<String, proto_pipeline::PCollection>,
    windowing_strategies: HashMap<String, proto_pipeline::WindowingStrategy>,
}

impl DescriptorBuilder {
    pub fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            source_coder_id: CODER_WINDOWED_STRING.to_string(),
            source_output: "pcoll_input".to_string(),
            stages: Vec::new(),
            sinks: Vec::new(),
            coders: windowed_string_coders(),
            pcollections: HashMap::new(),
            windowing_strategies: HashMap::new(),
        }
    }

    /// Replaces the coder set and points the source port at `source_coder_id`.
    pub fn with_coders(
        self,
        source_coder_id: &str,
        coders: HashMap<String, proto_pipeline::Coder>,
    ) -> Self {
        Self {
            source_coder_id: source_coder_id.to_string(),
            coders,
            ..self
        }
    }

    /// Adds one coder to the existing set, e.g. a sink port coder the source does not share.
    pub fn with_coder(mut self, id: &str, urn: &str, components: &[&str]) -> Self {
        self.coders.insert(id.to_string(), coder(urn, components));
        self
    }

    /// Points the source port at `coder_id`, keeping the coder set as it is.
    pub fn with_source_coder(self, coder_id: &str) -> Self {
        Self {
            source_coder_id: coder_id.to_string(),
            ..self
        }
    }

    /// Appends a user transform reading `input` and writing `output`.
    pub fn stage(self, id: &str, input: &str, output: &str) -> Self {
        self.stage_with_outputs(id, input, &[("out", output)])
    }

    /// Appends a user transform reading `input` and writing each `(tag, pcollection)`.
    pub fn stage_with_outputs(mut self, id: &str, input: &str, outputs: &[(&str, &str)]) -> Self {
        self.stages.push(Stage {
            id: id.to_string(),
            input: input.to_string(),
            outputs: outputs
                .iter()
                .map(|(tag, pcoll)| (tag.to_string(), pcoll.to_string()))
                .collect(),
            state_ids: Vec::new(),
        });
        self
    }

    /// Appends a user transform whose `ParDoPayload` declares the user states
    /// `state_ids`, so the harness treats it as keyed.
    pub fn stateful_stage(
        mut self,
        id: &str,
        input: &str,
        output: &str,
        state_ids: &[&str],
    ) -> Self {
        self.stages.push(Stage {
            id: id.to_string(),
            input: input.to_string(),
            outputs: vec![("out".to_string(), output.to_string())],
            state_ids: state_ids.iter().map(|s| s.to_string()).collect(),
        });
        self
    }

    /// Declares PCollection `id` with element coder `coder_id`, windowed by the
    /// strategy `windowing_strategy_id` (empty for none).
    pub fn pcollection(mut self, id: &str, coder_id: &str, windowing_strategy_id: &str) -> Self {
        self.pcollections.insert(
            id.to_string(),
            proto_pipeline::PCollection {
                unique_name: id.to_string(),
                coder_id: coder_id.to_string(),
                windowing_strategy_id: windowing_strategy_id.to_string(),
                ..Default::default()
            },
        );
        self
    }

    /// Declares windowing strategy `id`, whose windows are encoded by `window_coder_id`.
    pub fn windowing_strategy(mut self, id: &str, window_coder_id: &str) -> Self {
        self.windowing_strategies.insert(
            id.to_string(),
            proto_pipeline::WindowingStrategy {
                window_coder_id: window_coder_id.to_string(),
                ..Default::default()
            },
        );
        self
    }

    /// Appends a DATA_SINK reading `input`, using the source's port coder.
    pub fn sink(mut self, id: &str, input: &str) -> Self {
        let coder_id = self.source_coder_id.clone();
        self.sinks.push(Sink {
            id: id.to_string(),
            input: input.to_string(),
            coder_id,
        });
        self
    }

    /// Appends a DATA_SINK reading `input` with a port coder of its own.
    pub fn sink_with_coder(mut self, id: &str, input: &str, coder_id: &str) -> Self {
        self.sinks.push(Sink {
            id: id.to_string(),
            input: input.to_string(),
            coder_id: coder_id.to_string(),
        });
        self
    }

    fn assemble(self) -> ProcessBundleDescriptor {
        let source = (
            SOURCE_ID.to_string(),
            transform(
                "Source",
                function_spec(URN_DATA_SOURCE, port_payload(&self.source_coder_id)),
                HashMap::new(),
                port("out", &self.source_output),
            ),
        );

        let stages = self.stages.into_iter().map(|stage| {
            let name = stage.id.clone();
            let spec = if stage.state_ids.is_empty() {
                pardo_spec(&name)
            } else {
                stateful_pardo_spec(&name, &stage.state_ids)
            };
            (
                stage.id,
                transform(
                    &name,
                    spec,
                    port("in", &stage.input),
                    stage.outputs.into_iter().collect(),
                ),
            )
        });

        let sinks = self.sinks.into_iter().map(|sink| {
            let name = sink.id.clone();
            (
                sink.id,
                transform(
                    &name,
                    function_spec(URN_DATA_SINK, port_payload(&sink.coder_id)),
                    port("in", &sink.input),
                    HashMap::new(),
                ),
            )
        });

        ProcessBundleDescriptor {
            id: self.id,
            transforms: std::iter::once(source).chain(stages).chain(sinks).collect(),
            pcollections: self.pcollections,
            windowing_strategies: self.windowing_strategies,
            coders: self.coders,
            environments: HashMap::new(),
            state_api_service_descriptor: None,
            timer_api_service_descriptor: None,
        }
    }

    /// Builds a descriptor that exercises the operator graph. Panics without a stage or a sink,
    /// or if a port references an undefined coder.
    pub fn build(self) -> ProcessBundleDescriptor {
        assert!(
            !self.stages.is_empty(),
            "descriptor '{}' has no intermediate transform: the bundle processor would take \
             its raw-forwarding path and invoke no handler. Add a stage, or call \
             build_raw_forwarding().",
            self.id
        );
        assert!(
            !self.sinks.is_empty(),
            "descriptor '{}' has no sink",
            self.id
        );
        self.checked()
    }

    /// Builds a source-to-sink descriptor exercising the raw-forwarding path.
    pub fn build_raw_forwarding(self) -> ProcessBundleDescriptor {
        assert!(
            self.stages.is_empty(),
            "descriptor '{}' has an intermediate transform; call build() instead",
            self.id
        );
        self.checked()
    }

    fn checked(self) -> ProcessBundleDescriptor {
        let referenced: Vec<String> = std::iter::once(self.source_coder_id.clone())
            .chain(self.sinks.iter().map(|s| s.coder_id.clone()))
            .collect();
        let id = self.id.clone();
        let descriptor = self.assemble();

        referenced.iter().for_each(|coder_id| {
            assert!(
                descriptor.coders.contains_key(coder_id),
                "descriptor '{id}' references coder '{coder_id}' but does not define it"
            );
        });
        descriptor
    }
}

/// A source -> `STAGE_ID` -> sink descriptor over opaque bytes: one chunk in, one element
/// out. Pair with [`identity_handlers`] unless the test registers its own [`STAGE_ID`] handler.
pub fn linear_descriptor(id: &str) -> ProcessBundleDescriptor {
    DescriptorBuilder::new(id)
        .with_coders(CODER_RAW, bytes_coder())
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build()
}

/// As [`linear_descriptor`], but over `WindowedValue<String, GlobalWindow>`.
pub fn windowed_linear_descriptor(id: &str) -> ProcessBundleDescriptor {
    DescriptorBuilder::new(id)
        .stage(STAGE_ID, "pcoll_input", "pcoll_output")
        .sink(SINK_ID, "pcoll_output")
        .build()
}

/// Handlers that forward each element unchanged, one per named transform.
pub fn identity_handlers(ids: &[&str]) -> HashMap<String, TransformFn> {
    ids.iter()
        .map(|id| {
            let handler: TransformFn =
                std::sync::Arc::new(|element: &[u8], sink: &mut dyn ElementSink| {
                    sink.push(element.to_vec())
                });
            (id.to_string(), handler)
        })
        .collect()
}

/// One call into an [`Observer`]: what the harness handed the operator.
#[derive(Clone, Debug, PartialEq)]
pub struct Observed {
    /// The element bytes, after any inbound reframing.
    pub element: Vec<u8>,
    /// The windowed-value header the element was delivered under.
    pub header: WindowedHeader,
    /// The encoded key the harness sliced out of the element, if any.
    pub key_bytes: Option<Vec<u8>>,
}

impl Observed {
    pub fn pane(&self) -> PaneInfo {
        self.header.pane()
    }
}

/// A terminal handler that records every element and emits nothing. Unlike a closure
/// handler, it sees the whole `HandlerContext`, including header and key.
#[derive(Clone, Default)]
pub struct Observer {
    seen: std::sync::Arc<std::sync::Mutex<Vec<Observed>>>,
}

impl Observer {
    /// Everything recorded so far, in delivery order.
    pub fn seen(&self) -> Vec<Observed> {
        self.seen.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// This observer as a registrable handler; copies share one record.
    pub fn handler(&self) -> TransformFn {
        std::sync::Arc::new(self.clone())
    }
}

impl BundleHandler for Observer {
    fn process(&mut self, element: &[u8], ctx: &mut HandlerContext<'_>) -> Result<(), String> {
        self.seen
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Observed {
                element: element.to_vec(),
                header: ctx.header.clone(),
                key_bytes: ctx.key_bytes.as_deref().map(<[u8]>::to_vec),
            });
        Ok(())
    }

    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }
}

/// The outcome of [`run_bundle`].
pub struct BundleRun {
    /// The ProcessBundle instruction's response.
    pub response: model::fn_execution::InstructionResponse,
    /// Every byte each sink wrote, keyed by sink transform id.
    pub sink_bytes: HashMap<String, Vec<u8>>,
}

impl BundleRun {
    /// The successful bundle response; panics if the bundle failed.
    pub fn bundle_response(&self) -> &model::fn_execution::ProcessBundleResponse {
        assert!(
            self.response.error.is_empty(),
            "bundle failed: {}",
            self.response.error
        );
        match &self.response.response {
            Some(model::fn_execution::instruction_response::Response::ProcessBundle(resp)) => resp,
            other => panic!("expected a ProcessBundle response, got {other:?}"),
        }
    }
}

/// Registers `descriptor`, then runs one bundle whose source receives `chunks` and end of
/// stream. Every wait is bounded by [`WAIT`].
pub async fn run_bundle(
    handlers: HashMap<String, TransformFn>,
    descriptor: ProcessBundleDescriptor,
    chunks: Vec<Vec<u8>>,
) -> BundleRun {
    use model::fn_execution::{
        Elements, InstructionRequest, ProcessBundleRequest, RegisterRequest, elements,
        instruction_request::Request,
    };

    let (data_out_tx, mut data_out_rx) = tokio::sync::mpsc::channel::<Elements>(256);
    let data_manager = harness::data::DataManager::new(data_out_tx);
    let control = harness::control::ControlClient::new(std::sync::Arc::new(
        harness::bundle_processor::BundleProcessor::with_handlers(data_manager.clone(), handlers),
    ));

    let descriptor_id = descriptor.id.clone();
    let instruction_id = format!("bundle_{descriptor_id}");
    let registered = within(
        "registration",
        control.handle_instruction(InstructionRequest {
            instruction_id: format!("register_{descriptor_id}"),
            request: Some(Request::Register(RegisterRequest {
                process_bundle_descriptor: vec![descriptor],
            })),
        }),
    )
    .await;
    assert!(
        registered.error.is_empty(),
        "registration failed: {}",
        registered.error
    );

    tokio::spawn({
        let data_manager = data_manager.clone();
        let instruction_id = instruction_id.clone();
        async move {
            let data = |data: Vec<u8>, is_last: bool| elements::Data {
                instruction_id: instruction_id.clone(),
                transform_id: SOURCE_ID.to_string(),
                data,
                is_last,
            };
            let mut data_chunks: Vec<elements::Data> =
                chunks.into_iter().map(|chunk| data(chunk, false)).collect();
            data_chunks.push(data(Vec::new(), true));
            for chunk in data_chunks {
                data_manager
                    .handle_inbound_elements(Elements {
                        data: vec![chunk],
                        timers: Vec::new(),
                    })
                    .await;
            }
        }
    });

    let response = within(
        "the bundle response",
        control.handle_instruction(InstructionRequest {
            instruction_id,
            request: Some(Request::ProcessBundle(ProcessBundleRequest {
                process_bundle_descriptor_id: descriptor_id,
                ..Default::default()
            })),
        }),
    )
    .await;

    let mut sink_bytes: HashMap<String, Vec<u8>> = HashMap::new();
    while let Ok(elements) = data_out_rx.try_recv() {
        elements.data.into_iter().for_each(|d| {
            sink_bytes.entry(d.transform_id).or_default().extend(d.data);
        });
    }
    BundleRun {
        response,
        sink_bytes,
    }
}

/// Decodes the VarInts of a monitoring info payload.
fn payload_varints(payload: &[u8]) -> Vec<i64> {
    let mut cursor = std::io::Cursor::new(payload);
    std::iter::from_fn(|| {
        ((cursor.position() as usize) < payload.len()).then(|| {
            beam::coders::VarIntCoder::decode_varint(&mut cursor)
                .expect("monitoring info payload holds VarInts")
        })
    })
    .collect()
}

/// The `element_count` of every PCollection the infos report, keyed by PCollection id.
pub fn element_counts(infos: &[proto_pipeline::MonitoringInfo]) -> HashMap<String, i64> {
    infos
        .iter()
        .filter(|info| info.urn == beam::metrics::URN_ELEMENT_COUNT)
        .filter_map(|info| {
            let pcoll = info.labels.get(beam::metrics::LABEL_PCOLLECTION)?;
            Some((pcoll.clone(), *payload_varints(&info.payload).first()?))
        })
        .collect()
}

/// A `sampled_byte_size` distribution, as reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SampledSizes {
    pub count: i64,
    pub sum: i64,
    pub min: i64,
    pub max: i64,
}

/// The `sampled_byte_size` of every PCollection the infos report, keyed by PCollection id.
pub fn sampled_byte_sizes(
    infos: &[proto_pipeline::MonitoringInfo],
) -> HashMap<String, SampledSizes> {
    infos
        .iter()
        .filter(|info| info.urn == beam::metrics::URN_SAMPLED_BYTE_SIZE)
        .filter_map(|info| {
            let pcoll = info.labels.get(beam::metrics::LABEL_PCOLLECTION)?;
            let [count, sum, min, max] = payload_varints(&info.payload)[..] else {
                panic!("sampled_byte_size payload for '{pcoll}' is not four VarInts");
            };
            Some((
                pcoll.clone(),
                SampledSizes {
                    count,
                    sum,
                    min,
                    max,
                },
            ))
        })
        .collect()
}
