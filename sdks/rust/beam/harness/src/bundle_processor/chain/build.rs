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

use std::collections::HashMap;
use std::sync::Arc;

use beam::coders::{
    URN_INTERVAL_WINDOW, URN_LENGTH_PREFIX, URN_NULLABLE, URN_PARAM_WINDOWED_VALUE,
    URN_WINDOWED_VALUE, coder_urn, length_prefix_component, peel_length_prefixes,
};
use beam::internals::{HandlerInstance, TransformFn};
use model::fn_execution::{ProcessBundleDescriptor, RemoteGrpcPort};

use super::super::BundleError;
use super::super::handlers::resolve_handler;
use super::instances::Instances;

/// The targets of the outputs of a transform, as indices into [`OperatorGraph::pcollections`].
#[derive(Clone, Debug, Default)]
pub(super) struct OutputRouting {
    pub(super) by_tag: HashMap<String, PCollIdx>,
    pub(super) by_index: HashMap<usize, PCollIdx>,
    pub(super) default_pcoll: Option<PCollIdx>,
}

impl OutputRouting {
    fn from_transform(transform: &model::pipeline::PTransform, pcolls: &PCollectionIds) -> Self {
        let by_tag: HashMap<String, PCollIdx> = transform
            .outputs
            .iter()
            .filter_map(|(tag, pcoll)| Some((tag.clone(), pcolls.get(pcoll)?)))
            .collect();
        let default_pcoll = if by_tag.len() == 1 {
            by_tag.values().next().copied()
        } else {
            ["out", "main", "", "0"]
                .iter()
                .find_map(|&k| by_tag.get(k))
                .copied()
        };
        let by_index = transform
            .display_data
            .iter()
            .filter_map(|dd| beam::transforms::display_data::DisplayDataItem::from_proto(dd).ok())
            .filter_map(|item| {
                let idx = item
                    .key
                    .strip_prefix("output_tag_")?
                    .parse::<usize>()
                    .ok()?;
                let &pcoll = by_tag.get(&item.value)?;
                Some((idx, pcoll))
            })
            .collect();
        Self {
            by_tag,
            by_index,
            default_pcoll,
        }
    }

    pub(super) fn resolve(&self, tag: &str) -> Option<PCollIdx> {
        if tag.is_empty() {
            return self.default_pcoll;
        }
        self.by_tag.get(tag).copied().or_else(|| {
            let idx = tag.parse::<usize>().ok()?;
            self.by_index
                .get(&idx)
                .copied()
                .or_else(|| self.by_tag.get(&format!("part_{idx}")).copied())
                .or_else(|| self.by_tag.get(&format!("out_{idx}")).copied())
                .or_else(|| {
                    let mut sorted_tags: Vec<&str> =
                        self.by_tag.keys().map(String::as_str).collect();
                    sorted_tags.sort_unstable();
                    sorted_tags
                        .get(idx)
                        .and_then(|tag| self.by_tag.get(*tag))
                        .copied()
                })
        })
    }
}

#[derive(Clone, Debug)]
pub(super) struct SinkTarget {
    pub(super) id: String,
    /// The position of the sink in the bundle sinks. It is also the outbound buffer index.
    pub(super) index: usize,
    pub(super) expects_length_prefix: bool,
    pub(super) expects_windowed_value: bool,
    pub(super) nested_row_prefix_coder: Option<String>,
}

/// Returns true when the runner expects a `beam:coder:windowed_value:v1` header (timestamp,
/// windows, pane and metadata) on the elements of this sink.
fn sink_expects_windowed_value(
    sink_coder_id: &str,
    coders: &HashMap<String, model::pipeline::Coder>,
) -> bool {
    coders
        .get(sink_coder_id)
        .map(|coder| peel_length_prefixes(coder, coders))
        .is_some_and(|coder| coder_urn(coder) == URN_WINDOWED_VALUE)
}

/// Returns true when the window coder of a PCollection never writes zero bytes.
///
/// Only the interval window applies: its coder writes an 8-byte end timestamp and a VarInt
/// span. The global window coder writes zero bytes. Other window coders are left out, so the
/// check catches only provable mistakes. Find the window coder through the windowing
/// strategy: in the Fn API the PCollection `coder_id` names the *element* coder, and the
/// windowed value wrapper is only on the port coders at the edges of the bundle.
fn requires_window_bytes(
    pcoll: &model::pipeline::PCollection,
    windowing_strategies: &HashMap<String, model::pipeline::WindowingStrategy>,
    coders: &HashMap<String, model::pipeline::Coder>,
) -> bool {
    windowing_strategies
        .get(&pcoll.windowing_strategy_id)
        .and_then(|strategy| coders.get(&strategy.window_coder_id))
        // A runner can wrap a window coder that it cannot interpret in one length prefix.
        .map(|window_coder| length_prefix_component(window_coder, coders).unwrap_or(window_coder))
        .is_some_and(|window_coder| coder_urn(window_coder) == URN_INTERVAL_WINDOW)
}

/// Returns true when the runner expects a VarInt length prefix on the elements of this sink.
///
/// Runners wrap element coders that they cannot interpret (for example `RowCoder`) in
/// `beam:coder:length_prefix:v1`. Without the prefix, the runner reads the first element
/// bytes as a length and fails far from the cause. Only the windowed-value wrappers are
/// removed: the sink writes those headers itself. Returns
/// [`BundleError::UnsupportedSinkCoder`] for an element coder that the sink cannot frame.
fn sink_expects_length_prefix(
    sink_coder_id: &str,
    coders: &HashMap<String, model::pipeline::Coder>,
) -> Result<bool, BundleError> {
    let Some(coder) = coders.get(sink_coder_id) else {
        return Ok(false);
    };

    let element = match coder_urn(coder) {
        URN_WINDOWED_VALUE | URN_PARAM_WINDOWED_VALUE => coder
            .component_coder_ids
            .first()
            .and_then(|element_id| coders.get(element_id)),
        _ => Some(coder),
    };

    element.map_or(Ok(false), |element| element_framing(element, sink_coder_id))
}

/// Returns the element coder id of the sink when the runner length-prefixes nested Rows.
///
/// The SDK row encoding has no such prefixes. A runner that cannot interpret
/// `beam:coder:row:v1` prefixes it at every position, so it expects a `KV<varint, row>`
/// output as `KV<varint, length_prefix<row>>`.
fn sink_nested_row_prefix_coder(
    sink_coder_id: &str,
    coders: &HashMap<String, model::pipeline::Coder>,
) -> Option<String> {
    let coder = coders.get(sink_coder_id)?;
    let element_id = match coder_urn(coder) {
        URN_WINDOWED_VALUE | URN_PARAM_WINDOWED_VALUE => coder.component_coder_ids.first()?,
        _ => sink_coder_id,
    };
    // The sink writes a root length prefix itself, so look below it.
    let element_id = match coders.get(element_id) {
        Some(element) if coder_urn(element) == URN_LENGTH_PREFIX => {
            element.component_coder_ids.first()?.as_str()
        }
        _ => element_id,
    };
    beam::coders::has_nested_row_length_prefix(element_id, coders).then(|| element_id.to_string())
}

/// Returns true when the element coder of a sink needs a VarInt length prefix on the wire.
///
/// The sink writes either the element bytes or the element bytes after a VarInt length. It
/// rejects a coder that adds its own framing with [`BundleError::UnsupportedSinkCoder`].
/// `beam:coder:nullable:v1` writes a presence byte before its component: either answer
/// drops that byte and the runner misreads the stream far from the cause.
fn element_framing(
    element: &model::pipeline::Coder,
    sink_coder_id: &str,
) -> Result<bool, BundleError> {
    match coder_urn(element) {
        URN_LENGTH_PREFIX => Ok(true),
        URN_NULLABLE => Err(BundleError::UnsupportedSinkCoder(format!(
            "sink coder '{sink_coder_id}' frames elements with '{URN_NULLABLE}', which \
             prepends a presence byte the data sink cannot emit. Supported element coders \
             are '{URN_LENGTH_PREFIX}' and any coder written without extra framing."
        ))),
        _ => Ok(false),
    }
}

/// The position of a PCollection in [`OperatorGraph::pcollections`].
pub(super) type PCollIdx = usize;

/// One transform of the bundle, in the form that the per-element path uses.
pub(super) struct Operator {
    /// The transform id, used for failures and for user metrics recorded during its calls.
    pub(super) id: Arc<str>,
    pub(super) routing: OutputRouting,
    /// The position in [`OperatorGraph::operators`]. It is also the slot of the handler in
    /// the [`Instances`] of the processor and in the execution-time sampler.
    pub(super) index: usize,
    /// True when the operator reads the encoded key of the element, for user state or
    /// timers. Such an operator always gets bytes, because the key is sliced from them.
    pub(super) needs_key_bytes: bool,
}

/// The data, resolved once, that the per-element path needs to deliver on one PCollection.
pub(super) struct PCollectionRoute {
    pub(super) id: String,
    /// The consumer operators, as indices into [`OperatorGraph::operators`].
    pub(super) consumers: Vec<usize>,
    /// The data sinks that read the PCollection.
    pub(super) sinks: Vec<SinkTarget>,
    /// The single operator that receives the elements by value, if any. Set only with exactly
    /// one consumer operator, no data sink, and an operator that does not need key bytes.
    pub(super) typed_consumer: Option<usize>,
    /// The Row schema, for schema-aware transforms.
    pub(super) schema: Option<Arc<beam::schema::Schema>>,
    /// True when the elements must carry encoded window bytes (see [`requires_window_bytes`]).
    pub(super) needs_window_bytes: bool,
    /// The key coder of the elements, if they are key-value pairs.
    pub(super) key_coder_id: Option<String>,
}

/// Assigns every PCollection id a [`PCollIdx`], in first-seen order.
#[derive(Default)]
struct PCollectionIds {
    ids: Vec<String>,
    index: HashMap<String, PCollIdx>,
}

impl PCollectionIds {
    fn insert(mut self, id: &str) -> Self {
        if !self.index.contains_key(id) {
            self.index.insert(id.to_string(), self.ids.len());
            self.ids.push(id.to_string());
        }
        self
    }

    fn get(&self, id: &str) -> Option<PCollIdx> {
        self.index.get(id).copied()
    }
}

/// Returns true when a transform declares user state or timers and needs the element key.
fn declares_state_or_timers(transform: &model::pipeline::PTransform) -> bool {
    use prost::Message;
    transform
        .spec
        .as_ref()
        .filter(|spec| spec.urn == beam::pipeline::URN_PAR_DO)
        .and_then(|spec| model::pipeline::ParDoPayload::decode(spec.payload.as_slice()).ok())
        .is_some_and(|pardo| !pardo.state_specs.is_empty() || !pardo.timer_family_specs.is_empty())
}

/// The origin of the bundle elements: the PCollections that the data source feeds and the
/// port coder of the elements.
pub(in crate::bundle_processor) struct Source<'a> {
    pub(in crate::bundle_processor) pcollections: &'a [String],
    pub(in crate::bundle_processor) coder_id: &'a str,
}

/// The operators of one bundle processor and the routing between them.
///
/// Built once per processor and reused for every bundle. Each PCollection and operator has
/// an index, so the per-element path does no lookup by id.
pub(in crate::bundle_processor) struct OperatorGraph {
    /// Operators in topological order, indexed by [`Operator::index`].
    pub(super) operators: Vec<Operator>,
    /// Transform ids in topological order, indexed by [`Operator::index`].
    pub(in crate::bundle_processor) operator_ids: Vec<String>,
    /// Maps a transform id to an operator index, for callers that start from an id (timers).
    operator_index: HashMap<String, usize>,
    /// Transform ids by operator index, in the form that the metrics scope selects them.
    pub(super) transform_ids: Arc<[Arc<str>]>,
    /// Every PCollection of the bundle, indexed by [`PCollIdx`].
    pub(super) pcollections: Vec<PCollectionRoute>,
    /// The PCollections that the data source feeds.
    pub(super) sources: Vec<PCollIdx>,
}

impl OperatorGraph {
    /// Resolves the routing tables and gives each transform its own handler instance.
    ///
    /// Rejects a bad graph before the first element: [`BundleError::MissingHandler`] for a
    /// transform without a handler, [`BundleError::InvalidGraph`] when an operator feeds an
    /// earlier one, and [`BundleError::UnsupportedSinkCoder`] for a sink coder it cannot frame.
    pub(in crate::bundle_processor) fn build(
        descriptor: &ProcessBundleDescriptor,
        source: Source<'_>,
        ordered_transform_ids: &[String],
        sinks: &[(String, RemoteGrpcPort)],
        registry: &HashMap<String, TransformFn>,
    ) -> Result<(Self, Instances), BundleError> {
        // Each transform resolves to a handler or goes into the error report.
        let (resolved, unresolved): (Vec<_>, Vec<String>) = ordered_transform_ids
            .iter()
            .filter_map(|t_id| descriptor.transforms.get(t_id).map(|t| (t_id, t)))
            .fold(
                (Vec::new(), Vec::new()),
                |(mut resolved, mut unresolved), (t_id, transform)| {
                    match resolve_handler(registry, transform, descriptor) {
                        // The registry holds prototypes. This processor runs its own copy.
                        Some(prototype) => resolved.push((t_id, transform, prototype)),
                        None => unresolved.push(describe_unresolved(t_id, transform)),
                    }
                    (resolved, unresolved)
                },
            );

        if !unresolved.is_empty() {
            return Err(BundleError::MissingHandler(format!(
                "{count} of {total} transforms in descriptor '{descriptor_id}' have no registered \
                 handler: {unresolved}. Registered handler keys: {keys:?}. The worker rebuilt a \
                 different graph than the one submitted; check that the pipeline options the \
                 driver ran with were all forwarded to the worker.",
                count = unresolved.len(),
                total = ordered_transform_ids.len(),
                descriptor_id = descriptor.id,
                unresolved = unresolved.join(", "),
                keys = registry.keys().collect::<Vec<_>>(),
            )));
        }

        // Every PCollection of the bundle gets an index. The source PCollections come first,
        // then the descriptor PCollections, then undeclared PCollections that transforms name.
        let pcoll_ids = source
            .pcollections
            .iter()
            .map(String::as_str)
            .chain(descriptor.pcollections.keys().map(String::as_str))
            .chain(
                descriptor
                    .transforms
                    .values()
                    .flat_map(|t| t.inputs.values().chain(t.outputs.values()))
                    .map(String::as_str),
            )
            .fold(PCollectionIds::default(), PCollectionIds::insert);

        let operators: Vec<Operator> = resolved
            .iter()
            .enumerate()
            .map(|(index, (t_id, transform, _))| Operator {
                id: t_id.as_str().into(),
                routing: OutputRouting::from_transform(transform, &pcoll_ids),
                index,
                needs_key_bytes: declares_state_or_timers(transform),
            })
            .collect();

        // Side inputs come on demand through the state channel. Do not make their transform
        // a consumer of the element stream.
        let mut consumers = group_by_pcoll(
            &pcoll_ids,
            resolved
                .iter()
                .enumerate()
                .flat_map(|(index, (_, transform, _))| {
                    let side_tags = beam::internals::extract_side_input_tags(transform);
                    transform
                        .inputs
                        .iter()
                        .filter(move |(tag, _)| !side_tags.contains(*tag))
                        .map(move |(_, in_pcol)| (in_pcol.as_str(), index))
                }),
        );

        // Resolve framing here so that an unsupported sink coder fails the graph, as a
        // missing handler does. It must not corrupt the first element written.
        let framed_sinks = sinks
            .iter()
            .enumerate()
            .filter_map(|(index, (sink_id, port))| {
                let sink_t = descriptor.transforms.get(sink_id)?;
                Some((index, sink_id, sink_t, &port.coder_id))
            })
            .map(|(index, sink_id, sink_t, coder_id)| {
                let target = SinkTarget {
                    id: sink_id.clone(),
                    index,
                    expects_length_prefix: sink_expects_length_prefix(
                        coder_id,
                        &descriptor.coders,
                    )?,
                    expects_windowed_value: sink_expects_windowed_value(
                        coder_id,
                        &descriptor.coders,
                    ),
                    nested_row_prefix_coder: sink_nested_row_prefix_coder(
                        coder_id,
                        &descriptor.coders,
                    ),
                };
                Ok((sink_t, target))
            })
            .collect::<Result<Vec<_>, BundleError>>()?;
        let mut sink_targets = group_by_pcoll(
            &pcoll_ids,
            framed_sinks.iter().flat_map(|(sink_t, target)| {
                sink_t
                    .inputs
                    .values()
                    .map(move |in_pcol| (in_pcol.as_str(), target.clone()))
            }),
        );

        let pcollections = pcoll_ids
            .ids
            .iter()
            .enumerate()
            .map(|(idx, id)| {
                let declared = descriptor.pcollections.get(id);
                // In the Fn API, the coder of a PCollection is its element coder. An
                // undeclared PCollection uses the coder of the source port.
                let coder_id = declared.map_or(source.coder_id, |p| p.coder_id.as_str());
                let consumers = std::mem::take(&mut consumers[idx]);
                let sinks = std::mem::take(&mut sink_targets[idx]);
                let typed_consumer = match consumers.as_slice() {
                    [only] if sinks.is_empty() && !operators[*only].needs_key_bytes => Some(*only),
                    _ => None,
                };
                PCollectionRoute {
                    id: id.clone(),
                    consumers,
                    sinks,
                    typed_consumer,
                    schema: declared.and_then(|p| {
                        beam::coders::extract_row_schema(&p.coder_id, &descriptor.coders)
                    }),
                    needs_window_bytes: declared.is_some_and(|p| {
                        requires_window_bytes(
                            p,
                            &descriptor.windowing_strategies,
                            &descriptor.coders,
                        )
                    }),
                    key_coder_id: beam::coders::kv_key_coder_id(coder_id, &descriptor.coders)
                        .map(str::to_string),
                }
            })
            .collect();

        let handlers: Vec<HandlerInstance> = resolved
            .iter()
            .map(|(_, _, prototype)| prototype.instantiate())
            .collect();

        let graph = Self {
            transform_ids: operators.iter().map(|op| Arc::clone(&op.id)).collect(),
            operator_ids: resolved
                .iter()
                .map(|(t_id, _, _)| (*t_id).clone())
                .collect(),
            operator_index: resolved
                .iter()
                .enumerate()
                .map(|(index, (t_id, _, _))| ((*t_id).clone(), index))
                .collect(),
            operators,
            pcollections,
            sources: source
                .pcollections
                .iter()
                .filter_map(|id| pcoll_ids.get(id))
                .collect(),
        };
        graph.check_feeds_forward()?;
        Ok((graph, Instances::new(handlers)))
    }

    /// Returns the operator index of transform `t_id`, if it is part of this graph.
    pub(in crate::bundle_processor) fn operator_index(&self, t_id: &str) -> Option<usize> {
        self.operator_index.get(t_id).copied()
    }

    /// Checks that every operator feeds only later operators: a call lends its outputs only
    /// the handlers after it (see [`Instances`]), so the chain cannot follow a backward edge.
    fn check_feeds_forward(&self) -> Result<(), BundleError> {
        let backwards = self.operators.iter().find_map(|operator| {
            operator
                .routing
                .by_tag
                .values()
                .flat_map(|&pcoll| &self.pcollections[pcoll].consumers)
                .find(|&&consumer| consumer <= operator.index)
                .map(|&consumer| (&operator.id, &self.operators[consumer].id))
        });
        match backwards {
            None => Ok(()),
            Some((producer, consumer)) => Err(BundleError::InvalidGraph(format!(
                "transform '{producer}' feeds '{consumer}', which is not after it in \
                 topological order; the bundle's transforms contain a cycle"
            ))),
        }
    }
}

/// Formats a transform without a handler for the error message.
fn describe_unresolved(t_id: &str, transform: &model::pipeline::PTransform) -> String {
    let urn = transform.spec.as_ref().map_or("none", |s| s.urn.as_str());
    format!("'{}' (id='{t_id}', urn='{urn}')", transform.unique_name)
}

/// Groups `(pcollection id, value)` pairs into lists indexed by [`PCollIdx`], in input order.
/// Pairs that name an unknown PCollection are dropped.
fn group_by_pcoll<'p, V>(
    pcolls: &PCollectionIds,
    pairs: impl IntoIterator<Item = (&'p str, V)>,
) -> Vec<Vec<V>> {
    pairs.into_iter().fold(
        std::iter::repeat_with(Vec::new)
            .take(pcolls.ids.len())
            .collect(),
        |mut acc: Vec<Vec<V>>, (pcoll, val)| {
            if let Some(idx) = pcolls.get(pcoll) {
                acc[idx].push(val);
            }
            acc
        },
    )
}
