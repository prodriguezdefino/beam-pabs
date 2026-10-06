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

//! [`CoGroupByKey`] groups two or more [`PCollection`]s that share a key type `K`. Inner and
//! outer joins are built on it.

use std::collections::HashMap;
use std::hash::Hash;
use std::io::Write;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::{
    ClosureFn, DisplayDataBuilder, Flatten, GroupByKey, HasDisplayData, PTransform, ParDo,
    TryPTransform,
};
use crate::coders::{
    BeamIterable, BytesCoder, Coder, CoderError, CoderRegistry, Context, DefaultCoder, URN_KV,
};
use crate::pipeline::{Pipeline, PipelineError};
use crate::values::{IsBounded, PCollection, PInput};

/// An element from one joined input, tagged with its input index.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawUnionValue {
    /// Index of the input collection.
    pub tag: i32,
    /// Encoded input element.
    pub value: Vec<u8>,
}

/// Coder for [`RawUnionValue`]; the wire format is a standard `KvCoder<i32, Vec<u8>>`.
#[derive(Clone, Debug)]
pub struct RawUnionValueCoder;

impl Coder<RawUnionValue> for RawUnionValueCoder {
    fn urn(&self) -> &'static str {
        URN_KV
    }

    fn encode(
        &self,
        value: &RawUnionValue,
        writer: &mut Vec<u8>,
        context: Context,
    ) -> Result<(), CoderError> {
        value.tag.encode_element(writer)?;
        BytesCoder.encode(&value.value, writer, context)
    }

    fn decode(&self, reader: &mut &[u8], context: Context) -> Result<RawUnionValue, CoderError> {
        Ok(RawUnionValue {
            tag: i32::decode_element(reader)?,
            value: BytesCoder.decode(reader, context)?,
        })
    }
}

impl DefaultCoder for RawUnionValue {
    type Coder = RawUnionValueCoder;

    fn coder() -> Self::Coder {
        RawUnionValueCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        RawUnionValueCoder.encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        RawUnionValueCoder.decode(reader, Context::Nested)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        <(i32, Vec<u8>)>::register_coder(registry)
    }
}

/// Result of a [`CoGroupByKey`] for one key: the grouped values of each input, by tag or index.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "CoGbkResultSerde", into = "CoGbkResultSerde")]
pub struct CoGbkResult {
    tag_names: Arc<[String]>,
    values_by_tag: Vec<Vec<Vec<u8>>>,
}

#[derive(Serialize, Deserialize)]
struct CoGbkResultSerde {
    tag_names: Vec<String>,
    values_by_tag: Vec<Vec<Vec<u8>>>,
}

impl From<CoGbkResultSerde> for CoGbkResult {
    fn from(s: CoGbkResultSerde) -> Self {
        Self {
            tag_names: s.tag_names.into(),
            values_by_tag: s.values_by_tag,
        }
    }
}

impl From<CoGbkResult> for CoGbkResultSerde {
    fn from(r: CoGbkResult) -> Self {
        Self {
            tag_names: r.tag_names.to_vec(),
            values_by_tag: r.values_by_tag,
        }
    }
}

/// Coder for [`CoGbkResult`].
#[derive(Clone, Debug)]
pub struct CoGbkResultCoder;

impl Coder<CoGbkResult> for CoGbkResultCoder {
    fn urn(&self) -> &'static str {
        URN_KV
    }

    fn encode(
        &self,
        value: &CoGbkResult,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        value.encode_element(writer)
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<CoGbkResult, CoderError> {
        CoGbkResult::decode_element(reader)
    }
}

fn write_count(len: usize, writer: &mut Vec<u8>) -> Result<(), CoderError> {
    let count = i32::try_from(len)
        .map_err(|_| CoderError::Format(format!("length {len} exceeds the i32 wire limit")))?;
    Ok(writer.write_all(&count.to_be_bytes())?)
}

impl DefaultCoder for CoGbkResult {
    type Coder = CoGbkResultCoder;

    fn coder() -> Self::Coder {
        CoGbkResultCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        write_count(self.tag_names.len(), writer)?;
        self.tag_names
            .iter()
            .try_for_each(|name| name.encode_element(writer))?;
        let non_empty_count = self.values_by_tag.iter().filter(|v| !v.is_empty()).count();
        write_count(non_empty_count, writer)?;
        self.values_by_tag
            .iter()
            .enumerate()
            .filter(|(_, values)| !values.is_empty())
            .try_for_each(|(idx, values)| {
                let tag_idx = i32::try_from(idx).map_err(|_| {
                    CoderError::Format(format!("tag index {idx} exceeds i32 limit"))
                })?;
                tag_idx.encode_element(writer)?;
                values.encode_element(writer)
            })
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let tag_names: Arc<[String]> = Vec::<String>::decode_element(reader)?.into();
        let entries = Vec::<(i32, Vec<Vec<u8>>)>::decode_element(reader)?;
        let mut values_by_tag = vec![Vec::new(); tag_names.len()];
        entries.into_iter().for_each(|(idx, values)| {
            if let Ok(u_idx) = usize::try_from(idx) {
                if u_idx >= values_by_tag.len() {
                    values_by_tag.resize(u_idx + 1, Vec::new());
                }
                values_by_tag[u_idx] = values;
            }
        });
        Ok(Self {
            tag_names,
            values_by_tag,
        })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        <(Vec<String>, Vec<(i32, Vec<Vec<u8>>)>)>::register_coder(registry)
    }
}

impl CoGbkResult {
    /// Creates a `CoGbkResult` from tag names and grouped values.
    #[doc(hidden)]
    pub fn new(tag_names: impl Into<Arc<[String]>>, values_by_tag: Vec<Vec<Vec<u8>>>) -> Self {
        Self {
            tag_names: tag_names.into(),
            values_by_tag,
        }
    }

    /// Tag names in index order.
    pub fn tag_names(&self) -> &[String] {
        &self.tag_names
    }

    /// Decodes all elements of `tag` for this key; empty if there are none. Returns an error if
    /// `tag` is unknown or a value does not decode.
    pub fn get<V: DefaultCoder>(&self, tag: &str) -> crate::Result<BeamIterable<V>> {
        let tag_index = self
            .tag_names
            .iter()
            .position(|t| t == tag)
            .ok_or_else(|| {
                format!(
                    "Tag '{tag}' not found in CoGbkResult schema: {:?}",
                    self.tag_names
                )
            })? as i32;
        self.get_by_index(tag_index)
    }

    /// Like [`Self::get`], but collects into a `Vec<V>`.
    pub fn get_vec<V: DefaultCoder>(&self, tag: &str) -> crate::Result<Vec<V>> {
        self.get::<V>(tag).and_then(|it| Ok(it.into_vec()?))
    }

    /// Decodes all elements of the input at `index`; empty for an unknown index. Returns an
    /// error if a value does not decode.
    pub fn get_by_index<V: DefaultCoder>(&self, index: i32) -> crate::Result<BeamIterable<V>> {
        let raw_list = usize::try_from(index)
            .ok()
            .and_then(|i| self.values_by_tag.get(i));
        match raw_list {
            Some(raw_list) if !raw_list.is_empty() => {
                let vec: crate::Result<Vec<V>> = raw_list
                    .iter()
                    .map(|bytes| {
                        V::decode(bytes).map_err(|e| {
                            crate::Error::from(e).context(format!(
                                "Failed to decode CoGbkResult value for tag index {index}"
                            ))
                        })
                    })
                    .collect();
                Ok(BeamIterable::from_vec(vec?))
            }
            _ => Ok(BeamIterable::default()),
        }
    }

    /// Returns true if no input collection has an element for this key.
    pub fn is_empty(&self) -> bool {
        self.values_by_tag.iter().all(|v| v.is_empty())
    }
}

/// Keyed [`PCollection`]s with the same key type `K`, each under a unique tag name.
pub struct KeyedPCollectionTuple<K> {
    pipeline: Pipeline,
    tag_names: Vec<String>,
    inputs: Vec<TaggedInput<K>>,
}

/// One input of a [`KeyedPCollectionTuple`]. The transform that consumes the tuple calls
/// `tag` in its own name scope, so the tagging step is one of its children.
struct TaggedInput<K> {
    pcollection_id: String,
    /// Adds the step that encodes each value as a [`RawUnionValue`] with the input index.
    tag: Box<dyn Fn() -> PCollection<(K, RawUnionValue)>>,
}

impl<K: DefaultCoder + Eq + Hash> KeyedPCollectionTuple<K> {
    /// Creates an empty `KeyedPCollectionTuple` attached to `pipeline`.
    pub fn empty(pipeline: Pipeline) -> Self {
        Self {
            pipeline,
            tag_names: Vec::new(),
            inputs: Vec::new(),
        }
    }

    /// Creates a `KeyedPCollectionTuple` that contains one keyed collection.
    pub fn of<V: DefaultCoder>(tag: impl Into<String>, pcoll: &PCollection<(K, V)>) -> Self {
        Self::empty(pcoll.pipeline().clone()).and(tag, pcoll)
    }

    /// Adds a keyed collection under `tag`. A `tag` that is already in this tuple is a
    /// construction error, and the collection is not added. A value that does not encode fails
    /// its bundle at run time.
    pub fn and<V: DefaultCoder>(
        mut self,
        tag: impl Into<String>,
        pcoll: &PCollection<(K, V)>,
    ) -> Self {
        let tag_name = tag.into();
        if self.tag_names.contains(&tag_name) {
            self.pipeline
                .record_construction_error(PipelineError::InvalidConstruction {
                    transform_id: format!("Tag[{tag_name}]"),
                    reason: format!("duplicate tag '{tag_name}' in KeyedPCollectionTuple"),
                });
            return self;
        }
        let union_tag = self.tag_names.len() as i32;
        let step_name = format!("Tag[{tag_name}]");
        self.tag_names.push(tag_name);

        // The closure owns a handle to the input, because the step is added later.
        let input = pcoll.clone();
        let tag = move || {
            // A value that does not encode fails the bundle, so the runner retries it.
            input.apply(
                step_name.as_str(),
                ParDo::new(ClosureFn::new("Map", move |(k, v): (K, V), out| {
                    let value = v.encode()?;
                    out.emit((
                        k,
                        RawUnionValue {
                            tag: union_tag,
                            value,
                        },
                    ))
                })),
            )
        };
        self.inputs.push(TaggedInput {
            pcollection_id: pcoll.id().to_string(),
            tag: Box::new(tag),
        });
        self
    }

    /// Applies `transform` to this tuple under the name `name`.
    pub fn apply<Tform>(&self, name: impl Into<String>, transform: Tform) -> Tform::Output
    where
        Tform: PTransform<Self>,
    {
        self.pipeline
            .apply_scoped(&name.into(), PInput::pcollection_ids(self), || {
                transform.expand(self)
            })
    }

    /// Applies a fallible transform to this tuple under the name `name`.
    pub fn try_apply<Tform>(
        &self,
        name: impl Into<String>,
        transform: Tform,
    ) -> Result<Tform::Output, Tform::Error>
    where
        Tform: TryPTransform<Self>,
    {
        self.pipeline
            .try_apply_scoped(&name.into(), PInput::pcollection_ids(self), || {
                transform.try_expand(self)
            })
    }

    /// Adds the tagging step of each input in the innermost name scope, in tag order.
    fn tagged_collections(&self) -> Vec<PCollection<(K, RawUnionValue)>> {
        self.inputs.iter().map(|input| (input.tag)()).collect()
    }
}

impl<K> PInput for KeyedPCollectionTuple<K> {
    fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    /// The input PCollection of each tag.
    fn pcollection_ids(&self) -> HashMap<String, String> {
        self.tag_names
            .iter()
            .zip(&self.inputs)
            .map(|(tag, input)| (tag.clone(), input.pcollection_id.clone()))
            .collect()
    }
}

/// Groups elements of multiple keyed [`PCollection`]s by key.
#[derive(Default)]
pub struct CoGroupByKey;

impl CoGroupByKey {
    /// Creates a `CoGroupByKey` transform.
    pub fn new() -> Self {
        Self
    }
}

impl HasDisplayData for CoGroupByKey {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "CoGroupByKey");
    }
}

impl<K: DefaultCoder + Eq + Hash> PTransform<KeyedPCollectionTuple<K>> for CoGroupByKey {
    type Output = PCollection<(K, CoGbkResult)>;

    fn expand(&self, input: &KeyedPCollectionTuple<K>) -> Self::Output {
        if input.inputs.is_empty() {
            // A configuration error: record it and return an output with no producer, so the
            // later transforms still attach to it.
            let pipeline = &input.pipeline;
            let name = pipeline.scope_name().unwrap_or_default();
            pipeline.record_construction_error(PipelineError::InvalidConstruction {
                transform_id: name.clone(),
                reason: "the KeyedPCollectionTuple is empty".to_string(),
            });
            let coder_id = <(K, CoGbkResult)>::register_coder(pipeline);
            return pipeline.add_pcollection(&format!("{name}.out"), &coder_id, IsBounded::Bounded);
        }

        {
            let tagged = input.tagged_collections();
            let flattened = Flatten::pcollections("Flatten", &tagged.iter().collect::<Vec<_>>());

            let grouped = flattened.apply("GroupByKey", GroupByKey::new());

            let tag_names: Arc<[String]> = input.tag_names.clone().into();
            let tag_count = tag_names.len();
            grouped.apply("ConstructCoGbkResult", ParDo::new(ClosureFn::new(
                    "ConstructCoGbkResult",
                    move |(k, raw_values): (K, BeamIterable<RawUnionValue>), ctx| {
                        // Tags are the union indices `0..n` from `and`. Group by position, then
                        // build the `CoGbkResult` directly without a hash map.
                        let mut by_tag: Vec<Vec<Vec<u8>>> = vec![Vec::new(); tag_count];
                        raw_values
                            .try_into_iter()
                            .try_for_each(|rv| -> crate::Result<()> {
                                let rv = rv.map_err(|e| {
                                    format!("CoGroupByKey failed to read a grouped value: {e}")
                                })?;
                                let values = usize::try_from(rv.tag)
                                    .ok()
                                    .and_then(|index| by_tag.get_mut(index))
                                    .ok_or_else(|| {
                                        format!(
                                            "CoGroupByKey read a value with tag {} but has {} inputs",
                                            rv.tag, tag_count
                                        )
                                    })?;
                                values.push(rv.value);
                                Ok(())
                            })?;
                        ctx.emit((k, CoGbkResult::new(Arc::clone(&tag_names), by_tag)))
                    },
                ),
            ))
        }
    }
}
