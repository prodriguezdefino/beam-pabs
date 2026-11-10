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

use super::{
    ClosureFn, DisplayDataBuilder, Flatten, GroupByKey, HasDisplayData, PTransform, ParDo,
    TryPTransform,
};
use crate::coders::{
    BeamIterable, BytesCoder, Coder, CoderError, CoderRegistry, Context, DefaultCoder, URN_KV,
    coder_urn, extract_row_schema,
};
use crate::pipeline::{Pipeline, PipelineError};
use crate::schema::Schema;
use crate::values::{PCollection, PInput};

/// An element from one joined input, tagged with its input index.
#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug)]
pub struct CoGbkResult {
    tag_names: Arc<[String]>,
    values_by_tag: Vec<Vec<Vec<u8>>>,
    /// The row schema of the values of each input, if they have rows. The coder does not
    /// write it, so a decoded result has none.
    schemas: Arc<[Option<Arc<Schema>>]>,
}

/// Compares the tags and the values. The schemas only help to decode the values.
impl PartialEq for CoGbkResult {
    fn eq(&self, other: &Self) -> bool {
        self.tag_names == other.tag_names && self.values_by_tag == other.values_by_tag
    }
}

impl Eq for CoGbkResult {}

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
        entries.into_iter().try_for_each(|(idx, values)| {
            let slot = usize::try_from(idx)
                .ok()
                .and_then(|index| values_by_tag.get_mut(index))
                .ok_or_else(|| {
                    CoderError::Format(format!(
                        "CoGbkResult tag index {idx} is outside its {} tags",
                        tag_names.len()
                    ))
                })?;
            *slot = values;
            Ok::<_, CoderError>(())
        })?;
        Ok(Self {
            tag_names,
            values_by_tag,
            schemas: Arc::new([]),
        })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        <(Vec<String>, Vec<(i32, Vec<Vec<u8>>)>)>::register_coder(registry)
    }
}

impl CoGbkResult {
    /// Creates a `CoGbkResult` from tag names, grouped values and the row schema of each input.
    pub(crate) fn new(
        tag_names: Arc<[String]>,
        values_by_tag: Vec<Vec<Vec<u8>>>,
        schemas: Arc<[Option<Arc<Schema>>]>,
    ) -> Self {
        Self {
            tag_names,
            values_by_tag,
            schemas,
        }
    }

    /// Tag names in index order.
    pub fn tag_names(&self) -> &[String] {
        &self.tag_names
    }

    /// Decodes all elements of `tag` for this key; empty if there are none. Returns an error if
    /// `tag` is unknown or a value does not decode.
    pub fn get<V: DefaultCoder>(&self, tag: &str) -> crate::Result<BeamIterable<V>> {
        let position = self
            .tag_names
            .iter()
            .position(|t| t == tag)
            .ok_or_else(|| {
                format!(
                    "Tag '{tag}' not found in CoGbkResult schema: {:?}",
                    self.tag_names
                )
            })?;
        self.get_by_index(position)
    }

    /// Like [`Self::get`], but collects into a `Vec<V>`.
    pub fn get_vec<V: DefaultCoder>(&self, tag: &str) -> crate::Result<Vec<V>> {
        Ok(self.get::<V>(tag)?.into_vec()?)
    }

    /// Decodes all elements of the input at `index`; empty for an unknown index. Returns an
    /// error if a value does not decode.
    pub fn get_by_index<V: DefaultCoder>(&self, index: usize) -> crate::Result<BeamIterable<V>> {
        let Some(raw_list) = self.values_by_tag.get(index) else {
            return Ok(BeamIterable::default());
        };
        let schema = self.schemas.get(index).and_then(Option::as_ref);
        let values = raw_list
            .iter()
            .map(|bytes| {
                V::decode_with_schema(bytes, schema).map_err(|e| {
                    crate::Error::from(e).context(format!(
                        "Failed to decode CoGbkResult value for tag index {index}"
                    ))
                })
            })
            .collect::<crate::Result<Vec<V>>>()?;
        Ok(BeamIterable::from_vec(values))
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
    /// The row schema of the values of each input, in tag order.
    schemas: Vec<Option<Arc<Schema>>>,
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
            schemas: Vec::new(),
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
        let Ok(union_tag) = i32::try_from(self.tag_names.len()) else {
            self.pipeline
                .record_construction_error(PipelineError::InvalidConstruction {
                    transform_id: format!("Tag[{tag_name}]"),
                    reason: "a KeyedPCollectionTuple has at most i32::MAX inputs".to_string(),
                });
            return self;
        };
        let step_name = format!("Tag[{tag_name}]");
        self.tag_names.push(tag_name);
        self.schemas.push(value_row_schema(pcoll));

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

    fn expand(self, input: &KeyedPCollectionTuple<K>) -> Self::Output {
        input.pipeline.record_display_data(&self);
        if input.inputs.is_empty() {
            let pipeline = &input.pipeline;
            return pipeline.invalid_construction_output(
                "the KeyedPCollectionTuple is empty",
                &<(K, CoGbkResult)>::register_coder(pipeline),
            );
        }

        let tagged = input.tagged_collections();
        let flattened = Flatten::pcollections("Flatten", &tagged.iter().collect::<Vec<_>>());
        let grouped = flattened.apply("GroupByKey", GroupByKey::new());
        let tag_names: Arc<[String]> = input.tag_names.clone().into();
        let schemas: Arc<[Option<Arc<Schema>>]> = input.schemas.clone().into();
        grouped.apply(
            "ConstructCoGbkResult",
            ParDo::new(ClosureFn::new(
                "ConstructCoGbkResult",
                move |(k, raw_values): (K, BeamIterable<RawUnionValue>), ctx| {
                    ctx.emit((k, construct_result(&tag_names, &schemas, raw_values)?))
                },
            )),
        )
    }
}

/// Builds the `CoGbkResult` of one key. The tags are the union indices `0..n` that `and`
/// gave, so the values go into a `Vec` by position, without a hash map.
fn construct_result(
    tag_names: &Arc<[String]>,
    schemas: &Arc<[Option<Arc<Schema>>]>,
    raw_values: BeamIterable<RawUnionValue>,
) -> crate::Result<CoGbkResult> {
    let tag_count = tag_names.len();
    let mut by_tag: Vec<Vec<Vec<u8>>> = vec![Vec::new(); tag_count];
    raw_values
        .try_into_iter()
        .try_for_each(|rv| -> crate::Result<()> {
            let RawUnionValue { tag, value } =
                rv.map_err(|e| format!("CoGroupByKey failed to read a grouped value: {e}"))?;
            let values = usize::try_from(tag)
                .ok()
                .and_then(|index| by_tag.get_mut(index))
                .ok_or_else(|| {
                    format!("CoGroupByKey read a value with tag {tag} but has {tag_count} inputs")
                })?;
            values.push(value);
            Ok(())
        })?;
    Ok(CoGbkResult::new(
        Arc::clone(tag_names),
        by_tag,
        Arc::clone(schemas),
    ))
}

/// The row schema of the values of `pcoll`, from the value component of its `KV` coder.
fn value_row_schema<K: 'static, V: 'static>(pcoll: &PCollection<(K, V)>) -> Option<Arc<Schema>> {
    let pipeline = pcoll.pipeline().lock();
    let coders = &pipeline.components.coders;
    let value_coder_id = coders
        .get(pcoll.coder_id())
        .filter(|coder| coder_urn(coder) == URN_KV)?
        .component_coder_ids
        .get(1)?;
    extract_row_schema(value_coder_id, coders)
}
