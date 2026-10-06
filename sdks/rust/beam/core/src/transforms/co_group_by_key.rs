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
use std::io::{Read, Write};
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use super::{
    ClosureFn, DisplayDataBuilder, Flatten, GroupByKey, HasDisplayData, Map, PTransform, ParDo,
};
use crate::coders::{
    BeamIterable, BytesCoder, Coder, CoderError, CoderRegistry, Context, DefaultCoder, URN_KV,
};
use crate::pipeline::Pipeline;
use crate::values::{PCollection, PInput};

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
        writer: &mut dyn Write,
        context: Context,
    ) -> Result<(), CoderError> {
        value.tag.encode_element(writer)?;
        BytesCoder.encode(&value.value, writer, context)
    }

    fn decode(&self, reader: &mut dyn Read, context: Context) -> Result<RawUnionValue, CoderError> {
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

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        RawUnionValueCoder.encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        RawUnionValueCoder.decode(reader, Context::Nested)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        <(i32, Vec<u8>)>::register_coder(registry)
    }
}

/// Result of a [`CoGroupByKey`] for one key: the grouped values of each input, by tag or index.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoGbkResult {
    tag_names: Vec<String>,
    values_by_tag: HashMap<i32, Vec<Vec<u8>>>,
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
        writer: &mut dyn Write,
        _context: Context,
    ) -> Result<(), CoderError> {
        value.encode_element(writer)
    }

    fn decode(&self, reader: &mut dyn Read, _context: Context) -> Result<CoGbkResult, CoderError> {
        CoGbkResult::decode_element(reader)
    }
}

impl DefaultCoder for CoGbkResult {
    type Coder = CoGbkResultCoder;

    fn coder() -> Self::Coder {
        CoGbkResultCoder
    }

    fn encode_element(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        let mut sorted_entries: Vec<(i32, Vec<Vec<u8>>)> =
            self.values_by_tag.clone().into_iter().collect();
        sorted_entries.sort_by_key(|(k, _)| *k);
        self.tag_names.encode_element(writer)?;
        sorted_entries.encode_element(writer)?;
        Ok(())
    }

    fn decode_element(reader: &mut dyn Read) -> Result<Self, CoderError> {
        let tag_names = Vec::<String>::decode_element(reader)?;
        let entries = Vec::<(i32, Vec<Vec<u8>>)>::decode_element(reader)?;
        Ok(Self {
            tag_names,
            values_by_tag: entries.into_iter().collect(),
        })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        <(Vec<String>, Vec<(i32, Vec<Vec<u8>>)>)>::register_coder(registry)
    }
}

impl CoGbkResult {
    /// Creates a `CoGbkResult` from tag names and grouped values.
    pub fn new(tag_names: Vec<String>, values_by_tag: HashMap<i32, Vec<Vec<u8>>>) -> Self {
        Self {
            tag_names,
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
        match self.values_by_tag.get(&index) {
            Some(raw_list) => {
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
            None => Ok(BeamIterable::default()),
        }
    }

    /// Returns true if no input collection has an element for this key.
    pub fn is_empty(&self) -> bool {
        self.values_by_tag.values().all(|v| v.is_empty())
    }
}

/// Keyed [`PCollection`]s with the same key type `K`, each under a unique tag name.
pub struct KeyedPCollectionTuple<K> {
    pipeline: Pipeline,
    tag_names: Vec<String>,
    collections: Vec<PCollection<(K, RawUnionValue)>>,
    _marker: PhantomData<K>,
}

impl<K: DefaultCoder + Eq + Hash> KeyedPCollectionTuple<K> {
    /// Creates an empty `KeyedPCollectionTuple` attached to `pipeline`.
    pub fn empty(pipeline: Pipeline) -> Self {
        Self {
            pipeline,
            tag_names: Vec::new(),
            collections: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Creates a `KeyedPCollectionTuple` that contains one keyed collection.
    pub fn of<V: DefaultCoder>(tag: impl Into<String>, pcoll: &PCollection<(K, V)>) -> Self {
        Self::empty(pcoll.pipeline().clone()).and(tag, pcoll)
    }

    /// Adds a keyed collection under `tag`. Panics if `tag` is already in this tuple. The added
    /// transform panics at run time if a value does not encode.
    pub fn and<V: DefaultCoder>(
        mut self,
        tag: impl Into<String>,
        pcoll: &PCollection<(K, V)>,
    ) -> Self {
        let tag_name = tag.into();
        assert!(
            !self.tag_names.contains(&tag_name),
            "Duplicate tag '{tag_name}' in KeyedPCollectionTuple"
        );
        let union_tag = self.tag_names.len() as i32;
        self.tag_names.push(tag_name.clone());

        let tagged = pcoll.apply(Map::new(
            format!("Tag[{tag_name}]"),
            move |(k, v): (K, V)| {
                let bytes = v.encode().expect("Failed to encode CoGroupByKey value");
                (
                    k,
                    RawUnionValue {
                        tag: union_tag,
                        value: bytes,
                    },
                )
            },
        ));
        self.collections.push(tagged);
        self
    }

    /// Applies a transform to this tuple.
    pub fn apply<Tform>(&self, transform: Tform) -> Tform::Output
    where
        Tform: PTransform<Self>,
    {
        transform.expand(self)
    }
}

impl<K> PInput for KeyedPCollectionTuple<K> {
    fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }
}

/// Groups elements of multiple keyed [`PCollection`]s by key. `expand` panics if the input
/// [`KeyedPCollectionTuple`] is empty.
pub struct CoGroupByKey {
    name: String,
}

impl CoGroupByKey {
    /// Creates a `CoGroupByKey` transform with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl HasDisplayData for CoGroupByKey {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "CoGroupByKey");
        builder.add_text("name", &self.name);
    }
}

impl<K: DefaultCoder + Eq + Hash> PTransform<KeyedPCollectionTuple<K>> for CoGroupByKey {
    type Output = PCollection<(K, CoGbkResult)>;

    fn expand(&self, input: &KeyedPCollectionTuple<K>) -> Self::Output {
        assert!(
            !input.collections.is_empty(),
            "Cannot CoGroupByKey with an empty KeyedPCollectionTuple"
        );

        let flattened = Flatten::pcollections(
            format!("{}/Flatten", self.name),
            &input.collections.iter().collect::<Vec<_>>(),
        );

        let grouped = flattened.apply(GroupByKey::new(format!("{}/GroupByKey", self.name)));

        let tag_names = input.tag_names.clone();
        grouped.apply(ParDo::new(
            format!("{}/ConstructCoGbkResult", self.name),
            ClosureFn::new(
                "ConstructCoGbkResult",
                move |(k, raw_values): (K, BeamIterable<RawUnionValue>), ctx| {
                    // Tags are the union indices `0..n` from `and`. Group by position, then
                    // build the `CoGbkResult` map once per key, so there is no hash per value.
                    let mut by_tag: Vec<Vec<Vec<u8>>> = vec![Vec::new(); tag_names.len()];
                    for rv in raw_values.try_into_iter() {
                        let rv = rv.map_err(|e| {
                            format!("CoGroupByKey failed to read a grouped value: {e}")
                        })?;
                        let values = usize::try_from(rv.tag)
                            .ok()
                            .and_then(|index| by_tag.get_mut(index))
                            .ok_or_else(|| {
                                format!(
                                    "CoGroupByKey read a value with tag {} but has {} inputs",
                                    rv.tag,
                                    tag_names.len()
                                )
                            })?;
                        values.push(rv.value);
                    }
                    let values_by_tag: HashMap<i32, Vec<Vec<u8>>> = (0_i32..)
                        .zip(by_tag)
                        .filter(|(_, values)| !values.is_empty())
                        .collect();
                    ctx.emit((k, CoGbkResult::new(tag_names.clone(), values_by_tag)))
                },
            ),
        ))
    }
}
