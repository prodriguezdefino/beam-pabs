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

//! [`PCollection`], the immutable distributed dataset, and [`PCollectionList`].
//!
//! A `PCollection<T>` is a handle that holds the pipeline-proto IDs of the collection and
//! its coder. All mutation goes through the owning [`Pipeline`], so a clone is cheap.

use std::collections::HashMap;
use std::marker::PhantomData;

use super::pvalue::{PInput, POutput};
use crate::pipeline::Pipeline;
use crate::transforms::{PTransform, TryPTransform};
use model::pipeline as proto;

/// Whether a [`PCollection`] is bounded (finite) or unbounded (streaming). Pipeline
/// serialization translates it to the Runner API wire enum.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IsBounded {
    #[default]
    Bounded,
    Unbounded,
}

impl From<IsBounded> for proto::is_bounded::Enum {
    fn from(value: IsBounded) -> Self {
        match value {
            IsBounded::Bounded => proto::is_bounded::Enum::Bounded,
            IsBounded::Unbounded => proto::is_bounded::Enum::Unbounded,
        }
    }
}

/// An unspecified mark counts as bounded.
impl From<proto::is_bounded::Enum> for IsBounded {
    fn from(value: proto::is_bounded::Enum) -> Self {
        match value {
            proto::is_bounded::Enum::Unbounded => IsBounded::Unbounded,
            proto::is_bounded::Enum::Bounded | proto::is_bounded::Enum::Unspecified => {
                IsBounded::Bounded
            }
        }
    }
}

/// Combines the boundedness of the inputs of a transform: unbounded if one input is unbounded,
/// else bounded. An empty set is bounded.
impl FromIterator<IsBounded> for IsBounded {
    fn from_iter<I: IntoIterator<Item = IsBounded>>(iter: I) -> Self {
        iter.into_iter()
            .find(|bounded| *bounded == IsBounded::Unbounded)
            .unwrap_or_default()
    }
}

pub struct PCollection<T> {
    id: String,
    coder_id: String,
    pipeline: Pipeline,
    _marker: PhantomData<T>,
}

impl<T> Clone for PCollection<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            coder_id: self.coder_id.clone(),
            pipeline: self.pipeline.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for PCollection<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PCollection")
            .field("id", &self.id)
            .field("coder_id", &self.coder_id)
            .finish()
    }
}

impl<T: 'static> PCollection<T> {
    pub fn new(id: String, coder_id: String, pipeline: Pipeline) -> Self {
        Self {
            id,
            coder_id,
            pipeline,
            _marker: PhantomData,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn coder_id(&self) -> &str {
        &self.coder_id
    }

    pub fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    /// The boundedness that the graph gives this PCollection. An id that the graph does not
    /// hold counts as bounded.
    pub fn is_bounded(&self) -> IsBounded {
        self.pipeline
            .lock()
            .components
            .pcollections
            .get(&self.id)
            .map_or(IsBounded::Bounded, |p| IsBounded::from(p.is_bounded()))
    }

    /// Returns the windowing strategy ID associated with this PCollection.
    pub fn windowing_strategy_id(&self) -> String {
        let lock = self.pipeline.lock();
        lock.components
            .pcollections
            .get(&self.id)
            .map(|p| p.windowing_strategy_id.clone())
            .unwrap_or_else(|| lock.default_windowing_strategy_id.clone())
    }

    /// Applies `transform` to this PCollection under the name `name`. The transforms that it
    /// adds get names in the scope `name`.
    pub fn apply<Tform>(&self, name: impl Into<String>, transform: Tform) -> Tform::Output
    where
        Tform: PTransform<Self>,
    {
        self.pipeline
            .apply_scoped(&name.into(), PInput::pcollection_ids(self), || {
                transform.expand(self)
            })
    }

    /// Applies a [`TryPTransform`] to this PCollection under the name `name`.
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

    /// Replaces the coder of this PCollection in the pipeline graph. The coder must encode `T`.
    /// Call it only on the output of the transform that is being expanded.
    pub(crate) fn with_coder_id(mut self, coder_id: String) -> Self {
        if let Some(pcoll) = self
            .pipeline
            .lock()
            .components
            .pcollections
            .get_mut(&self.id)
        {
            pcoll.coder_id.clone_from(&coder_id);
        }
        self.coder_id = coder_id;
        self
    }

    /// The coder of this PCollection, if the graph has it. The output of a cross-language
    /// transform has no coder until the expansion service replies.
    pub(crate) fn registered_coder_id(&self) -> Option<&str> {
        self.pipeline
            .lock()
            .components
            .coders
            .contains_key(&self.coder_id)
            .then_some(self.coder_id.as_str())
    }

    /// Gives this PCollection the coder that `derive` builds from the coder of `source`, for a
    /// transform whose output holds the elements of `source`: a row then keeps its schema.
    /// Keeps the coder of `T` while `source` has no coder, for example before a cross-language
    /// expansion replies. Call it only on the output of the transform that is being expanded.
    pub fn with_coder_derived_from<U: 'static>(
        self,
        source: &PCollection<U>,
        derive: impl FnOnce(&Pipeline, &str) -> String,
    ) -> Self {
        match source.registered_coder_id() {
            Some(coder_id) => {
                let derived = derive(&self.pipeline, coder_id);
                self.with_coder_id(derived)
            }
            None => self,
        }
    }

    /// Gives this PCollection the coder of `source`, which has the same element type. A
    /// transform that passes its elements through keeps their coder, so a row keeps its schema.
    pub(crate) fn with_coder_of(self, source: &PCollection<T>) -> Self {
        match source.registered_coder_id() {
            Some(coder_id) => self.with_coder_id(coder_id.to_string()),
            None => self,
        }
    }

    /// Replaces the windowing strategy of this PCollection in the pipeline graph.
    pub(crate) fn with_windowing_strategy_id(self, windowing_strategy_id: String) -> Self {
        if let Some(pcoll) = self
            .pipeline
            .lock()
            .components
            .pcollections
            .get_mut(&self.id)
        {
            pcoll
                .windowing_strategy_id
                .clone_from(&windowing_strategy_id);
        }
        self
    }

    /// Gives this PCollection the windowing strategy of `source`.
    pub(crate) fn with_windowing_strategy_of<U: 'static>(self, source: &PCollection<U>) -> Self {
        let ws_id = source.windowing_strategy_id();
        self.with_windowing_strategy_id(ws_id)
    }
}

impl PCollection<crate::schema::RowElement> {
    /// The same PCollection, with each element as a [`Row`](crate::schema::Row). The coder
    /// stays the same.
    pub fn as_rows(&self) -> PCollection<crate::schema::Row> {
        PCollection::new(
            self.id.clone(),
            self.coder_id.clone(),
            self.pipeline.clone(),
        )
    }
}

impl PCollection<crate::schema::Row> {
    /// The same PCollection, with each element as a [`RowElement`](crate::schema::RowElement):
    /// the input of a `DoFn` that reads the row coder bytes. The coder stays the same.
    pub fn as_row_elements(&self) -> PCollection<crate::schema::RowElement> {
        PCollection::new(
            self.id.clone(),
            self.coder_id.clone(),
            self.pipeline.clone(),
        )
    }

    /// Replaces the coder with a `beam:coder:row:v1` coder that carries `schema`. Call it
    /// before a Row PCollection crosses into another SDK, which reads the field layout from
    /// the coder payload, not from the elements.
    pub fn with_row_schema(self, schema: &crate::schema::Schema) -> Self {
        let coder_id = self.pipeline.register_row_coder(schema);
        self.with_coder_id(coder_id)
    }

    /// Returns the schema associated with this Row PCollection, if present in its coder.
    pub fn row_schema(&self) -> Option<std::sync::Arc<crate::schema::Schema>> {
        let lock = self.pipeline.lock();
        crate::coders::extract_row_schema(&self.coder_id, &lock.components.coders)
    }
}

impl<T: 'static> PInput for PCollection<T> {
    fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    /// The tag `in`, the main input of a transform.
    fn pcollection_ids(&self) -> HashMap<String, String> {
        HashMap::from([("in".to_string(), self.id.clone())])
    }
}

impl<T: 'static> POutput for PCollection<T> {
    fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    /// The tag `out`, the main output of a transform.
    fn pcollection_ids(&self) -> HashMap<String, String> {
        HashMap::from([("out".to_string(), self.id.clone())])
    }
}

/// [`PCollection`]s of one element type `T`: the input of [`Flatten`] or the output of
/// [`Partition`].
///
/// [`Flatten`]: crate::transforms::Flatten
/// [`Partition`]: crate::transforms::Partition
pub struct PCollectionList<T> {
    pipeline: Pipeline,
    collections: Vec<PCollection<T>>,
}

impl<T> Clone for PCollectionList<T> {
    fn clone(&self) -> Self {
        Self {
            pipeline: self.pipeline.clone(),
            collections: self.collections.clone(),
        }
    }
}

impl<T> std::fmt::Debug for PCollectionList<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PCollectionList")
            .field("collections", &self.collections)
            .finish()
    }
}

impl<T: 'static> PCollectionList<T> {
    pub fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    /// Creates an empty list in `pipeline`.
    pub fn empty(pipeline: Pipeline) -> Self {
        Self {
            pipeline,
            collections: Vec::new(),
        }
    }

    /// Creates a list with one [`PCollection`], in the pipeline of `collection`.
    pub fn of(collection: PCollection<T>) -> Self {
        let pipeline = collection.pipeline().clone();
        Self {
            pipeline,
            collections: vec![collection],
        }
    }

    /// Creates a collection list from `pipeline` and `collections`.
    ///
    /// Accepts an empty `collections` vector. Records a construction error if any collection
    /// belongs to a different pipeline.
    pub fn from_vec(pipeline: Pipeline, collections: Vec<PCollection<T>>) -> Self {
        for collection in &collections {
            if !collection.pipeline().same_as(&pipeline) {
                pipeline.record_construction_error(
                    crate::pipeline::PipelineError::InvalidConstruction {
                        transform_id: "PCollectionList".to_string(),
                        reason:
                            "all collections in a PCollectionList must belong to the same pipeline"
                                .to_string(),
                    },
                );
                break;
            }
        }
        Self {
            pipeline,
            collections,
        }
    }

    /// Appends `collection` and returns the list.
    pub fn and(mut self, collection: PCollection<T>) -> Self {
        self.push(collection);
        self
    }

    /// Appends `collection` to the list. Records a construction error if `collection` belongs to
    /// a different pipeline.
    pub fn push(&mut self, collection: PCollection<T>) {
        if !collection.pipeline().same_as(&self.pipeline) {
            self.pipeline.record_construction_error(
                crate::pipeline::PipelineError::InvalidConstruction {
                    transform_id: "PCollectionList".to_string(),
                    reason: "all collections in a PCollectionList must belong to the same pipeline"
                        .to_string(),
                },
            );
        }
        self.collections.push(collection);
    }

    pub fn collections(&self) -> &[PCollection<T>] {
        &self.collections
    }

    pub fn into_vec(self) -> Vec<PCollection<T>> {
        self.collections
    }

    pub fn len(&self) -> usize {
        self.collections.len()
    }

    pub fn is_empty(&self) -> bool {
        self.collections.is_empty()
    }

    /// Returns a reference to the [`PCollection`] at the given index, or `None` if out of bounds.
    pub fn get(&self, index: usize) -> Option<&PCollection<T>> {
        self.collections.get(index)
    }

    /// Applies `transform` to this collection list under the name `name`.
    pub fn apply<Tform>(&self, name: impl Into<String>, transform: Tform) -> Tform::Output
    where
        Tform: PTransform<Self>,
    {
        self.pipeline
            .apply_scoped(&name.into(), PInput::pcollection_ids(self), || {
                transform.expand(self)
            })
    }

    /// Applies a [`TryPTransform`] to this collection list under the name `name`.
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

    /// The id of each collection, by `{prefix}_{index}`.
    fn indexed_ids(&self, prefix: &str) -> HashMap<String, String> {
        self.collections
            .iter()
            .enumerate()
            .map(|(i, c)| (format!("{prefix}_{i}"), c.id.clone()))
            .collect()
    }
}

impl<T: 'static> std::ops::Index<usize> for PCollectionList<T> {
    type Output = PCollection<T>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.collections[index]
    }
}

impl<T: 'static> IntoIterator for PCollectionList<T> {
    type Item = PCollection<T>;
    type IntoIter = std::vec::IntoIter<PCollection<T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.collections.into_iter()
    }
}

impl<T: 'static> PInput for PCollectionList<T> {
    fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    /// The tags `input_0`, `input_1`, ..., as `Flatten` uses them.
    fn pcollection_ids(&self) -> HashMap<String, String> {
        self.indexed_ids("input")
    }
}

impl<T: 'static> POutput for PCollectionList<T> {
    fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }

    /// The tags `out_0`, `out_1`, ...
    fn pcollection_ids(&self) -> HashMap<String, String> {
        self.indexed_ids("out")
    }
}
