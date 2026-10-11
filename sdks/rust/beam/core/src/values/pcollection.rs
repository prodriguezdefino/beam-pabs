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

use std::marker::PhantomData;

use super::pvalue::{PInput, POutput};
use crate::pipeline::Pipeline;
use crate::transforms::PTransform;
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

    /// Returns the windowing strategy ID associated with this PCollection.
    pub fn windowing_strategy_id(&self) -> String {
        let lock = self.pipeline.lock();
        lock.components
            .pcollections
            .get(&self.id)
            .map(|p| p.windowing_strategy_id.clone())
            .unwrap_or_else(|| lock.default_windowing_strategy_id.clone())
    }

    /// Applies a [`PTransform`] to this PCollection.
    pub fn apply<Tform>(&self, transform: Tform) -> Tform::Output
    where
        Tform: PTransform<Self>,
    {
        transform.expand(self)
    }
}

impl PCollection<crate::schema::Row> {
    /// Replaces the coder with a `beam:coder:row:v1` coder that carries `schema`. Call it
    /// before a Row PCollection crosses into another SDK, which reads the field layout from
    /// the coder payload, not from the elements.
    pub fn with_row_schema(mut self, schema: &crate::schema::Schema) -> Self {
        self.coder_id = self.pipeline.register_row_coder(schema);
        if let Some(pcoll) = self
            .pipeline
            .lock()
            .components
            .pcollections
            .get_mut(&self.id)
        {
            pcoll.coder_id = self.coder_id.clone();
        }
        self
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
}

impl<T: 'static> POutput for PCollection<T> {
    fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }
}

/// [`PCollection`]s of one element type `T`: the input of `Flatten` or the output of
/// `Partition`.
///
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

    pub fn from_vec(pipeline: Pipeline, collections: Vec<PCollection<T>>) -> Self {
        Self {
            pipeline,
            collections,
        }
    }

    /// Appends `collection` and returns the list, for chaining.
    pub fn and(mut self, collection: PCollection<T>) -> Self {
        self.collections.push(collection);
        self
    }

    pub fn push(&mut self, collection: PCollection<T>) {
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

    /// Applies a [`PTransform`] to this collection list.
    pub fn apply<Tform>(&self, transform: Tform) -> Tform::Output
    where
        Tform: PTransform<Self>,
    {
        transform.expand(self)
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
}

impl<T: 'static> POutput for PCollectionList<T> {
    fn pipeline(&self) -> &Pipeline {
        &self.pipeline
    }
}
