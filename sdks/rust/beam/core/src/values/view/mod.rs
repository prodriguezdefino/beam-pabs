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

//! Side input views over a [`PCollection`].
//!
//! A view ([`SingletonView`], [`IterView`], [`MultimapView`]) is a `PValue`, not a transform.
//! It names a collection that already exists and says how a `DoFn` reads it, which fixes the
//! type of the value the `DoFn` receives. The runner-facing reader is
//! [`SideInputReader`](crate::internals::SideInputReader).

mod access;
mod inputs;
mod windowing;
mod wire;

pub use access::{SideInputScope, SideIter, SideMap};
pub use inputs::{IntoSideInputs, SideInputs};
pub use windowing::SideInputWindowing;

use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

use beam_model::pipeline as proto;

use crate::coders::{DefaultCoder, FieldMap, extract_row_schema};
use crate::pipeline::UNEXPANDED_PLACEHOLDER_ID;
use crate::schema::{Schema, SchemaError};
use crate::values::PCollection;
use windowing::ViewWindowing;

/// The field map of the rows of a side input for its row schema, resolved on the first read.
type FieldMapSlot = OnceLock<Result<Option<FieldMap>, SchemaError>>;

/// Standard URN for iterable side input access pattern.
pub const URN_SIDE_INPUT_ITERABLE: &str = "beam:side_input:iterable:v1";

/// Standard URN for multimap side input access pattern.
pub const URN_SIDE_INPUT_MULTIMAP: &str = "beam:side_input:multimap:v1";

/// Standard URN for global window mapping function.
pub const URN_WINDOW_MAPPING_GLOBAL: &str = "beam:window_mapping_fn:global_window:v1";

/// Standard URN for identity window mapping function.
pub const URN_WINDOW_MAPPING_IDENTITY: &str = "beam:window_mapping_fn:identity:v1";

/// Rust SDK URN for mapping with the side input's window function.
/// The payload is the encoded window function `FunctionSpec`.
pub const URN_WINDOW_MAPPING_WINDOW_FN: &str = "beam:rust:windowmapping:window_fn:v1";

/// Standard URN for singleton view fn.
pub const URN_VIEW_FN_SINGLETON: &str = "beam:view_fn:singleton:v1";

/// Standard URN for iterable view fn.
pub const URN_VIEW_FN_ITERABLE: &str = "beam:view_fn:iter:v1";

/// Standard URN for multimap view fn.
pub const URN_VIEW_FN_MULTIMAP: &str = "beam:view_fn:multimap:v1";

/// Distinguishes the consumption pattern of a side input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SideInputKind {
    /// Single element per window.
    Singleton,
    /// All elements in the window as an iterable.
    Iter,
    /// Keyed lookup returning all matching values for a key in the window.
    Multimap,
}

impl SideInputKind {
    const fn tag_prefix(self) -> &'static str {
        match self {
            Self::Singleton => "side_singleton",
            Self::Iter => "side_iter",
            Self::Multimap => "side_multimap",
        }
    }

    const fn access_pattern(self) -> &'static str {
        match self {
            Self::Singleton | Self::Iter => URN_SIDE_INPUT_ITERABLE,
            Self::Multimap => URN_SIDE_INPUT_MULTIMAP,
        }
    }

    pub const fn view_fn_urn(self) -> &'static str {
        match self {
            Self::Singleton => URN_VIEW_FN_SINGLETON,
            Self::Iter => URN_VIEW_FN_ITERABLE,
            Self::Multimap => URN_VIEW_FN_MULTIMAP,
        }
    }
}

/// How a [`PCollectionView`] is read: [`Singleton`], [`Iter`] or [`Multimap`]. Sealed.
pub trait ViewKind: sealed::Sealed + Send + Sync + 'static {
    /// The consumption pattern written into the transform graph.
    const KIND: SideInputKind;
}

/// Read as the single element of the window.
#[derive(Clone, Copy, Debug)]
pub struct Singleton;

/// Read as all elements of the window.
#[derive(Clone, Copy, Debug)]
pub struct Iter;

/// Read by key.
#[derive(Clone, Copy, Debug)]
pub struct Multimap;

impl ViewKind for Singleton {
    const KIND: SideInputKind = SideInputKind::Singleton;
}

impl ViewKind for Iter {
    const KIND: SideInputKind = SideInputKind::Iter;
}

impl ViewKind for Multimap {
    const KIND: SideInputKind = SideInputKind::Multimap;
}

/// A view read as `T`. See [`PCollection::as_singleton`].
pub type SingletonView<T> = PCollectionView<T, Singleton>;

/// A view read as a lazy [`SideIter`]. See [`PCollection::as_iter`].
pub type IterView<T> = PCollectionView<T, Iter>;

/// A view read as a [`SideMap`] lookup. See [`PCollection::as_multimap`].
pub type MultimapView<K, V> = PCollectionView<(K, V), Multimap>;

/// A handle on a side [`PCollection`] of `T`, read as `Kind`. Use the aliases [`SingletonView`],
/// [`IterView`] and [`MultimapView`]. Serializable, so a registered `DoFn` can hold it.
#[derive(Debug)]
pub struct PCollectionView<T, Kind> {
    tag: String,
    pcollection_id: String,
    coder_id: String,
    windowing: ViewWindowing,
    /// The schema of the rows in an element, from the coder of the side input. A dynamic
    /// [`Row`](crate::schema::Row) needs it to decode.
    row_schema: Option<Arc<Schema>>,
    field_map: Arc<FieldMapSlot>,
    _marker: PhantomData<fn() -> (T, Kind)>,
}

impl<T, Kind> Clone for PCollectionView<T, Kind> {
    fn clone(&self) -> Self {
        Self {
            tag: self.tag.clone(),
            pcollection_id: self.pcollection_id.clone(),
            coder_id: self.coder_id.clone(),
            windowing: self.windowing.clone(),
            row_schema: self.row_schema.clone(),
            field_map: Arc::clone(&self.field_map),
            _marker: PhantomData,
        }
    }
}

impl<T, Kind: ViewKind> PCollectionView<T, Kind> {
    /// The local tag used to identify this side input on the transform.
    pub fn tag(&self) -> &str {
        &self.tag
    }

    /// ID of the backing PCollection.
    pub fn pcollection_id(&self) -> &str {
        &self.pcollection_id
    }

    /// ID of the coder used to decode elements of this side input.
    pub fn coder_id(&self) -> &str {
        &self.coder_id
    }

    /// Consumption kind of this view.
    pub fn kind(&self) -> SideInputKind {
        Kind::KIND
    }

    /// Beam URN for this side input's access pattern.
    pub fn access_pattern(&self) -> &'static str {
        Kind::KIND.access_pattern()
    }

    /// Beam URN for this side input's view function.
    pub fn view_fn_urn(&self) -> &'static str {
        Kind::KIND.view_fn_urn()
    }

    /// Converts this view's metadata to a Runner API `SideInput` protobuf message. The window
    /// mapping is omitted if the graph does not have a supported windowing for the side input.
    pub fn to_proto(&self) -> proto::SideInput {
        proto::SideInput {
            access_pattern: Some(proto::FunctionSpec {
                urn: self.access_pattern().to_string(),
                payload: Vec::new(),
            }),
            view_fn: Some(proto::FunctionSpec {
                urn: self.view_fn_urn().to_string(),
                payload: Vec::new(),
            }),
            window_mapping_fn: match &self.windowing {
                ViewWindowing::Known(windowing) => Some(windowing.to_proto()),
                ViewWindowing::FromRunner | ViewWindowing::Unsupported(_) => None,
            },
        }
    }
}

/// Type-erased side input view for pipeline graph construction.
pub trait AnySideInput: Send + Sync {
    /// Local input tag.
    fn tag(&self) -> &str;
    /// ID of the backing PCollection.
    fn pcollection_id(&self) -> &str;
    /// Runner API `SideInput` protobuf representation.
    fn to_proto(&self) -> proto::SideInput;
}

impl<T: DefaultCoder, Kind: ViewKind> AnySideInput for PCollectionView<T, Kind> {
    fn tag(&self) -> &str {
        self.tag()
    }

    fn pcollection_id(&self) -> &str {
        self.pcollection_id()
    }

    fn to_proto(&self) -> proto::SideInput {
        self.to_proto()
    }
}

mod sealed {
    pub trait Sealed {}
}

impl sealed::Sealed for Singleton {}
impl sealed::Sealed for Iter {}
impl sealed::Sealed for Multimap {}

impl<T: DefaultCoder> PCollection<T> {
    /// A view read through `Kind`. The tag derives from `Kind` and the PCollection id, so two
    /// views of the same kind on one collection share a materialization.
    fn view<Kind: ViewKind>(&self) -> PCollectionView<T, Kind> {
        let pipeline = self.pipeline();
        let p = pipeline.lock();
        let windowing = match p.components.pcollections.get(self.id()) {
            Some(pc) if pc.windowing_strategy_id == UNEXPANDED_PLACEHOLDER_ID => {
                ViewWindowing::FromRunner
            }
            pc => pc
                .and_then(|pc| {
                    p.components
                        .windowing_strategies
                        .get(&pc.windowing_strategy_id)
                })
                .and_then(|ws| ws.window_fn.as_ref())
                .ok_or_else(|| format!("PCollection '{}' has no window function", self.id()))
                .and_then(SideInputWindowing::of)
                .map_or_else(ViewWindowing::Unsupported, ViewWindowing::Known),
        };
        let row_schema = extract_row_schema(self.coder_id(), &p.components.coders);
        drop(p);

        PCollectionView {
            tag: format!("{}_{}", Kind::KIND.tag_prefix(), self.id()),
            pcollection_id: self.id().to_string(),
            coder_id: self.coder_id().to_string(),
            windowing,
            row_schema,
            field_map: Arc::new(OnceLock::new()),
            _marker: PhantomData,
        }
    }

    /// A side input read as the single element of the window.
    pub fn as_singleton(&self) -> SingletonView<T> {
        self.view()
    }

    /// A side input read as all elements of the window.
    pub fn as_iter(&self) -> IterView<T> {
        self.view()
    }
}

impl<K: DefaultCoder, V: DefaultCoder> PCollection<(K, V)>
where
    (K, V): DefaultCoder,
{
    /// A side input read by key.
    pub fn as_multimap(&self) -> MultimapView<K, V> {
        let mut view = self.view::<Multimap>();
        let p = self.pipeline().lock();
        if let Some(value_coder_id) = p
            .components
            .coders
            .get(self.coder_id())
            .and_then(|coder| coder.component_coder_ids.get(1))
        {
            view.row_schema = extract_row_schema(value_coder_id, &p.components.coders);
        }
        drop(p);
        view
    }
}
