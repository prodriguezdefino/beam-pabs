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
//! A [`PCollectionView`] is a `PValue`, not a transform. It names a collection that
//! already exists and describes how a `DoFn` reads it. The runner-facing reader is
//! `SideInputReader`.

use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;

use model::pipeline as proto;
use prost::Message;

use crate::coders::{Coder, Context, DefaultCoder, IntervalWindowCoder};
use crate::pipeline::{URN_WINDOW_FN_GLOBAL_WINDOWS, URN_WINDOW_FN_SESSION_WINDOWS};
use crate::values::PCollection;
use crate::windowing::{WindowFn, decode_window_fn};

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

/// Distinguishes the consumption pattern of a side input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SideInputKind {
    /// Single element per window.
    Singleton,
    /// All elements in the window as an iterable.
    Iter,
    /// Keyed lookup returning all matching values for a key in the window.
    Multimap,
}

/// How a side input's window is found from the window of the main input element.
#[derive(Clone, Debug)]
pub enum SideInputWindowing {
    /// The side input is in the global window.
    Global,
    /// The side input has the same windows as the main input.
    Identity,
    /// The end of the main window is assigned with the side input's window function.
    Mapped(Arc<dyn WindowFn>),
}

impl SideInputWindowing {
    /// Returns the windowing of a side input whose PCollection uses `window_fn`. Sessions and
    /// unknown window functions are rejected.
    pub fn of(window_fn: &proto::FunctionSpec) -> Result<Self, String> {
        match window_fn.urn.as_str() {
            URN_WINDOW_FN_GLOBAL_WINDOWS => Ok(Self::Global),
            URN_WINDOW_FN_SESSION_WINDOWS => {
                Err("Sessions windowing is not allowed in side inputs".to_string())
            }
            _ => decode_window_fn(window_fn)
                .map(Self::Mapped)
                .map_err(|e| format!("Unsupported window function for side inputs: {e}")),
        }
    }

    /// Decodes a `window_mapping_fn` written by [`to_proto`](Self::to_proto).
    pub fn from_proto(spec: &proto::FunctionSpec) -> Result<Self, String> {
        match spec.urn.as_str() {
            URN_WINDOW_MAPPING_GLOBAL => Ok(Self::Global),
            URN_WINDOW_MAPPING_IDENTITY => Ok(Self::Identity),
            URN_WINDOW_MAPPING_WINDOW_FN => proto::FunctionSpec::decode(spec.payload.as_slice())
                .map_err(|e| format!("Failed to decode window mapping payload: {e}"))
                .and_then(|window_fn| decode_window_fn(&window_fn))
                .map(Self::Mapped),
            urn => Err(format!("Unsupported window mapping fn '{urn}'")),
        }
    }

    /// Returns the `window_mapping_fn` of a Runner API `SideInput`.
    pub fn to_proto(&self) -> proto::FunctionSpec {
        let (urn, payload) = match self {
            Self::Global => (URN_WINDOW_MAPPING_GLOBAL, Vec::new()),
            Self::Identity => (URN_WINDOW_MAPPING_IDENTITY, Vec::new()),
            Self::Mapped(window_fn) => {
                let spec = proto::FunctionSpec {
                    urn: window_fn.urn().to_string(),
                    payload: window_fn.payload(),
                };
                (URN_WINDOW_MAPPING_WINDOW_FN, spec.encode_to_vec())
            }
        };
        proto::FunctionSpec {
            urn: urn.to_string(),
            payload,
        }
    }

    /// Returns the encoded side input window for the encoded `main_window`. A mapped window is
    /// the earliest window that contains the end of the main window.
    pub fn map<'a>(&self, main_window: &'a [u8]) -> Result<Cow<'a, [u8]>, String> {
        let window_fn = match self {
            Self::Global => return Ok(Cow::Borrowed(&[])),
            Self::Identity => return Ok(Cow::Borrowed(main_window)),
            Self::Mapped(window_fn) => window_fn,
        };
        // The global window encodes to zero bytes and cannot be mapped to an interval window.
        if main_window.is_empty() {
            return Err("Cannot map the global window to a non-global side input".to_string());
        }
        let main = IntervalWindowCoder
            .decode(&mut std::io::Cursor::new(main_window), Context::Nested)
            .map_err(|e| format!("Failed to decode main input window: {e}"))?;
        let side = window_fn
            .assign_windows(main.max_timestamp())
            .into_iter()
            .min()
            .ok_or_else(|| format!("{window_fn:?} assigned no window to {main:?}"))?;
        let mut bytes = Vec::new();
        side.encode(&mut bytes)
            .map_err(|e| format!("Failed to encode side input window: {e}"))?;
        Ok(Cow::Owned(bytes))
    }
}

/// A strongly typed view into a side [`PCollection`].
#[derive(Debug)]
pub struct PCollectionView<T> {
    tag: String,
    pcollection_id: String,
    coder_id: String,
    access_pattern: &'static str,
    kind: SideInputKind,
    /// An error if the side PCollection's windowing cannot be read; validation reports it.
    windowing: Result<SideInputWindowing, String>,
    _marker: PhantomData<T>,
}

impl<T> Clone for PCollectionView<T> {
    fn clone(&self) -> Self {
        Self {
            tag: self.tag.clone(),
            pcollection_id: self.pcollection_id.clone(),
            coder_id: self.coder_id.clone(),
            access_pattern: self.access_pattern,
            kind: self.kind,
            windowing: self.windowing.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> PCollectionView<T> {
    pub fn new(
        tag: impl Into<String>,
        pcollection_id: impl Into<String>,
        coder_id: impl Into<String>,
        access_pattern: &'static str,
        kind: SideInputKind,
    ) -> Self {
        Self {
            tag: tag.into(),
            pcollection_id: pcollection_id.into(),
            coder_id: coder_id.into(),
            access_pattern,
            kind,
            windowing: Ok(SideInputWindowing::Global),
            _marker: PhantomData,
        }
    }

    /// Sets how the side input's window is found from the main input window.
    pub fn with_windowing(mut self, windowing: SideInputWindowing) -> Self {
        self.windowing = Ok(windowing);
        self
    }

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

    /// Beam URN for this side input's access pattern.
    pub fn access_pattern(&self) -> &'static str {
        self.access_pattern
    }

    /// Consumption kind of this view.
    pub fn kind(&self) -> SideInputKind {
        self.kind
    }

    /// Returns the encoded window to read this side input under, for the main input window.
    pub fn mapped_window<'a>(&self, main_window: &'a [u8]) -> Result<Cow<'a, [u8]>, String> {
        self.windowing
            .as_ref()
            .map_err(Clone::clone)?
            .map(main_window)
    }

    /// Converts this view's metadata to a Runner API `SideInput` protobuf message. The window
    /// mapping is omitted if the windowing is unsupported, which pipeline validation rejects.
    pub fn to_proto(&self) -> proto::SideInput {
        proto::SideInput {
            access_pattern: Some(proto::FunctionSpec {
                urn: self.access_pattern.to_string(),
                payload: Vec::new(),
            }),
            view_fn: Some(proto::FunctionSpec {
                urn: format!("beam:view_fn:{:?}:v1", self.kind).to_lowercase(),
                payload: Vec::new(),
            }),
            window_mapping_fn: self
                .windowing
                .as_ref()
                .ok()
                .map(SideInputWindowing::to_proto),
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

impl<T: DefaultCoder> AnySideInput for PCollectionView<T> {
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

/// The tag derives from the PCollection id, so a collection consumed as a side input twice
/// resolves to the same materialization.
fn view_of<T: DefaultCoder>(
    pcoll: &PCollection<T>,
    access_pattern: &'static str,
    kind: SideInputKind,
) -> PCollectionView<T> {
    let pipeline = pcoll.pipeline();
    let p = pipeline.lock();
    let windowing = p
        .components
        .pcollections
        .get(pcoll.id())
        .and_then(|pc| {
            p.components
                .windowing_strategies
                .get(&pc.windowing_strategy_id)
        })
        .and_then(|ws| ws.window_fn.as_ref())
        .ok_or_else(|| format!("PCollection '{}' has no window function", pcoll.id()))
        .and_then(SideInputWindowing::of);
    drop(p);

    PCollectionView {
        windowing,
        ..PCollectionView::new(
            format!("side_{}", pcoll.id()),
            pcoll.id(),
            pcoll.coder_id(),
            access_pattern,
            kind,
        )
    }
}

/// Extension methods on [`PCollection`] for creating side input views.
impl<T: DefaultCoder> PCollection<T> {
    /// Creates a singleton side input view from this collection.
    pub fn as_singleton(&self) -> PCollectionView<T> {
        view_of(self, URN_SIDE_INPUT_ITERABLE, SideInputKind::Singleton)
    }

    /// Creates an iterable side input view from this collection.
    pub fn as_iter(&self) -> PCollectionView<T> {
        view_of(self, URN_SIDE_INPUT_ITERABLE, SideInputKind::Iter)
    }
}

/// Extension methods on keyed [`PCollection`]s for creating multimap side input views.
impl<K: DefaultCoder, V: DefaultCoder> PCollection<(K, V)> {
    /// Creates a multimap side input view from this keyed collection.
    pub fn as_multimap(&self) -> PCollectionView<(K, V)> {
        view_of(self, URN_SIDE_INPUT_MULTIMAP, SideInputKind::Multimap)
    }
}
