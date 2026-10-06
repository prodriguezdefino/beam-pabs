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

use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;

use model::pipeline as proto;
use prost::Message;

use crate::coders::{Coder, Context, DefaultCoder, IntervalWindowCoder};
use crate::pipeline::{URN_WINDOW_FN_GLOBAL_WINDOWS, URN_WINDOW_FN_SESSION_WINDOWS};
use crate::transforms::dofn::side_input::SideInputReader;
use crate::values::PCollection;
use crate::windowing::WindowFn;
use crate::windowing::window_fn::decode_window_fn;

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
                .and_then(|window_fn| decode_window_fn(&window_fn).map_err(|e| e.to_string()))
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

    /// Returns the encoded side input window for the encoded `main_window`.
    ///
    /// Global and identity mappings return borrowed slices without allocation. Mapped window
    /// functions decode the main window, assign the side window, and allocate an owned byte buffer.
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
    /// An error if the side PCollection's windowing cannot be read; validation reports it.
    windowing: Result<SideInputWindowing, String>,
    _marker: PhantomData<fn() -> (T, Kind)>,
}

impl<T, Kind> Clone for PCollectionView<T, Kind> {
    fn clone(&self) -> Self {
        Self {
            tag: self.tag.clone(),
            pcollection_id: self.pcollection_id.clone(),
            coder_id: self.coder_id.clone(),
            windowing: self.windowing.clone(),
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
    /// mapping is omitted if the windowing is unsupported, which pipeline validation rejects.
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
            window_mapping_fn: self
                .windowing
                .as_ref()
                .ok()
                .map(SideInputWindowing::to_proto),
        }
    }

    /// The reader and the side input window for the main window of `scope`.
    fn locate<'s>(
        &self,
        scope: SideInputScope<'s>,
    ) -> crate::Result<(&'s dyn SideInputReader, Cow<'s, [u8]>)> {
        let reader = scope.reader.ok_or_else(|| {
            format!(
                "Side input '{}' is not declared on this transform: return its view from \
                 `side_inputs()`, or pass it to `from_fn_with_side_inputs`",
                self.tag
            )
        })?;
        let window = self
            .windowing
            .as_ref()
            .map_err(Clone::clone)?
            .map(scope.window)?;
        Ok((reader, window))
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

/// The reader and main window for side input reads. Built by
/// [`ProcessContext`](crate::transforms::ProcessContext), never by user code.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct SideInputScope<'s> {
    reader: Option<&'s dyn SideInputReader>,
    window: &'s [u8],
}

impl<'s> SideInputScope<'s> {
    pub(crate) fn new(reader: Option<&'s dyn SideInputReader>, window: &'s [u8]) -> Self {
        Self { reader, window }
    }
}

/// The value of an [`IterView`]: the elements of the side input window, read lazily.
pub struct SideIter<'s, T> {
    reader: &'s dyn SideInputReader,
    tag: &'s str,
    window: Cow<'s, [u8]>,
    _marker: PhantomData<fn() -> T>,
}

impl<'s, T: DefaultCoder> SideIter<'s, T> {
    /// The elements, fetched as the iterator advances. Each call starts over.
    pub fn iter(&self) -> crate::Result<impl Iterator<Item = crate::Result<T>> + 's> {
        let tag = self.tag;
        Ok(self
            .reader
            .get_iterable(tag, &self.window)?
            .map(move |bytes| decode_side_input(&bytes?, tag)))
    }

    /// All elements, collected.
    pub fn to_vec(&self) -> crate::Result<Vec<T>> {
        self.iter()?.collect()
    }
}

/// The value of a [`MultimapView`]: lookups by key in the side input window.
pub struct SideMap<'s, K, V> {
    reader: &'s dyn SideInputReader,
    tag: &'s str,
    window: Cow<'s, [u8]>,
    _marker: PhantomData<fn() -> (K, V)>,
}

impl<'s, K: DefaultCoder, V: DefaultCoder> SideMap<'s, K, V> {
    /// All values for `key`, fetched as the iterator advances.
    pub fn get_all(&self, key: &K) -> crate::Result<impl Iterator<Item = crate::Result<V>> + 's> {
        let tag = self.tag;
        let key = key
            .encode()
            .map_err(|e| crate::Error::from(e).context("Failed to encode side input key"))?;
        Ok(self
            .reader
            .get_multimap(tag, &self.window, &key)?
            .map(move |bytes| decode_side_input(&bytes?, tag)))
    }

    /// The first value for `key`, or `None` if the key is absent.
    pub fn get(&self, key: &K) -> crate::Result<Option<V>> {
        self.get_all(key)?.next().transpose()
    }
}

fn decode_side_input<S: DefaultCoder>(bytes: &[u8], tag: &str) -> crate::Result<S> {
    S::decode(bytes)
        .map_err(|e| crate::Error::from(e).context(format!("Failed to decode side input '{tag}'")))
}

mod sealed {
    pub trait Sealed {}
}

impl sealed::Sealed for Singleton {}
impl sealed::Sealed for Iter {}
impl sealed::Sealed for Multimap {}

/// Side input views and the values read from them: [`SingletonView<T>`] gives `T`,
/// [`IterView<T>`] a [`SideIter`], [`MultimapView<K, V>`] a [`SideMap`]. Tuples (up to six),
/// arrays and `Vec`s give their values in the same shape. Sealed.
pub trait SideInputs: sealed::Sealed + Clone + Send + Sync + 'static {
    /// What the function reads in the current window.
    type Values<'s>
    where
        Self: 's;

    /// The type-erased views to wire into the transform graph.
    fn views(&self) -> Vec<&dyn AnySideInput>;

    /// Reads the values in the window of `scope`.
    #[doc(hidden)]
    fn read<'s>(&'s self, scope: SideInputScope<'s>) -> crate::Result<Self::Values<'s>>;
}

impl sealed::Sealed for () {}

impl SideInputs for () {
    type Values<'s> = ();

    fn views(&self) -> Vec<&dyn AnySideInput> {
        Vec::new()
    }

    #[inline]
    fn read<'s>(&'s self, _scope: SideInputScope<'s>) -> crate::Result<()> {
        Ok(())
    }
}

impl<T, Kind> sealed::Sealed for PCollectionView<T, Kind> {}

impl<T: DefaultCoder> SideInputs for SingletonView<T> {
    type Values<'s> = T;

    fn views(&self) -> Vec<&dyn AnySideInput> {
        vec![self]
    }

    fn read<'s>(&'s self, scope: SideInputScope<'s>) -> crate::Result<T> {
        let (reader, window) = self.locate(scope)?;
        let mut elements = reader.get_iterable(&self.tag, &window)?;
        let Some(single) = elements.next().transpose()? else {
            return Err(format!("Empty singleton side input for tag '{}'", self.tag).into());
        };
        match elements.next().transpose()? {
            None => decode_side_input(&single, &self.tag),
            Some(_) => Err(format!(
                "PCollection with more than one element accessed as a singleton for tag '{}'",
                self.tag
            )
            .into()),
        }
    }
}

impl<T: DefaultCoder> SideInputs for IterView<T> {
    type Values<'s> = SideIter<'s, T>;

    fn views(&self) -> Vec<&dyn AnySideInput> {
        vec![self]
    }

    fn read<'s>(&'s self, scope: SideInputScope<'s>) -> crate::Result<SideIter<'s, T>> {
        let (reader, window) = self.locate(scope)?;
        Ok(SideIter {
            reader,
            tag: &self.tag,
            window,
            _marker: PhantomData,
        })
    }
}

impl<K: DefaultCoder, V: DefaultCoder> SideInputs for MultimapView<K, V>
where
    (K, V): DefaultCoder,
{
    type Values<'s> = SideMap<'s, K, V>;

    fn views(&self) -> Vec<&dyn AnySideInput> {
        vec![self]
    }

    fn read<'s>(&'s self, scope: SideInputScope<'s>) -> crate::Result<SideMap<'s, K, V>> {
        let (reader, window) = self.locate(scope)?;
        Ok(SideMap {
            reader,
            tag: &self.tag,
            window,
            _marker: PhantomData,
        })
    }
}

impl<S: SideInputs> sealed::Sealed for Vec<S> {}

impl<S: SideInputs> SideInputs for Vec<S> {
    type Values<'s> = Vec<S::Values<'s>>;

    fn views(&self) -> Vec<&dyn AnySideInput> {
        self.iter().flat_map(SideInputs::views).collect()
    }

    fn read<'s>(&'s self, scope: SideInputScope<'s>) -> crate::Result<Self::Values<'s>> {
        self.iter().map(|views| views.read(scope)).collect()
    }
}

impl<S: SideInputs, const N: usize> sealed::Sealed for [S; N] {}

impl<S: SideInputs, const N: usize> SideInputs for [S; N] {
    type Values<'s> = [S::Values<'s>; N];

    fn views(&self) -> Vec<&dyn AnySideInput> {
        self.iter().flat_map(SideInputs::views).collect()
    }

    fn read<'s>(&'s self, scope: SideInputScope<'s>) -> crate::Result<Self::Values<'s>> {
        let values = self
            .iter()
            .map(|views| views.read(scope))
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(values
            .try_into()
            .unwrap_or_else(|_| unreachable!("one value per view of the array")))
    }
}

/// Owned or borrowed views (or groups of them), converted to the owned [`SideInputs`].
pub trait IntoSideInputs {
    /// The owned [`SideInputs`] representation stored in the transform.
    type Owned: SideInputs;

    /// Clones borrowed view handles if needed to produce [`Self::Owned`].
    fn into_side_inputs(self) -> Self::Owned;
}

impl IntoSideInputs for () {
    type Owned = ();

    fn into_side_inputs(self) {}
}

impl<T, Kind> IntoSideInputs for PCollectionView<T, Kind>
where
    Self: SideInputs,
{
    type Owned = Self;

    fn into_side_inputs(self) -> Self {
        self
    }
}

impl<T, Kind> IntoSideInputs for &PCollectionView<T, Kind>
where
    PCollectionView<T, Kind>: SideInputs,
{
    type Owned = PCollectionView<T, Kind>;

    fn into_side_inputs(self) -> Self::Owned {
        self.clone()
    }
}

impl<S: IntoSideInputs> IntoSideInputs for Vec<S> {
    type Owned = Vec<S::Owned>;

    fn into_side_inputs(self) -> Self::Owned {
        self.into_iter()
            .map(IntoSideInputs::into_side_inputs)
            .collect()
    }
}

impl<S: IntoSideInputs, const N: usize> IntoSideInputs for [S; N] {
    type Owned = [S::Owned; N];

    fn into_side_inputs(self) -> Self::Owned {
        self.map(IntoSideInputs::into_side_inputs)
    }
}

macro_rules! impl_tuple_side_inputs {
    ($($T:ident : $idx:tt),+) => {
        impl<$($T: SideInputs),+> sealed::Sealed for ($($T,)+) {}

        impl<$($T: SideInputs),+> SideInputs for ($($T,)+) {
            type Values<'s> = ($($T::Values<'s>,)+);

            fn views(&self) -> Vec<&dyn AnySideInput> {
                let mut views = Vec::new();
                $(views.extend(self.$idx.views());)+
                views
            }

            fn read<'s>(&'s self, scope: SideInputScope<'s>) -> crate::Result<Self::Values<'s>> {
                Ok(($(self.$idx.read(scope)?,)+))
            }
        }

        impl<$($T: IntoSideInputs),+> IntoSideInputs for ($($T,)+) {
            type Owned = ($($T::Owned,)+);

            fn into_side_inputs(self) -> Self::Owned {
                ($(self.$idx.into_side_inputs(),)+)
            }
        }
    };
}

impl_tuple_side_inputs!(A: 0);
impl_tuple_side_inputs!(A: 0, B: 1);
impl_tuple_side_inputs!(A: 0, B: 1, C: 2);
impl_tuple_side_inputs!(A: 0, B: 1, C: 2, D: 3);
impl_tuple_side_inputs!(A: 0, B: 1, C: 2, D: 3, E: 4);
impl_tuple_side_inputs!(A: 0, B: 1, C: 2, D: 3, E: 4, F: 5);

/// How a view finds its side input window, as serialized.
#[derive(serde::Serialize, serde::Deserialize)]
enum WireWindowing {
    /// The `window_mapping_fn` of the view.
    Mapping { urn: String, payload: Vec<u8> },
    /// The side PCollection's windowing is unsupported; pipeline validation reports it.
    Unsupported(String),
}

/// The serialized form of a view. `kind` and `element_type` are checked on read, so a type
/// mismatch fails when the function is built, not when an element is decoded.
#[derive(serde::Serialize, serde::Deserialize)]
struct WireView {
    tag: String,
    pcollection_id: String,
    coder_id: String,
    kind: SideInputKind,
    element_type: String,
    windowing: WireWindowing,
}

impl<T, Kind: ViewKind> serde::Serialize for PCollectionView<T, Kind> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let windowing = match &self.windowing {
            Ok(windowing) => {
                let spec = windowing.to_proto();
                WireWindowing::Mapping {
                    urn: spec.urn,
                    payload: spec.payload,
                }
            }
            Err(reason) => WireWindowing::Unsupported(reason.clone()),
        };
        WireView {
            tag: self.tag.clone(),
            pcollection_id: self.pcollection_id.clone(),
            coder_id: self.coder_id.clone(),
            kind: Kind::KIND,
            // Rust does not guarantee stability of type_name across compiler releases.
            // This check assumes launcher and worker run the same compiled binary.
            element_type: std::any::type_name::<T>().to_string(),
            windowing,
        }
        .serialize(serializer)
    }
}

impl<'de, T, Kind: ViewKind> serde::Deserialize<'de> for PCollectionView<T, Kind> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        let wire = WireView::deserialize(deserializer)?;
        if wire.kind != Kind::KIND {
            return Err(D::Error::custom(format!(
                "side input '{}' is a {:?} view, read as a {:?} view",
                wire.tag,
                wire.kind,
                Kind::KIND
            )));
        }
        // Verify the element type. Type name stability requires identical binaries.
        let element_type = std::any::type_name::<T>();
        if wire.element_type != element_type {
            return Err(D::Error::custom(format!(
                "side input '{}' holds `{}`, read as `{element_type}`",
                wire.tag, wire.element_type
            )));
        }
        let windowing = match wire.windowing {
            WireWindowing::Mapping { urn, payload } => {
                SideInputWindowing::from_proto(&proto::FunctionSpec { urn, payload })
            }
            WireWindowing::Unsupported(reason) => Err(reason),
        };
        Ok(Self {
            tag: wire.tag,
            pcollection_id: wire.pcollection_id,
            coder_id: wire.coder_id,
            windowing,
            _marker: PhantomData,
        })
    }
}

impl<T: DefaultCoder> PCollection<T> {
    /// A view read through `Kind`. The tag derives from the PCollection id, so two views of one
    /// collection share a materialization.
    fn view<Kind: ViewKind>(&self) -> PCollectionView<T, Kind> {
        let pipeline = self.pipeline();
        let p = pipeline.lock();
        let windowing = p
            .components
            .pcollections
            .get(self.id())
            .and_then(|pc| {
                p.components
                    .windowing_strategies
                    .get(&pc.windowing_strategy_id)
            })
            .and_then(|ws| ws.window_fn.as_ref())
            .ok_or_else(|| format!("PCollection '{}' has no window function", self.id()))
            .and_then(SideInputWindowing::of);
        drop(p);

        PCollectionView {
            tag: format!("side_{}", self.id()),
            pcollection_id: self.id().to_string(),
            coder_id: self.coder_id().to_string(),
            windowing,
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
        self.view()
    }
}
