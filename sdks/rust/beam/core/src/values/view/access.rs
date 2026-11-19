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

//! Reads of side input values in the window of the main input element.

use std::marker::PhantomData;
use std::sync::Arc;

use super::windowing::MappedWindow;
use super::{FieldMapSlot, PCollectionView, ViewKind, ViewWindowing};
use crate::coders::{CoderError, DefaultCoder};
use crate::schema::Schema;
use crate::transforms::dofn::side_input::SideInputReader;

impl<T, Kind: ViewKind> PCollectionView<T, Kind> {
    /// The reader and the side input window for the main window of `scope`.
    pub(super) fn locate<'s>(
        &self,
        scope: SideInputScope<'s>,
    ) -> crate::Result<(&'s dyn SideInputReader, MappedWindow<'s>)> {
        let reader = scope.reader.ok_or_else(|| {
            format!(
                "Side input '{}' is not declared on this transform: return its view from \
                 `side_inputs()`, or pass it to `from_fn_with_side_inputs`",
                self.tag
            )
        })?;
        let window = match &self.windowing {
            ViewWindowing::Known(windowing) => windowing.map_window(scope.window)?,
            ViewWindowing::FromRunner => {
                reader.window_mapping(&self.tag)?.map_window(scope.window)?
            }
            ViewWindowing::Unsupported(reason) => return Err(reason.clone().into()),
        };
        Ok((reader, window))
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

/// The value of an [`IterView`](super::IterView): the elements of the side input window, read
/// lazily.
pub struct SideIter<'s, T> {
    pub(super) reader: &'s dyn SideInputReader,
    pub(super) tag: &'s str,
    pub(super) window: MappedWindow<'s>,
    pub(super) row_schema: Option<&'s Arc<Schema>>,
    pub(super) field_map: &'s FieldMapSlot,
    pub(super) _marker: PhantomData<fn() -> T>,
}

impl<'s, T: DefaultCoder> SideIter<'s, T> {
    /// The elements, fetched as the iterator advances. Each call starts over.
    pub fn iter(&self) -> crate::Result<impl Iterator<Item = crate::Result<T>> + 's> {
        let (tag, schema, field_map) = (self.tag, self.row_schema, self.field_map);
        Ok(self
            .reader
            .get_iterable(tag, &self.window)?
            .map(move |bytes| decode_side_input(&bytes?, tag, schema, field_map)))
    }

    /// All elements, collected.
    pub fn to_vec(&self) -> crate::Result<Vec<T>> {
        self.iter()?.collect()
    }
}

/// The value of a [`MultimapView`](super::MultimapView): lookups by key in the side input
/// window.
pub struct SideMap<'s, K, V> {
    pub(super) reader: &'s dyn SideInputReader,
    pub(super) tag: &'s str,
    pub(super) window: MappedWindow<'s>,
    pub(super) row_schema: Option<&'s Arc<Schema>>,
    pub(super) field_map: &'s FieldMapSlot,
    pub(super) key_row_schema: Option<&'s Arc<Schema>>,
    pub(super) key_field_map: &'s FieldMapSlot,
    pub(super) _marker: PhantomData<fn() -> (K, V)>,
}

impl<'s, K: DefaultCoder, V: DefaultCoder> SideMap<'s, K, V> {
    /// All values for `key`, fetched as the iterator advances.
    pub fn get_all(&self, key: &K) -> crate::Result<impl Iterator<Item = crate::Result<V>> + 's> {
        let key = key
            .encode()
            .map_err(|e| crate::Error::from(e).context("Failed to encode side input key"))?;
        self.values(&key)
    }

    /// The first value for `key`, or `None` if the key is absent.
    pub fn get(&self, key: &K) -> crate::Result<Option<V>> {
        self.get_all(key)?.next().transpose()
    }

    /// The keys of the window, fetched as the iterator advances. Each call starts over.
    pub fn keys(&self) -> crate::Result<impl Iterator<Item = crate::Result<K>> + 's> {
        let (tag, schema, field_map) = (self.tag, self.key_row_schema, self.key_field_map);
        Ok(self
            .reader
            .get_multimap_keys(tag, &self.window)?
            .map(move |bytes| decode_side_input(&bytes?, tag, schema, field_map)))
    }

    /// Each key of the window with all its values. The keys come as the iterator advances.
    pub fn entries(&self) -> crate::Result<impl Iterator<Item = crate::Result<(K, Vec<V>)>> + '_> {
        self.entries_with(|map, key| map.values(key)?.collect())
    }

    /// Each key of the window with the result of `value` for its SDK-encoded bytes.
    fn entries_with<'m, T>(
        &'m self,
        value: impl Fn(&'m Self, &[u8]) -> crate::Result<T> + 'm,
    ) -> crate::Result<impl Iterator<Item = crate::Result<(K, T)>> + 'm> {
        let (tag, schema, field_map) = (self.tag, self.key_row_schema, self.key_field_map);
        Ok(self
            .reader
            .get_multimap_keys(tag, &self.window)?
            .map(move |bytes| {
                let bytes = bytes?;
                let key = decode_side_input(&bytes, tag, schema, field_map)?;
                Ok((key, value(self, &bytes)?))
            }))
    }

    /// The values for the SDK-encoded `key`.
    fn values(
        &self,
        key: &[u8],
    ) -> crate::Result<impl Iterator<Item = crate::Result<V>> + use<'s, K, V>> {
        let (tag, schema, field_map) = (self.tag, self.row_schema, self.field_map);
        Ok(self
            .reader
            .get_multimap(tag, &self.window, key)?
            .map(move |bytes| decode_side_input(&bytes?, tag, schema, field_map)))
    }

    /// The only value for the SDK-encoded `key`, or `None` if the key is absent. A second value
    /// is an error.
    fn unique(&self, key: &[u8]) -> crate::Result<Option<V>> {
        let mut values = self.values(key)?;
        let Some(first) = values.next().transpose()? else {
            return Ok(None);
        };
        match values.next() {
            None => Ok(Some(first)),
            Some(_) => Err(format!(
                "Side input '{}' has more than one value for a key: read it with `as_multimap`",
                self.tag
            )
            .into()),
        }
    }
}

/// The value of a [`MapView`](super::MapView): lookups of the one value for a key in the side
/// input window. A read of a key with more than one value fails.
pub struct SideDict<'s, K, V> {
    pub(super) map: SideMap<'s, K, V>,
}

impl<'s, K: DefaultCoder, V: DefaultCoder> SideDict<'s, K, V> {
    /// The value for `key`, or `None` if the key is absent.
    pub fn get(&self, key: &K) -> crate::Result<Option<V>> {
        let key = key
            .encode()
            .map_err(|e| crate::Error::from(e).context("Failed to encode side input key"))?;
        self.map.unique(&key)
    }

    /// The keys of the window, fetched as the iterator advances. Each call starts over.
    pub fn keys(&self) -> crate::Result<impl Iterator<Item = crate::Result<K>> + 's> {
        self.map.keys()
    }

    /// Each key of the window with its value. The keys come as the iterator advances.
    pub fn entries(&self) -> crate::Result<impl Iterator<Item = crate::Result<(K, V)>> + '_> {
        self.map.entries_with(|map, key| {
            map.unique(key)?.ok_or_else(|| {
                format!("Side input '{}' lists a key that has no value", map.tag).into()
            })
        })
    }
}

/// Decodes one side input value. `schema` is the schema of the rows in it, if it has rows.
pub(super) fn decode_side_input<S: DefaultCoder>(
    bytes: &[u8],
    tag: &str,
    schema: Option<&Arc<Schema>>,
    field_map: &FieldMapSlot,
) -> crate::Result<S> {
    let map = match schema {
        Some(s) => field_map
            .get_or_init(|| S::resolve_field_map(s))
            .as_ref()
            .map_err(|e| {
                crate::Error::from(CoderError::Schema(e.clone()))
                    .context(format!("Failed to decode side input '{tag}'"))
            })?
            .as_ref(),
        None => None,
    };
    S::decode_with_context(bytes, schema, map, None)
        .map_err(|e| crate::Error::from(e).context(format!("Failed to decode side input '{tag}'")))
}
