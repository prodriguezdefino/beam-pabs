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

//! Groups of views that a function reads: single views, tuples, arrays and `Vec`s.

use std::marker::PhantomData;

use super::access::{SideInputScope, SideIter, SideMap, decode_side_input};
use super::{AnySideInput, IterView, MultimapView, PCollectionView, SingletonView, sealed};
use crate::coders::DefaultCoder;

/// Side input views and the values read from them: [`SingletonView<T>`] gives `T`,
/// [`IterView<T>`] a [`SideIter`], [`MultimapView<K, V>`] a [`SideMap`]. Tuples of 2 to 6
/// views, arrays and `Vec`s give their values in the same shape. Sealed.
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
            None => decode_side_input(
                &single,
                &self.tag,
                self.row_schema.as_ref(),
                &self.field_map,
            ),
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
            row_schema: self.row_schema.as_ref(),
            field_map: &self.field_map,
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
            row_schema: self.row_schema.as_ref(),
            field_map: &self.field_map,
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

impl_tuple_side_inputs!(A: 0, B: 1);
impl_tuple_side_inputs!(A: 0, B: 1, C: 2);
impl_tuple_side_inputs!(A: 0, B: 1, C: 2, D: 3);
impl_tuple_side_inputs!(A: 0, B: 1, C: 2, D: 3, E: 4);
impl_tuple_side_inputs!(A: 0, B: 1, C: 2, D: 3, E: 4, F: 5);
