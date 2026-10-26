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

//! Removing duplicate elements.

use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::{CombineFn, CombinePerKey, Map, PTransform};
use crate::coders::DefaultCoder;
use crate::values::PCollection;

/// Keeps one element of each group of equal elements, in each window.
///
/// Two elements are equal when their encodings are equal. Uses [`CombinePerKey`], so each
/// worker removes duplicates before the shuffle. To compare elements by a part of them, use
/// [`DistinctBy`].
pub struct Distinct<T> {
    _marker: PhantomData<fn() -> T>,
}

impl<T> Distinct<T> {
    /// Creates a transform that removes duplicate elements.
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> Default for Distinct<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> PTransform<PCollection<T>> for Distinct<T>
where
    T: DefaultCoder + Eq + Hash,
{
    type Output = PCollection<T>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<T> {
        input
            .apply("PairWithUnit", Map::new(|element: T| (element, ())))
            .apply("Combine", CombinePerKey::new(KeepOne))
            .apply("Keys", Map::new(|(element, ()): (T, ())| element))
    }
}

/// Keeps one element for each value of a key function, in each window. Which element of a
/// group it keeps is not defined.
pub struct DistinctBy<T, K> {
    key_fn: Arc<dyn Fn(&T) -> K + Send + Sync>,
}

impl<T, K> DistinctBy<T, K> {
    /// Creates a transform that keeps one element for each value of `key_fn`.
    pub fn new<F>(key_fn: F) -> Self
    where
        F: Fn(&T) -> K + Send + Sync + 'static,
    {
        Self {
            key_fn: Arc::new(key_fn),
        }
    }
}

impl<T, K> PTransform<PCollection<T>> for DistinctBy<T, K>
where
    T: DefaultCoder,
    K: DefaultCoder + Eq + Hash,
{
    type Output = PCollection<T>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<T> {
        let key_fn = Arc::clone(&self.key_fn);
        input
            .apply(
                "KeyByRepresentative",
                Map::new(move |element: T| (key_fn(&element), element)),
            )
            .apply("Combine", CombinePerKey::new(AnyValue::default()))
            .apply("Values", Map::new(|(_, element): (K, T)| element))
    }
}

/// The combine function of [`Distinct`]: the values are all `()`, so it keeps one.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
struct KeepOne;

impl CombineFn for KeepOne {
    type Input = ();
    type Accum = ();
    type Output = ();

    fn create_accumulator(&self) -> crate::Result {
        Ok(())
    }

    fn add_input(&self, (): (), (): ()) -> crate::Result {
        Ok(())
    }

    fn merge_accumulators(&self, _: Vec<()>) -> crate::Result {
        Ok(())
    }

    fn extract_output(&self, (): ()) -> crate::Result {
        Ok(())
    }
}

crate::register!(KeepOne, id = "beam::transforms::Distinct");

/// The combine function of [`DistinctBy`]: keeps any one value of each key.
struct AnyValue<T>(PhantomData<fn() -> T>);

impl<T> Default for AnyValue<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T: DefaultCoder> CombineFn for AnyValue<T> {
    type Input = T;
    type Accum = Option<T>;
    type Output = T;

    fn create_accumulator(&self) -> crate::Result<Option<T>> {
        Ok(None)
    }

    fn add_input(&self, accumulator: Option<T>, input: T) -> crate::Result<Option<T>> {
        Ok(accumulator.or(Some(input)))
    }

    fn merge_accumulators(&self, accumulators: Vec<Option<T>>) -> crate::Result<Option<T>> {
        Ok(accumulators.into_iter().flatten().next())
    }

    fn extract_output(&self, accumulator: Option<T>) -> crate::Result<T> {
        accumulator.ok_or_else(|| crate::Error::msg("Distinct found a key without a value"))
    }
}
