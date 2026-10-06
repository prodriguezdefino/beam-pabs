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

//! Pairs each element with a key that a function computes from the element.

use std::marker::PhantomData;
use std::sync::Arc;

use super::{ClosureFn, PTransform, ParDo};
use crate::coders::DefaultCoder;
use crate::values::PCollection;

/// Pairs each element with the key that a function returns for it.
///
/// The output coder is the key coder and the input coder, so an element keeps its coder and
/// a [`Row`](crate::schema::Row) keeps its schema through a later
/// [`GroupByKey`](crate::transforms::GroupByKey).
///
/// ```
/// use beam::prelude::*;
/// use beam::transforms::WithKeys;
///
/// # let p = Pipeline::default();
/// let words = p.apply(Create::new("Words", vec!["apple".to_string(), "bean".to_string()]));
/// let by_length = words.apply(WithKeys::new("ByLength", |word: &String| word.len() as i64));
/// # let _ = by_length;
/// ```
pub struct WithKeys<T, K, F> {
    name: String,
    key_fn: Arc<F>,
    _marker: PhantomData<fn(&T) -> K>,
}

impl<T, K, F> Clone for WithKeys<T, K, F> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            key_fn: Arc::clone(&self.key_fn),
            _marker: PhantomData,
        }
    }
}

impl<T, K, F> WithKeys<T, K, F>
where
    F: Fn(&T) -> K + Send + Sync + 'static,
{
    /// Creates a transform that extracts a key from each element with `key_fn`.
    pub fn new(name: impl Into<String>, key_fn: F) -> Self {
        Self {
            name: name.into(),
            key_fn: Arc::new(key_fn),
            _marker: PhantomData,
        }
    }
}

impl<T: DefaultCoder, K: DefaultCoder, F> PTransform<PCollection<T>> for WithKeys<T, K, F>
where
    F: Fn(&T) -> K + Send + Sync + 'static,
{
    type Output = PCollection<(K, T)>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<(K, T)> {
        let key_fn = Arc::clone(&self.key_fn);
        let keyed = input.apply(ParDo::new(
            self.name.clone(),
            ClosureFn::new("WithKeys", move |element: T, out| {
                out.emit((key_fn(&element), element))
            }),
        ));
        match input.registered_coder_id() {
            Some(value) => {
                let pipeline = input.pipeline();
                let key = K::register_coder(pipeline);
                keyed.with_coder_id(pipeline.register_kv_coder(key, value.to_string()))
            }
            None => keyed,
        }
    }
}
