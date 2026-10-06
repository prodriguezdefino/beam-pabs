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

//! Predicate-based element filtering.

use std::sync::Arc;

use super::{ClosureFn, PTransform, ParDo};
use crate::coders::DefaultCoder;
use crate::values::PCollection;

/// Keeps only the elements satisfying a predicate.
pub struct Filter<T> {
    name: String,
    predicate: Arc<dyn Fn(&T) -> bool + Send + Sync>,
}

impl<T> Filter<T> {
    pub fn new<F>(name: impl Into<String>, predicate: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            predicate: Arc::new(predicate),
        }
    }
}

impl<T: DefaultCoder> PTransform<PCollection<T>> for Filter<T> {
    type Output = PCollection<T>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<T> {
        let predicate = Arc::clone(&self.predicate);
        input.apply(ParDo::new(
            self.name.clone(),
            ClosureFn::new("Filter", move |element, out| {
                if predicate(&element) {
                    out.emit(element)?;
                }
                Ok(())
            }),
        ))
    }
}
