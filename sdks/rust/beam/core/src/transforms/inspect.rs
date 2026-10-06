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

//! Pass-through inspection for debugging and logging.

use std::sync::Arc;

use super::{ClosureFn, PTransform, ParDo};
use crate::coders::DefaultCoder;
use crate::values::PCollection;

/// Runs a side-effecting function on each element and passes it through unchanged.
pub struct Inspect<T> {
    name: String,
    func: Arc<dyn Fn(&T) + Send + Sync>,
}

impl<T> Inspect<T> {
    pub fn new<F>(name: impl Into<String>, func: F) -> Self
    where
        F: Fn(&T) + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            func: Arc::new(func),
        }
    }
}

impl<T: DefaultCoder> PTransform<PCollection<T>> for Inspect<T> {
    type Output = PCollection<T>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<T> {
        let func = Arc::clone(&self.func);
        input.apply(ParDo::new(
            self.name.clone(),
            ClosureFn::new("Inspect", move |element, out| {
                func(&element);
                out.emit(element)
            }),
        ))
    }
}
