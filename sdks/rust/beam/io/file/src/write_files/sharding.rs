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

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;

use beam::coders::{BeamIterable, DefaultCoder};
use beam::transforms::{DoFn, ProcessContext};

use super::writer::{OpenFile, WriterConfig};

/// Spreads elements round-robin over the fixed shards. Public only for tests.
#[doc(hidden)]
pub struct AssignShardFn<T> {
    num_shards: u32,
    next: u32,
    _marker: std::marker::PhantomData<fn(T)>,
}

impl<T> AssignShardFn<T> {
    pub fn new(num_shards: u32) -> Self {
        // Start at a random shard so small bundles on many workers do not all hit shard 0.
        let start = (RandomState::new().build_hasher().finish() % u64::from(num_shards)) as u32;
        Self {
            num_shards,
            next: start,
            _marker: std::marker::PhantomData,
        }
    }
}

/// Each copy starts at its own random shard, like a separately deserialized instance.
impl<T> Clone for AssignShardFn<T> {
    fn clone(&self) -> Self {
        Self::new(self.num_shards)
    }
}

impl<T: DefaultCoder> DoFn for AssignShardFn<T> {
    type In = T;
    type Out = (i32, T);

    fn process_element(&mut self, element: T, out: &mut ProcessContext<(i32, T)>) -> beam::Result {
        let shard = self.next % self.num_shards;
        self.next = self.next.wrapping_add(1);
        let shard =
            i32::try_from(shard).map_err(|_| format!("Shard index {shard} overflows i32"))?;
        out.emit((shard, element))
    }
}

/// Writes each fixed shard of each window to its own temporary file(s).
pub(super) struct WriteShardsFn<T> {
    pub(super) writer: Arc<WriterConfig<T>>,
}

impl<T> Clone for WriteShardsFn<T> {
    fn clone(&self) -> Self {
        Self {
            writer: Arc::clone(&self.writer),
        }
    }
}

impl<T: DefaultCoder> DoFn for WriteShardsFn<T> {
    type In = (i32, BeamIterable<T>);
    type Out = Vec<u8>;

    fn process_element(
        &mut self,
        (shard, elements): (i32, BeamIterable<T>),
        out: &mut ProcessContext<Vec<u8>>,
    ) -> beam::Result {
        let mut sequence = 0;
        let mut file: Option<OpenFile<T>> = None;
        for element in elements.try_into_iter() {
            let element = element.map_err(|e| {
                beam::Error::from(e).context(format!("Failed to read shard {shard}"))
            })?;
            let current = match file.as_mut() {
                Some(current) => current,
                None => file.insert(self.writer.open(shard, sequence)?),
            };
            current.write(&element)?;
            if self.writer.is_full(current) {
                if let Some(full) = file.take() {
                    out.emit(full.finish()?)?;
                }
                sequence += 1;
            }
        }
        if let Some(last) = file {
            out.emit(last.finish()?)?;
        }
        Ok(())
    }
}
