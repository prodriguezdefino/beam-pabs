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
use std::sync::atomic::{AtomicU32, Ordering};

use beam::coders::{BeamIterable, DefaultCoder};
use beam::transforms::{DoFn, ProcessContext};

use super::writer::{OpenFile, WriterConfig};

/// Spreads elements round-robin over the fixed shards: the key function of the `WithKeys`
/// step. Public only for tests.
#[doc(hidden)]
pub struct RoundRobinShards {
    num_shards: u32,
    next: AtomicU32,
}

impl RoundRobinShards {
    /// Spreads over `num_shards` shards. Clamps `num_shards` to `1..=i32::MAX`.
    pub fn new(num_shards: u32) -> Self {
        let num_shards = num_shards.clamp(1, i32::MAX.unsigned_abs());
        // Start at a random shard so small bundles on many workers do not all hit shard 0.
        let start = (RandomState::new().build_hasher().finish() % u64::from(num_shards)) as u32;
        Self {
            num_shards,
            next: AtomicU32::new(start),
        }
    }

    /// The shard of the next element.
    pub fn next_shard(&self) -> i32 {
        let shard = self.next.fetch_add(1, Ordering::Relaxed) % self.num_shards;
        i32::try_from(shard).expect("num_shards is at most i32::MAX")
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
        let mut sequence: u32 = 0;
        let mut file: Option<OpenFile<T>> = None;
        let shard_idx = u32::try_from(shard).ok();
        for element in elements.try_into_iter() {
            let element = element.map_err(|e| {
                beam::Error::from(e).context(format!("Failed to read shard {shard}"))
            })?;
            let current = match file.as_mut() {
                Some(current) => current,
                None => file.insert(self.writer.open(shard_idx, sequence)?),
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
