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

//! Query 14 (`RESHUFFLE`): one redistribution barrier over a fixed number of key buckets.

use beam::prelude::*;

use crate::config::NexmarkConfig;
use crate::model::Event;

/// Redistributes [`Event`] records through one reshuffle with `num_key_buckets` shard keys, as
/// Java's `Reshuffle.viaRandomKey().withNumBuckets(numKeyBuckets)`.
#[derive(Clone, Copy, Debug)]
pub struct Query14 {
    num_key_buckets: u32,
}

impl Query14 {
    /// Creates a reshuffle over `num_key_buckets` shard keys (at least 1).
    pub fn new(num_key_buckets: i32) -> Self {
        Self {
            num_key_buckets: u32::try_from(num_key_buckets).unwrap_or(1).max(1),
        }
    }

    /// Creates a reshuffle configured from `config`.
    pub fn from_config(config: &NexmarkConfig) -> Self {
        Self::new(config.num_key_buckets)
    }
}

impl PTransform<PCollection<Event>> for Query14 {
    type Output = PCollection<Event>;

    fn expand(&self, input: &PCollection<Event>) -> PCollection<Event> {
        input.apply(
            "Reshuffle",
            Reshuffle::new().with_num_buckets(self.num_key_buckets),
        )
    }
}
