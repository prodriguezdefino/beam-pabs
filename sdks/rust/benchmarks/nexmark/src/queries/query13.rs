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

//! Query 13 (`PORTABILITY_BATCH`): sharded `GroupByKey` followed by sampled coder round-trips.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};

use beam::coders::{Coder, Context};
use beam::prelude::*;
use serde::{Deserialize, Serialize};

use crate::config::NexmarkConfig;
use crate::generator::JavaRandom;
use crate::model::{Event, EventCoder};

/// Shuffles events across key buckets and performs sampled [`EventCoder`] round-trips.
#[derive(Clone, Copy, Debug)]
pub struct Query13 {
    num_key_buckets: i32,
    pardo_cpu_factor: f64,
}

impl Query13 {
    /// Creates a portability batch benchmark transform with `num_key_buckets` shuffle shards.
    pub fn new(num_key_buckets: i32) -> Self {
        Self {
            num_key_buckets: num_key_buckets.max(1),
            pardo_cpu_factor: 1.0,
        }
    }

    /// Sets the fraction (`0.0..=1.0`; out-of-range values reset to `1.0`) of events round-tripped.
    pub fn with_pardo_cpu_factor(mut self, pardo_cpu_factor: f64) -> Self {
        self.pardo_cpu_factor = normalize_cpu_factor(pardo_cpu_factor);
        self
    }

    /// Creates a portability batch benchmark transform configured from `config`.
    pub fn from_config(config: &NexmarkConfig) -> Self {
        Self::new(config.num_key_buckets).with_pardo_cpu_factor(config.pardo_cpu_factor)
    }
}

impl PTransform<PCollection<Event>> for Query13 {
    type Output = PCollection<Event>;

    fn expand(&self, input: &PCollection<Event>) -> PCollection<Event> {
        input
            .par_do(
                "PairWithRandomKey",
                AssignShardFn::new(self.num_key_buckets),
            )
            .group_by_key("GroupByKey")
            .par_do_fn(
                "ExpandIterable",
                |(_shard, events): (i32, BeamIterable<Event>), ctx| {
                    events.into_iter().try_for_each(|event| ctx.emit(event?))
                },
            )
            .par_do("Serialize", SerializeEventFn::new(self.pardo_cpu_factor))
    }
}

fn normalize_cpu_factor(factor: f64) -> f64 {
    if (0.0..=1.0).contains(&factor) {
        factor
    } else {
        1.0
    }
}

fn random_seed_i64() -> i64 {
    RandomState::new().build_hasher().finish() as i64
}

fn random_shard() -> i32 {
    random_seed_i64() as i32
}

fn seeded_rng() -> JavaRandom {
    JavaRandom::new(random_seed_i64())
}

#[derive(Debug, Serialize, Deserialize, Register)]
struct AssignShardFn {
    buckets: i32,
    #[serde(skip, default = "random_shard")]
    next_shard: i32,
}

impl AssignShardFn {
    fn new(buckets: i32) -> Self {
        Self {
            buckets: buckets.max(1),
            next_shard: random_shard(),
        }
    }
}

impl Clone for AssignShardFn {
    fn clone(&self) -> Self {
        Self::new(self.buckets)
    }
}

impl DoFn for AssignShardFn {
    type In = Event;
    type Out = (i32, Event);

    fn process_element(
        &mut self,
        element: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let shard = self.next_shard.rem_euclid(self.buckets);
        self.next_shard = self.next_shard.wrapping_add(1);
        ctx.emit((shard, element))
    }
}

#[derive(Debug, Serialize, Deserialize, Register)]
struct SerializeEventFn {
    pardo_cpu_factor: f64,
    #[serde(skip)]
    buf: Vec<u8>,
    #[serde(skip, default = "seeded_rng")]
    rng: JavaRandom,
}

impl SerializeEventFn {
    fn new(pardo_cpu_factor: f64) -> Self {
        Self {
            pardo_cpu_factor: normalize_cpu_factor(pardo_cpu_factor),
            buf: Vec::new(),
            rng: seeded_rng(),
        }
    }
}

impl Clone for SerializeEventFn {
    fn clone(&self) -> Self {
        Self::new(self.pardo_cpu_factor)
    }
}

impl DoFn for SerializeEventFn {
    type In = Event;
    type Out = Event;

    fn process_element(
        &mut self,
        element: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        if self.rng.next_double() <= self.pardo_cpu_factor {
            self.buf.clear();
            EventCoder.encode(&element, &mut self.buf, Context::WholeStream)?;
            let byte_len = i64::try_from(self.buf.len()).unwrap_or(i64::MAX);
            Metrics::counter("Query13", "serde-bytes").inc_by(byte_len);
            let mut reader: &[u8] = &self.buf;
            let decoded = EventCoder.decode(&mut reader, Context::WholeStream)?;
            ctx.emit(decoded)
        } else {
            ctx.emit(element)
        }
    }
}
