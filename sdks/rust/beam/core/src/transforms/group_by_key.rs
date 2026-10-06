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

//! Distributed grouping of values by key.

use std::collections::HashMap;
use std::marker::PhantomData;

use super::{DisplayDataBuilder, HasDisplayData, PTransform};
use crate::coders::{BeamIterable, DefaultCoder};
use crate::pipeline::URN_GROUP_BY_KEY;
use crate::values::{IsBounded, PCollection};

/// Collects all values of a key into a single element.
///
/// A Beam primitive: the runner does the shuffle, so there is no handler to register.
/// The values are a [`BeamIterable<V>`]. A group larger than the runner buffers can arrive
/// as a state-backed iterable (`beam:coder:state_backed_iterable:v1`), read lazily.
pub struct GroupByKey<K, V> {
    name: String,
    _marker: PhantomData<(K, V)>,
}

impl<K, V> GroupByKey<K, V> {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _marker: PhantomData,
        }
    }
}

impl<K, V> HasDisplayData for GroupByKey<K, V> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", "GroupByKey");
        builder.add_text("name", &self.name);
    }
}

impl<K, V> PTransform<PCollection<(K, V)>> for GroupByKey<K, V>
where
    K: DefaultCoder,
    V: DefaultCoder,
{
    type Output = PCollection<(K, BeamIterable<V>)>;

    fn expand(&self, input: &PCollection<(K, V)>) -> PCollection<(K, BeamIterable<V>)> {
        let pipeline = input.pipeline();
        let kv_out_coder_id = <(K, BeamIterable<V>)>::register_coder(pipeline);

        let in_ws_id = input.windowing_strategy_id();
        let out_ws_id = {
            let mut lock = pipeline.lock();
            lock.components
                .windowing_strategies
                .get(&in_ws_id)
                .filter(|s| {
                    s.merge_status == model::pipeline::merge_status::Enum::NeedsMerge as i32
                })
                .cloned()
                .map_or(in_ws_id, |mut s| {
                    s.merge_status = model::pipeline::merge_status::Enum::AlreadyMerged as i32;
                    lock.register_windowing_strategy(s)
                })
        };

        let out_pcoll = pipeline.add_pcollection_with_windowing::<(K, BeamIterable<V>)>(
            &format!("{}_out", self.name),
            &kv_out_coder_id,
            IsBounded::Bounded,
            &out_ws_id,
        );

        let inputs = HashMap::from([("in".to_string(), input.id().to_string())]);
        let outputs = HashMap::from([("out".to_string(), out_pcoll.id().to_string())]);

        let mut builder = DisplayDataBuilder::with_namespace(self.name.clone());
        self.populate_display_data(&mut builder);

        pipeline.add_transform_with_display_data(
            &self.name,
            URN_GROUP_BY_KEY,
            Vec::new(),
            inputs,
            outputs,
            builder.into_proto(),
        );

        out_pcoll
    }
}
