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

//! The native Kafka connector, on librdkafka, with the `native` feature.
//!
//! [`read`] reads the records of a topic into rows, and [`write`] writes rows to a topic.
//! [`format`] maps record values to rows. [`client`] holds the connection to a cluster, and
//! [`auth`] the Google authentication of Google Cloud Managed Service for Apache Kafka.

pub mod auth;
pub mod client;
pub mod format;
pub mod read;
pub mod write;

use std::collections::{HashMap, HashSet};

use beam::internals::PipelineInternals;
use beam::pipeline::Pipeline;

fn transform_ids(pipeline: &Pipeline) -> HashSet<String> {
    pipeline
        .lock()
        .components
        .transforms
        .keys()
        .cloned()
        .collect()
}

/// Groups the transforms added since `existing` under the composite `name`.
fn add_composite(
    pipeline: &Pipeline,
    name: &str,
    existing: &HashSet<String>,
    outputs: HashMap<String, String>,
) {
    let children = {
        let lock = pipeline.lock();
        let transforms = &lock.components.transforms;
        let added: Vec<&String> = transforms
            .keys()
            .filter(|id| !existing.contains(*id))
            .collect();
        let nested: HashSet<&String> = added
            .iter()
            .filter_map(|id| transforms.get(*id))
            .flat_map(|t| &t.subtransforms)
            .collect();
        let mut children: Vec<String> = added
            .into_iter()
            .filter(|id| !nested.contains(id))
            .cloned()
            .collect();
        children.sort();
        children
    };
    pipeline.add_composite_transform(name, None, Vec::new(), HashMap::new(), outputs, children);
}
