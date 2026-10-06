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

//! Forest example: demonstrates recursive pipeline construction and `Flatten`.
//!
//! Pipeline construction is procedural Rust code. This example creates a pipeline
//! recursively with multiple disconnected trees (disconnected graph components).
//!
//! The output of each singleton leaf is flattened over recursive rounds.

use std::sync::atomic::{AtomicUsize, Ordering};

use beam::prelude::*;
use clap::Args;
use serde::{Deserialize, Serialize};

/// Command line arguments for the Forest example pipeline.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(name = "forest", about = "Apache Beam Rust Forest Example")]
pub struct ForestArgs {
    /// Number of trees to construct in the forest.
    #[arg(long, default_value_t = 2)]
    pub count: usize,

    /// Depth of each tree in the forest.
    #[arg(long, default_value_t = 3)]
    pub depth: usize,
}

impl PipelineOptionGroup for ForestArgs {}

/// Recursively builds a single tree of depth `depth` whose sub-branches are flattened together.
pub fn tree(
    pipeline: &Pipeline,
    depth: usize,
    tree_idx: usize,
    leaf_counter: &AtomicUsize,
) -> PCollection<i64> {
    if depth == 0 {
        return leaf(pipeline, tree_idx, leaf_counter);
    }

    let a = tree(pipeline, depth - 1, tree_idx, leaf_counter);
    let b = tree(pipeline, depth - 1, tree_idx, leaf_counter);
    let c = if depth >= 2 {
        tree(pipeline, depth - 2, tree_idx, leaf_counter)
    } else {
        leaf(pipeline, tree_idx, leaf_counter)
    };

    let flatten_name = format!(
        "Tree_{tree_idx}_Flatten_d{depth}_{}",
        leaf_counter.load(Ordering::SeqCst)
    );
    a.flatten(flatten_name, &[&b, &c])
}

/// Constructs a single leaf node emitting a unique integer.
pub fn leaf(pipeline: &Pipeline, tree_idx: usize, leaf_counter: &AtomicUsize) -> PCollection<i64> {
    let leaf_id = leaf_counter.fetch_add(1, Ordering::SeqCst) + 1;
    pipeline.apply(Create::new(
        format!("Tree_{tree_idx}_Leaf_{leaf_id}"),
        vec![leaf_id as i64],
    ))
}

/// Builds a forest composed of `count` independent trees of depth `depth`.
pub fn build_forest(pipeline: &Pipeline, count: usize, depth: usize) -> Vec<PCollection<i64>> {
    let leaf_counter = AtomicUsize::new(0);
    (0..count)
        .map(|i| {
            let root = tree(pipeline, depth, i, &leaf_counter);
            root.inspect(format!("LogTree_{i}"), move |val: &i64| {
                tracing::info!("Tree {i} leaf value: {val}");
            })
        })
        .collect()
}
