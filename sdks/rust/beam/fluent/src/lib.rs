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

//! Fluent method syntax for the Apache Beam Rust SDK.
//!
//! The extension traits add one method per core transform (`.group_by_key` applies
//! `GroupByKey::new`). Use `.apply(name, Transform::new(..))` for advanced configuration.
//!
//! | Trait | Collections | Methods |
//! |---|---|---|
//! | [`PCollectionExt`](prelude::PCollectionExt) | any | element-wise transforms, `ParDo`, side inputs, partitions, windows, batches |
//! | [`CombineExt`](prelude::CombineExt) | any | global combines and counts, and one shortcut per SDK combine function (`.sum_globally`, `.top_globally(n)`, ...) |
//! | [`PCollectionKeyedExt`](prelude::PCollectionKeyedExt) | `(K, V)` | grouping, cogrouping and joins |
//! | [`KeyedCombineExt`](prelude::KeyedCombineExt) | `(K, V)` | combines per key, and one shortcut per SDK combine function (`.sum_per_key`, `.top_per_key(n)`, ...) |
//! | [`SetsExt`](prelude::SetsExt) | any | `distinct`, `distinct_by` and the set operations (`union_*`, `intersect_*`, `except_*`) |
//! | [`PCollectionListExt`](prelude::PCollectionListExt) | lists | `Flatten` |

mod collection;
mod combine;
mod composite;
mod fold;
mod keyed;
mod keyed_combine;
mod list;
mod sets;

pub use fold::FoldCombineFn;

/// [`beam::prelude`] plus the fluent extension traits.
pub mod prelude {
    pub use super::collection::PCollectionExt;
    pub use super::combine::CombineExt;
    pub use super::fold::FoldCombineFn;
    pub use super::keyed::PCollectionKeyedExt;
    pub use super::keyed_combine::KeyedCombineExt;
    pub use super::list::PCollectionListExt;
    pub use super::sets::SetsExt;
    pub use beam::prelude::*;
}
