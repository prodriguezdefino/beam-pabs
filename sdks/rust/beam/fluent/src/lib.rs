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
//! [`PCollectionExt`](prelude::PCollectionExt),
//! [`PCollectionKeyedExt`](prelude::PCollectionKeyedExt) and
//! [`PCollectionListExt`](prelude::PCollectionListExt) add one method per core transform
//! (`.group_by_key` applies `GroupByKey::new`). Use `.apply(Transform::new(..))` for advanced
//! configuration.

mod combinators;
mod keyed;

pub use combinators::FoldCombineFn;

/// [`beam::prelude`] plus the fluent extension traits.
pub mod prelude {
    pub use super::combinators::{
        FoldCombineFn, PCollectionExt, PCollectionKeyedExt, PCollectionListExt,
    };
    pub use beam::prelude::*;
}
