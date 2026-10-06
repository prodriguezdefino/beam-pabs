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

//! The values that flow into and out of [`PTransform`]s.
//!
//! [`PDone`] implements only [`POutput`], so code that reads from the result of a sink
//! fails to compile.
//!
//! [`PTransform`]: crate::transforms::PTransform

pub mod pcollection;
pub mod pvalue;
pub mod view;

pub use pcollection::{IsBounded, PCollection, PCollectionList};
pub use pvalue::{PBegin, PDone, PInput, POutput};
pub use view::{
    AnySideInput, PCollectionView, SideInputKind, SideInputWindowing, URN_SIDE_INPUT_ITERABLE,
    URN_SIDE_INPUT_MULTIMAP, URN_WINDOW_MAPPING_GLOBAL, URN_WINDOW_MAPPING_IDENTITY,
    URN_WINDOW_MAPPING_WINDOW_FN,
};
