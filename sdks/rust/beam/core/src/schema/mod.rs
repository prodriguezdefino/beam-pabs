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

//! Apache Beam Schema and Row model for schema-aware and cross-language transforms.
//!
//! A schema is a language-independent type definition for rows. Cross-language transforms,
//! SchemaTransforms and SQL transforms use schemas as their payload representation.

mod convert;
mod error;
mod logical;
mod proto;
mod row;
mod types;

pub use convert::*;
pub use error::*;
pub use logical::*;
pub use row::*;
pub use types::*;

// The derive macros share the trait names; macros and traits are separate namespaces.
#[cfg(feature = "derive")]
pub use derive::{BeamEnum, BeamRow};
