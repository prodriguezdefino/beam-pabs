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

//! Coders for the standard Beam wire format, which the runner and the SDK harness use to
//! exchange PCollection elements, window metadata and KV records.

mod composite;
mod header;
mod iterable;
mod metadata;
mod pane;
mod runner_framing;
mod standard;
mod timer;
mod traits;
mod traversal;
mod windowed;

pub use composite::*;
pub use header::WindowedHeader;
pub use iterable::*;
pub use metadata::*;
pub use pane::*;
pub use runner_framing::*;
pub use standard::*;
pub use timer::*;
pub use traits::*;
pub use traversal::*;
pub use windowed::*;

pub const URN_BYTES: &str = "beam:coder:bytes:v1";
pub const URN_BOOL: &str = "beam:coder:bool:v1";
pub const URN_STRING_UTF8: &str = "beam:coder:string_utf8:v1";
pub const URN_VARINT: &str = "beam:coder:varint:v1";
pub const URN_DOUBLE: &str = "beam:coder:double:v1";
pub const URN_KV: &str = "beam:coder:kv:v1";
pub const URN_ITERABLE: &str = "beam:coder:iterable:v1";
pub const URN_LENGTH_PREFIX: &str = "beam:coder:length_prefix:v1";
pub const URN_GLOBAL_WINDOW: &str = "beam:coder:global_window:v1";
pub const URN_INTERVAL_WINDOW: &str = "beam:coder:interval_window:v1";
pub const URN_NULLABLE: &str = "beam:coder:nullable:v1";
pub const URN_WINDOWED_VALUE: &str = "beam:coder:windowed_value:v1";
pub const URN_PARAM_WINDOWED_VALUE: &str = "beam:coder:param_windowed_value:v1";
pub const URN_TIMER: &str = "beam:coder:timer:v1";
pub const URN_ROW: &str = "beam:coder:row:v1";
pub const URN_STATE_BACKED_ITERABLE: &str = "beam:coder:state_backed_iterable:v1";

/// Coder URNs that this SDK can decode.
///
/// Each URN must have a dispatch arm in [`skip_coder_value`]. An advertised coder lets the
/// runner send that encoding without a length prefix. Do not list a coder that the SDK
/// cannot parse: the result is a corrupt data stream, not a clean error.
pub const SUPPORTED_CODER_URNS: &[&str] = &[
    URN_BYTES,
    URN_BOOL,
    URN_STRING_UTF8,
    URN_VARINT,
    URN_DOUBLE,
    URN_KV,
    URN_ITERABLE,
    URN_STATE_BACKED_ITERABLE,
    URN_LENGTH_PREFIX,
    URN_GLOBAL_WINDOW,
    URN_INTERVAL_WINDOW,
    URN_NULLABLE,
    URN_WINDOWED_VALUE,
    URN_PARAM_WINDOWED_VALUE,
    URN_TIMER,
];
