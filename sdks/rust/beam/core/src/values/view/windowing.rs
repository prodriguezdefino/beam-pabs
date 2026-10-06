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

//! How a side input finds its window from the window of the main input element.

use std::borrow::Cow;
use std::sync::Arc;

use beam_model::pipeline as proto;
use prost::Message;

use super::{URN_WINDOW_MAPPING_GLOBAL, URN_WINDOW_MAPPING_IDENTITY, URN_WINDOW_MAPPING_WINDOW_FN};
use crate::coders::{Coder, Context, IntervalWindowCoder};
use crate::pipeline::{URN_WINDOW_FN_GLOBAL_WINDOWS, URN_WINDOW_FN_SESSION_WINDOWS};
use crate::windowing::WindowFn;
use crate::windowing::window_fn::decode_window_fn;

/// How a side input's window is found from the window of the main input element.
#[derive(Clone, Debug)]
pub enum SideInputWindowing {
    /// The side input is in the global window.
    Global,
    /// The side input has the same windows as the main input.
    Identity,
    /// The end of the main window is assigned with the side input's window function.
    Mapped(Arc<dyn WindowFn>),
}

impl SideInputWindowing {
    /// Returns the windowing of a side input whose PCollection uses `window_fn`. Sessions and
    /// unknown window functions are rejected.
    pub fn of(window_fn: &proto::FunctionSpec) -> Result<Self, String> {
        match window_fn.urn.as_str() {
            URN_WINDOW_FN_GLOBAL_WINDOWS => Ok(Self::Global),
            URN_WINDOW_FN_SESSION_WINDOWS => {
                Err("Sessions windowing is not allowed in side inputs".to_string())
            }
            _ => decode_window_fn(window_fn)
                .map(Self::Mapped)
                .map_err(|e| format!("Unsupported window function for side inputs: {e}")),
        }
    }

    /// Decodes a `window_mapping_fn` written by [`to_proto`](Self::to_proto).
    pub fn from_proto(spec: &proto::FunctionSpec) -> Result<Self, String> {
        match spec.urn.as_str() {
            URN_WINDOW_MAPPING_GLOBAL => Ok(Self::Global),
            URN_WINDOW_MAPPING_IDENTITY => Ok(Self::Identity),
            URN_WINDOW_MAPPING_WINDOW_FN => proto::FunctionSpec::decode(spec.payload.as_slice())
                .map_err(|e| format!("Failed to decode window mapping payload: {e}"))
                .and_then(|window_fn| decode_window_fn(&window_fn).map_err(|e| e.to_string()))
                .map(Self::Mapped),
            urn => Err(format!("Unsupported window mapping fn '{urn}'")),
        }
    }

    /// Returns the `window_mapping_fn` of a Runner API `SideInput`.
    pub fn to_proto(&self) -> proto::FunctionSpec {
        let (urn, payload) = match self {
            Self::Global => (URN_WINDOW_MAPPING_GLOBAL, Vec::new()),
            Self::Identity => (URN_WINDOW_MAPPING_IDENTITY, Vec::new()),
            Self::Mapped(window_fn) => {
                let spec = proto::FunctionSpec {
                    urn: window_fn.urn().to_string(),
                    payload: window_fn.payload(),
                };
                (URN_WINDOW_MAPPING_WINDOW_FN, spec.encode_to_vec())
            }
        };
        proto::FunctionSpec {
            urn: urn.to_string(),
            payload,
        }
    }

    /// Returns the encoded side input window for the encoded `main_window`.
    ///
    /// Global and identity mappings return borrowed slices without allocation. Mapped window
    /// functions decode the main window, assign the side window, and allocate an owned byte buffer.
    pub fn map<'a>(&self, main_window: &'a [u8]) -> Result<Cow<'a, [u8]>, String> {
        let window_fn = match self {
            Self::Global => return Ok(Cow::Borrowed(&[])),
            Self::Identity => return Ok(Cow::Borrowed(main_window)),
            Self::Mapped(window_fn) => window_fn,
        };
        // The global window encodes to zero bytes and cannot be mapped to an interval window.
        if main_window.is_empty() {
            return Err("Cannot map the global window to a non-global side input".to_string());
        }
        let main = IntervalWindowCoder
            .decode(&mut &main_window[..], Context::Nested)
            .map_err(|e| format!("Failed to decode main input window: {e}"))?;
        let side = window_fn
            .assign_windows(main.max_timestamp())
            .into_iter()
            .min()
            .ok_or_else(|| format!("{window_fn:?} assigned no window to {main:?}"))?;
        let mut bytes = Vec::new();
        side.encode(&mut bytes)
            .map_err(|e| format!("Failed to encode side input window: {e}"))?;
        Ok(Cow::Owned(bytes))
    }
}

/// The windowing of a view, from the pipeline graph that built it.
#[derive(Clone, Debug)]
pub(super) enum ViewWindowing {
    /// The graph has the window function of the side PCollection.
    Known(SideInputWindowing),
    /// The side PCollection comes from a cross-language transform that is not expanded. The
    /// [`SideInputReader`](crate::internals::SideInputReader) gives the window mapping from
    /// the runner's descriptor.
    FromRunner,
    /// The windowing is unsupported; pipeline validation reports it.
    Unsupported(String),
}
