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

//! Resolves the handler of each transform in a bundle.

use prost::Message;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use beam::coders::{ElementFormatter, URN_KV, VarIntCoder, coder_urn, read_length_prefixed_slice};
use beam::internals::{ElementSink, TransformFn};
use beam::pipeline::{
    COMBINE_STAGE_EXTRACT, COMBINE_STAGE_MERGE, COMBINE_STAGE_PRECOMBINE,
    URN_COMBINE_PER_KEY_EXTRACT_OUTPUTS, URN_COMBINE_PER_KEY_MERGE_ACCUMULATORS,
    URN_COMBINE_PER_KEY_PRECOMBINE, URN_FLATTEN, URN_MAP_WINDOWS, URN_PAR_DO,
    URN_SDF_PAIR_WITH_RESTRICTION, URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS,
    URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS, URN_TO_STRING, URN_WINDOW_INTO, combine_stage_key,
};
use beam::values::SideInputWindowing;
use beam::windowing::window_into_handler_key;
use model::fn_execution::ProcessBundleDescriptor;
use model::pipeline::{CombinePayload, FunctionSpec, PTransform, ParDoPayload, WindowIntoPayload};

/// Resolves the handler registered for a transform.
///
/// Resolution reads only the `spec` of the transform, never its id or `unique_name`. A
/// runner can rename, fuse or synthesize transforms, but it does not change payloads.
/// Each handler has exactly one key, which comes from the payload:
///
/// - `ParDo` and the expanded splittable-`DoFn` stages: the `do_fn` payload, which
///   [`ParDoRegistration`](beam::internals::ParDoRegistration) sets to the transform's
///   unique name.
/// - Lifted `CombinePerKey` stages: the `combine_fn` payload plus the stage.
/// - `WindowInto`: built in for the standard window functions, otherwise the key from
///   [`window_into_handler_key`].
/// - `Flatten`: built in.
/// - `MapWindows`: built in. It maps main input windows to side input windows.
pub fn lookup_handler(
    handlers: &HashMap<String, TransformFn>,
    transform: &PTransform,
) -> Option<TransformFn> {
    let spec = transform.spec.as_ref()?;
    match spec.urn.as_str() {
        URN_PAR_DO => handlers.get(&do_fn_key(&spec.payload)?).cloned(),
        URN_SDF_PAIR_WITH_RESTRICTION
        | URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS
        | URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS => handlers
            .get(&do_fn_key(&spec.payload)?)
            .and_then(|h| h.stage_handler(&spec.urn)),
        URN_COMBINE_PER_KEY_PRECOMBINE => {
            lookup_combine_stage(handlers, spec.payload.as_slice(), COMBINE_STAGE_PRECOMBINE)
                .cloned()
        }
        URN_COMBINE_PER_KEY_MERGE_ACCUMULATORS => {
            lookup_combine_stage(handlers, spec.payload.as_slice(), COMBINE_STAGE_MERGE).cloned()
        }
        URN_COMBINE_PER_KEY_EXTRACT_OUTPUTS => {
            lookup_combine_stage(handlers, spec.payload.as_slice(), COMBINE_STAGE_EXTRACT).cloned()
        }
        URN_WINDOW_INTO => {
            let window_fn_spec = WindowIntoPayload::decode(spec.payload.as_slice())
                .ok()?
                .window_fn?;
            match beam::windowing::decode_window_fn(&window_fn_spec) {
                Ok(window_fn) => Some(Arc::new(beam::windowing::WindowIntoHandler::new(window_fn))),
                Err(_) => handlers
                    .get(&window_into_handler_key(&window_fn_spec))
                    .cloned(),
            }
        }
        URN_FLATTEN => Some(Arc::new(|elem: &[u8], sink: &mut dyn ElementSink| {
            sink.push(elem.to_vec())
        })),
        URN_MAP_WINDOWS => map_windows_handler(&spec.payload),
        _ => None,
    }
}

/// Resolves the handler for a transform, including built-ins that need the descriptor.
///
/// `beam:transform:to_string:v1` renders elements with their coder, and only the
/// descriptor has that coder. All other transforms resolve through [`lookup_handler`].
pub fn resolve_handler(
    handlers: &HashMap<String, TransformFn>,
    transform: &PTransform,
    descriptor: &ProcessBundleDescriptor,
) -> Option<TransformFn> {
    match transform.spec.as_ref().map(|spec| spec.urn.as_str()) {
        Some(URN_TO_STRING) => Some(to_string_handler(transform, descriptor)),
        _ => lookup_handler(handlers, transform),
    }
}

/// Builds the handler for `beam:transform:to_string:v1`.
///
/// - Input: `KV<nonce, element>`
/// - Output: `KV<nonce, string>`
///
/// The nonce is opaque, and the handler copies it unchanged. An [`ElementFormatter`],
/// compiled once from the coder of the input PCollection, renders the element. Runners send
/// only sampled elements through this handler. A bad element must not fail the bundle, so
/// an element that does not match its coder is rendered as escaped bytes.
fn to_string_handler(transform: &PTransform, descriptor: &ProcessBundleDescriptor) -> TransformFn {
    let coders = &descriptor.coders;
    let kv_components = transform
        .inputs
        .values()
        .next()
        .and_then(|pcoll| descriptor.pcollections.get(pcoll))
        .and_then(|pcoll| coders.get(&pcoll.coder_id))
        .filter(|coder| coder_urn(coder) == URN_KV)
        .and_then(|coder| match coder.component_coder_ids.as_slice() {
            [key, value, ..] => Some((key.as_str(), value.as_str())),
            _ => None,
        });
    // Without a KV coder, decode the nonce as bytes, which is the type the protocol specifies.
    let (nonce, element) = kv_components.map_or_else(
        || (None, ElementFormatter::new("", coders)),
        |(key, value)| {
            (
                Some(ElementFormatter::new(key, coders)),
                ElementFormatter::new(value, coders),
            )
        },
    );
    let formatters = Arc::new((nonce, element));
    Arc::new(move |elem: &[u8], sink: &mut dyn ElementSink| {
        let (nonce, element) = &*formatters;
        let nonce_len = match nonce {
            Some(nonce) => {
                let mut cursor = Cursor::new(elem);
                nonce
                    .write(&mut cursor, &mut Discard)
                    .map(|()| usize::try_from(cursor.position()).unwrap_or(elem.len()))
            }
            None => nested_prefix(elem).map(<[u8]>::len),
        }
        .map_err(|e| format!("Failed to decode nonce in to_string: {e}"))?;
        let (nonce_bytes, value_bytes) = elem.split_at(nonce_len.min(elem.len()));
        let rendered = element.format(value_bytes);
        let mut out = Vec::with_capacity(nonce_bytes.len() + rendered.len() + 5);
        out.extend_from_slice(nonce_bytes);
        VarIntCoder::encode_varint(rendered.len() as i64, &mut out)
            .map_err(|e| format!("Failed to encode to_string output: {e}"))?;
        out.extend_from_slice(rendered.as_bytes());
        sink.push(out)
    })
}

/// A `fmt::Write` that keeps nothing. It lets an [`ElementFormatter`] skip over a value.
struct Discard;

impl std::fmt::Write for Discard {
    fn write_str(&mut self, _: &str) -> std::fmt::Result {
        Ok(())
    }
}

/// Builds a handler for `beam:transform:map_windows:v1`.
///
/// In the Fn API, `map_windows` maps main input windows to side input windows:
/// - Input: `KV<nonce, MainInputWindow>`
/// - Output: `KV<nonce, SideInputWindow>`
///
/// The KV coder encodes `nonce` in nested context (VarInt length, then bytes), followed by
/// the window. `GlobalWindow` encodes as 0 bytes, so `KV<nonce, GlobalWindow>` is only the
/// nested nonce. A payload that names a mapping this SDK does not implement returns no
/// handler, so the transform is reported as unsupported instead of running wrongly.
fn map_windows_handler(payload: &[u8]) -> Option<TransformFn> {
    let spec = FunctionSpec::decode(payload).ok()?;
    let windowing = SideInputWindowing::from_proto(&spec).ok()?;
    Some(Arc::new(move |elem: &[u8], sink: &mut dyn ElementSink| {
        let nonce = nested_prefix(elem)
            .map_err(|e| format!("Failed to decode nonce in map_windows: {e}"))?;
        let mapped = windowing.map(&elem[nonce.len()..])?;
        sink.push([nonce, &mapped].concat())
    }))
}

/// Returns the leading nested-context, length-prefixed value of `elem`, with its prefix.
fn nested_prefix(elem: &[u8]) -> std::io::Result<&[u8]> {
    let mut cursor = Cursor::new(elem);
    read_length_prefixed_slice(&mut cursor)?;
    let end = usize::try_from(cursor.position()).unwrap_or(elem.len());
    Ok(&elem[..end])
}

/// Returns the handler key in the `do_fn` of a `ParDoPayload`, if it has one.
fn do_fn_key(payload: &[u8]) -> Option<String> {
    let do_fn = ParDoPayload::decode(payload).ok()?.do_fn?;
    String::from_utf8(do_fn.payload)
        .ok()
        .filter(|key| !key.is_empty())
}

/// Resolves a stage of a lifted `CombinePerKey` from its `CombinePayload`.
fn lookup_combine_stage<'a>(
    handlers: &'a HashMap<String, TransformFn>,
    payload: &[u8],
    stage: &str,
) -> Option<&'a TransformFn> {
    let combine_payload = CombinePayload::decode(payload).ok()?;
    let combine_fn = combine_payload.combine_fn?;
    let key = std::str::from_utf8(&combine_fn.payload).ok()?;
    if key.is_empty() {
        None
    } else {
        handlers.get(&combine_stage_key(key, stage))
    }
}
