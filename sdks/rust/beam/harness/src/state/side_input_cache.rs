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

use super::channel::StateChannel;
use super::tables::{FastHashMap, StateCellKey, StateCellRef, lookup};
use std::collections::HashMap;

use std::io::Cursor;
use std::sync::Mutex;

use beam::coders::skip_coder_value;
use beam::internals::{SideInputReader, extract_side_input_tags};
use model::fn_execution::{ProcessBundleDescriptor, StateKey, state_key};
use model::pipeline::Coder;

#[derive(Clone, Debug)]
struct SideInputMeta {
    transform_id: String,
    side_input_id: String,
    elem_coder_id: String,
    value_coder_id: String,
}

/// [`SideInputReader`] backed by the Beam Fn State gRPC channel (`BeamFnState`).
///
/// Side inputs are read per element, so a cache hit allocates only the owned result:
/// metadata is looked up by `&str`, and the cache is keyed by `StateCellKey` (side input id
/// as `state_id`) and probed with a borrowed key.
pub struct FnApiSideInputReader {
    /// Transform id, then side input tag.
    by_transform_and_tag: FastHashMap<String, FastHashMap<String, SideInputMeta>>,
    by_tag: FastHashMap<String, SideInputMeta>,
    coders: HashMap<String, Coder>,
    channel: Option<StateChannel>,
    cache: Mutex<FastHashMap<StateCellKey, Vec<Vec<u8>>>>,
}

impl FnApiSideInputReader {
    /// A reader for the bundle, or `None` if no transform declares side inputs.
    pub fn from_descriptor(
        instruction_id: &str,
        descriptor: &ProcessBundleDescriptor,
        worker_id: &str,
    ) -> Option<Self> {
        let channel = StateChannel::from_descriptor(instruction_id, descriptor, worker_id);
        Self::from_channel(descriptor, channel)
    }

    /// Like [`Self::from_descriptor`], over an existing `StateChannel`.
    pub fn from_channel(
        descriptor: &ProcessBundleDescriptor,
        channel: Option<StateChannel>,
    ) -> Option<Self> {
        let metas: Vec<SideInputMeta> = descriptor
            .transforms
            .iter()
            .flat_map(|(t_id, transform)| {
                extract_side_input_tags(transform)
                    .into_iter()
                    .filter_map(move |tag| {
                        let pcol_id = transform.inputs.get(&tag)?;
                        let elem_coder_id = descriptor
                            .pcollections
                            .get(pcol_id)
                            .map(|p| p.coder_id.clone())
                            .unwrap_or_default();
                        let value_coder_id = descriptor
                            .coders
                            .get(&elem_coder_id)
                            .and_then(|c| c.component_coder_ids.get(1).cloned())
                            .unwrap_or_else(|| elem_coder_id.clone());
                        Some(SideInputMeta {
                            transform_id: t_id.clone(),
                            side_input_id: tag,
                            elem_coder_id,
                            value_coder_id,
                        })
                    })
            })
            .collect();

        if metas.is_empty() {
            return None;
        }

        let by_transform_and_tag: FastHashMap<String, FastHashMap<String, SideInputMeta>> =
            metas.iter().fold(FastHashMap::default(), |mut acc, m| {
                acc.entry(m.transform_id.clone())
                    .or_default()
                    .insert(m.side_input_id.clone(), m.clone());
                acc
            });
        let by_tag = metas
            .into_iter()
            .map(|m| (m.side_input_id.clone(), m))
            .collect();

        Some(Self {
            by_transform_and_tag,
            by_tag,
            coders: descriptor.coders.clone(),
            channel,
            cache: Mutex::default(),
        })
    }

    fn resolve_meta_for_transform(
        &self,
        transform_id: Option<&str>,
        tag: &str,
    ) -> Result<&SideInputMeta, String> {
        transform_id
            .and_then(|curr_t| self.by_transform_and_tag.get(curr_t)?.get(tag))
            .or_else(|| self.by_tag.get(tag))
            .ok_or_else(|| format!("FnApiSideInputReader: unknown side input tag '{tag}'"))
    }

    fn fetch_or_cached(
        &self,
        cache_key: StateCellRef<'_>,
        state_key: impl FnOnce() -> StateKey,
        coder_id: &str,
    ) -> Result<Vec<Vec<u8>>, String> {
        if let Some(cached) = self
            .cache
            .lock()
            .ok()
            .and_then(|g| lookup(&g, cache_key).cloned())
        {
            return Ok(cached);
        }
        let channel = self.channel.as_ref().ok_or_else(|| {
            "FnApiSideInputReader: ProcessBundleDescriptor has no state_api_service_descriptor"
                .to_string()
        })?;
        let raw = channel.get(state_key())?;
        let elements = split_concatenated_elements(&raw, coder_id, &self.coders)?;
        if let Ok(mut guard) = self.cache.lock() {
            let (transform_id, side_input_id, window, key) = cache_key;
            guard.insert(
                StateCellKey::new(transform_id, side_input_id, window, key),
                elements.clone(),
            );
        }
        Ok(elements)
    }
}

impl SideInputReader for FnApiSideInputReader {
    fn get_iterable(&self, tag: &str, window_bytes: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        self.get_iterable_for_transform("", tag, window_bytes)
    }

    fn get_multimap(
        &self,
        tag: &str,
        window_bytes: &[u8],
        key_bytes: &[u8],
    ) -> Result<Vec<Vec<u8>>, String> {
        self.get_multimap_for_transform("", tag, window_bytes, key_bytes)
    }

    fn get_iterable_for_transform(
        &self,
        transform_id: &str,
        tag: &str,
        window_bytes: &[u8],
    ) -> Result<Vec<Vec<u8>>, String> {
        let meta = self
            .resolve_meta_for_transform((!transform_id.is_empty()).then_some(transform_id), tag)?;
        self.fetch_or_cached(
            (&meta.transform_id, &meta.side_input_id, window_bytes, &[]),
            || StateKey {
                r#type: Some(state_key::Type::IterableSideInput(
                    state_key::IterableSideInput {
                        transform_id: meta.transform_id.clone(),
                        side_input_id: meta.side_input_id.clone(),
                        window: window_bytes.to_vec(),
                    },
                )),
            },
            &meta.elem_coder_id,
        )
    }

    fn get_multimap_for_transform(
        &self,
        transform_id: &str,
        tag: &str,
        window_bytes: &[u8],
        key_bytes: &[u8],
    ) -> Result<Vec<Vec<u8>>, String> {
        let meta = self
            .resolve_meta_for_transform((!transform_id.is_empty()).then_some(transform_id), tag)?;
        self.fetch_or_cached(
            (
                &meta.transform_id,
                &meta.side_input_id,
                window_bytes,
                key_bytes,
            ),
            || StateKey {
                r#type: Some(state_key::Type::MultimapSideInput(
                    state_key::MultimapSideInput {
                        transform_id: meta.transform_id.clone(),
                        side_input_id: meta.side_input_id.clone(),
                        window: window_bytes.to_vec(),
                        key: key_bytes.to_vec(),
                    },
                )),
            },
            &meta.value_coder_id,
        )
    }
}

/// Splits a concatenated nested-context byte stream into individual encoded elements.
pub(crate) fn split_concatenated_elements(
    data: &[u8],
    coder_id: &str,
    coders: &HashMap<String, Coder>,
) -> Result<Vec<Vec<u8>>, String> {
    let mut cursor = Cursor::new(data);
    let is_length_prefix = coders
        .get(coder_id)
        .and_then(|c| c.spec.as_ref())
        .map(|s| s.urn.as_str() == "beam:coder:length_prefix:v1")
        .unwrap_or(false);

    std::iter::from_fn(|| {
        let start = cursor.position() as usize;
        (start < data.len()).then(|| {
            if is_length_prefix {
                let len = beam::coders::VarIntCoder::decode_varint(&mut cursor)
                    .map_err(|e| format!("Failed to read length prefix: {e}"))?
                    as usize;
                let payload_start = cursor.position() as usize;
                let payload_end = payload_start + len;
                if payload_end > data.len() {
                    return Err(format!(
                        "Length prefix {len} exceeds data length {}",
                        data.len()
                    ));
                }
                cursor.set_position(payload_end as u64);
                Ok(data[payload_start..payload_end].to_vec())
            } else {
                skip_coder_value(&mut cursor, coder_id, coders, true)
                    .map(|()| data[start..cursor.position() as usize].to_vec())
                    .map_err(|e| {
                        format!("Failed to frame side input element with coder '{coder_id}': {e}")
                    })
            }
        })
    })
    .collect()
}
