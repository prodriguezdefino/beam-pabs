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

//! Finds the key of an encoded KV element and the Row schema of a coder.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use model::pipeline::Coder as ProtoCoder;

use super::{coder_urn, skip_coder_value};
use crate::coders::standard::consumed;
use crate::coders::{
    URN_ITERABLE, URN_KV, URN_LENGTH_PREFIX, URN_NULLABLE, URN_PARAM_WINDOWED_VALUE, URN_ROW,
    URN_SHARDED_KEY, URN_STATE_BACKED_ITERABLE, URN_WINDOWED_VALUE,
};
use crate::schema::Schema;

/// Copies the key out of an encoded `KV` element. `coder_id` can be a windowed value coder.
pub fn extract_kv_key_bytes(
    element: &[u8],
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
) -> Option<Vec<u8>> {
    kv_key_slice(element, coder_id, coders).map(<[u8]>::to_vec)
}

/// Borrows the encoded key out of `element`. [`extract_kv_key_bytes`] copies it.
pub fn kv_key_slice<'e>(
    element: &'e [u8],
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
) -> Option<&'e [u8]> {
    key_slice(element, kv_key_coder_id(coder_id, coders)?, coders)
}

/// Returns the key coder ID of a `KV` coder, also inside a windowed value coder, or `None`.
/// Resolve it once per collection, then call [`key_slice`] for each element.
pub fn kv_key_coder_id<'c>(
    coder_id: &str,
    coders: &'c HashMap<String, ProtoCoder>,
) -> Option<&'c str> {
    let coder = coders.get(coder_id)?;
    let kv_coder = match coder_urn(coder) {
        URN_KV => Some(coder),
        URN_WINDOWED_VALUE | URN_PARAM_WINDOWED_VALUE => coder
            .component_coder_ids
            .first()
            .and_then(|inner_id| coders.get(inner_id))
            .filter(|inner| coder_urn(inner) == URN_KV),
        _ => None,
    }?;
    kv_coder.component_coder_ids.first().map(String::as_str)
}

/// Borrows the leading key out of an encoded KV `element`. See [`kv_key_coder_id`].
pub fn key_slice<'e>(
    element: &'e [u8],
    key_coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
) -> Option<&'e [u8]> {
    if element.is_empty() {
        return None;
    }
    let mut cursor = element;
    skip_coder_value(&mut cursor, key_coder_id, coders, true).ok()?;
    Some(consumed(element, cursor))
}

/// Returns the Row [`Schema`] of the rows in an element of `coder_id`.
///
/// Looks through length prefix, windowed value, nullable, iterable and key-value coders. The
/// harness gives this one schema to every row in the element. Returns `None` if the coder has
/// no Row coder with a valid schema, or Row coders with different schemas.
pub fn extract_row_schema(
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
) -> Option<Arc<Schema>> {
    let mut payloads = Vec::new();
    collect_row_payloads(coder_id, coders, &mut HashSet::new(), &mut payloads);
    match payloads.as_slice() {
        [payload] => Schema::from_proto_bytes(payload).ok().map(Arc::new),
        _ => None,
    }
}

/// Adds the distinct schema payloads of the Row coders under `coder_id` to `payloads`. Visits
/// each coder once, so a cyclic coder graph ends.
fn collect_row_payloads<'a>(
    coder_id: &'a str,
    coders: &'a HashMap<String, ProtoCoder>,
    visited: &mut HashSet<&'a str>,
    payloads: &mut Vec<&'a [u8]>,
) {
    if !visited.insert(coder_id) {
        return;
    }
    let Some(coder) = coders.get(coder_id) else {
        return;
    };
    match coder_urn(coder) {
        URN_ROW => {
            let payload = coder
                .spec
                .as_ref()
                .map_or(&[][..], |s| s.payload.as_slice());
            // A Row coder with no payload has no schema to give.
            if !payload.is_empty() && !payloads.contains(&payload) {
                payloads.push(payload);
            }
        }
        URN_WINDOWED_VALUE
        | URN_PARAM_WINDOWED_VALUE
        | URN_LENGTH_PREFIX
        | URN_NULLABLE
        | URN_SHARDED_KEY
        | URN_ITERABLE
        | URN_STATE_BACKED_ITERABLE
        | URN_KV => coder
            .component_coder_ids
            .iter()
            .for_each(|id| collect_row_payloads(id, coders, visited, payloads)),
        _ => {}
    }
}
