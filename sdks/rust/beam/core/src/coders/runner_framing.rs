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

//! Reconciles runner-inserted length prefixes around nested Row coders.
//!
//! Runners that do not decode `beam:coder:row:v1` wrap nested Row values in
//! `beam:coder:length_prefix:v1` on the data plane. These functions add or strip those
//! nested prefixes under `KV`, `iterable`, `state_backed_iterable`, and `nullable`.

use std::collections::HashMap;
use std::io::{Cursor, Error, ErrorKind};

use model::pipeline::Coder as ProtoCoder;

use super::standard::{VarIntCoder, read_array, read_be_i32};
use super::traversal::{
    coder_urn, length_prefix_component, read_length_prefixed_slice, skip_coder_value,
};
use super::{
    URN_ITERABLE, URN_KV, URN_LENGTH_PREFIX, URN_NULLABLE, URN_ROW, URN_STATE_BACKED_ITERABLE,
};

/// Direction of the element byte rewrite.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Direction {
    /// Runner wire bytes to SDK-native bytes: drop the nested prefixes.
    Strip,
    /// SDK-native bytes to runner wire bytes: add the nested prefixes.
    Add,
}

/// Returns true if `coder_id` has a runner-inserted length prefix around a nested coder (see
/// the module docs). A root prefix does not count. If `false`, no rewrite is necessary.
pub fn has_nested_row_length_prefix(coder_id: &str, coders: &HashMap<String, ProtoCoder>) -> bool {
    coders
        .get(coder_id)
        .is_some_and(|coder| components_have_row_prefix(coder, coders, 0))
}

/// Maximum coder nesting to explore. The limit stops recursion on a cyclic coder graph.
const MAX_DEPTH: usize = 64;

fn components_have_row_prefix(
    coder: &ProtoCoder,
    coders: &HashMap<String, ProtoCoder>,
    depth: usize,
) -> bool {
    if depth > MAX_DEPTH || !is_traversed_composite(coder) {
        return false;
    }
    coder
        .component_coder_ids
        .iter()
        .filter_map(|id| coders.get(id))
        .any(|component| {
            is_runner_prefixed(component, coders)
                || components_have_row_prefix(component, coders, depth + 1)
        })
}

fn is_traversed_composite(coder: &ProtoCoder) -> bool {
    matches!(
        coder_urn(coder),
        URN_KV | URN_ITERABLE | URN_STATE_BACKED_ITERABLE | URN_NULLABLE
    )
}

/// Returns true if `coder` is a runner-inserted `length_prefix`.
fn is_runner_prefixed(coder: &ProtoCoder, coders: &HashMap<String, ProtoCoder>) -> bool {
    length_prefix_component(coder, coders)
        .is_some_and(|inner| coder_urn(inner) != URN_LENGTH_PREFIX)
}

/// Rewrites an element from runner coder `coder_id` into the SDK native encoding.
pub fn strip_nested_row_length_prefixes(
    element: &[u8],
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
) -> Result<Vec<u8>, Error> {
    rewrite(element, coder_id, coders, Direction::Strip)
}

/// Rewrites an element from the SDK native encoding into the encoding of runner coder
/// `coder_id`. The Row length comes from its schema, so each nested Row coder payload must have
/// a schema; the coder of a schema-aware PCollection always does.
pub fn add_nested_row_length_prefixes(
    element: &[u8],
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
) -> Result<Vec<u8>, Error> {
    rewrite(element, coder_id, coders, Direction::Add)
}

fn rewrite(
    element: &[u8],
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
    direction: Direction,
) -> Result<Vec<u8>, Error> {
    let mut out = Vec::with_capacity(element.len() + 16);
    rewrite_into(element, coder_id, coders, direction, &mut out)?;
    Ok(out)
}

/// Appends the rewrite of `element`, encoded with `coder_id`, to `out`.
fn rewrite_into(
    element: &[u8],
    coder_id: &str,
    coders: &HashMap<String, ProtoCoder>,
    direction: Direction,
    out: &mut Vec<u8>,
) -> Result<(), Error> {
    let mut rewriter = Rewriter {
        cursor: Cursor::new(element),
        out,
        coders,
        direction,
    };
    // The element is the whole payload, so decode the root in the whole-stream context.
    rewriter.value(coder_id, false, 0)?;
    let rest = rewriter.remaining();
    rewriter.out.extend_from_slice(rest);
    Ok(())
}

/// Returns an error if `coder_id` is a Row coder without a schema, which gives the Row length.
fn require_row_schema(coder_id: &str, coders: &HashMap<String, ProtoCoder>) -> Result<(), Error> {
    let schemaless_row = coders.get(coder_id).is_some_and(|coder| {
        coder_urn(coder) == URN_ROW && coder.spec.as_ref().is_none_or(|s| s.payload.is_empty())
    });
    if schemaless_row {
        return Err(Error::new(
            ErrorKind::InvalidData,
            format!(
                "cannot length-prefix nested Row: coder '{coder_id}' carries no schema \
                 to measure the Row with"
            ),
        ));
    }
    Ok(())
}

struct Rewriter<'a, 'o> {
    cursor: Cursor<&'a [u8]>,
    out: &'o mut Vec<u8>,
    coders: &'a HashMap<String, ProtoCoder>,
    direction: Direction,
}

impl<'a> Rewriter<'a, '_> {
    fn remaining(&self) -> &'a [u8] {
        let data: &'a [u8] = self.cursor.get_ref();
        let pos = usize::try_from(self.cursor.position()).unwrap_or(data.len());
        data.get(pos..).unwrap_or(&[])
    }

    /// Copies one value without change. `coder_id` gives its length.
    fn copy(&mut self, coder_id: &str, nested: bool) -> Result<(), Error> {
        let start = self.position();
        skip_coder_value(&mut self.cursor, coder_id, self.coders, nested)?;
        let end = self.position();
        let data: &'a [u8] = self.cursor.get_ref();
        self.out.extend_from_slice(&data[start..end]);
        Ok(())
    }

    fn position(&self) -> usize {
        usize::try_from(self.cursor.position()).unwrap_or(usize::MAX)
    }

    fn value(&mut self, coder_id: &str, nested: bool, depth: usize) -> Result<(), Error> {
        if depth > MAX_DEPTH {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("coder '{coder_id}' nests deeper than {MAX_DEPTH} levels"),
            ));
        }
        let coders = self.coders;
        let Some(coder) = coders.get(coder_id) else {
            return self.copy(coder_id, nested);
        };
        if depth > 0 && is_runner_prefixed(coder, coders) {
            let inner_id = coder.component_coder_ids.first().map_or("", String::as_str);
            return self.prefixed_component(inner_id);
        }
        match coder_urn(coder) {
            URN_KV => {
                let [key_id, value_id, ..] = coder.component_coder_ids.as_slice() else {
                    return self.copy(coder_id, nested);
                };
                self.value(key_id, true, depth + 1)?;
                self.value(value_id, nested, depth + 1)
            }
            URN_ITERABLE | URN_STATE_BACKED_ITERABLE => {
                let elem_id = coder.component_coder_ids.first().map_or("", String::as_str);
                self.iterable(elem_id, depth)
            }
            URN_NULLABLE => {
                let [tag] = read_array::<1>(&mut self.cursor)?;
                self.out.push(tag);
                match coder.component_coder_ids.first().filter(|_| tag != 0) {
                    Some(inner_id) => self.value(inner_id, nested, depth + 1),
                    None => Ok(()),
                }
            }
            _ => self.copy(coder_id, nested),
        }
    }

    fn prefixed_component(&mut self, inner_id: &str) -> Result<(), Error> {
        match self.direction {
            Direction::Strip => {
                let payload = read_length_prefixed_slice(&mut self.cursor)?;
                rewrite_into(payload, inner_id, self.coders, Direction::Strip, self.out)
            }
            Direction::Add => {
                require_row_schema(inner_id, self.coders)?;
                let start = self.position();
                skip_coder_value(&mut self.cursor, inner_id, self.coders, true)?;
                let end = self.position();
                let data: &'a [u8] = self.cursor.get_ref();
                // The prefix holds the rewritten length, so insert it after those bytes.
                let prefix_at = self.out.len();
                rewrite_into(
                    &data[start..end],
                    inner_id,
                    self.coders,
                    Direction::Add,
                    self.out,
                )?;
                let mut prefix = [0u8; 10];
                let unused = {
                    let mut writer = &mut prefix[..];
                    VarIntCoder::encode_varint((self.out.len() - prefix_at) as i64, &mut writer)?;
                    writer.len()
                };
                let prefix = &prefix[..prefix.len() - unused];
                self.out
                    .splice(prefix_at..prefix_at, prefix.iter().copied());
                Ok(())
            }
        }
    }

    fn iterable(&mut self, elem_id: &str, depth: usize) -> Result<(), Error> {
        let count = read_be_i32(&mut self.cursor)?;
        self.out.extend_from_slice(&count.to_be_bytes());
        if count >= 0 {
            return (0..count).try_for_each(|_| self.value(elem_id, true, depth + 1));
        }
        if count != -1 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Invalid iterable count: {count}"),
            ));
        }
        loop {
            let chunk_len = VarIntCoder::decode_varint(&mut self.cursor)?;
            VarIntCoder::encode_varint(chunk_len, &mut self.out)?;
            match chunk_len {
                0 => return Ok(()),
                // A continuation token ends the inlined elements.
                -1 => {
                    let token = read_length_prefixed_slice(&mut self.cursor)?;
                    VarIntCoder::encode_varint(token.len() as i64, &mut self.out)?;
                    self.out.extend_from_slice(token);
                    return Ok(());
                }
                n if n < 0 => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("Invalid iterable chunk header: {n}"),
                    ));
                }
                n => (0..n).try_for_each(|_| self.value(elem_id, true, depth + 1))?,
            }
        }
    }
}
