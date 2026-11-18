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

//! The random-access list view: elements stored by position in a multimap side input.
//!
//! [`AsList`](crate::transforms::AsList) keys each element by a position in an offset range
//! of its bundle and window, and adds the ranges under [`LIST_RANGES_KEY`]. A read sorts the
//! ranges, so index `i` maps to one position and one value at that position.

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::Arc;

use super::{ListView, SideMap};
use crate::coders::standard::read_array;
use crate::coders::{
    Coder, CoderError, CoderRegistry, Context, DefaultCoder, FieldMap, StateStreamReader, URN_KV,
};
use crate::schema::Schema;
use crate::transforms::sdf::OffsetRange;
use crate::values::PCollection;

/// The position key of the offset ranges of a [`ListView`].
pub const LIST_RANGES_KEY: i64 = i64::MIN;

/// An element of the collection that [`AsList`](crate::transforms::AsList) makes: a list
/// value, or an offset range of positions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListEntry<T> {
    /// A list element at the position of its key.
    Value(T),
    /// Positions that one bundle used in one window. The key is [`LIST_RANGES_KEY`].
    Range(OffsetRange),
}

/// Coder for [`ListEntry`]. The wire format is `KV<Nullable<T>, Nullable<OffsetRange>>` with
/// exactly one part set.
#[derive(Clone, Debug)]
pub struct ListEntryCoder<C> {
    value_coder: C,
}

impl<T, C> Coder<ListEntry<T>> for ListEntryCoder<C>
where
    T: Send + Sync + 'static,
    C: Coder<T>,
{
    fn urn(&self) -> &'static str {
        URN_KV
    }

    fn encode(
        &self,
        entry: &ListEntry<T>,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        encode_entry(entry, writer, |value, writer| {
            self.value_coder.encode(value, writer, Context::Nested)
        })
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<ListEntry<T>, CoderError> {
        decode_entry(reader, |reader| {
            self.value_coder.decode(reader, Context::Nested)
        })
    }
}

impl<T: DefaultCoder> DefaultCoder for ListEntry<T> {
    type Coder = ListEntryCoder<T::Coder>;

    fn coder() -> Self::Coder {
        ListEntryCoder {
            value_coder: T::coder(),
        }
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        encode_entry(self, writer, T::encode_element)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        decode_entry(reader, T::decode_element)
    }

    fn decode_whole_stream_with_context(
        reader: &mut &[u8],
        _schema: Option<&Arc<Schema>>,
        _field_map: Option<&FieldMap>,
        _state_reader: Option<&Arc<dyn StateStreamReader>>,
    ) -> Result<Self, CoderError> {
        Self::coder().decode(reader, Context::WholeStream)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        <(Option<T>, Option<OffsetRange>)>::register_coder(registry)
    }
}

fn encode_entry<T>(
    entry: &ListEntry<T>,
    writer: &mut Vec<u8>,
    encode_value: impl FnOnce(&T, &mut Vec<u8>) -> Result<(), CoderError>,
) -> Result<(), CoderError> {
    match entry {
        ListEntry::Value(value) => {
            writer.write_all(&[0x01])?;
            encode_value(value, writer)?;
            Ok(writer.write_all(&[0x00])?)
        }
        ListEntry::Range(range) => {
            writer.write_all(&[0x00, 0x01])?;
            range.encode_element(writer)
        }
    }
}

fn decode_entry<T>(
    reader: &mut &[u8],
    decode_value: impl FnOnce(&mut &[u8]) -> Result<T, CoderError>,
) -> Result<ListEntry<T>, CoderError> {
    let [has_value] = read_array(reader)?;
    let value = match has_value {
        0x01 => Some(decode_value(reader)?),
        0x00 => None,
        other => return Err(CoderError::Format(format!("Invalid nullable tag: {other}"))),
    };
    let [has_range] = read_array(reader)?;
    match (value, has_range) {
        (Some(value), 0x00) => Ok(ListEntry::Value(value)),
        (None, 0x01) => Ok(ListEntry::Range(OffsetRange::decode_element(reader)?)),
        (_, tag) => Err(CoderError::Format(format!(
            "A list entry holds exactly one of a value and a range, got range tag {tag}"
        ))),
    }
}

/// Positions `[start, end)` that each hold `per_position` values. A read builds them from
/// the offset ranges, which can overlap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Segment {
    start: i64,
    end: i64,
    per_position: u64,
}

impl Segment {
    /// The number of values in the segment.
    fn len(self) -> u128 {
        u128::from(self.end.abs_diff(self.start)) * u128::from(self.per_position)
    }
}

/// The segments of `ranges`, in position order. A position holds one value for each range
/// that contains it.
fn segments(ranges: impl IntoIterator<Item = OffsetRange>) -> Vec<Segment> {
    let deltas = ranges.into_iter().filter(|range| !range.is_empty()).fold(
        BTreeMap::<i64, i64>::new(),
        |mut deltas, range| {
            *deltas.entry(range.start).or_default() += 1;
            *deltas.entry(range.end).or_default() -= 1;
            deltas
        },
    );
    let counts = deltas
        .into_iter()
        .scan(0_i64, |count, (position, delta)| {
            *count += delta;
            Some((position, *count))
        })
        .collect::<Vec<_>>();
    counts
        .windows(2)
        .filter_map(|pair| match *pair {
            [(start, count), (end, _)] => {
                u64::try_from(count)
                    .ok()
                    .filter(|&count| count > 0)
                    .map(|per_position| Segment {
                        start,
                        end,
                        per_position,
                    })
            }
            _ => None,
        })
        .collect()
}

/// The value of a [`ListView`]: the elements of the side input window by index. The ranges
/// are read once; each element read is one multimap read of its position.
pub struct SideList<'s, T> {
    pub(super) map: SideMap<'s, i64, ListEntry<T>>,
    segments: Vec<Segment>,
    len: usize,
}

impl<'s, T: DefaultCoder> SideList<'s, T> {
    /// Reads the offset ranges of the window of `map`.
    pub(super) fn new(map: SideMap<'s, i64, ListEntry<T>>) -> crate::Result<Self> {
        let ranges = map
            .get_all(&LIST_RANGES_KEY)?
            .map(|entry| match entry? {
                ListEntry::Range(range) => Ok(range),
                ListEntry::Value(_) => Err(crate::Error::msg(format!(
                    "Side input '{}' has a list value at the range key",
                    map.tag
                ))),
            })
            .collect::<crate::Result<Vec<_>>>()?;
        let segments = segments(ranges);
        let len = segments.iter().map(|segment| segment.len()).sum::<u128>();
        let len = usize::try_from(len).map_err(|_| {
            crate::Error::msg(format!(
                "Side input '{}' has more than usize::MAX values",
                map.tag
            ))
        })?;
        Ok(Self { map, segments, len })
    }

    /// The number of elements.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the list has no element.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The element at `index`, or `None` if `index` is not less than [`len`](Self::len).
    pub fn get(&self, index: usize) -> crate::Result<Option<T>> {
        let Some((position, offset)) = self.position(index) else {
            return Ok(None);
        };
        self.values_at(position)?.nth(offset).transpose()
    }

    /// The elements in index order. Each position is read when the iterator reaches it.
    pub fn iter(&self) -> impl Iterator<Item = crate::Result<T>> + '_ {
        self.segments
            .iter()
            .flat_map(|segment| {
                (segment.start..segment.end).map(move |p| (p, segment.per_position))
            })
            .flat_map(move |(position, per_position)| {
                let take = usize::try_from(per_position).unwrap_or(usize::MAX);
                let (values, error) = match self.values_at(position) {
                    Ok(values) => (Some(values.take(take)), None),
                    Err(e) => (None, Some(Err(e))),
                };
                values.into_iter().flatten().chain(error)
            })
    }

    /// All elements in index order, collected.
    pub fn to_vec(&self) -> crate::Result<Vec<T>> {
        self.iter().collect()
    }

    /// The position of `index` and the offset of its value among the values at the position.
    fn position(&self, index: usize) -> Option<(i64, usize)> {
        let mut rest = u128::try_from(index).ok()?;
        self.segments.iter().find_map(|segment| {
            let len = segment.len();
            if rest >= len {
                rest -= len;
                return None;
            }
            let per_position = u128::from(segment.per_position);
            let step = i64::try_from(rest / per_position).ok()?;
            let offset = usize::try_from(rest % per_position).ok()?;
            Some((segment.start + step, offset))
        })
    }

    /// The values at `position`.
    fn values_at(
        &self,
        position: i64,
    ) -> crate::Result<impl Iterator<Item = crate::Result<T>> + use<'s, T>> {
        let tag = self.map.tag;
        let key = position
            .encode()
            .map_err(|e| crate::Error::from(e).context("Failed to encode a list position"))?;
        Ok(self.map.values(&key)?.map(move |entry| match entry? {
            ListEntry::Value(value) => Ok(value),
            ListEntry::Range(_) => Err(crate::Error::msg(format!(
                "Side input '{tag}' has a range at list position {position}"
            ))),
        }))
    }
}

impl<T: DefaultCoder> PCollection<(i64, ListEntry<T>)> {
    /// A side input read by index. Apply [`AsList`](crate::transforms::AsList) first to get
    /// this collection.
    pub fn as_list(&self) -> ListView<T> {
        self.keyed_view()
    }
}
