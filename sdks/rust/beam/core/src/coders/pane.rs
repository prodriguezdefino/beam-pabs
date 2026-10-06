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

//! `PaneInfo`: the variable-length pane descriptor carried by windowed values and timers.

use std::io::{Error, ErrorKind, Read, Write};

use super::standard::{VarIntCoder, read_array};

/// When a pane fired relative to the watermark of its window. The discriminants are the wire
/// encoding that all SDKs decode: do not change them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Timing {
    /// Fired before the input watermark passed the end of the window.
    Early = 0,
    /// Fired because the input watermark passed the end of the window.
    OnTime = 1,
    /// Fired after the output watermark passed the end of the window.
    Late = 2,
    /// Not from a trigger firing. Every element has this timing before its first `GroupByKey`.
    #[default]
    Unknown = 3,
}

impl Timing {
    /// Maps the two-bit timing field of a pane's leading byte.
    fn from_ordinal(ordinal: u8) -> Self {
        match ordinal {
            0 => Self::Early,
            1 => Self::OnTime,
            2 => Self::Late,
            _ => Self::Unknown,
        }
    }
}

/// The trigger firing that produced an element, and its position among the window's firings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PaneInfo {
    /// True if this is the first pane for the window.
    pub is_first: bool,
    /// True if this is the last pane for the window.
    pub is_last: bool,
    /// When this pane fired relative to the window's watermark.
    pub timing: Timing,
    /// Zero-based index of this firing among all firings for the window.
    pub index: i64,
    /// Zero-based index of this firing among the non-speculative firings. It is `-1` for a
    /// [`Timing::Early`] pane, which has no non-speculative position.
    pub on_time_index: i64,
}

impl Default for PaneInfo {
    fn default() -> Self {
        Self::NO_FIRING
    }
}

impl PaneInfo {
    /// The pane for elements that did not pass through a trigger, such as source reads.
    pub const NO_FIRING: Self = Self {
        is_first: true,
        is_last: true,
        timing: Timing::Unknown,
        index: 0,
        on_time_index: 0,
    };

    /// The pane for a window that fires exactly once, on time.
    pub const ON_TIME_AND_ONLY_FIRING: Self = Self {
        is_first: true,
        is_last: true,
        timing: Timing::OnTime,
        index: 0,
        on_time_index: 0,
    };

    /// Flag in the leading byte that marks a length-prefixed element metadata blob after the
    /// pane. It is not part of the encoding tag. Mask it off before you read the tag.
    const ELEMENT_METADATA_MASK: u8 = 0x80;
    /// The pane is only the leading byte.
    const TAG_FIRST: u8 = 0x00;
    /// One varint index follows. The decoder derives the on-time index.
    const TAG_ONE_INDEX: u8 = 0x10;
    /// Both indices follow as varints.
    const TAG_TWO_INDICES: u8 = 0x20;

    pub fn new(
        is_first: bool,
        is_last: bool,
        timing: Timing,
        index: i64,
        on_time_index: i64,
    ) -> Self {
        Self {
            is_first,
            is_last,
            timing,
            index,
            on_time_index,
        }
    }

    /// Returns the low nibble of the leading byte: the two flags and the timing ordinal.
    fn flags(&self) -> u8 {
        u8::from(self.is_first) | (u8::from(self.is_last) << 1) | ((self.timing as u8) << 2)
    }

    /// Picks the shortest encoding. A [`Timing::Unknown`] pane always uses [`Self::TAG_FIRST`],
    /// which drops its indices. Keep this rule: other SDKs decode other bytes differently.
    fn encoding_tag(&self) -> u8 {
        if (self.index == 0 && self.on_time_index == 0) || self.timing == Timing::Unknown {
            Self::TAG_FIRST
        } else if self.index == self.on_time_index || self.timing == Timing::Early {
            Self::TAG_ONE_INDEX
        } else {
            Self::TAG_TWO_INDICES
        }
    }

    /// Writes the pane. Set `element_metadata` if a metadata blob follows the pane.
    pub fn encode(&self, element_metadata: bool, writer: &mut dyn Write) -> Result<(), Error> {
        let tag = self.encoding_tag();
        let metadata_bit = if element_metadata {
            Self::ELEMENT_METADATA_MASK
        } else {
            0
        };
        writer.write_all(&[self.flags() | tag | metadata_bit])?;
        if tag != Self::TAG_FIRST {
            VarIntCoder::encode_varint(self.index, writer)?;
        }
        if tag == Self::TAG_TWO_INDICES {
            VarIntCoder::encode_varint(self.on_time_index, writer)?;
        }
        Ok(())
    }

    /// Reads a pane. The `bool` is true if a length-prefixed element metadata blob follows.
    pub fn decode(reader: &mut dyn Read) -> Result<(Self, bool), Error> {
        let [byte] = read_array(reader)?;
        let element_metadata = byte & Self::ELEMENT_METADATA_MASK != 0;
        let is_first = byte & 0x01 != 0;
        let is_last = byte & 0x02 != 0;
        let timing = Timing::from_ordinal((byte & 0x0C) >> 2);

        // Derive the indices that the encoding omits. `-1` means "no such position". A
        // non-first pane has no known index, and a speculative pane has no on-time index.
        let (index, on_time_index) = match byte & !Self::ELEMENT_METADATA_MASK & 0xF0 {
            Self::TAG_FIRST => (
                if is_first { 0 } else { -1 },
                if timing == Timing::Early { -1 } else { 0 },
            ),
            Self::TAG_ONE_INDEX => {
                let index = VarIntCoder::decode_varint(reader)?;
                (index, if timing == Timing::Early { -1 } else { index })
            }
            Self::TAG_TWO_INDICES => (
                VarIntCoder::decode_varint(reader)?,
                VarIntCoder::decode_varint(reader)?,
            ),
            tag => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Invalid pane encoding {}", tag >> 4),
                ));
            }
        };

        Ok((
            Self {
                is_first,
                is_last,
                timing,
                index,
                on_time_index,
            },
            element_metadata,
        ))
    }
}
