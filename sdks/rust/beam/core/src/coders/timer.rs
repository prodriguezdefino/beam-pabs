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

//! Standard Beam timer coder (`beam:coder:timer:v1`) for the timer sets, clears and firings in
//! the Fn API `Elements.timers`.

use std::collections::HashMap;
use std::io::{Error, ErrorKind, Write};

use model::pipeline::Coder as ProtoCoder;

use super::pane::PaneInfo;
use super::standard::{
    VarIntCoder, consumed, encode_timestamp, read_array, read_be_i32, read_exact_vec,
    read_timestamp,
};
use super::traversal::{SkipPlan, skip_length_prefixed};
use crate::Instant;

/// One timer event on the Fn API data plane, inbound or outbound. `user_key` and `windows` hold
/// encoded bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerRecord {
    pub user_key: Vec<u8>,
    /// Dynamic timer tag. It is an empty string for a static timer.
    pub dynamic_tag: String,
    pub windows: Vec<Vec<u8>>,
    pub action: TimerAction,
}

/// What a timer event does. A clear has no timestamps and no pane, so it cannot hold them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerAction {
    /// Removes the timer.
    Clear,
    /// Sets the timer, or fires it (inbound).
    Set {
        /// Fire timestamp in event time.
        fire_timestamp: Instant,
        /// Output watermark hold timestamp in event time.
        hold_timestamp: Instant,
        pane: PaneInfo,
    },
}

impl TimerRecord {
    /// Creates a timer set record in the global window, with the default pane.
    pub fn new_set(
        user_key: Vec<u8>,
        dynamic_tag: impl Into<String>,
        fire_timestamp: Instant,
        hold_timestamp: Instant,
    ) -> Self {
        Self {
            user_key,
            dynamic_tag: dynamic_tag.into(),
            windows: vec![Vec::new()], // One global window, which encodes as 0 bytes.
            action: TimerAction::Set {
                fire_timestamp,
                hold_timestamp,
                pane: PaneInfo::NO_FIRING,
            },
        }
    }

    /// Creates a timer clear record in the global window.
    pub fn new_clear(user_key: Vec<u8>, dynamic_tag: impl Into<String>) -> Self {
        Self {
            user_key,
            dynamic_tag: dynamic_tag.into(),
            windows: vec![Vec::new()],
            action: TimerAction::Clear,
        }
    }
}

pub struct TimerCoder;

impl TimerCoder {
    /// `user_key` and each window must already be encoded in the nested context.
    pub fn encode(record: &TimerRecord, writer: &mut impl Write) -> Result<(), Error> {
        writer.write_all(&record.user_key)?;

        let tag_bytes = record.dynamic_tag.as_bytes();
        VarIntCoder::encode_varint(tag_bytes.len() as i64, writer)?;
        writer.write_all(tag_bytes)?;

        let count = i32::try_from(record.windows.len())
            .map_err(|_| Error::new(ErrorKind::InvalidData, "timer window count exceeds i32"))?;
        writer.write_all(&count.to_be_bytes())?;
        record
            .windows
            .iter()
            .try_for_each(|win| writer.write_all(win))?;

        match record.action {
            TimerAction::Clear => writer.write_all(&[0x01]),
            TimerAction::Set {
                fire_timestamp,
                hold_timestamp,
                pane,
            } => {
                writer.write_all(&[0x00])?;
                writer.write_all(&encode_timestamp(fire_timestamp.as_millis()))?;
                writer.write_all(&encode_timestamp(hold_timestamp.as_millis()))?;
                pane.encode(false, writer)
            }
        }
    }

    /// The key and window coders give the length of the key and of each window. To decode
    /// many timers, compile the coders once and call [`decode_with`](Self::decode_with).
    pub fn decode(
        cursor: &mut &[u8],
        key_coder_id: &str,
        window_coder_id: &str,
        coders: &HashMap<String, ProtoCoder>,
    ) -> Result<TimerRecord, Error> {
        Self::decode_with(
            cursor,
            &SkipPlan::new(key_coder_id, coders),
            &SkipPlan::new(window_coder_id, coders),
        )
    }

    /// Decodes one timer. `key` and `window` give the length of the key and of each window.
    pub fn decode_with(
        cursor: &mut &[u8],
        key: &SkipPlan,
        window: &SkipPlan,
    ) -> Result<TimerRecord, Error> {
        let key_start = *cursor;
        key.skip(cursor, true)?;
        let user_key = consumed(key_start, cursor).to_vec();

        // A negative length would wrap to a huge `usize` and reserve that much memory.
        let tag_len = usize::try_from(VarIntCoder::decode_varint(cursor)?)
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Negative timer tag length"))?;
        let tag_buf = read_exact_vec(cursor, tag_len)?;
        let dynamic_tag = String::from_utf8(tag_buf)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

        // A negative count is an empty range: a corrupt record would decode with no windows.
        let num_windows = read_be_i32(cursor)?;
        if num_windows < 0 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Negative timer window count: {num_windows}"),
            ));
        }
        let windows: Vec<Vec<u8>> = (0..num_windows)
            .map(|_| {
                let win_start = *cursor;
                window.skip(cursor, true)?;
                Ok(consumed(win_start, cursor).to_vec())
            })
            .collect::<Result<Vec<_>, Error>>()?;

        let [clear_byte] = read_array(cursor)?;
        let action = match clear_byte {
            0x01 => TimerAction::Clear,
            0x00 => {
                let fire_timestamp = Instant::from_millis(read_timestamp(cursor)?);
                let hold_timestamp = Instant::from_millis(read_timestamp(cursor)?);
                let (pane, element_metadata) = PaneInfo::decode(cursor)?;
                // A timer has no element metadata, but a flagged blob must still be read past.
                if element_metadata {
                    skip_length_prefixed(cursor)?;
                }
                TimerAction::Set {
                    fire_timestamp,
                    hold_timestamp,
                    pane,
                }
            }
            other => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Invalid timer clear byte: {other}"),
                ));
            }
        };

        Ok(TimerRecord {
            user_key,
            dynamic_tag,
            windows,
            action,
        })
    }
}
