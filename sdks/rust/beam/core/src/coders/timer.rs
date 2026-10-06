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
use std::io::{Cursor, Error, ErrorKind, Write};

use model::pipeline::Coder as ProtoCoder;

use super::pane::PaneInfo;
use super::standard::{
    VarIntCoder, encode_timestamp, read_array, read_be_i32, read_exact_vec, read_timestamp,
};
use super::traversal::skip_coder_value;

/// One timer event on the Fn API data plane, inbound or outbound. `user_key` and `windows` hold
/// encoded bytes. A `clear` event ignores the timestamps and the pane.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerRecord {
    pub user_key: Vec<u8>,
    /// Dynamic timer tag. It is an empty string for a static timer.
    pub dynamic_tag: String,
    pub windows: Vec<Vec<u8>>,
    pub clear: bool,
    /// Fire timestamp in milliseconds since the Unix epoch.
    pub fire_timestamp: i64,
    /// Output watermark hold timestamp in milliseconds.
    pub hold_timestamp: i64,
    pub pane: PaneInfo,
}

impl TimerRecord {
    /// Creates a timer set record in the global window, with the default pane.
    pub fn new_set(
        user_key: Vec<u8>,
        dynamic_tag: impl Into<String>,
        fire_timestamp: i64,
        hold_timestamp: i64,
    ) -> Self {
        Self {
            user_key,
            dynamic_tag: dynamic_tag.into(),
            windows: vec![Vec::new()], // One global window, which encodes as 0 bytes.
            clear: false,
            fire_timestamp,
            hold_timestamp,
            pane: PaneInfo::NO_FIRING,
        }
    }

    /// Creates a timer clear record in the global window.
    pub fn new_clear(user_key: Vec<u8>, dynamic_tag: impl Into<String>) -> Self {
        Self {
            user_key,
            dynamic_tag: dynamic_tag.into(),
            windows: vec![Vec::new()],
            clear: true,
            fire_timestamp: 0,
            hold_timestamp: 0,
            pane: PaneInfo::NO_FIRING,
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

        writer.write_all(&(record.windows.len() as i32).to_be_bytes())?;
        record
            .windows
            .iter()
            .try_for_each(|win| writer.write_all(win))?;

        writer.write_all(&[if record.clear { 0x01 } else { 0x00 }])?;

        if !record.clear {
            writer.write_all(&encode_timestamp(record.fire_timestamp))?;
            writer.write_all(&encode_timestamp(record.hold_timestamp))?;
            record.pane.encode(false, writer)?;
        }

        Ok(())
    }

    /// The key and window coders give the length of the key and of each window.
    pub fn decode(
        cursor: &mut Cursor<&[u8]>,
        key_coder_id: &str,
        window_coder_id: &str,
        coders: &HashMap<String, ProtoCoder>,
    ) -> Result<TimerRecord, Error> {
        let key_start = cursor.position() as usize;
        skip_coder_value(cursor, key_coder_id, coders, true)?;
        let key_end = cursor.position() as usize;
        let user_key = cursor.get_ref()[key_start..key_end].to_vec();

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
                let win_start = cursor.position() as usize;
                skip_coder_value(cursor, window_coder_id, coders, true)?;
                let win_end = cursor.position() as usize;
                Ok(cursor.get_ref()[win_start..win_end].to_vec())
            })
            .collect::<Result<Vec<_>, Error>>()?;

        let [clear_byte] = read_array(cursor)?;
        let clear = clear_byte != 0;

        let (fire_timestamp, hold_timestamp, pane) = if clear {
            (0, 0, PaneInfo::NO_FIRING)
        } else {
            let fire_ts = read_timestamp(cursor)?;
            let hold_ts = read_timestamp(cursor)?;
            let (pane, _element_metadata) = PaneInfo::decode(cursor)?;
            (fire_ts, hold_ts, pane)
        };

        Ok(TimerRecord {
            user_key,
            dynamic_tag,
            windows,
            clear,
            fire_timestamp,
            hold_timestamp,
            pane,
        })
    }
}
