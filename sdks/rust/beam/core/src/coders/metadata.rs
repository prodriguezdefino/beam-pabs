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

//! Element metadata: per-element data that the runner writes after the pane.

use std::io::{Read, Write};

use model::fn_execution::elements::{
    ElementMetadata as ElementMetadataProto, drain_mode, value_kind,
};
use prost::Message;

use super::standard::{VarIntCoder, read_exact_vec};
use super::traits::CoderError;

/// Tells if an element was produced during a drain: sources stop and the rest of the pipeline
/// finishes the elements in flight. A DoFn can use it to decide not to schedule more work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum CausedByDrain {
    /// Normal processing.
    #[default]
    Normal,
    /// The element is processed as part of a drain.
    CausedByDrain,
}

/// The change-data-capture operation an element represents.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ValueKind {
    /// A new record was created in the source system. An absent kind also means insert.
    #[default]
    Insert,
    /// The state of a record immediately before an update.
    UpdateBefore,
    /// The state of a record immediately after an update.
    UpdateAfter,
    /// An existing record was removed from the source system.
    Delete,
}

/// Per-element data: a length-prefixed `Elements.ElementMetadata` proto between the pane and
/// the value. The `0x80` pane bit marks it, and a reader can skip contents it cannot interpret.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ElementMetadata {
    pub drain: CausedByDrain,
    pub value_kind: ValueKind,
    /// W3C trace context identifying the request this element belongs to.
    pub traceparent: Option<String>,
    /// W3C trace context vendor state accompanying `traceparent`.
    pub tracestate: Option<String>,
}

impl ElementMetadata {
    /// Returns true if every field is default. Default metadata is not written.
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    pub fn is_draining(&self) -> bool {
        self.drain == CausedByDrain::CausedByDrain
    }

    /// Writes a length-prefixed proto, always populating `drain` and `value_kind`.
    pub fn encode(&self, writer: &mut dyn Write) -> Result<(), CoderError> {
        let drain = match self.drain {
            CausedByDrain::Normal => drain_mode::Enum::NotDraining,
            CausedByDrain::CausedByDrain => drain_mode::Enum::Draining,
        };
        let value_kind = match self.value_kind {
            ValueKind::Insert => value_kind::Enum::Insert,
            ValueKind::UpdateBefore => value_kind::Enum::UpdateBefore,
            ValueKind::UpdateAfter => value_kind::Enum::UpdateAfter,
            ValueKind::Delete => value_kind::Enum::Delete,
        };
        let bytes = ElementMetadataProto {
            drain: Some(drain as i32),
            traceparent: self.traceparent.clone(),
            tracestate: self.tracestate.clone(),
            value_kind: Some(value_kind as i32),
        }
        .encode_to_vec();

        VarIntCoder::encode_varint(bytes.len() as i64, writer)?;
        writer.write_all(&bytes)?;
        Ok(())
    }

    /// Reads a length-prefixed metadata proto. An unknown value kind is an error.
    pub fn decode(reader: &mut dyn Read) -> Result<Self, CoderError> {
        let len = VarIntCoder::decode_varint(reader)?;
        let len = usize::try_from(len)
            .map_err(|_| CoderError::Format(format!("element metadata declares {len} bytes")))?;
        let proto = ElementMetadataProto::decode(read_exact_vec(reader, len)?.as_slice())
            .map_err(|e| CoderError::Format(format!("malformed element metadata: {e}")))?;

        // Only an explicit DRAINING value is a drain, also for an unknown value from a newer
        // runner: a false drain would stop a healthy pipeline.
        let drain = if proto.drain == Some(drain_mode::Enum::Draining as i32) {
            CausedByDrain::CausedByDrain
        } else {
            CausedByDrain::Normal
        };

        // A guess of INSERT could turn a delete into an insert without an error.
        let value_kind = match proto.value_kind.map(value_kind::Enum::try_from) {
            None | Some(Ok(value_kind::Enum::ValueKindUnspecified | value_kind::Enum::Insert)) => {
                ValueKind::Insert
            }
            Some(Ok(value_kind::Enum::UpdateBefore)) => ValueKind::UpdateBefore,
            Some(Ok(value_kind::Enum::UpdateAfter)) => ValueKind::UpdateAfter,
            Some(Ok(value_kind::Enum::Delete)) => ValueKind::Delete,
            Some(Err(_)) => {
                return Err(CoderError::Format(format!(
                    "unrecognised value kind {}",
                    proto.value_kind.unwrap_or_default()
                )));
            }
        };

        Ok(Self {
            drain,
            value_kind,
            traceparent: proto.traceparent,
            tracestate: proto.tracestate,
        })
    }
}
