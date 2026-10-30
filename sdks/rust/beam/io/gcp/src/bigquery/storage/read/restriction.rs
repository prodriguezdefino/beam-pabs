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

//! The restriction of one read stream, its coder, and the tracker that claims its rows,
//! splits it and reports progress.

use std::io;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use beam::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use beam::transforms::sdf::{RestrictionError, RestrictionProgress, RestrictionTracker};
use google_cloud_bigquery::client::Read;
use google_cloud_bigquery::model::ReadStream;

use super::session::split_client;
use crate::runtime::block_on_async;

/// The longest wait for a server split and the check of its primary stream. The tracker blocks
/// claims meanwhile, so a split that takes longer is refused.
const SPLIT_TIMEOUT: Duration = Duration::from_secs(5);

/// The smallest server fraction that sets the estimate of the rows in a stream. Below it, the
/// claimed rows divided by the fraction change too much from one response to the next, so the
/// estimate from the session applies.
const MIN_ESTIMATE_FRACTION: f64 = 0.05;

/// The rows `[start, end)` of one stream. `end` is `i64::MAX` until a checkpoint bounds it, because
/// the server ends a stream only when its rows are read. `estimated_rows` is the estimate of the
/// rows in the whole stream, and `row_bytes` the average size of a row. Runners get sizes and
/// progress in bytes, as the Java and Python sources report them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamRestriction {
    pub stream: String,
    pub start: i64,
    pub end: i64,
    pub estimated_rows: i64,
    pub row_bytes: i64,
}

impl StreamRestriction {
    /// All rows of `stream`.
    pub fn new(stream: impl Into<String>, estimated_rows: i64, row_bytes: i64) -> Self {
        Self {
            stream: stream.into(),
            start: 0,
            end: i64::MAX,
            estimated_rows,
            row_bytes: row_bytes.max(1),
        }
    }

    /// The estimate of the rows from `offset` to the end of the restriction.
    pub fn rows_after(&self, offset: i64) -> i64 {
        (self.estimated_rows.min(self.end) - offset).max(0)
    }

    /// The estimate of the bytes from `offset` to the end of the restriction.
    pub fn bytes_after(&self, offset: i64) -> f64 {
        self.rows_after(offset) as f64 * self.row_bytes as f64
    }
}

/// Wire format: a KV of `(stream, (estimated_rows, row_bytes))` and `(start, end)`.
#[derive(Clone, Copy, Debug, Default)]
pub struct StreamRestrictionCoder;

impl Coder<StreamRestriction> for StreamRestrictionCoder {
    fn urn(&self) -> &'static str {
        beam::coders::URN_KV
    }

    fn encode(
        &self,
        value: &StreamRestriction,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        value.encode_element(writer)
    }

    fn decode(
        &self,
        reader: &mut &[u8],
        _context: Context,
    ) -> Result<StreamRestriction, CoderError> {
        StreamRestriction::decode_element(reader)
    }
}

impl DefaultCoder for StreamRestriction {
    type Coder = StreamRestrictionCoder;

    fn coder() -> Self::Coder {
        StreamRestrictionCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        (self.stream.clone(), (self.estimated_rows, self.row_bytes)).encode_element(writer)?;
        (self.start, self.end).encode_element(writer)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let (stream, (estimated_rows, row_bytes)) = <(String, (i64, i64))>::decode_element(reader)?;
        let (start, end) = <(i64, i64)>::decode_element(reader)?;
        Ok(Self {
            stream,
            start,
            end,
            estimated_rows,
            row_bytes,
        })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        <((String, (i64, i64)), (i64, i64))>::register_coder(registry)
    }
}

/// Tracks the rows of one stream. A position is the stream of a response and the end offset of
/// its rows, so one claim covers a whole response. A split replaces the stream, so a claim for
/// rows of the replaced stream fails.
pub struct StreamTracker {
    state: Mutex<StreamState>,
    client: Option<Read>,
}

struct StreamState {
    restriction: StreamRestriction,
    claimed_until: i64,
    finished: bool,
    fraction: f64,
}

impl StreamState {
    /// The estimate of the rows in the whole stream: from the server fraction once it is large
    /// enough, else from the session.
    fn stream_rows(&self) -> f64 {
        match self.fraction >= MIN_ESTIMATE_FRACTION && self.claimed_until > 0 {
            true => self.claimed_until as f64 / self.fraction,
            false => self.restriction.estimated_rows as f64,
        }
    }
}

impl StreamTracker {
    pub fn new(restriction: StreamRestriction) -> Self {
        Self {
            state: Mutex::new(StreamState {
                claimed_until: restriction.start,
                restriction,
                finished: false,
                fraction: 0.0,
            }),
            client: None,
        }
    }

    /// Splits streams with `client` instead of a client with Application Default Credentials.
    pub fn with_client(mut self, client: Option<Read>) -> Self {
        self.client = client;
        self
    }

    /// The offset after the rows claimed so far.
    pub fn claimed_until(&self) -> i64 {
        self.state().claimed_until
    }

    fn state(&self) -> std::sync::MutexGuard<'_, StreamState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records the fraction of the stream that the server reports as read.
    pub fn set_fraction(&self, fraction: f64) {
        self.state().fraction = fraction.clamp(0.0, 1.0);
    }

    /// Records that the server ended the stream at `offset`, so no work is left.
    pub fn finish(&self, offset: i64) {
        let mut state = self.state();
        state.restriction.end = offset.max(state.claimed_until);
        state.finished = true;
    }
}

impl RestrictionTracker for StreamTracker {
    type Position = (String, i64);
    type Restriction = StreamRestriction;

    fn try_claim(&self, (stream, end): &(String, i64)) -> bool {
        let mut state = self.state();
        if state.finished
            || *stream != state.restriction.stream
            || *end <= state.claimed_until
            || *end > state.restriction.end
        {
            return false;
        }
        state.claimed_until = *end;
        true
    }

    /// At `0.0`, splits at the claimed rows: the residual reads the same stream from the next
    /// offset. Otherwise asks the server to split the stream. The primary stream holds the same
    /// first rows as the stream, so reading continues from the same offset, and the residual is the
    /// remainder stream. The split fails if the server split falls before the claimed rows.
    fn try_split(
        &self,
        fraction_of_remainder: f64,
    ) -> Option<(StreamRestriction, StreamRestriction)> {
        let mut state = self.state();
        if state.finished || state.claimed_until >= state.restriction.end {
            return None;
        }
        if fraction_of_remainder <= 0.0 {
            if state.claimed_until == state.restriction.start {
                return None;
            }
            let primary = StreamRestriction {
                end: state.claimed_until,
                ..state.restriction.clone()
            };
            let residual = StreamRestriction {
                start: state.claimed_until,
                estimated_rows: state.stream_rows().ceil() as i64,
                ..state.restriction.clone()
            };
            state.restriction = primary.clone();
            return Some((primary, residual));
        }
        // A checkpoint bounded the restriction, and a server split does not keep the bound.
        if state.restriction.end != i64::MAX {
            return None;
        }
        let point = state.fraction + (1.0 - state.fraction) * fraction_of_remainder.min(1.0);
        if point >= 1.0 {
            return None;
        }
        let (client, stream, offset) = (
            self.client.clone(),
            state.restriction.stream.clone(),
            state.claimed_until,
        );
        let started = std::time::Instant::now();
        let split_name = stream.clone();
        let outcome = block_on_async(async move {
            let split = async {
                let Some((primary, remainder)) =
                    split_stream(client.clone(), stream, point).await?
                else {
                    return Ok(Err((false, "the server did not split the stream")));
                };
                if !can_read(client, primary.clone(), offset).await {
                    return Ok(Err((false, "the split falls before the claimed rows")));
                }
                Ok(Ok((primary, remainder)))
            };
            match tokio::time::timeout(SPLIT_TIMEOUT, split).await {
                Ok(result) => result,
                Err(_) => Ok(Err((true, "the split timed out"))),
            }
        })
        .unwrap_or(Err((true, "the split call failed")));
        let elapsed = started.elapsed().as_millis();
        let (primary_stream, remainder_stream) = match outcome {
            Ok(streams) => streams,
            // A split that the server declines or that falls before the read rows is normal.
            Err((false, reason)) => {
                tracing::debug!(
                    "Refused a split of BigQuery stream '{split_name}' at {point:.3}: {reason}"
                );
                return None;
            }
            Err((true, reason)) => {
                tracing::warn!(
                    "Refused a split of BigQuery stream '{split_name}' at {point:.3} after \
                     {elapsed} ms: {reason}"
                );
                return None;
            }
        };
        tracing::debug!(
            "Split BigQuery stream '{split_name}' at {point:.3} at offset {offset} in {elapsed} ms"
        );
        let rows = state.stream_rows();
        let primary = StreamRestriction {
            stream: primary_stream,
            estimated_rows: (rows * point).ceil() as i64,
            ..state.restriction.clone()
        };
        let residual = StreamRestriction::new(
            remainder_stream,
            (rows * (1.0 - point)).ceil() as i64,
            state.restriction.row_bytes,
        );
        state.restriction = primary.clone();
        // The primary stream holds the first `point` of the rows, so the same claimed rows are a
        // larger fraction of it. The progress does not change until the next server fraction.
        state.fraction = (state.fraction / point).min(1.0);
        Some((primary, residual))
    }

    fn current_restriction(&self) -> StreamRestriction {
        self.state().restriction.clone()
    }

    /// Reports bytes. The rows left come from the estimate of the rows in the stream: from the
    /// server fraction once it is large enough, else from the session.
    fn current_progress(&self) -> RestrictionProgress {
        let state = self.state();
        let restriction = &state.restriction;
        let completed = (state.claimed_until - restriction.start) as f64;
        let remaining = match state.finished {
            true => 0.0,
            false => (state.stream_rows() - state.claimed_until as f64)
                .max(0.0)
                .min((restriction.end - state.claimed_until) as f64),
        };
        let row_bytes = restriction.row_bytes as f64;
        RestrictionProgress::new(completed * row_bytes, remaining * row_bytes)
    }

    fn check_done(&self) -> Result<(), RestrictionError> {
        let state = self.state();
        if state.finished || state.claimed_until >= state.restriction.end {
            Ok(())
        } else {
            Err(RestrictionError::IncompleteWork(format!(
                "rows from offset {} were not read",
                state.claimed_until
            )))
        }
    }
}

/// Splits `stream` at `fraction` of its rows. Returns the primary and remainder streams, or
/// `None` when the server does not split, for example near the end of the stream.
async fn split_stream(
    client: Option<Read>,
    stream: String,
    fraction: f64,
) -> io::Result<Option<(String, String)>> {
    let client = match client {
        Some(client) => client,
        None => split_client().await?,
    };
    let response = client
        .split_read_stream()
        .set_name(stream)
        .set_fraction(fraction)
        .send()
        .await
        .map_err(io::Error::other)?;
    let name = |stream: Option<ReadStream>| stream.map(|s| s.name).filter(|n| !n.is_empty());
    Ok(name(response.primary_stream).zip(name(response.remainder_stream)))
}

/// Returns whether `stream` can be read from `offset`. A stream fails at an offset after its
/// last row.
async fn can_read(client: Option<Read>, stream: String, offset: i64) -> bool {
    let client = match client {
        Some(client) => client,
        None => match split_client().await {
            Ok(client) => client,
            Err(_) => return false,
        },
    };
    match client
        .read_rows()
        .set_read_stream(stream)
        .set_offset(offset)
        .send()
        .await
    {
        Ok(mut responses) => !matches!(responses.next().await, Some(Err(_))),
        Err(_) => false,
    }
}
