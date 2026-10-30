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

//! The exactly-once writer of an unbounded input: each shard appends to a buffered stream at
//! offsets that state keeps, and a timer flushes the rows after the bundle commits.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use beam::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder, URN_KV};
use beam::schema::{Row, Schema};
use beam::transforms::{ProcessContext, State, StatefulContext, StatefulDoFn, Timers, ValueState};
use beam::values::{AnySideInput, SingletonView};
use google_cloud_bigquery::client::Write;
use google_cloud_bigquery::error::WriterBuilderError;
use google_cloud_bigquery::write::BufferedWriter;
use google_cloud_bigquery::write::format::Arrow;
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::Code;

use super::append::{Failure, append_at};
use super::client::{APPEND_RETRY, CALL_RETRY, is_transient, with_retries, write_client};
use super::encode::{Encoded, Encoder};
use super::encoded_batch::{EncodedBatch, Request};
use super::projection::Selection;
use super::table::Destination;
use crate::runtime::block_on_async;

/// A stream with no append for this long is finalized. The next batch of its shard opens a
/// new stream.
const IDLE: Duration = Duration::from_secs(60 * 60);

/// Times a batch moves to a new stream before the bundle fails.
const MAX_STREAMS_PER_BATCH: u32 = 3;

/// How many times an append opens a new connection to its stream before the bundle fails, as
/// the Java SDK retries an append that fails for another reason than quota.
const MAX_RECONNECTS: u32 = 5;

/// The state of a shard.
#[derive(State)]
pub struct ShardState {
    /// The buffered stream of the shard.
    stream: ValueState<StreamState>,
}

/// The timers of a shard.
#[derive(Timers)]
pub enum ShardTimer {
    /// Flushes the rows after the last flush, or finalizes an idle stream.
    #[timer(processing_time, id = "flush")]
    Flush,
    /// Flushes and finalizes the stream at the end of the global window.
    #[timer(event_time, id = "expiry")]
    Expiry,
}

/// The buffered stream of a shard. Rows before `next_offset` come from committed bundles.
/// Rows up to `flushed` are visible.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamState {
    pub stream: String,
    pub next_offset: i64,
    pub flushed: i64,
    /// A flush timer is set for the rows after `flushed`.
    pub flush_pending: bool,
}

impl StreamState {
    pub fn new(stream: String) -> Self {
        Self {
            stream,
            next_offset: 0,
            flushed: -1,
            flush_pending: false,
        }
    }

    /// The last offset that a flush makes visible, if a row is not flushed yet.
    pub fn unflushed(&self) -> Option<i64> {
        (self.next_offset - 1 > self.flushed).then_some(self.next_offset - 1)
    }
}

/// Encodes a [`StreamState`] as `KV<string, KV<varint, KV<varint, bool>>>`. A runner can
/// replace a coder that it does not know, such as a row coder, by a length prefix in state,
/// and only standard coders keep the state bytes the same on every runner.
#[derive(Clone, Debug, Default)]
pub struct StreamStateCoder;

impl Coder<StreamState> for StreamStateCoder {
    fn urn(&self) -> &'static str {
        URN_KV
    }

    fn encode(
        &self,
        value: &StreamState,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        value.encode_element(writer)
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<StreamState, CoderError> {
        StreamState::decode_element(reader)
    }
}

impl DefaultCoder for StreamState {
    type Coder = StreamStateCoder;

    fn coder() -> Self::Coder {
        StreamStateCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        self.stream.encode_element(writer)?;
        self.next_offset.encode_element(writer)?;
        self.flushed.encode_element(writer)?;
        self.flush_pending.encode_element(writer)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        Ok(Self {
            stream: String::decode_element(reader)?,
            next_offset: i64::decode_element(reader)?,
            flushed: i64::decode_element(reader)?,
            flush_pending: bool::decode_element(reader)?,
        })
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        let offsets = registry.register_coder(
            URN_KV,
            vec![
                i64::register_coder(registry),
                bool::register_coder(registry),
            ],
        );
        let offsets = registry.register_coder(URN_KV, vec![i64::register_coder(registry), offsets]);
        registry.register_coder(URN_KV, vec![String::register_coder(registry), offsets])
    }
}

/// Appends each batch of a shard to the buffered stream of the shard, at the offset in state.
///
/// The runner commits the new offset with the bundle. A timer set in the bundle fires only
/// after that commit, so its flush makes only committed rows visible. A retried bundle finds
/// its offset written: it flushes the rows before that offset, which committed bundles wrote,
/// finalizes the stream and appends to a new one. The rows of the failed attempt stay in the
/// old stream, after the flushed offset, so nobody reads them.
///
/// The flush timer fires `frequency` after the first append that it covers. After a flush it
/// fires again after [`IDLE`], and finalizes the stream if no append came. A timer at the end
/// of the global window flushes and finalizes the stream when the pipeline drains.
pub struct WriteBufferedFn {
    destination: SingletonView<Destination>,
    selection: Arc<Selection>,
    errors: Arc<Schema>,
    client: Option<Arc<Write>>,
    frequency: Duration,
    append_timeout: Duration,
    encoder: Option<Arc<(Encoder, String)>>,
    writers: HashMap<String, Arc<BufferedWriter<Arrow>>>,
}

impl WriteBufferedFn {
    pub fn new(
        destination: SingletonView<Destination>,
        selection: Arc<Selection>,
        errors: Arc<Schema>,
        client: Option<Arc<Write>>,
        frequency: Duration,
        append_timeout: Duration,
    ) -> Self {
        Self {
            destination,
            selection,
            errors,
            client,
            frequency,
            append_timeout,
            encoder: None,
            writers: HashMap::new(),
        }
    }

    fn encoder(&mut self, ctx: &ProcessContext<'_, Row>) -> beam::Result<Arc<(Encoder, String)>> {
        if let Some(encoder) = &self.encoder {
            return Ok(Arc::clone(encoder));
        }
        let destination = ctx.side_input(&self.destination)?;
        let encoder = Encoder::new(&self.selection, Arc::clone(&self.errors), &destination)?;
        let encoder = Arc::new((encoder, destination.table));
        self.encoder = Some(Arc::clone(&encoder));
        Ok(encoder)
    }

    /// Returns the writer of `stream`, or of a new stream when `stream` is `None`. A copy keeps
    /// the writers of its shards, so it attaches to a stream once.
    fn writer(
        &mut self,
        encoder: &(Encoder, String),
        stream: Option<&str>,
    ) -> beam::Result<Arc<BufferedWriter<Arrow>>> {
        if let Some(writer) = stream.and_then(|stream| self.writers.get(stream)) {
            return Ok(Arc::clone(writer));
        }
        let (client, schema, table) = (
            self.client.clone(),
            encoder.0.writer_schema(),
            encoder.1.clone(),
        );
        let stream = stream.map(str::to_string);
        let writer: BufferedWriter<Arrow> = block_on_async(async move {
            let client = write_client(client).await?;
            with_retries(
                APPEND_RETRY,
                |e: &WriterBuilderError| match e {
                    WriterBuilderError::Rpc { source, .. } => {
                        is_transient(source) || source.http_status_code() == Some(404)
                    }
                    _ => false,
                },
                || {
                    let builder = match &stream {
                        Some(stream) => client.attach_to_stream(stream),
                        None => client.create_stream(&table),
                    };
                    builder.build_arrow(schema.clone())
                },
            )
            .await
            .map_err(io::Error::other)
        })
        .map_err(|e| beam::Error::from(e).context("Cannot open a buffered stream"))?;
        let writer = Arc::new(writer);
        tracing::debug!("Opened buffered stream '{}'", writer.write_stream());
        self.writers
            .insert(writer.write_stream().to_string(), Arc::clone(&writer));
        Ok(writer)
    }

    /// Flushes the committed rows of `state` and finalizes its stream. Fails if BigQuery does
    /// not confirm that every committed row is visible.
    pub fn retire(&mut self, encoder: &(Encoder, String), state: &StreamState) -> beam::Result {
        let writer = self.writer(encoder, Some(&state.stream))?;
        self.writers.remove(&state.stream);
        let (unflushed, written) = (state.unflushed(), state.next_offset);
        tracing::debug!(
            "Retiring buffered stream '{}': written {written}, flushed {}",
            state.stream,
            state.flushed
        );
        block_on_async(async move {
            if let Some(offset) = unflushed {
                flush(&writer, offset).await?;
            }
            finalize(&writer, written).await
        })
        .map_err(|e| beam::Error::from(e).context(format!("Cannot retire '{}'", state.stream)))
    }
}

/// Flushes the rows of `writer` up to `offset`, and fails unless BigQuery confirms that the
/// rows up to `offset` are visible.
///
/// - A response at or after `offset` confirms the flush.
/// - `ALREADY_EXISTS` tells that an earlier flush went past `offset`, as after a retry.
/// - An error that tells that the stream is finalized also confirms the flush: this writer
///   finalizes a stream only after a flush of all its committed rows.
///
/// Any other answer, `NOT_FOUND` included, means that the rows are not visible.
async fn flush(writer: &BufferedWriter<Arrow>, offset: i64) -> io::Result<()> {
    let stream = writer.write_stream();
    let flushed = with_retries(CALL_RETRY, is_transient, || writer.flush(offset)).await;
    tracing::debug!("Flushed buffered stream '{stream}' to offset {offset}: {flushed:?}");
    match flushed {
        Ok(response) if response.offset >= offset => Ok(()),
        Ok(response) => Err(io::Error::other(format!(
            "The flush of '{stream}' to offset {offset} stopped at offset {}",
            response.offset
        ))),
        Err(e) if is_already_flushed(&e) || is_finalized(&e) => {
            tracing::warn!(
                "The flush of '{stream}' to offset {offset} found the rows flushed: {e}"
            );
            Ok(())
        }
        Err(e) => Err(io::Error::other(format!(
            "Cannot flush '{stream}' to offset {offset}: {e}"
        ))),
    }
}

/// Finalizes `writer`, and fails unless the stream holds at least the `written` rows of the
/// committed bundles. Rows of failed attempts can follow them. A stream that is already
/// finalized counts as finalized: the flush before the finalize confirmed its rows.
async fn finalize(writer: &BufferedWriter<Arrow>, written: i64) -> io::Result<()> {
    let stream = writer.write_stream();
    let finalized = with_retries(CALL_RETRY, is_transient, || writer.finalize()).await;
    tracing::debug!(
        "Finalized buffered stream '{stream}' with {written} rows written: {finalized:?}"
    );
    match finalized {
        Ok(response) if response.row_count >= written => Ok(()),
        Ok(response) => Err(io::Error::other(format!(
            "'{stream}' has {} rows after finalize, but the committed bundles wrote {written}",
            response.row_count
        ))),
        Err(e) if is_finalized(&e) => Ok(()),
        Err(e) => Err(io::Error::other(format!("Cannot finalize '{stream}': {e}"))),
    }
}

/// Whether a flush failed because an earlier flush went past its offset.
fn is_already_flushed(error: &Error) -> bool {
    error.status().map(|status| status.code) == Some(Code::AlreadyExists)
}

/// Whether a call failed because the stream is finalized. The code alone does not tell, so
/// the message must also name the state.
fn is_finalized(error: &Error) -> bool {
    error.status().is_some_and(|status| {
        matches!(
            status.code,
            Code::FailedPrecondition | Code::InvalidArgument
        ) && status.message.to_ascii_lowercase().contains("finalized")
    })
}

/// Each copy starts with no writers and its own encoder.
impl Clone for WriteBufferedFn {
    fn clone(&self) -> Self {
        Self::new(
            self.destination.clone(),
            Arc::clone(&self.selection),
            Arc::clone(&self.errors),
            self.client.clone(),
            self.frequency,
            self.append_timeout,
        )
    }
}

impl StatefulDoFn for WriteBufferedFn {
    type Key = i64;
    type Value = EncodedBatch;
    type Out = Row;
    type State = ShardState;
    type Timer = ShardTimer;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.destination]
    }

    fn process_element(
        &mut self,
        &shard: &i64,
        batch: EncodedBatch,
        cells: &mut ShardState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> beam::Result {
        let encoder = self.encoder(ctx)?;
        let stored = cells.stream.read()?;
        let mut state = match &stored {
            Some(state) => state.clone(),
            None => StreamState::new(self.writer(&encoder, None)?.write_stream().to_string()),
        };
        let mut pending = VecDeque::from([Request::from(batch)]);
        let (mut moves, mut reconnects) = (0, 0);
        let mut retried = false;
        while let Some(request) = pending.pop_front() {
            let writer = self.writer(&encoder, Some(&state.stream))?;
            let (rows, offset, timeout) =
                (request.rows.clone(), state.next_offset, self.append_timeout);
            let result = block_on_async(async move {
                Ok(append_at(
                    |rows, offset| writer.append(rows).set_offset(offset).send(),
                    &rows,
                    offset,
                    retried,
                    timeout,
                )
                .await)
            })?;
            retried = false;
            match result {
                Ok(()) => state.next_offset += request.count,
                Err(Failure::Rows(rejected)) if !rejected.is_empty() => {
                    let Encoded { batches, errors } =
                        encoder.0.split_rejected(&request, rejected)?;
                    ctx.emit_all(errors)?;
                    batches
                        .into_iter()
                        .rev()
                        .for_each(|batch| pending.push_front(Request::from(batch)));
                }
                Err(
                    failure @ (Failure::Exists(_) | Failure::OutOfRange(_) | Failure::Stream(_)),
                ) if moves < MAX_STREAMS_PER_BATCH => {
                    tracing::warn!(
                        "Moving shard {shard} from buffered stream '{}' at offset {}: {}",
                        state.stream,
                        state.next_offset,
                        failure.message()
                    );
                    moves += 1;
                    self.retire(&encoder, &state)?;
                    state =
                        StreamState::new(self.writer(&encoder, None)?.write_stream().to_string());
                    pending.push_front(request);
                }
                Err(failure) if failure.needs_new_connection() && reconnects < MAX_RECONNECTS => {
                    // The next call takes a writer on a new connection to the same stream. The
                    // failed call may have written the rows, so `Exists` counts as success.
                    tracing::warn!(
                        "Opening a new connection to buffered stream '{}': {}",
                        state.stream,
                        failure.message()
                    );
                    reconnects += 1;
                    retried = true;
                    self.writers.remove(&state.stream);
                    pending.push_front(request);
                }
                Err(failure) => {
                    return Err(beam::Error::msg(format!(
                        "An append to buffered stream '{}' failed: {}",
                        state.stream,
                        failure.message()
                    )));
                }
            }
        }
        if !state.flush_pending {
            ctx.timer(ShardTimer::Flush).after(self.frequency)?;
            state.flush_pending = true;
        }
        if stored.is_none() {
            ctx.timer(ShardTimer::Expiry)
                .at(ctx.window()?.max_timestamp())?;
        }
        cells.stream.write(state)
    }

    fn on_timer(
        &mut self,
        timer: ShardTimer,
        _shard: &i64,
        cells: &mut ShardState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> beam::Result {
        let encoder = self.encoder(ctx)?;
        let Some(state) = cells.stream.read()? else {
            return Ok(());
        };
        match (timer, state.unflushed()) {
            (ShardTimer::Flush, Some(offset)) => {
                let writer = self.writer(&encoder, Some(&state.stream))?;
                block_on_async(async move { flush(&writer, offset).await })?;
                ctx.timer(ShardTimer::Flush).after(IDLE)?;
                cells.stream.write(StreamState {
                    flushed: offset,
                    flush_pending: false,
                    ..state
                })
            }
            _ => {
                self.retire(&encoder, &state)?;
                // A pending flush timer holds the watermark of the step until it fires, so a
                // drained pipeline stops only after the timer is cleared.
                ctx.timer(ShardTimer::Flush).clear()?;
                cells.stream.clear()
            }
        }
    }
}
