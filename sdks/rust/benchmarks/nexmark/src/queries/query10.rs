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

//! Query 10 (`LOG_TO_SHARDED_FILES`): windowed event archival into sharded log and index files.

use std::hash::{Hash, Hasher};
use std::time::Duration;

use beam::coders::{
    BeamIterable, Coder, CoderError, CoderRegistry, Context, DefaultCoder, Timing, URN_BYTES,
    VarIntCoder,
};
use beam::io::file::write_bytes;
use beam::prelude::*;
use beam::transforms::Windowed;
use serde::{Deserialize, Serialize};

use crate::config::NexmarkConfig;
use crate::model::{Done, Event, EventCoder, decode_framed, encode_framed};

const NUM_SHARDS_PER_WORKER: i32 = 5;
const LATE_BATCHING_PERIOD: Duration = Duration::from_secs(10);
const DEFAULT_ALLOWED_LATENESS: Duration = Duration::from_secs(86_400);

/// Archives fixed-window [`Event`] panes into sharded `LOG-*` files and per-window `INDEX-*` manifests.
#[derive(Clone, Debug)]
pub struct Query10 {
    window_size_sec: i64,
    max_log_events: i32,
    output_path: Option<String>,
    max_num_workers: i32,
    allowed_lateness: Duration,
}

impl Query10 {
    /// Builds a sharded archival transform for fixed windows and bounded pane sizes.
    pub fn new(window_size_sec: i64, max_log_events: i32) -> Self {
        Self {
            window_size_sec: window_size_sec.max(1),
            max_log_events: max_log_events.max(1),
            output_path: None,
            max_num_workers: 1,
            allowed_lateness: DEFAULT_ALLOWED_LATENESS,
        }
    }

    /// Builds a sharded archival transform from benchmark settings.
    pub fn from_config(config: &NexmarkConfig) -> Self {
        Self::new(config.window_size_sec, config.max_log_events)
    }

    /// Sets the directory or URI prefix for `LOG-*` and `INDEX-*` files (`None` skips file writes).
    pub fn with_output_path(mut self, output_path: Option<String>) -> Self {
        self.output_path = output_path;
        self
    }

    /// Sets the worker count multiplier (`5` log shards per worker).
    pub fn with_max_num_workers(mut self, max_num_workers: i32) -> Self {
        self.max_num_workers = max_num_workers.max(1);
        self
    }

    /// Overrides the late-data horizon on both fixed-window stages.
    pub fn with_allowed_lateness(mut self, allowed_lateness: Duration) -> Self {
        self.allowed_lateness = allowed_lateness;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct OutputFile {
    max_timestamp_ms: i64,
    shard: String,
    index: i64,
    timing: Timing,
    filename: Option<String>,
}

impl OutputFile {
    fn to_index_line(&self) -> String {
        format!(
            "{} {} {} {} {}\n",
            Instant::from_millis(self.max_timestamp_ms),
            self.shard,
            self.index,
            timing_name(self.timing),
            self.filename.as_deref().unwrap_or("null"),
        )
    }

    fn encode_raw(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        VarIntCoder::encode_varint(self.max_timestamp_ms, writer)?;
        self.shard.encode_element(writer)?;
        VarIntCoder::encode_varint(self.index, writer)?;
        VarIntCoder::encode_varint(i64::from(self.timing as u8), writer)?;
        self.filename.encode_element(writer)
    }

    fn decode_raw(reader: &mut &[u8]) -> Result<Self, CoderError> {
        let max_timestamp_ms = VarIntCoder::decode_varint(reader)?;
        let shard = String::decode_element(reader)?;
        let index = VarIntCoder::decode_varint(reader)?;
        let timing = match VarIntCoder::decode_varint(reader)? {
            0 => Timing::Early,
            1 => Timing::OnTime,
            2 => Timing::Late,
            3 => Timing::Unknown,
            other => return Err(CoderError::Format(format!("invalid timing tag: {other}"))),
        };
        Ok(Self {
            max_timestamp_ms,
            shard,
            index,
            timing,
            filename: Option::<String>::decode_element(reader)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct OutputFileCoder;

impl Coder<OutputFile> for OutputFileCoder {
    fn urn(&self) -> &'static str {
        URN_BYTES
    }

    fn encode(
        &self,
        value: &OutputFile,
        writer: &mut Vec<u8>,
        context: Context,
    ) -> Result<(), CoderError> {
        encode_framed(writer, context, |out| value.encode_raw(out))
    }

    fn decode(&self, reader: &mut &[u8], context: Context) -> Result<OutputFile, CoderError> {
        decode_framed(reader, context, OutputFile::decode_raw)
    }
}

impl DefaultCoder for OutputFile {
    type Coder = OutputFileCoder;

    fn coder() -> Self::Coder {
        OutputFileCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        OutputFileCoder.encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        OutputFileCoder.decode(reader, Context::Nested)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_BYTES, Vec::new())
    }
}

struct FnvHasher(u64);

impl FnvHasher {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0100_0000_01b3;

    const fn new() -> Self {
        Self(Self::OFFSET_BASIS)
    }
}

impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0 = bytes.iter().fold(self.0, |acc, &b| {
            (acc ^ u64::from(b)).wrapping_mul(Self::PRIME)
        });
    }
}

fn hash_value<T: Hash>(value: &T) -> u64 {
    let mut hasher = FnvHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

/// Stable hash of the fixed-size fields of `event`. The strings do not change the shard.
fn shard_hash(event: &Event) -> u64 {
    match event {
        Event::Person(p) => mix_words(&[0, p.id, p.date_time.as_millis()]),
        Event::Auction(a) => mix_words(&[
            1,
            a.id,
            a.initial_bid,
            a.reserve,
            a.date_time.as_millis(),
            a.expires.as_millis(),
            a.seller,
            a.category,
        ]),
        Event::Bid(b) => mix_words(&[2, b.auction, b.bidder, b.price, b.date_time.as_millis()]),
    }
}

/// Combines 64-bit words, then applies the MurmurHash3 finalizer so that all bits change the
/// shard.
fn mix_words(words: &[i64]) -> u64 {
    const MULTIPLIER: u64 = 0x517c_c1b7_2722_0a95;
    let h = words.iter().fold(FnvHasher::OFFSET_BASIS, |acc, &w| {
        (acc.rotate_left(5) ^ (w as u64)).wrapping_mul(MULTIPLIER)
    });
    let h = (h ^ (h >> 33)).wrapping_mul(0xff51_afd7_ed55_8ccd);
    let h = (h ^ (h >> 33)).wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

fn shard_name(shard_num: usize, num_shards: usize) -> String {
    format!("shard-{shard_num:05}-of-{num_shards:05}")
}

fn late_event_counter() -> Counter {
    Metrics::counter("Query10", "actuallyLateEvent")
}

fn on_time_counter() -> Counter {
    Metrics::counter("Query10", "onTimeCounter")
}

/// Keys each event by its log shard. `setup` builds the shard names one time.
#[derive(Clone, Debug, Serialize, Deserialize, Register)]
struct ShardEventsFn {
    num_shards: usize,
    #[serde(skip)]
    shard_names: Vec<String>,
    #[serde(skip, default = "late_event_counter")]
    late_events: Counter,
    #[serde(skip, default = "on_time_counter")]
    on_time_events: Counter,
}

impl ShardEventsFn {
    fn new(num_shards: usize) -> Self {
        Self {
            num_shards: num_shards.max(1),
            shard_names: Vec::new(),
            late_events: late_event_counter(),
            on_time_events: on_time_counter(),
        }
    }
}

impl DoFn for ShardEventsFn {
    type In = Event;
    type Out = (String, Event);

    fn setup(&mut self) -> Result {
        self.shard_names = (0..self.num_shards)
            .map(|shard_num| shard_name(shard_num, self.num_shards))
            .collect();
        Ok(())
    }

    fn process_element(
        &mut self,
        event: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        if event.has_annotation("LATE") {
            self.late_events.inc();
        } else {
            self.on_time_events.inc();
        }
        let shard_num = (shard_hash(&event) % self.num_shards as u64) as usize;
        let shard = match self.shard_names.get(shard_num) {
            Some(name) => name.clone(),
            None => shard_name(shard_num, self.num_shards),
        };
        ctx.emit((shard, event))
    }
}

fn timing_short_tag(timing: Timing) -> &'static str {
    match timing {
        Timing::Early => "E",
        Timing::OnTime => "O",
        Timing::Late => "L",
        Timing::Unknown => "U",
    }
}

fn timing_name(timing: Timing) -> &'static str {
    match timing {
        Timing::Early => "EARLY",
        Timing::OnTime => "ON_TIME",
        Timing::Late => "LATE",
        Timing::Unknown => "UNKNOWN",
    }
}

impl PTransform<PCollection<Event>> for Query10 {
    type Output = PCollection<Done>;

    fn expand(self, input: &PCollection<Event>) -> PCollection<Done> {
        let window_size = Duration::from_secs(u64::try_from(self.window_size_sec).unwrap_or(1));
        let max_log_events = self.max_log_events;
        let num_log_shards =
            (self.max_num_workers.max(1) as usize) * (NUM_SHARDS_PER_WORKER as usize);
        let upload_prefix = self.output_path.clone();
        let index_prefix = self.output_path;

        input
            .par_do("ShardEvents", ShardEventsFn::new(num_log_shards))
            .apply(
                "WindowEvents",
                WindowInto::new(FixedWindows::of(window_size))
                    .with_trigger(
                        Trigger::after_end_of_window()
                            .with_early_firings(Trigger::after_count(max_log_events))
                            .with_late_firings(Trigger::after_any(vec![
                                Trigger::after_count(max_log_events),
                                Trigger::after_processing_time(LATE_BATCHING_PERIOD),
                            ])),
                    )
                    .with_accumulation_mode(AccumulationMode::Discarding)
                    .with_allowed_lateness(self.allowed_lateness),
            )
            .group_by_key("GroupByKey")
            .par_do_fn(
                "CheckForLateEvents",
                |(shard, events): (String, BeamIterable<Event>), ctx| {
                    let batch = events.into_vec()?;
                    let (num_late, num_on_time) =
                        batch.iter().fold((0_usize, 0_usize), |(late, on_time), ev| {
                            if ev.has_annotation("LATE") {
                                (late + 1, on_time)
                            } else {
                                (late, on_time + 1)
                            }
                        });
                    match ctx.pane().timing() {
                        Timing::Late => {
                            if num_late == 0 {
                                Metrics::counter("Query10", "ERROR_unexpectedLatePane").inc();
                            }
                            if num_on_time > 0 {
                                Metrics::counter("Query10", "ERROR_unexpectedOnTimeElement").inc();
                            }
                            Metrics::counter("Query10", "lateShard").inc();
                        }
                        Timing::Early => {
                            Metrics::counter("Query10", "earlyShard").inc();
                        }
                        Timing::OnTime | Timing::Unknown => {
                            Metrics::counter("Query10", "onTimeShard").inc();
                        }
                    }
                    ctx.emit((shard, batch))
                },
            )
            .par_do_fn(
                "UploadEvents",
                move |Windowed {
                          value: (shard, batch),
                          window,
                      }: Windowed<(String, Vec<Event>)>,
                      ctx| {
                    let max_ts = window.max_timestamp().as_millis();
                    let pane = ctx.pane();
                    let pane_idx = pane
                        .index()
                        .and_then(|idx| i64::try_from(idx).ok())
                        .unwrap_or(0);
                    let timing_char = timing_short_tag(pane.timing());

                    let filename = match upload_prefix.as_deref() {
                        Some(prefix) => {
                            let payload = batch.iter().try_fold(
                                Vec::with_capacity(batch.len() * 128),
                                |mut buf, event| {
                                    EventCoder.encode(event, &mut buf, Context::Nested)?;
                                    Ok::<_, CoderError>(buf)
                                },
                            )?;
                            let hash_hex =
                                hash_value(&(max_ts, &shard, pane_idx, timing_char, &payload));
                            let path = format!(
                                "{}/LOG-{max_ts}-{shard}-{pane_idx:03}-{timing_char}-{hash_hex:016x}",
                                prefix.trim_end_matches('/')
                            );
                            write_bytes(&path, &payload)?;
                            Metrics::counter("Query10", "writtenRecords")
                                .inc_by(batch.len() as i64);
                            Some(path)
                        }
                        None => None,
                    };

                    Metrics::counter("Query10", "savedFile").inc();
                    ctx.emit((
                        (),
                        OutputFile {
                            max_timestamp_ms: max_ts,
                            shard,
                            index: pane_idx,
                            timing: pane.timing(),
                            filename,
                        },
                    ))
                },
            )
            .apply(
                "WindowLogFiles",
                WindowInto::new(FixedWindows::of(window_size))
                    .with_trigger(Trigger::Default)
                    .with_accumulation_mode(AccumulationMode::Discarding)
                    .with_allowed_lateness(self.allowed_lateness),
            )
            .group_by_key("GroupByKey2")
            .par_do_fn(
                "Index",
                move |Windowed {
                          value: (_, output_files),
                          window,
                      }: Windowed<((), BeamIterable<OutputFile>)>,
                      ctx| {
                    let pane = ctx.pane();
                    let pane_idx = pane.index().unwrap_or(0);
                    match pane.timing() {
                        Timing::Late => {
                            Metrics::counter("Query10", "ERROR_unexpectedLate").inc();
                            Ok(())
                        }
                        Timing::Early => {
                            Metrics::counter("Query10", "ERROR_unexpectedEarly").inc();
                            Ok(())
                        }
                        Timing::OnTime if pane_idx != 0 => {
                            Metrics::counter("Query10", "ERROR_unexpectedIndex").inc();
                            Ok(())
                        }
                        Timing::OnTime | Timing::Unknown => {
                            let max_ts = window.max_timestamp().as_millis();
                            if let Some(prefix) = index_prefix.as_deref() {
                                let mut files = output_files.into_vec()?;
                                files.sort();
                                let body: String =
                                    files.iter().map(OutputFile::to_index_line).collect();
                                let index_path =
                                    format!("{}/INDEX-{max_ts}", prefix.trim_end_matches('/'));
                                write_bytes(&index_path, body.as_bytes())?;
                            }
                            Metrics::counter("Query10", "indexed").inc();
                            ctx.emit(Done::new(format!("written for timestamp {max_ts}")))
                        }
                    }
                },
            )
    }
}
