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

//! Reads the records of a Kafka topic into rows.
//!
//! [`ReadRecords`] decodes the value of each record with the [`ReadFormat`] of its options.
//! One DoFn lists the partitions of the topic and resolves the offset where each one starts:
//! the committed offset of the consumer group, or the [`StartOffset`]. A splittable DoFn reads
//! each partition, and the runner spreads the partitions across workers. The restriction of
//! a partition is `[offset, i64::MAX)`, so the read has no end. With
//! [`ReadOptions::with_max_read_time`] the read stops after that time and is bounded. A
//! bounded read can also reshuffle the partitions before it reads them, see
//! [`ReadOptions::with_reshuffle_partitions`].
//!
//! The output timestamp of a row is the timestamp of its record. The watermark of a partition
//! is the largest record timestamp. When a reader call waited the max idle time per call with
//! no record and has read every record of the partition, the watermark moves to the wall time
//! less the fetch wait of the consumer (`fetch.wait.max.ms`, 500 ms by default). The reader
//! knows that it has every record from the cached high watermark of librdkafka, once a record
//! or the end-of-partition event proved a fetch response for the assignment. A record with
//! an earlier timestamp that arrives after that is late. Such a record can be one that the
//! broker got during the last fetch wait, or one that the producer held for longer.
//!
//! # Metrics
//!
//! The reader counts `new_assignments` (namespace `kafka_read`, a counter): the calls that
//! assign their partition again instead of taking a parked assignment. A new assignment seeks,
//! waits for a first fetch, and can wait up to 2 seconds for its first record. A healthy value
//! is about the number of partitions that the worker reads, plus the partitions that moved in
//! a rebalance or after a worker restart. A value that grows with the calls means that parked
//! assignments are not reused, and that costs latency. The source counters `bytes_read` and
//! `elements_read` (namespace `source`) count the key and value bytes and the records that
//! the read consumed.
//!
//! With a `group.id` in the consumer properties, the read commits the offset after the last
//! record of each bundle when the runner has committed the bundle. Only a later offset is
//! committed, so the committed offset of a partition never moves back. The read does not need
//! the commits: a restarted pipeline uses them as its start offsets.
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use beam::prelude::*;
//! use kafka_io::native::format::ReadFormat;
//! use kafka_io::native::read::{ReadOptions, ReadRecords, StartOffset};
//!
//! let p = Pipeline::new();
//! let rows = p.apply(ReadRecords::new(
//!     "ReadEvents",
//!     ReadOptions::new("broker-1:9092", "events", ReadFormat::String)
//!         .with_start_offset(StartOffset::Earliest)
//!         .with_consumer_properties([("group.id".to_string(), "events-reader".to_string())])
//!         .with_max_read_time(Duration::from_secs(60)),
//! ));
//! ```

#[doc(hidden)]
pub mod call;
#[doc(hidden)]
pub mod commits;
mod discover;
#[doc(hidden)]
pub mod parking;
mod reader;
#[doc(hidden)]
pub mod restriction;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use beam::schema::{Row, RowElement};
use beam::transforms::sdf::SplittableParDo;
use beam::transforms::{
    DEFAULT_FINALIZER_EXPIRY, PTransform, ParDo, Reshuffle, WithFailures, WithFinalizer,
};
use beam::values::{PBegin, PCollection};

use super::client::{Auth, Connection, MAX_CONSUMERS, ReadId};
use super::format::{ReadFormat, read_error_schema};
use super::{add_composite, transform_ids};
use discover::DiscoverPartitionsFn;
pub use discover::PartitionStart;
use reader::{MAX_IDLE_PER_CALL, MAX_READ_CALL, ReadOutput, ReadPartitionFn, Source};

/// The output name of the records that do not convert.
const FAILURES_TAG: &str = "failures";

/// The default of the librdkafka consumer property `fetch.wait.max.ms`.
const DEFAULT_FETCH_WAIT: Duration = Duration::from_millis(500);

/// The fetch wait of the consumers of `connection`: its `fetch.wait.max.ms`, or the default.
fn fetch_wait(connection: &Connection) -> Duration {
    connection
        .properties
        .get("fetch.wait.max.ms")
        .and_then(|millis| millis.trim().parse().ok())
        .map_or(DEFAULT_FETCH_WAIT, Duration::from_millis)
}

/// Where a partition starts when the consumer group has no committed offset for it, or when
/// the read has no consumer group.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StartOffset {
    /// The first record that the partition holds.
    Earliest,
    /// The offset after the last record, so the read gets only new records.
    #[default]
    Latest,
    /// No start: the read fails for a partition with no committed offset.
    Committed,
}

impl fmt::Display for StartOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Earliest => "earliest",
            Self::Latest => "latest",
            Self::Committed => "none",
        })
    }
}

impl FromStr for StartOffset {
    type Err = beam::Error;

    /// Parses the values of the Kafka property `auto.offset.reset`, in any case.
    fn from_str(name: &str) -> beam::Result<Self> {
        match name.to_ascii_lowercase().as_str() {
            "earliest" => Ok(Self::Earliest),
            "latest" => Ok(Self::Latest),
            "none" => Ok(Self::Committed),
            _ => Err(beam::Error::msg(format!(
                "Unknown start offset '{name}': expected earliest, latest or none"
            ))),
        }
    }
}

/// The number of consumers that a worker process uses to read the partitions of one read.
///
/// A partition keeps one consumer for the life of the process, and a new partition gets the
/// consumer with the fewest partitions. Each consumer fetches its partitions in its own
/// requests, so more consumers give each partition a larger part of a fetch. Each consumer
/// also has its own librdkafka threads, broker connections and prefetch queues
/// (`queued.max.messages.kbytes` for each partition). Two reads never share a consumer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ConsumerPool {
    /// One consumer for each partition that the worker reads, at most [`MAX_CONSUMERS`].
    #[default]
    OnePerPartition,
    /// This number of consumers.
    Size(PoolSize),
}

impl ConsumerPool {
    /// The most consumers of one read in a worker process.
    pub fn size(self) -> usize {
        match self {
            Self::OnePerPartition => MAX_CONSUMERS,
            Self::Size(size) => size.get(),
        }
    }
}

/// A number of consumers from 1 to [`MAX_CONSUMERS`]. Build it with [`TryFrom`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolSize(u8);

impl PoolSize {
    /// The number of consumers.
    pub fn get(self) -> usize {
        usize::from(self.0)
    }
}

impl TryFrom<usize> for PoolSize {
    type Error = beam::Error;

    /// Fails if `size` is 0 or larger than [`MAX_CONSUMERS`].
    fn try_from(size: usize) -> beam::Result<Self> {
        u8::try_from(size)
            .ok()
            .filter(|_| (1..=MAX_CONSUMERS).contains(&size))
            .map(Self)
            .ok_or_else(|| {
                beam::Error::msg(format!(
                    "Set a consumer pool of 1 to {MAX_CONSUMERS} consumers, not {size}"
                ))
            })
    }
}

/// The cluster, the topic and the record format of a read.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadOptions {
    pub connection: Connection,
    pub topic: String,
    pub format: ReadFormat,
    pub start_offset: StartOffset,
    /// The time after which the read stops. `None` reads with no end.
    pub max_read_time: Option<Duration>,
    /// The time after a bundle during which its offset commit can still run.
    pub commit_expiry: Duration,
    /// The longest time that one call of the reader reads a partition. See
    /// [`ReadOptions::with_max_read_time_per_call`].
    pub max_read_time_per_call: Duration,
    /// The longest time that one call of the reader waits for the next record. See
    /// [`ReadOptions::with_max_idle_per_call`].
    pub max_idle_per_call: Duration,
    /// The consumers of a worker process. See [`ConsumerPool`].
    pub consumer_pool: ConsumerPool,
    /// Whether a Reshuffle spreads the partitions before the reader. See
    /// [`ReadOptions::with_reshuffle_partitions`].
    pub reshuffle_partitions: bool,
}

impl ReadOptions {
    pub fn new(
        bootstrap_servers: impl Into<String>,
        topic: impl Into<String>,
        format: ReadFormat,
    ) -> Self {
        Self {
            connection: Connection::new(bootstrap_servers),
            topic: topic.into(),
            format,
            start_offset: StartOffset::default(),
            max_read_time: None,
            commit_expiry: DEFAULT_FINALIZER_EXPIRY,
            max_read_time_per_call: MAX_READ_CALL,
            max_idle_per_call: MAX_IDLE_PER_CALL,
            consumer_pool: ConsumerPool::default(),
            reshuffle_partitions: false,
        }
    }

    pub fn with_start_offset(mut self, start_offset: StartOffset) -> Self {
        self.start_offset = start_offset;
        self
    }

    /// Sets the librdkafka consumer properties. A `group.id` makes the read commit offsets.
    /// They replace the consumer defaults `enable.auto.commit=false`,
    /// `enable.auto.offset.store=false`, `auto.offset.reset=earliest` and
    /// `enable.partition.eof=true` when they set the same names. Without the end-of-partition
    /// event, an idle partition with no record moves its watermark only after a broker request,
    /// at most once in 10 seconds.
    ///
    /// `auto.offset.reset` does not choose where the read starts; see
    /// [`with_start_offset`](Self::with_start_offset). It applies only when the start offset of
    /// a partition, or a later fetch position, is out of the range of the partition: for
    /// example when retention deleted the committed offset, or records that the read did not
    /// reach yet. With the default `earliest`, the read continues at the first record that the
    /// partition still has. With `latest`, it also skips every record up to the end of the
    /// partition. When the log of the partition was truncated, or the topic was created again,
    /// the reset gives offsets at or before the claimed ones. The read then fails its bundle
    /// with an error that names the partition and the offsets, and the runner retries the
    /// bundle until someone starts the read again from a new start offset. The read claims
    /// each offset at most once, so on a runner with exactly-once processing no record is
    /// processed twice because of a reset. After deleted records, only the deleted offsets are
    /// missing.
    pub fn with_consumer_properties(
        mut self,
        properties: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        self.connection.properties.extend(properties);
        self
    }

    /// Authenticates with Google Application Default Credentials, for Google Cloud Managed
    /// Service for Apache Kafka. See [`Auth::GoogleAdc`].
    pub fn with_google_auth(mut self) -> Self {
        self.connection.auth = Auth::GoogleAdc;
        self
    }

    /// Stops the read `max_read_time` after it lists the partitions, so the output is bounded.
    pub fn with_max_read_time(mut self, max_read_time: Duration) -> Self {
        self.max_read_time = Some(max_read_time);
        self
    }

    /// Sets the longest time that one call of the reader reads a partition. The default is
    /// 5 seconds. After this time the reader checkpoints, so the runner commits the bundle
    /// and the read commits its offsets. A shorter time makes more and smaller bundles: if the
    /// runner rejects a bundle, it reads fewer records again, so the write after the read
    /// writes fewer duplicates. A longer time makes fewer bundles, with less cost for each
    /// record.
    pub fn with_max_read_time_per_call(mut self, max_read_time_per_call: Duration) -> Self {
        self.max_read_time_per_call = max_read_time_per_call;
        self
    }

    /// Sets the longest time that one call of the reader waits for the next record. The
    /// default is 1 second. A partition with no record for this time ends the call, and the
    /// read resumes it in a new call. A short gap between two batches of records then does
    /// not end the call. Each new call costs a resume of the read, so a longer time gives
    /// fewer calls. An idle partition holds its thread for this time, and its watermark moves
    /// only at the end of such a call (see the module documentation).
    pub fn with_max_idle_per_call(mut self, max_idle_per_call: Duration) -> Self {
        self.max_idle_per_call = max_idle_per_call;
        self
    }

    /// Sets the number of consumers of a worker process. The default is
    /// [`ConsumerPool::OnePerPartition`]: one consumer for each partition, at most
    /// [`MAX_CONSUMERS`].
    pub fn with_consumer_pool(mut self, consumer_pool: ConsumerPool) -> Self {
        self.consumer_pool = consumer_pool;
        self
    }

    /// Sets how long after a bundle its offset commit can still run. A commit that the runner
    /// confirms later is dropped, and a later bundle commits a later offset.
    pub fn with_commit_expiry(mut self, commit_expiry: Duration) -> Self {
        self.commit_expiry = commit_expiry;
        self
    }

    /// Sets whether a Reshuffle spreads the partitions over the workers before the reader.
    /// The default is no Reshuffle. The Reshuffle moves only the list of partitions, not the
    /// records.
    ///
    /// Only a bounded read accepts it, so also set
    /// [`with_max_read_time`](Self::with_max_read_time). Use it on a batch runner that does
    /// not spread the input of a splittable DoFn over its workers. Such a runner reads all
    /// partitions in one task, and the partitions that start after the stop time read no
    /// record. An unbounded read does not accept it.
    pub fn with_reshuffle_partitions(mut self, reshuffle_partitions: bool) -> Self {
        self.reshuffle_partitions = reshuffle_partitions;
        self
    }

    /// Fails if the connection is not valid, the topic is empty, the max read time, the max
    /// read time per call or the max idle time per call is zero, or a read with no max read
    /// time reshuffles its partitions.
    pub fn validate(&self) -> beam::Result {
        self.connection.validate()?;
        match (
            self.topic.trim().is_empty(),
            self.max_read_time,
            self.max_read_time_per_call,
            self.max_idle_per_call,
            self.reshuffle_partitions,
        ) {
            (true, _, _, _, _) => Err(beam::Error::msg("Set the topic")),
            (false, Some(Duration::ZERO), _, _, _) => {
                Err(beam::Error::msg("Set a max read time larger than zero"))
            }
            (false, _, Duration::ZERO, _, _) => Err(beam::Error::msg(
                "Set a max read time per call larger than zero",
            )),
            (false, _, _, Duration::ZERO, _) => Err(beam::Error::msg(
                "Set a max idle time per call larger than zero",
            )),
            (false, None, _, _, true) => Err(beam::Error::msg(
                "Set a max read time to reshuffle the partitions: only a bounded read \
                 reshuffles them",
            )),
            (false, _, _, _, _) => Ok(()),
        }
    }
}

/// Reads the records of a Kafka topic into rows of the schema of its [`ReadFormat`].
#[derive(Clone, Debug)]
pub struct ReadRecords {
    name: String,
    options: ReadOptions,
}

/// The steps of a read up to the discovered partitions, and what the reader needs.
struct Partitions {
    name: String,
    existing: HashSet<String>,
    source: Arc<Source>,
    partitions: PCollection<PartitionStart>,
}

impl ReadRecords {
    pub fn new(name: impl Into<String>, options: ReadOptions) -> Self {
        Self {
            name: name.into(),
            options,
        }
    }

    /// Fallible form of [`PTransform::expand`]: fails if the options are not valid. The first
    /// record that does not convert fails the pipeline.
    pub fn try_expand(&self, input: &PBegin) -> beam::Result<PCollection<Row>> {
        let partitions = self.expand_partitions(input)?;
        let rows = match partitions.source.connection.group_id() {
            Some(_) => read::<WithFinalizer<RowElement>>(&partitions),
            None => read::<RowElement>(&partitions),
        }
        .as_rows()
        .with_row_schema(&partitions.source.schema);
        finish(input, partitions, &rows, None);
        Ok(rows)
    }

    /// Like [`try_expand`](Self::try_expand), but a record that does not convert goes to
    /// [`failures`](WithFailures::failures) as a row of [`read_error_schema`], and the read
    /// continues.
    pub fn try_expand_with_failures(&self, input: &PBegin) -> beam::Result<WithFailures<Row, Row>> {
        let partitions = self.expand_partitions(input)?;
        let WithFailures { output, failures } = match partitions.source.connection.group_id() {
            Some(_) => read::<WithFinalizer<Result<RowElement, Row>>>(&partitions),
            None => read::<Result<RowElement, Row>>(&partitions),
        };
        let output = output.as_rows().with_row_schema(&partitions.source.schema);
        let failures = failures.with_row_schema(&partitions.source.errors);
        finish(input, partitions, &output, Some(&failures));
        Ok(WithFailures { output, failures })
    }

    /// Checks the options and adds the steps up to the discovered partitions.
    fn expand_partitions(&self, input: &PBegin) -> beam::Result<Partitions> {
        self.options
            .validate()
            .map_err(|e| e.context(self.name.clone()))?;
        let ReadOptions {
            connection,
            topic,
            format,
            start_offset,
            max_read_time,
            commit_expiry,
            max_read_time_per_call,
            max_idle_per_call,
            consumer_pool,
            reshuffle_partitions,
        } = self.options.clone();
        let pipeline = input.pipeline();
        let name = pipeline.unique_transform_name(&self.name);
        let existing = transform_ids(pipeline);
        let (_, impulse) = pipeline.add_impulse(&format!("{name}/Impulse"));
        let discovered = impulse.apply(ParDo::new(
            format!("{name}/DiscoverPartitions"),
            DiscoverPartitionsFn {
                connection: connection.clone(),
                topic: topic.clone(),
                start_offset,
                max_read_time,
            },
        ));
        let partitions = match reshuffle_partitions {
            true => discovered.apply(Reshuffle::new(format!("{name}/Reshuffle"))),
            false => discovered,
        };
        let source = Arc::new(Source {
            read: ReadId::unique(),
            fetch_wait: fetch_wait(&connection),
            connection,
            topic,
            schema: format.row_schema(),
            errors: Arc::new(read_error_schema()),
            format,
            bounded: max_read_time.is_some(),
            commit_expiry,
            max_read_time_per_call,
            max_idle_per_call,
            consumer_pool,
        });
        Ok(Partitions {
            name,
            existing,
            source,
            partitions,
        })
    }
}

/// Reads each partition with outputs of type `O`.
fn read<O: ReadOutput>(partitions: &Partitions) -> O::Collections {
    partitions.partitions.apply(SplittableParDo::new(
        format!("{}/ReadPartitions", partitions.name),
        ReadPartitionFn::<O>::new(Arc::clone(&partitions.source)),
    ))
}

/// Groups the steps of the read under its name.
fn finish(
    input: &PBegin,
    partitions: Partitions,
    rows: &PCollection<Row>,
    failures: Option<&PCollection<Row>>,
) {
    let Partitions { name, existing, .. } = partitions;
    let outputs = [("out", rows)]
        .into_iter()
        .chain(failures.map(|failures| (FAILURES_TAG, failures)))
        .map(|(tag, output)| (tag.to_string(), output.id().to_string()))
        .collect::<HashMap<_, _>>();
    add_composite(input.pipeline(), &name, &existing, outputs);
}

impl PTransform<PBegin> for ReadRecords {
    type Output = PCollection<Row>;

    fn expand(&self, input: &PBegin) -> PCollection<Row> {
        self.try_expand(input)
            .unwrap_or_else(|e| panic!("{} could not be expanded: {e}", self.name))
    }
}
