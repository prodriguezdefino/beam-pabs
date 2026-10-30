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

//! The connection to a cluster, and the producers and consumers that the DoFns of one worker
//! share.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::iter;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError, Weak};
use std::thread;
use std::time::{Duration, Instant};

use beam::Error;
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::producer::ThreadedProducer;
use tracing::warn;

use super::context::KafkaContext;

/// Serializer properties of the Kafka SchemaTransforms of the Managed API. A configuration
/// for those transforms can hold them, and librdkafka rejects them, so the client drops them.
const SERIALIZER_PROPERTIES: [&str; 4] = [
    "key.serializer",
    "value.serializer",
    "key.deserializer",
    "value.deserializer",
];

/// Properties of every producer, unless the connection sets them. Idempotence stops the
/// retries of librdkafka from writing a record twice, and keeps the order in each partition.
const PRODUCER_DEFAULTS: [(&str, &str); 2] = [("enable.idempotence", "true"), ("acks", "all")];

/// Properties of every consumer, unless the connection sets them. The read commits offsets
/// only in a bundle finalizer, so librdkafka must not commit or store them.
///
/// `auto.offset.reset` does not choose where a read starts: the read assigns each partition
/// at an explicit offset. It applies only when that offset, or a later fetch position, is
/// below the first record of the partition or after its last record. With `earliest`, a read
/// whose next records retention or compaction deleted continues at the first record that the
/// partition still has, and reads every record after it. Each offset is claimed at most once,
/// so the read skips only offsets that no longer exist. When the log of the partition was
/// truncated, or the topic was created again, the reset gives offsets before the claimed
/// ones. The read then fails its bundle, and the runner retries it until someone starts the
/// read again from a new start offset.
///
/// The end-of-partition event proves
/// that a fetch response came for an assignment, also for a partition with no record, so the
/// reader can trust the cached high watermark (see `read::call::idle_check`).
const CONSUMER_DEFAULTS: [(&str, &str); 4] = [
    ("enable.auto.commit", "false"),
    ("enable.auto.offset.store", "false"),
    ("auto.offset.reset", "earliest"),
    ("enable.partition.eof", "true"),
];

/// Whether `error` is the end-of-partition event that `enable.partition.eof` turns on (see
/// [`CONSUMER_DEFAULTS`]).
///
/// The reader handles this event on the queue of each partition. librdkafka also sends a copy
/// to the main queue of the consumer, once for each fetch that reaches the end of a partition.
/// A partition that the read keeps up with gives one copy in each fetch wait, so these copies
/// are not errors, and neither [`serve_events`] nor the context logs them as errors.
#[doc(hidden)]
pub fn is_partition_eof(error: &KafkaError) -> bool {
    matches!(error, KafkaError::PartitionEOF(_))
        || matches!(
            error.rdkafka_error_code(),
            Some(RDKafkaErrorCode::PartitionEOF)
        )
}

/// Properties of a client with [`Auth::GoogleAdc`], unless the connection sets them.
const GOOGLE_DEFAULTS: [(&str, &str); 2] = [
    ("security.protocol", "SASL_SSL"),
    ("sasl.mechanism", "OAUTHBEARER"),
];

/// Path of the system CA bundle in Debian and `gcr.io/distroless/cc-debian12`. The vendored
/// OpenSSL build of `rdkafka` looks in `/usr/local/ssl` unless `ssl.ca.location` is set.
const CA_BUNDLE_PATH: &str = "/etc/ssl/certs/ca-certificates.crt";

/// The longest wait for the first token of a consumer with [`Auth::GoogleAdc`].
const FIRST_TOKEN_TIMEOUT: Duration = Duration::from_secs(30);

/// How often the event thread polls the main queue of each shared consumer for token
/// refreshes and offset commit callbacks.
const EVENT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The group of a consumer whose connection sets none. No offset is committed for it.
#[doc(hidden)]
pub const UNCOMMITTED_GROUP: &str = "beam-rust-kafka-read-uncommitted";

/// How the clients of a connection authenticate.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Auth {
    /// As the client properties say, for example with no authentication or with SASL/PLAIN.
    #[default]
    Properties,
    /// SASL/OAUTHBEARER with tokens from Google Application Default Credentials, for Google
    /// Cloud Managed Service for Apache Kafka. Sets `security.protocol=SASL_SSL` and
    /// `sasl.mechanism=OAUTHBEARER` unless the properties set them.
    GoogleAdc,
}

/// The bootstrap servers of a cluster, the librdkafka properties of a client, and how it
/// authenticates.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Connection {
    pub bootstrap_servers: String,
    pub properties: BTreeMap<String, String>,
    pub auth: Auth,
}

impl Connection {
    pub fn new(bootstrap_servers: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            ..Self::default()
        }
    }

    /// Fails if the bootstrap servers are empty, if a property also sets them, or if a
    /// property sets another SASL mechanism than Google authentication needs.
    pub fn validate(&self) -> beam::Result {
        let mechanism = self
            .properties
            .get("sasl.mechanism")
            .filter(|mechanism| !mechanism.eq_ignore_ascii_case("OAUTHBEARER"));
        match (
            self.bootstrap_servers.trim().is_empty(),
            self.properties.contains_key("bootstrap.servers"),
            (self.auth, mechanism),
        ) {
            (true, _, _) => Err(Error::msg("Set the bootstrap servers")),
            (false, true, _) => Err(Error::msg(
                "Set the bootstrap servers once, not in the client properties too",
            )),
            (false, false, (Auth::GoogleAdc, Some(mechanism))) => Err(Error::msg(format!(
                "Google authentication uses sasl.mechanism=OAUTHBEARER, but the properties set \
                 {mechanism}"
            ))),
            (false, false, _) => Ok(()),
        }
    }

    /// The consumer group of the client properties, if they set a non-empty one.
    pub fn group_id(&self) -> Option<&str> {
        self.properties
            .get("group.id")
            .map(String::as_str)
            .filter(|group| !group.is_empty())
    }

    /// The librdkafka configuration: `defaults`, the Google defaults with
    /// [`Auth::GoogleAdc`], then the properties of this connection without the serializer
    /// properties, then the bootstrap servers.
    #[doc(hidden)]
    pub fn client_config(&self, defaults: &[(&str, &str)]) -> ClientConfig {
        let google: &[(&str, &str)] = match self.auth {
            Auth::Properties => &[],
            Auth::GoogleAdc => &GOOGLE_DEFAULTS,
        };
        let ca_bundle = Path::new(CA_BUNDLE_PATH)
            .is_file()
            .then_some(("ssl.ca.location", CA_BUNDLE_PATH));
        let mut config = ClientConfig::new();
        defaults
            .iter()
            .chain(google)
            .copied()
            .chain(ca_bundle)
            .chain(
                self.properties
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str()))
                    .filter(|(key, _)| !SERIALIZER_PROPERTIES.contains(key)),
            )
            .chain([("bootstrap.servers", self.bootstrap_servers.as_str())])
            .for_each(|(key, value)| {
                config.set(key, value);
            });
        config
    }

    /// The configuration of a producer: [`client_config`](Self::client_config) with the
    /// producer defaults.
    #[doc(hidden)]
    pub fn producer_config(&self) -> ClientConfig {
        self.client_config(&PRODUCER_DEFAULTS)
    }

    /// The configuration of a consumer: [`client_config`](Self::client_config) with the
    /// consumer defaults. librdkafka assigns partitions only to a consumer with a group, so a
    /// connection with no group gets [`UNCOMMITTED_GROUP`]. The read commits no offset for it.
    #[doc(hidden)]
    pub fn consumer_config(&self) -> ClientConfig {
        let mut config = self.client_config(&CONSUMER_DEFAULTS);
        if self.group_id().is_none() {
            config.set("group.id", UNCOMMITTED_GROUP);
        }
        config
    }

    fn context(&self) -> KafkaContext {
        KafkaContext::new(self.auth)
    }
}

/// A producer of the connector.
pub type Producer = ThreadedProducer<KafkaContext>;

/// A consumer of the connector.
pub type KafkaConsumer = BaseConsumer<KafkaContext>;

/// A new producer of `connection`. Its polling thread gives it its tokens.
#[doc(hidden)]
pub fn producer(connection: &Connection) -> beam::Result<Producer> {
    connection
        .producer_config()
        .create_with_context(connection.context())
        .map_err(|e| Error::msg(format!("Cannot create the Kafka producer: {e}")))
}

/// A new consumer of `connection`. A consumer gets a token only when its queue is polled, so
/// with Google authentication this polls until librdkafka has the first token.
#[doc(hidden)]
pub fn consumer(connection: &Connection) -> beam::Result<KafkaConsumer> {
    let consumer: KafkaConsumer = connection
        .consumer_config()
        .create_with_context(connection.context())
        .map_err(|e| Error::msg(format!("Cannot create the Kafka consumer: {e}")))?;
    if consumer.context().is_google() {
        let deadline = Instant::now() + FIRST_TOKEN_TIMEOUT;
        while !consumer.context().has_issued_token() {
            if Instant::now() >= deadline {
                return Err(Error::msg(format!(
                    "The Kafka consumer got no Google token in {} s. Check the Application \
                     Default Credentials and the worker logs",
                    FIRST_TOKEN_TIMEOUT.as_secs()
                )));
            }
            // The consumer has no partition yet, so the poll returns no record.
            let _event = consumer.poll(Duration::from_millis(100));
        }
    }
    Ok(consumer)
}

/// Whether a record reached the main queue of a consumer in this process.
static RECORD_IN_MAIN_QUEUE: AtomicBool = AtomicBool::new(false);

/// Serves all queued events of `consumer`: token refreshes, offset commit callbacks and
/// errors, and returns their number. Records of assigned partitions go to their own queues, so
/// a record here belongs to no reader. The first such record in the process logs a warning.
///
/// A poll that serves a callback or a log line returns no event, the same as an empty queue.
/// So the loop continues while the callback count of the context changes. A log line ends the
/// loop early, and the next call serves the rest.
#[doc(hidden)]
pub fn serve_events(consumer: &KafkaConsumer) -> usize {
    let context = consumer.context();
    iter::from_fn(|| {
        let callbacks = context.callbacks();
        match consumer.poll(Duration::ZERO) {
            Some(event) => {
                log_event(&event);
                Some(())
            }
            None => (context.callbacks() != callbacks).then_some(()),
        }
    })
    .count()
}

/// Logs an error, or the first record in the process that reached the main queue.
fn log_event(event: &KafkaResult<BorrowedMessage<'_>>) {
    match event {
        Ok(record) if !RECORD_IN_MAIN_QUEUE.swap(true, Ordering::Relaxed) => warn!(
            "Dropped the record at offset {} of partition {} of Kafka topic '{}': it reached \
             the main queue of the consumer, and no reader polls that queue. Other such \
             records are dropped with no log",
            record.offset(),
            record.partition(),
            record.topic()
        ),
        Ok(_) => {}
        // A copy of the end-of-partition event, which the reader handles on the queue of the
        // partition. See `is_partition_eof`.
        Err(e) if is_partition_eof(e) => {}
        Err(e) => warn!("Kafka consumer error: {e}"),
    }
}

/// The most producers of one connection in a process.
///
/// librdkafka takes a write lock of the whole producer to find the topic of each record. If
/// many DoFn copies send through one producer at the same time, they wait for this lock. So
/// the copies share one producer for each CPU, and each producer batches the records of
/// several copies. Each producer has its own librdkafka threads (one main thread and one for
/// each broker), its own polling thread and its own connections to the brokers.
const MAX_PRODUCERS: usize = 16;

/// The producers of this process: a few for each connection, see [`producer_slots`]. The
/// map holds weak references, so the last DoFn that drops a producer closes it. The lock is
/// held only to find or add a producer; creating one starts its threads and does not wait
/// for the network.
static PRODUCERS: LazyLock<Mutex<HashMap<ClientSlot, Weak<Producer>>>> =
    LazyLock::new(Mutex::default);

/// A connection and the number of one of its producers.
type ClientSlot = (Connection, usize);

/// The slot of the next producer that a DoFn copy gets.
static NEXT_PRODUCER: AtomicUsize = AtomicUsize::new(0);

/// The number of producers of one connection: one for each CPU, at most [`MAX_PRODUCERS`].
#[doc(hidden)]
pub fn producer_slots() -> usize {
    cpus().min(MAX_PRODUCERS)
}

/// The number of CPUs of this process, or 1 if it is not known.
#[doc(hidden)]
pub fn cpus() -> usize {
    thread::available_parallelism().map_or(1, |cpus| cpus.get())
}

/// Identifies one read in this process. The read gets its id when it expands, and every copy
/// of its reader shares it. Each read has its own consumers and parked assignments, so two
/// reads of the same topic never assign a partition to the same consumer.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ReadId(u64);

impl ReadId {
    /// An id that no other read of this process has.
    pub fn unique() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// A read and the number of one of its consumers.
type ConsumerKey = (ReadId, usize);

/// The consumers of this process: a pool for each read, held as the producers are.
///
/// A consumer has one fetch request at a time on each broker connection, and the response
/// holds at most `max.partition.fetch.bytes` for each of its partitions. So the partitions of
/// one consumer share its fetch rate. A pool gives each consumer fewer partitions. Each
/// consumer has its own librdkafka threads (one main thread and one for each broker) and its
/// own connections to the brokers.
static CONSUMERS: LazyLock<Mutex<HashMap<ConsumerKey, Weak<KafkaConsumer>>>> =
    LazyLock::new(Mutex::default);

/// Whether the background thread that serves the events of [`CONSUMERS`] runs.
static POLLING_EVENTS: AtomicBool = AtomicBool::new(false);

/// Whether the background thread that serves the events of shared consumers runs.
#[doc(hidden)]
pub fn is_polling_events() -> bool {
    POLLING_EVENTS.load(Ordering::Acquire)
}

/// The most consumers in the pool of one read. Each consumer has its own librdkafka threads,
/// broker connections and prefetch queues.
pub const MAX_CONSUMERS: usize = 16;

/// The consumer of each partition that this process read, by read and partition.
static CONSUMER_SLOTS: LazyLock<ConsumerSlots> = LazyLock::new(ConsumerSlots::default);

/// The slot of the consumer of `partition` in a pool of `slots` consumers of `read`. See
/// [`ConsumerSlots`].
#[doc(hidden)]
pub fn consumer_slot(read: ReadId, partition: i32, slots: usize) -> usize {
    CONSUMER_SLOTS.slot(read, partition, slots)
}

/// Gives each partition of a read one consumer of its pool for the life of the process.
///
/// The commits of a partition go through the commit tracker of its consumer, so they reach
/// the broker in order only if the partition keeps its consumer. A partition that is new to
/// the process gets the consumer with the fewest partitions of the read. So the partitions
/// that the runner gives a worker spread over its pool.
#[doc(hidden)]
#[derive(Debug, Default)]
pub struct ConsumerSlots(Mutex<HashMap<ReadId, HashMap<i32, usize>>>);

impl ConsumerSlots {
    /// The slot of `partition` in a pool of `slots` consumers of `read`. A pool of 0 counts
    /// as 1. A partition keeps its first slot.
    pub fn slot(&self, read: ReadId, partition: i32, slots: usize) -> usize {
        // Each change is one insert, so the map stays consistent when a holder panics.
        let mut assigned = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let partitions = assigned.entry(read).or_default();
        if let Some(&slot) = partitions.get(&partition) {
            return slot;
        }
        let load = partitions
            .values()
            .fold(vec![0_usize; slots.max(1)], |mut load, &slot| {
                if let Some(count) = load.get_mut(slot) {
                    *count += 1;
                }
                load
            });
        let slot = (0..load.len()).min_by_key(|&slot| load[slot]).unwrap_or(0);
        partitions.insert(partition, slot);
        slot
    }
}

/// A producer of `connection`. The DoFn copies get the producers of the connection in turn.
/// Creates the producer if no DoFn holds it.
#[doc(hidden)]
pub fn shared_producer(connection: &Connection) -> beam::Result<Arc<Producer>> {
    let slot = NEXT_PRODUCER.fetch_add(1, Ordering::Relaxed) % producer_slots();
    shared(&PRODUCERS, &(connection.clone(), slot), || {
        producer(connection)
    })
}

/// The consumer in `slot` of the pool of `read`, which reads from `connection`. Creates it if
/// no DoFn holds it, and starts the thread that serves consumer events while a consumer is
/// held. With Google authentication, the creation waits for the first token.
#[doc(hidden)]
pub fn shared_consumer(
    connection: &Connection,
    read: ReadId,
    slot: usize,
) -> beam::Result<Arc<KafkaConsumer>> {
    let consumer = shared(&CONSUMERS, &(read, slot), || consumer(connection))?;
    poll_events_in_background();
    Ok(consumer)
}

/// The shared consumers that still have a holder.
fn live_consumers() -> Vec<Arc<KafkaConsumer>> {
    let Ok(mut consumers) = CONSUMERS.lock() else {
        return Vec::new();
    };
    let live: Vec<Arc<KafkaConsumer>> = consumers.values().filter_map(Weak::upgrade).collect();
    consumers.retain(|_, weak| weak.strong_count() > 0);
    live
}

/// Whether any shared consumer still has a holder.
fn has_live_consumers() -> bool {
    CONSUMERS
        .lock()
        .is_ok_and(|consumers| consumers.values().any(|weak| weak.strong_count() > 0))
}

/// Starts a thread that serves the events of the shared consumers every
/// [`EVENT_POLL_INTERVAL`], unless one runs. The thread stops when no shared consumer has a
/// holder.
fn poll_events_in_background() {
    if POLLING_EVENTS.swap(true, Ordering::AcqRel) {
        return;
    }
    let spawned = thread::Builder::new()
        .name("kafka-consumer-events".to_string())
        .spawn(run_event_polls);
    if let Err(e) = spawned {
        POLLING_EVENTS.store(false, Ordering::Release);
        warn!("Cannot start the thread that serves Kafka consumer events: {e}");
    }
}

fn run_event_polls() {
    loop {
        thread::sleep(EVENT_POLL_INTERVAL);
        let live = live_consumers();
        if !live.is_empty() {
            live.iter().for_each(|consumer| {
                let _served = serve_events(consumer);
            });
            release(live);
            continue;
        }
        POLLING_EVENTS.store(false, Ordering::Release);
        let restart = has_live_consumers() && !POLLING_EVENTS.swap(true, Ordering::AcqRel);
        if !restart {
            return;
        }
    }
}

/// Drops `clients`. A client whose last holder is in `clients` drops on a new thread, because
/// a consumer leaves its group on drop and waits for the broker. If the thread cannot start,
/// the client drops on the calling thread.
#[doc(hidden)]
pub fn release<C: Send + 'static>(clients: impl IntoIterator<Item = Arc<C>>) {
    clients
        .into_iter()
        .filter_map(Arc::into_inner)
        .for_each(|client| {
            let spawned = thread::Builder::new()
                .name("kafka-client-close".to_string())
                .spawn(move || drop(client));
            if let Err(e) = spawned {
                warn!("Cannot start the thread that closes a Kafka client: {e}");
            }
        });
}

/// The client of `key` in `clients`, or a new one from `create`.
///
/// `create` runs without the lock, because a client can wait for a token. Two callers can
/// then create a client of the same key: the first to insert wins, and the other drops its
/// client.
fn shared<K: Clone + Eq + Hash, C>(
    clients: &Mutex<HashMap<K, Weak<C>>>,
    key: &K,
    create: impl FnOnce() -> beam::Result<C>,
) -> beam::Result<Arc<C>> {
    let lock = || {
        clients
            .lock()
            .map_err(|_| Error::msg("The Kafka client cache is poisoned"))
    };
    if let Some(client) = lock()?.get(key).and_then(Weak::upgrade) {
        return Ok(client);
    }
    let created = create().map(Arc::new)?;
    let mut clients = lock()?;
    match clients.get(key).and_then(Weak::upgrade) {
        Some(client) => Ok(client),
        None => {
            clients.retain(|_, weak| weak.strong_count() > 0);
            clients.insert(key.clone(), Arc::downgrade(&created));
            Ok(created)
        }
    }
}
