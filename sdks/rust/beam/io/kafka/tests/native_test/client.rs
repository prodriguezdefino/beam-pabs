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

//! The client configuration of a connection, and the producers and consumers that a worker
//! shares.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use kafka_io::native::client::{
    Connection, ConsumerSlots, ReadId, UNCOMMITTED_GROUP, consumer, producer_slots, release,
    serve_events, shared_producer,
};
use rdkafka::consumer::Consumer;

fn connection(properties: &[(&str, &str)]) -> Connection {
    Connection {
        bootstrap_servers: "localhost:9".to_string(),
        properties: properties
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<BTreeMap<_, _>>(),
        ..Connection::default()
    }
}

/// Waits up to 5 s for `done`.
fn eventually(done: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    done()
}

#[test]
fn properties_replace_the_producer_defaults_and_serializer_properties_are_dropped() {
    let config = connection(&[
        ("acks", "1"),
        (
            "value.serializer",
            "org.apache.kafka.common.serialization.ByteArraySerializer",
        ),
    ])
    .producer_config();
    assert_eq!(config.get("acks"), Some("1"));
    assert_eq!(config.get("enable.idempotence"), Some("true"));
    assert_eq!(config.get("value.serializer"), None);
    assert_eq!(config.get("bootstrap.servers"), Some("localhost:9"));
}

#[test]
fn a_connection_needs_servers_once() {
    assert!(Connection::new(" ").validate().is_err());
    assert!(connection(&[]).validate().is_ok());
    assert!(
        connection(&[("bootstrap.servers", "other:9092")])
            .validate()
            .is_err()
    );
}

#[test]
fn the_holders_of_a_connection_share_a_few_producers() {
    let holders: Vec<_> = (0..2 * producer_slots())
        .map(|_| shared_producer(&connection(&[("client.id", "shared-test")])).unwrap())
        .collect();
    let distinct = holders.iter().fold(Vec::new(), |mut distinct, producer| {
        if !distinct.iter().any(|known| Arc::ptr_eq(known, producer)) {
            distinct.push(Arc::clone(producer));
        }
        distinct
    });
    assert!(
        distinct.len() <= producer_slots(),
        "{} producers",
        distinct.len()
    );
    assert!(distinct.len() < holders.len());
    let other = shared_producer(&connection(&[("client.id", "other-test")])).unwrap();
    assert!(
        holders
            .iter()
            .all(|producer| !Arc::ptr_eq(producer, &other))
    );
}

#[test]
fn a_property_that_librdkafka_does_not_know_fails_the_producer() {
    let Err(error) = shared_producer(&connection(&[("no.such.property", "1")])) else {
        panic!("a producer with an unknown property");
    };
    let error = error.to_string();
    assert!(error.contains("no.such.property"), "{error}");
}

#[test]
fn a_consumer_never_commits_by_itself_and_always_has_a_group() {
    let config = connection(&[]).consumer_config();
    assert_eq!(config.get("enable.auto.commit"), Some("false"));
    assert_eq!(config.get("enable.auto.offset.store"), Some("false"));
    assert_eq!(config.get("enable.partition.eof"), Some("true"));
    assert_eq!(config.get("group.id"), Some(UNCOMMITTED_GROUP));
    let no_eof = connection(&[("enable.partition.eof", "false")]).consumer_config();
    assert_eq!(no_eof.get("enable.partition.eof"), Some("false"));
    assert_eq!(connection(&[]).group_id(), None);

    let empty = connection(&[("group.id", "")]);
    assert_eq!(
        empty.consumer_config().get("group.id"),
        Some(UNCOMMITTED_GROUP)
    );
    assert_eq!(empty.group_id(), None);

    let grouped = connection(&[("group.id", "readers"), ("auto.offset.reset", "latest")]);
    let config = grouped.consumer_config();
    assert_eq!(config.get("group.id"), Some("readers"));
    assert_eq!(config.get("auto.offset.reset"), Some("latest"));
    assert_eq!(grouped.group_id(), Some("readers"));
}

#[test]
fn serve_events_drains_the_event_queue_of_a_consumer() {
    let consumer = consumer(&connection(&[
        ("security.protocol", "SASL_PLAINTEXT"),
        ("sasl.mechanism", "OAUTHBEARER"),
    ]))
    .unwrap();
    // librdkafka queues a token request. The poll that serves it returns no event. The
    // context fails the request with no Google authentication, so an error event follows.
    let served = Cell::new(0);
    assert!(eventually(|| {
        served.set(served.get() + serve_events(&consumer));
        served.get() >= 2
    }));
    assert_eq!(consumer.context().callbacks(), 1);
    assert!(consumer.poll(Duration::ZERO).is_none());
}

#[test]
fn a_new_partition_gets_the_consumer_with_the_fewest_partitions() {
    let slots = ConsumerSlots::default();
    let read = ReadId::unique();
    // The runner gives this worker partitions 0, 4, 8 and 12 of 16: each gets its own slot.
    let assigned: Vec<usize> = [0, 4, 8, 12]
        .iter()
        .map(|&partition| slots.slot(read, partition, 4))
        .collect();
    assert_eq!(assigned, [0, 1, 2, 3]);
    assert_eq!(slots.slot(read, 1, 4), 0);
}

#[test]
fn a_partition_keeps_its_consumer() {
    let slots = ConsumerSlots::default();
    let read = ReadId::unique();
    let first = slots.slot(read, 7, 3);
    assert_eq!(slots.slot(read, 7, 3), first);
    assert_eq!(slots.slot(read, 7, 1), first);
}

#[test]
fn two_reads_of_one_topic_have_their_own_pools() {
    let slots = ConsumerSlots::default();
    let (first, second) = (ReadId::unique(), ReadId::unique());
    assert_ne!(first, second);
    assert_eq!(slots.slot(first, 0, 4), 0);
    assert_eq!(slots.slot(first, 1, 4), 1);
    // The partitions of the first read do not count in the load of the second.
    assert_eq!(slots.slot(second, 1, 4), 0);
    assert_eq!(slots.slot(second, 0, 4), 1);
}

#[test]
fn a_pool_of_one_gives_every_partition_the_same_consumer() {
    let slots = ConsumerSlots::default();
    let read = ReadId::unique();
    assert!((0..64).all(|partition| slots.slot(read, partition, 1) == 0));
    assert_eq!(slots.slot(read, 99, 0), 0);
}

/// A client whose drop waits for the test, as a consumer waits for the broker when it closes.
/// It sends the name of the thread that dropped it, or `None` if the test did not answer.
struct SlowClose {
    // A receiver is not `Sync`, and an `Arc` needs it.
    finish: Mutex<Receiver<()>>,
    closed: Sender<Option<String>>,
}

impl Drop for SlowClose {
    fn drop(&mut self) {
        let answered = self
            .finish
            .lock()
            .is_ok_and(|finish| finish.recv_timeout(Duration::from_secs(10)).is_ok());
        let thread = answered.then(|| thread::current().name().unwrap_or_default().to_string());
        let _sent = self.closed.send(thread);
    }
}

#[test]
fn the_last_holder_of_a_client_does_not_wait_for_its_close() {
    let (finish, wait) = mpsc::channel();
    let (closed, close) = mpsc::channel();
    let client = Arc::new(SlowClose {
        finish: Mutex::new(wait),
        closed,
    });
    release([Arc::clone(&client)]);
    assert!(close.try_recv().is_err(), "another holder keeps the client");
    // The call returns while the drop waits, so the drop runs on another thread.
    release([client]);
    finish.send(()).unwrap();
    assert_eq!(
        close
            .recv_timeout(Duration::from_secs(20))
            .unwrap()
            .as_deref(),
        Some("kafka-client-close")
    );
}

#[test]
fn only_the_end_of_a_partition_is_not_an_error() {
    use kafka_io::native::client::is_partition_eof;
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};

    // The copy on the main queue (a global error) and the event of a partition queue.
    assert!(is_partition_eof(&KafkaError::Global(
        RDKafkaErrorCode::PartitionEOF
    )));
    assert!(is_partition_eof(&KafkaError::PartitionEOF(3)));
    for error in [
        KafkaError::Global(RDKafkaErrorCode::AllBrokersDown),
        KafkaError::Global(RDKafkaErrorCode::BrokerTransportFailure),
        KafkaError::MessageConsumption(RDKafkaErrorCode::UnknownTopicOrPartition),
    ] {
        assert!(!is_partition_eof(&error), "{error}");
    }
}

#[test]
fn a_repeated_line_logs_once_per_window_with_the_count_of_the_others() {
    use std::time::{Duration, Instant};

    use kafka_io::native::context::RepeatLimiter;

    let limiter = RepeatLimiter::new(Duration::from_secs(60));
    let start = Instant::now();
    let line = |port: u16| format!("librdkafka: FAIL Connect to ipv6#[::1]:{port} failed");
    assert_eq!(limiter.admit(&line(9092), start), Some(line(9092)));
    // Lines that differ only in their digits are the same line.
    assert_eq!(
        limiter.admit(&line(9093), start + Duration::from_secs(1)),
        None
    );
    assert_eq!(
        limiter.admit(&line(9092), start + Duration::from_secs(59)),
        None
    );
    // Another line logs at once.
    assert_eq!(
        limiter.admit("librdkafka: other", start + Duration::from_secs(2)),
        Some("librdkafka: other".to_string())
    );
    let after = limiter
        .admit(&line(9092), start + Duration::from_secs(61))
        .expect("the first line after the window logs");
    assert!(after.starts_with(&line(9092)), "{after}");
    assert!(after.contains("2 more like it"), "{after}");
    // The count starts again.
    assert_eq!(
        limiter.admit(&line(9092), start + Duration::from_secs(122)),
        Some(line(9092))
    );
}
