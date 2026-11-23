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

use beam_io_kafka::client::{
    Connection, ConsumerSlots, ReadId, UNCOMMITTED_GROUP, consumer, producer_slots, release,
    serve_events, shared_producer,
};
use rdkafka::consumer::Consumer;

fn connection(properties: &[(&str, &str)]) -> Connection {
    Connection::new("localhost:9").with_properties(
        properties
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<BTreeMap<_, _>>(),
    )
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
    assert_eq!(config.get("statistics.interval.ms"), Some("5000"));
    assert_eq!(config.get("value.serializer"), None);
    assert_eq!(config.get("bootstrap.servers"), Some("localhost:9"));

    let custom_stats = connection(&[("statistics.interval.ms", "2000")]).producer_config();
    assert_eq!(custom_stats.get("statistics.interval.ms"), Some("2000"));
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
    use beam_io_kafka::client::is_partition_eof;
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

    use beam_io_kafka::context::RepeatLimiter;

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

/// A librdkafka statistics report at `ts_us` for a client of age `age_us` (both in
/// microseconds). Each broker has a throttle window with `cnt` responses and the given sum.
fn stats_json(ts_us: i64, age_us: i64, cnt: i64, brokers: &[(&str, Option<i64>)]) -> String {
    let broker_entries: Vec<String> = brokers
        .iter()
        .enumerate()
        .map(|(i, (name, throttle_sum))| {
            let throttle = match throttle_sum {
                Some(sum) => format!(
                    r#"{{"min":0,"max":{sum},"avg":{sum},"sum":{sum},"cnt":{cnt},"stddev":0,"hdrsize":0,"p50":0,"p75":0,"p90":0,"p95":0,"p99":0,"p99_99":0,"outofrange":0}}"#
                ),
                None => "null".to_string(),
            };
            format!(
                r#""{name}":{{"name":"{name}","nodeid":{i},"nodename":"{name}","source":"learned","state":"UP","stateage":0,"outbuf_cnt":0,"outbuf_msg_cnt":0,"waitresp_cnt":0,"waitresp_msg_cnt":0,"tx":0,"txbytes":0,"txerrs":0,"txretries":0,"txidle":0,"req_timeouts":0,"rx":0,"rxbytes":0,"rxerrs":0,"rxcorriderrs":0,"rxpartial":0,"rxidle":0,"req":{{}},"zbuf_grow":0,"buf_grow":0,"wakeups":0,"connects":0,"disconnects":0,"int_latency":null,"outbuf_latency":null,"rtt":null,"throttle":{throttle},"toppars":{{}}}}"#
            )
        })
        .collect();
    format!(
        r#"{{"name":"rdkafka#producer-1","client_id":"rdkafka","type":"producer","ts":{ts_us},"time":0,"age":{age_us},"replyq":0,"msg_cnt":0,"msg_size":0,"msg_max":0,"msg_size_max":0,"tx":0,"tx_bytes":0,"rx":0,"rx_bytes":0,"txmsgs":0,"txmsg_bytes":0,"rxmsgs":0,"rxmsg_bytes":0,"simple_cnt":0,"metadata_cache_cnt":0,"brokers":{{{}}},"topics":{{}},"cgrp":null,"eos":null}}"#,
        broker_entries.join(",")
    )
}

#[test]
fn context_observes_broker_throttle_statistics_and_drains_into_throttling_msecs() {
    use beam::internals::{MetricsContainer, MetricsScope};
    use beam::metrics::{MetricResults, THROTTLE_TIME_COUNTER_NAME, THROTTLE_TIME_NAMESPACE};
    use beam_io_kafka::context::KafkaContext;
    use rdkafka::ClientContext as _;
    use rdkafka::statistics::Statistics;

    let context = KafkaContext::default();
    let first: Statistics = serde_json::from_str(&stats_json(
        5_000_000,
        5_000_000,
        1,
        &[
            ("b1:9092/1", Some(120)),
            ("b2:9092/2", Some(30)),
            ("b3:9092/3", None),
            ("b4:9092/4", Some(-5)),
        ],
    ))
    .expect("valid statistics JSON");
    context.stats(first);

    // A second stats interval adds to the pending total before drain.
    let second: Statistics = serde_json::from_str(&stats_json(
        10_000_000,
        10_000_000,
        1,
        &[("b1:9092/1", Some(50))],
    ))
    .expect("valid JSON");
    context.stats(second);

    let container = Arc::new(MetricsContainer::new());
    {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("WriteKafka"));
        context.record_throttling();
        // A second drain with no new throttling leaves the counter unchanged.
        context.record_throttling();
    }

    let metrics = MetricResults::from_container(&container);
    assert_eq!(
        metrics.counter_for_transform(
            "WriteKafka",
            THROTTLE_TIME_NAMESPACE,
            THROTTLE_TIME_COUNTER_NAME,
        ),
        Some(200)
    );
}

/// Two brokers report throttle sums over many responses that pass the 1 s interval between
/// reports. Each broker counts at most the interval, and so does their sum.
#[test]
fn broker_throttling_counts_at_most_the_interval_between_reports() {
    use beam::internals::{MetricsContainer, MetricsScope};
    use beam::metrics::{MetricResults, THROTTLE_TIME_COUNTER_NAME, THROTTLE_TIME_NAMESPACE};
    use beam_io_kafka::context::KafkaContext;
    use rdkafka::ClientContext as _;
    use rdkafka::statistics::Statistics;

    let context = KafkaContext::default();
    let report = |ts_us, brokers: &[(&str, Option<i64>)]| -> Statistics {
        serde_json::from_str(&stats_json(ts_us, ts_us, 4, brokers)).expect("valid JSON")
    };
    // The first report covers the 1 s age of the client: 3 s and 0.4 s give 1 s in total.
    context.stats(report(
        1_000_000,
        &[("b1:9092/1", Some(3_000)), ("b2:9092/2", Some(400))],
    ));
    // The next report comes 1 s later: 0.3 s and 0.2 s give 0.5 s.
    context.stats(report(
        2_000_000,
        &[("b1:9092/1", Some(300)), ("b2:9092/2", Some(200))],
    ));

    let container = Arc::new(MetricsContainer::new());
    {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("WriteKafka"));
        context.record_throttling();
    }
    assert_eq!(
        MetricResults::from_container(&container).counter_for_transform(
            "WriteKafka",
            THROTTLE_TIME_NAMESPACE,
            THROTTLE_TIME_COUNTER_NAME,
        ),
        Some(1_500)
    );
}

#[test]
fn the_debug_form_of_a_connection_hides_property_values() {
    let conn = connection(&[("sasl.password", "super-secret"), ("group.id", "readers")]);
    let debug = format!("{conn:?}");
    assert!(!debug.contains("super-secret"), "{debug}");
    assert!(!debug.contains("readers"), "{debug}");
    assert!(debug.contains("sasl.password"), "{debug}");
    assert!(debug.contains("group.id"), "{debug}");
    assert!(debug.contains("localhost:9"), "{debug}");
    assert!(debug.contains(".."), "{debug}");
}
