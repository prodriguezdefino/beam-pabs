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

//! The delivery results of the write: one entry for each record in flight, set by the
//! delivery callback and read by the writer in send order.

use std::thread;
use std::time::{Duration, Instant};

use kafka_io::native::write::delivery::{Deliveries, Delivery};
use rdkafka::error::RDKafkaErrorCode;

#[test]
fn an_entry_is_pending_until_the_callback_sets_its_result() {
    let deliveries = Deliveries::default();
    let delivered = deliveries.send_entry(0);
    let failed = deliveries.send_entry(1);
    assert_eq!(deliveries.result(0), Delivery::Pending);
    assert_eq!(deliveries.result(1), Delivery::Pending);

    failed.set_result(Err(RDKafkaErrorCode::MessageSizeTooLarge));
    delivered.set_result(Ok(()));

    assert_eq!(deliveries.result(0), Delivery::Delivered);
    assert_eq!(
        deliveries.result(1),
        Delivery::Failed(RDKafkaErrorCode::MessageSizeTooLarge)
    );
}

#[test]
fn an_entry_gets_a_new_record_after_capacity_records() {
    let deliveries = Deliveries::default();
    deliveries
        .send_entry(3)
        .set_result(Err(RDKafkaErrorCode::InvalidRecord));
    assert_eq!(
        deliveries.result(3),
        Delivery::Failed(RDKafkaErrorCode::InvalidRecord)
    );
    let next = deliveries.send_entry(3 + Deliveries::CAPACITY);
    assert_eq!(deliveries.result(3), Delivery::Pending);
    next.set_result(Ok(()));
    assert_eq!(
        deliveries.result(3 + Deliveries::CAPACITY),
        Delivery::Delivered
    );
}

#[test]
fn a_waiting_writer_wakes_when_the_callback_sets_the_result() {
    let deliveries = Deliveries::default();
    let entry = deliveries.send_entry(7);
    let callback = thread::spawn(move || {
        thread::sleep(Duration::from_millis(30));
        entry.set_result(Ok(()));
    });
    let started = Instant::now();
    assert_eq!(deliveries.wait_for_result(7), Delivery::Delivered);
    // The callback wakes the writer before the check interval of 100 ms ends.
    assert!(
        started.elapsed() < Duration::from_millis(90),
        "{:?}",
        started.elapsed()
    );
    callback.join().expect("the callback does not panic");
}

#[test]
fn a_record_in_flight_keeps_its_entry_after_the_writer_drops_its_deliveries() {
    let deliveries = Deliveries::default();
    let late = deliveries.send_entry(0);
    drop(deliveries);
    late.set_result(Err(RDKafkaErrorCode::RequestTimedOut));
}
