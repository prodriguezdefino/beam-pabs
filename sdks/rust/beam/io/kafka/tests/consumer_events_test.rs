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

//! The thread that serves the events of the shared consumers. The thread and the client cache
//! are global to the process, so this binary has only this test.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use kafka_io::native::client::{Connection, ReadId, is_polling_events, shared_consumer};

/// Waits up to 5 s for `done`.
fn eventually(done: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    done()
}

#[test]
fn one_consumer_serves_every_holder_of_a_slot_and_the_event_thread_runs_while_one_is_held() {
    let connection = Connection {
        bootstrap_servers: "localhost:9".to_string(),
        properties: [("client.id", "consumer-test"), ("group.id", "pool-group")]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<BTreeMap<_, _>>(),
        ..Connection::default()
    };
    let read = ReadId::unique();
    let get = |slot| shared_consumer(&connection, read, slot);
    let first = get(0).unwrap();
    assert!(Arc::ptr_eq(&first, &get(0).unwrap()));
    let second = get(1).unwrap();
    assert!(!Arc::ptr_eq(&first, &second));
    let other_read = shared_consumer(&connection, ReadId::unique(), 0).unwrap();
    assert!(
        !Arc::ptr_eq(&first, &other_read),
        "two reads never share a consumer"
    );
    drop(other_read);
    assert!(is_polling_events());

    drop(first);
    drop(second);
    assert!(eventually(|| !is_polling_events()));

    let again = get(0).unwrap();
    assert!(is_polling_events());
    drop(again);
    assert!(eventually(|| !is_polling_events()));
}
