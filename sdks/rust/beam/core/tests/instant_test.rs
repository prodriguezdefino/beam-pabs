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

//! Tests for [`beam::Instant`].

use std::time::Duration;

use beam::Instant;
use beam::coders::DefaultCoder;

#[test]
fn arithmetic_saturates_at_the_beam_range() {
    assert_eq!(Instant::MAX + Duration::from_secs(1), Instant::MAX);
    assert_eq!(Instant::MIN - Duration::from_secs(1), Instant::MIN);
}

#[test]
fn duration_since_is_none_for_a_later_instant() {
    let t = Instant::from_millis(5_000);
    assert_eq!(
        t.duration_since(Instant::EPOCH),
        Some(Duration::from_secs(5))
    );
    assert_eq!(Instant::EPOCH.duration_since(t), None);
}

#[test]
fn displays_as_rfc3339() {
    assert_eq!(Instant::MAX.to_string(), "9999-12-31T23:59:59.999Z");
    assert_eq!(
        Instant::from_millis(-1).to_string(),
        "1969-12-31T23:59:59.999Z"
    );
}

#[test]
fn round_trips_through_its_coder() {
    let t = Instant::from_millis(-42);
    assert_eq!(Instant::decode(&t.encode().unwrap()).unwrap(), t);
}
