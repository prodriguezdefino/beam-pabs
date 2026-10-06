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

//! The `ctx.timer(t)` builder: firing times and default watermark holds per time domain.

use std::sync::Arc;
use std::time::Duration;

use beam::coders::{DefaultCoder, PaneInfo, TimerAction, TimerRecord, WindowedHeader};
use beam::internals::{HandlerContext, IntoParDoFn, TimerCollector};
use beam::prelude::*;

const ELEMENT_MILLIS: i64 = 10_000;
const OFFSET: Duration = Duration::from_secs(5);

#[derive(Timers)]
enum Delayed {
    #[timer(event_time, id = "event")]
    Event,
    #[timer(processing_time, id = "processing")]
    Processing,
}

/// Sets one timer `OFFSET` after "now" in the domain chosen by the element value.
#[derive(Clone)]
struct SetsDelayedTimer;

impl StatefulDoFn for SetsDelayedTimer {
    type Key = String;
    type Value = bool;
    type Out = String;
    type State = ();
    type Timer = Delayed;

    fn process_element(
        &mut self,
        _key: &String,
        event_time: bool,
        _state: &mut (),
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let timer = if event_time {
            Delayed::Event
        } else {
            Delayed::Processing
        };
        ctx.timer(timer).after(OFFSET)
    }
}

/// Processes one `(key, event_time)` element at `ELEMENT_MILLIS` and returns its timers.
fn timers_after_processing(event_time: bool) -> Vec<(String, TimerRecord)> {
    let collector = Arc::new(TimerCollector::new());
    let header = WindowedHeader::global(ELEMENT_MILLIS, PaneInfo::NO_FIRING);
    let element = ("k".to_string(), event_time).encode().expect("encode");
    let mut handler = SetsDelayedTimer.into_handler().instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    {
        let mut ctx = HandlerContext::new(&mut sink)
            .with_header(&header)
            .with_timer_collector(Some(&collector));
        handler.process(&element, &mut ctx).expect("process");
    }
    collector
        .drain_family_records()
        .into_iter()
        .flat_map(|(family, records)| records.into_iter().map(move |r| (family.clone(), r)))
        .collect()
}

#[test]
fn an_event_time_timer_after_fires_after_the_element_timestamp_and_holds_at_the_fire_time() {
    let timers = timers_after_processing(true);
    let [(family, record)] = timers.as_slice() else {
        panic!("expected one timer, got {}", timers.len());
    };
    let fire = Instant::from_millis(ELEMENT_MILLIS) + OFFSET;
    assert_eq!(family, "event");
    let TimerAction::Set {
        fire_timestamp,
        hold_timestamp,
        ..
    } = record.action
    else {
        panic!("expected a set timer, got {:?}", record.action);
    };
    assert_eq!(fire_timestamp, fire);
    assert_eq!(hold_timestamp, fire);
    assert_eq!(record.user_key, "k".to_string().encode().unwrap());
}

#[test]
fn a_processing_time_timer_after_fires_after_the_clock_and_holds_at_the_element_timestamp() {
    let before = Instant::now();
    let timers = timers_after_processing(false);
    let after = Instant::now();
    let [(family, record)] = timers.as_slice() else {
        panic!("expected one timer, got {}", timers.len());
    };
    assert_eq!(family, "processing");
    let TimerAction::Set {
        fire_timestamp,
        hold_timestamp,
        ..
    } = record.action
    else {
        panic!("expected a set timer, got {:?}", record.action);
    };
    let fire = fire_timestamp;
    assert!(
        (before + OFFSET) <= fire && fire <= (after + OFFSET),
        "fire {fire:?} must be the wall clock plus the offset"
    );
    assert_eq!(hold_timestamp, Instant::from_millis(ELEMENT_MILLIS));
}
