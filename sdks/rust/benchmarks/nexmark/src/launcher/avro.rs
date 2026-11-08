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

//! Events as schema rows: Avro event files and the `ROW` coder strategy.

use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::io::avro::avroio;
use beam::prelude::*;
use beam::schema::BeamRow;
use beam::windowing::FixedWindows;

use super::NexmarkArgs;
use super::error::LauncherError;
use super::transport::{decode_each, require, with_event_time_from_data};
use crate::config::{CoderStrategy, NexmarkConfig};
use crate::model::{AuctionRow, BidRow, Event, EventRow, PersonRow};

/// File name, under `--output_path`, of the Avro files of all events.
pub const AVRO_EVENT_FILES: &str = "event";

/// File name, under `--output_path`, of the Avro files of the bids.
pub const AVRO_BID_FILES: &str = "bid";

/// File name, under `--output_path`, of the Avro files of the new auctions.
pub const AVRO_AUCTION_FILES: &str = "auction";

/// File name, under `--output_path`, of the Avro files of the new persons.
pub const AVRO_PERSON_FILES: &str = "person";

/// Reads the events of the Avro files `<--input_path>*.avro`.
///
/// Files have no event time, so each event gets its own `date_time` as timestamp.
pub(super) fn read_avro_events(
    pipeline: &Pipeline,
    args: &NexmarkArgs,
) -> Result<PCollection<Event>, LauncherError> {
    let prefix = require(
        args.input_path.as_deref(),
        "--input_path",
        "--source_type=AVRO",
    )?;
    let rows = pipeline.apply(
        "ReadAvroEvents",
        avroio::Read::<EventRow>::new(format!("{prefix}*.avro")),
    );
    let events = decode_each(&rows, "AvroRowsToEvents", |row: EventRow| {
        Event::try_from(row).map_err(|err| err.to_string())
    });
    Ok(with_event_time_from_data(&events))
}

/// Writes `events` to Avro files under `output_path`: all events to `event*`, and the bids,
/// new auctions and new persons to `bid*`, `auction*` and `person*`.
///
/// In streaming mode each file holds one fixed window of `window_size_sec`.
pub(super) fn write_avro_events(
    events: &PCollection<Event>,
    output_path: &str,
    config: &NexmarkConfig,
    streaming: bool,
) {
    let base = output_path.trim_end_matches('/');
    let windowed;
    let events = if streaming {
        windowed = events.window_into(
            "AvroFixedWindows",
            FixedWindows::of(Duration::from_secs(
                u64::try_from(config.window_size_sec.max(1)).unwrap_or(1),
            )),
        );
        &windowed
    } else {
        events
    };
    write_rows(
        &events.map("EventRows", EventRow::from),
        base,
        AVRO_EVENT_FILES,
        streaming,
    );
    write_rows(
        &events.flat_map("BidRows", |event: Event| match event {
            Event::Bid(bid) => Some(BidRow::from(bid)),
            Event::Person(_) | Event::Auction(_) => None,
        }),
        base,
        AVRO_BID_FILES,
        streaming,
    );
    write_rows(
        &events.flat_map("AuctionRows", |event: Event| match event {
            Event::Auction(auction) => Some(AuctionRow::from(auction)),
            Event::Person(_) | Event::Bid(_) => None,
        }),
        base,
        AVRO_AUCTION_FILES,
        streaming,
    );
    write_rows(
        &events.flat_map("PersonRows", |event: Event| match event {
            Event::Person(person) => Some(PersonRow::from(person)),
            Event::Auction(_) | Event::Bid(_) => None,
        }),
        base,
        AVRO_PERSON_FILES,
        streaming,
    );
}

fn write_rows<T>(rows: &PCollection<T>, base: &str, name: &str, streaming: bool)
where
    T: BeamRow + DefaultCoder + Send + Sync + 'static,
{
    let write = avroio::Write::<T>::new(format!("{base}/{name}")).with_suffix(".avro");
    let write = if streaming {
        write.with_windowed_writes()
    } else {
        write
    };
    let _ = rows.apply(format!("WriteAvro_{name}"), write);
}

/// Applies the coder strategy to the event stream.
///
/// `ROW` sends the events through a reshuffle as [`EventRow`]s, so the runner moves them
/// with the schema row coder. The queries keep the hand coders of the model types.
pub(super) fn with_coder_strategy(
    events: PCollection<Event>,
    strategy: CoderStrategy,
) -> PCollection<Event> {
    match strategy {
        CoderStrategy::Hand => events,
        CoderStrategy::Row => {
            let rows = events
                .map("EventsToRows", EventRow::from)
                .reshuffle("RowCodedEvents");
            decode_each(&rows, "RowsToEvents", |row: EventRow| {
                Event::try_from(row).map_err(|err| err.to_string())
            })
        }
    }
}
