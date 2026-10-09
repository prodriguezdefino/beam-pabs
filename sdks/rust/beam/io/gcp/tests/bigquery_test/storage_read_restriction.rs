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

//! Stream restrictions, their coder, and the tracker of the native Storage Read connector:
//! claims, checkpoints, server splits, sizes and progress. A fake service stands in for
//! BigQuery.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use std::sync::{Arc, Mutex};

use beam::coders::DefaultCoder;
use beam::transforms::sdf::RestrictionTracker;
use gcp::bigquery::storage::read::{StreamRestriction, StreamTracker};
use google_cloud_bigquery::client::Read;
use google_cloud_bigquery::model::{
    ReadRowsRequest, ReadRowsResponse, ReadStream, SplitReadStreamRequest, SplitReadStreamResponse,
};
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::{Code, Status};
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::response::Response;
use google_cloud_gax::streaming::ResponseStream;
use tokio::sync::mpsc;

fn stream(start: i64) -> StreamRestriction {
    StreamRestriction {
        start,
        ..StreamRestriction::new("streams/0", 1_000, 1)
    }
}

#[test]
fn a_claim_covers_the_rows_up_to_its_end_offset() {
    let tracker = StreamTracker::new(stream(10));
    assert!(tracker.try_claim(&claim(15)));
    assert!(tracker.try_claim(&claim(40)));
    assert!(!tracker.try_claim(&claim(40)), "claims must grow");
    tracker.finish(40);
    assert!(!tracker.try_claim(&claim(41)), "the stream ended");
    let restriction = tracker.current_restriction();
    assert_eq!((restriction.start, restriction.end), (10, 40));
    tracker.check_done().unwrap();
}

#[test]
fn a_checkpoint_resumes_after_the_claimed_rows() {
    let tracker = StreamTracker::new(stream(0));
    assert_eq!(tracker.try_split(0.0), None, "nothing is claimed yet");
    assert!(tracker.try_claim(&claim(100)));
    let (primary, residual) = tracker.try_split(0.0).unwrap();
    assert_eq!((primary.start, primary.end), (0, 100));
    assert_eq!(
        residual,
        stream(100),
        "the residual keeps the stream and its estimate"
    );
    assert!(
        !tracker.try_claim(&claim(200)),
        "the primary ends at the checkpoint"
    );
    tracker.check_done().unwrap();
}

fn claim(end: i64) -> (String, i64) {
    ("streams/0".to_string(), end)
}

/// A Storage Read service that splits `streams/0` into `streams/0/p` and `streams/0/r`.
#[derive(Debug, Default)]
struct FakeSplits {
    split: bool,
    primary_readable: bool,
    fractions: Mutex<Vec<f64>>,
}

impl google_cloud_bigquery::stub::Read for FakeSplits {
    async fn split_read_stream(
        &self,
        req: SplitReadStreamRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<Response<SplitReadStreamResponse>> {
        self.fractions.lock().unwrap().push(req.fraction);
        let mut response = SplitReadStreamResponse::new();
        if self.split {
            response = response
                .set_primary_stream(ReadStream::new().set_name(format!("{}/p", req.name)))
                .set_remainder_stream(ReadStream::new().set_name(format!("{}/r", req.name)));
        }
        Ok(Response::from(response))
    }

    async fn read_rows(
        &self,
        _req: ReadRowsRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<ResponseStream<ReadRowsResponse>> {
        let (sender, receiver) = mpsc::channel(1);
        if !self.primary_readable {
            let status = Status::default()
                .set_code(Code::OutOfRange)
                .set_message("past the end");
            sender.try_send(Err(Error::service(status))).unwrap();
        }
        Ok(ResponseStream::from(receiver))
    }
}

fn tracker_with(fake: FakeSplits) -> (StreamTracker, Arc<FakeSplits>) {
    let fake = Arc::new(fake);
    let client = Read::from_stub::<FakeSplits>(Arc::clone(&fake));
    (
        StreamTracker::new(stream(0)).with_client(Some(client)),
        fake,
    )
}

#[test]
fn a_split_continues_in_the_primary_stream() {
    let (tracker, fake) = tracker_with(FakeSplits {
        split: true,
        primary_readable: true,
        ..FakeSplits::default()
    });
    assert!(tracker.try_claim(&claim(100)));
    tracker.set_fraction(0.1);

    let (primary, residual) = tracker.try_split(0.5).unwrap();
    // Half of the 90% that is left is a split at 55% of the stream.
    assert_eq!(*fake.fractions.lock().unwrap(), [0.55]);
    assert_eq!(
        primary,
        StreamRestriction {
            stream: "streams/0/p".to_string(),
            estimated_rows: 550,
            ..stream(0)
        }
    );
    assert_eq!(residual, StreamRestriction::new("streams/0/r", 450, 1));
    assert!(
        !tracker.try_claim(&claim(150)),
        "rows of the split stream are not claimed"
    );
    assert!(tracker.try_claim(&("streams/0/p".to_string(), 150)));
}

#[test]
fn a_split_before_the_claimed_rows_is_refused() {
    let (tracker, _) = tracker_with(FakeSplits {
        split: true,
        primary_readable: false,
        ..FakeSplits::default()
    });
    assert!(tracker.try_claim(&claim(100)));
    assert_eq!(tracker.try_split(0.5), None);
    assert_eq!(tracker.current_restriction(), stream(0));
    assert!(
        tracker.try_claim(&claim(150)),
        "reading continues in the same stream"
    );
}

#[test]
fn a_split_that_the_server_declines_is_refused() {
    let (tracker, _) = tracker_with(FakeSplits {
        split: false,
        primary_readable: true,
        ..FakeSplits::default()
    });
    assert!(tracker.try_claim(&claim(100)));
    assert_eq!(tracker.try_split(0.5), None);
    assert!(tracker.check_done().is_err(), "the stream did not end");
}

#[test]
fn a_checkpointed_restriction_is_not_split_by_the_server() {
    let (tracker, fake) = tracker_with(FakeSplits {
        split: true,
        primary_readable: true,
        ..FakeSplits::default()
    });
    assert!(tracker.try_claim(&claim(100)));
    tracker.try_split(0.0).unwrap();
    assert_eq!(tracker.try_split(0.5), None);
    assert!(fake.fractions.lock().unwrap().is_empty());
}

#[test]
fn the_size_is_the_estimate_of_the_rows_left() {
    assert_eq!(stream(0).rows_after(0), 1_000);
    assert_eq!(stream(250).rows_after(250), 750);
    assert_eq!(
        stream(1_500).rows_after(1_500),
        0,
        "the estimate can be low"
    );
    let bounded = StreamRestriction {
        end: 400,
        ..stream(0)
    };
    assert_eq!(bounded.rows_after(100), 300);
}

#[test]
fn progress_is_in_rows() {
    let tracker = StreamTracker::new(stream(0));
    let before = tracker.current_progress();
    assert_eq!(
        (before.work_completed, before.work_remaining),
        (0.0, 1_000.0)
    );

    // The server reports the fraction of the stream; 300 rows are 25%, so 900 are left.
    assert!(tracker.try_claim(&claim(300)));
    tracker.set_fraction(0.25);
    let during = tracker.current_progress();
    assert_eq!(
        (during.work_completed, during.work_remaining),
        (300.0, 900.0)
    );

    tracker.finish(300);
    assert_eq!(tracker.current_progress().work_remaining, 0.0);
}

#[test]
fn sizes_and_progress_are_in_bytes() {
    let restriction = StreamRestriction::new("streams/0", 1_000, 120);
    assert_eq!(restriction.bytes_after(0), 120_000.0);
    assert_eq!(restriction.bytes_after(250), 90_000.0);

    let tracker = StreamTracker::new(restriction);
    assert!(tracker.try_claim(&claim(300)));
    tracker.set_fraction(0.25);
    let progress = tracker.current_progress();
    assert_eq!(
        (progress.work_completed, progress.work_remaining),
        (36_000.0, 108_000.0)
    );
}

/// Work completed plus work remaining, in rows, of the restrictions that the tracker reports.
fn total_work(tracker: &StreamTracker) -> f64 {
    let progress = tracker.current_progress();
    progress.work_completed + progress.work_remaining
}

#[test]
fn a_split_keeps_the_total_work() {
    let (tracker, _) = tracker_with(FakeSplits {
        split: true,
        primary_readable: true,
        ..FakeSplits::default()
    });
    // The server reports 200 rows as 10% of the stream, so it has 2,000 rows, not the 1,000
    // of the session estimate.
    assert!(tracker.try_claim(&claim(200)));
    tracker.set_fraction(0.1);
    assert_eq!(total_work(&tracker), 2_000.0);

    let (primary, residual) = tracker.try_split(0.5).unwrap();
    assert_eq!(
        (primary.estimated_rows, residual.estimated_rows),
        (1_100, 900)
    );
    assert_eq!(
        total_work(&tracker) + residual.rows_after(residual.start) as f64,
        2_000.0,
        "the primary and the residual hold the work of the stream"
    );
}

#[test]
fn a_checkpoint_gives_the_residual_the_server_estimate() {
    let tracker = StreamTracker::new(stream(0));
    assert!(tracker.try_claim(&claim(200)));
    tracker.set_fraction(0.1);
    let (_, residual) = tracker.try_split(0.0).unwrap();
    assert_eq!(residual.estimated_rows, 2_000);
    assert_eq!(residual.rows_after(residual.start), 1_800);
}

#[test]
fn a_small_server_fraction_keeps_the_session_estimate() {
    let tracker = StreamTracker::new(stream(0));
    assert!(tracker.try_claim(&claim(10)));
    tracker.set_fraction(0.001);
    let progress = tracker.current_progress();
    assert_eq!(
        (progress.work_completed, progress.work_remaining),
        (10.0, 990.0),
        "10 rows at 0.1% would mean 10,000 rows"
    );
}

#[test]
fn the_restriction_coder_round_trips() {
    let restriction = StreamRestriction {
        end: 77,
        ..stream(5)
    };
    let bytes = restriction.encode().unwrap();
    assert_eq!(StreamRestriction::decode(&bytes).unwrap(), restriction);
}
