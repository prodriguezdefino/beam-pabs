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

//! The checks of the read options of the Storage Read connector, and the session request that
//! they make. A fake service stands in for BigQuery.

use std::sync::{Arc, Mutex};

use beam::pipeline::Pipeline;
use beam_io_arrow::arrow_schema::{DataType, Field, Schema as ArrowSchema};
use beam_io_gcp::bigquery::write::encode::schema_bytes;
use beam_io_gcp::bigquery::{ReadOptions, ReadRows};
use bytes::Bytes;
use google_cloud_bigquery::client::Read;
use google_cloud_bigquery::model::{
    ArrowSchema as SessionSchema, CreateReadSessionRequest, ReadSession, ReadStream,
};
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::response::Response;

#[test]
fn invalid_session_settings_fail_before_any_call() {
    let cases = [
        (
            ReadOptions::new("d.t")
                .with_max_stream_count(10)
                .with_preferred_min_stream_count(20),
            "preferred_min_stream_count 20 is greater than max_stream_count 10",
        ),
        (
            ReadOptions::new("d.t").with_snapshot_time("yesterday"),
            "snapshot_time 'yesterday' is not RFC 3339",
        ),
        (
            ReadOptions::from_query("SELECT 1").with_snapshot_time("2024-05-01T00:00:00Z"),
            "apply to table reads only",
        ),
        (
            ReadOptions::from_query("SELECT 1").with_sample_percentage(10.0),
            "apply to table reads only",
        ),
        (
            ReadOptions::new("d.t").with_sample_percentage(0.0),
            "sample_percentage 0 is not more than 0 and at most 100",
        ),
        (
            ReadOptions::new("d.t").with_sample_percentage(100.5),
            "sample_percentage 100.5",
        ),
        (
            ReadOptions::new("d.t").with_sample_percentage(f64::NAN),
            "sample_percentage NaN",
        ),
    ];
    for (options, expected) in cases {
        let error = options.validate().unwrap_err().to_string();
        assert!(error.contains(expected), "{options:?}: {error}");
    }
}

#[test]
fn valid_session_settings_pass() {
    let options = ReadOptions::new("d.t")
        .with_snapshot_time("2024-05-01T12:30:00.5Z")
        .with_sample_percentage(100.0)
        .with_preferred_min_stream_count(4);
    options.validate().unwrap();
    assert!(options.snapshot_timestamp().unwrap().is_some());
    ReadOptions::new("d.t")
        .with_preferred_min_stream_count(20)
        .validate()
        .unwrap();
}

/// A Storage Read service that keeps the session requests and answers with one stream.
#[derive(Debug, Default)]
struct FakeSessions {
    requests: Mutex<Vec<CreateReadSessionRequest>>,
}

impl google_cloud_bigquery::stub::Read for FakeSessions {
    async fn create_read_session(
        &self,
        req: CreateReadSessionRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<Response<ReadSession>> {
        self.requests.lock().unwrap().push(req);
        let arrow = ArrowSchema::new(vec![Field::new("id", DataType::Int64, false)]);
        let schema =
            SessionSchema::new().set_serialized_schema(Bytes::from(schema_bytes(&arrow).unwrap()));
        let session = ReadSession::new()
            .set_arrow_schema(schema)
            .set_streams(vec![ReadStream::new().set_name("streams/0")]);
        Ok(Response::from(session))
    }
}

#[test]
fn the_session_request_holds_the_sample_percentage() {
    let fake = Arc::new(FakeSessions::default());
    let client = Read::from_stub::<FakeSessions>(Arc::clone(&fake));
    let read =
        ReadRows::new(ReadOptions::new("p.d.t").with_sample_percentage(37.5)).with_client(client);

    Pipeline::new().try_apply("ReadRows", read).unwrap();

    let requests = fake.requests.lock().unwrap();
    let percentages: Vec<_> = requests
        .iter()
        .map(|req| {
            req.read_session
                .as_ref()
                .and_then(|session| session.read_options.as_ref())
                .and_then(|options| options.sample_percentage)
        })
        .collect();
    assert_eq!(percentages, [Some(37.5)], "the schema lookup session");
}
