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

//! Native BigQuery Storage reads on PrismRunner against fake Storage Read and BigQuery clients.
//! No test calls Google Cloud; `bigquery_storage_it` reads real tables.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beam::io::arrow::arrow_array::{ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray};
use beam::io::arrow::arrow_ipc::CompressionType;
use beam::io::arrow::arrow_ipc::writer::{IpcWriteOptions, StreamWriter};
use beam::io::arrow::arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use beam::io::gcp::bigquery::storage::read::{
    ArrowCompression, QueryPriority, ReadOptions, ReadRows,
};
use beam::io::gcp::bigquery::storage::schema::beam_schema;
use beam::io::gcp::options::GcpOptions;
use beam::options::PipelineOptions;
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Row};
use fluent::prelude::PCollectionExt;
use google_cloud_bigquery::client::Read;
use google_cloud_bigquery::model::arrow_serialization_options::CompressionCodec;
use google_cloud_bigquery::model::read_session::TableReadOptions;
use google_cloud_bigquery::model::stream_stats::Progress;
use google_cloud_bigquery::model::{
    ArrowRecordBatch, ArrowSchema as SessionSchema, ArrowSerializationOptions,
    CreateReadSessionRequest, ReadRowsRequest, ReadRowsResponse, ReadSession, ReadStream,
    SplitReadStreamRequest, SplitReadStreamResponse, StreamStats,
};
use google_cloud_bigquery_v2 as bigquery_v2;
use google_cloud_bigquery_v2::client::{DatasetService, JobService, TableService};
use google_cloud_bigquery_v2::model::{
    Dataset, DeleteDatasetRequest, DeleteTableRequest, GetDatasetRequest, InsertDatasetRequest,
    InsertJobRequest, Job, JobConfigurationQuery, JobReference, JobStatistics, JobStatistics2,
    JobStatus, TableFieldSchema, TableReference, TableSchema,
};
use google_cloud_gax::error::Error;
use google_cloud_gax::error::rpc::{Code, Status};
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax::response::Response;
use google_cloud_gax::streaming::ResponseStream;
use prism::PrismRunner;
use tokio::sync::{Notify, mpsc};

/// Rows in each response of the fake.
const BATCH_ROWS: i64 = 4;

fn arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "civil",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]))
}

/// The schema message, as the session holds it: what a stream writer writes first.
fn ipc_schema() -> Vec<u8> {
    StreamWriter::try_new(Vec::new(), &arrow_schema())
        .unwrap()
        .get_ref()
        .clone()
}

/// A record batch message, with its buffers compressed with `codec` as the session asked.
fn ipc_batch(ids: &[i64], codec: Option<CompressionType>) -> Vec<u8> {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids.to_vec())),
        Arc::new(TimestampMicrosecondArray::from(
            ids.iter().map(|id| id * 1_000_000).collect::<Vec<_>>(),
        )),
    ];
    let batch = RecordBatch::try_new(arrow_schema(), columns).unwrap();
    let options = IpcWriteOptions::default()
        .try_with_compression(codec)
        .unwrap();
    let mut writer =
        StreamWriter::try_new_with_options(Vec::new(), &arrow_schema(), options).unwrap();
    let schema_len = writer.get_ref().len();
    writer.write(&batch).unwrap();
    writer.get_ref()[schema_len..].to_vec()
}

/// A Storage Read service with one stream for each entry of `rows`. The first `ReadRows`
/// call on a stream in `fail_once` fails after its first response. A read of the `stall`
/// stream stops after its first response until the stream is split, so the runner asks for a
/// split. `SplitReadStream` splits a stream into `<stream>/p` and `<stream>/r`.
#[derive(Debug, Default)]
struct FakeRead {
    streams: Mutex<HashMap<String, Vec<i64>>>,
    session_streams: usize,
    estimated_rows: usize,
    fail_once: Mutex<Vec<String>>,
    stall: Option<String>,
    split_done: Arc<Notify>,
    sessions: Mutex<Vec<CreateReadSessionRequest>>,
    /// The buffer compression of the last session.
    codec: Mutex<Option<CompressionType>>,
    reads: Mutex<Vec<(String, i64)>>,
    splits: Mutex<Vec<(String, f64)>>,
}

impl FakeRead {
    fn new(rows: Vec<Vec<i64>>, fail_once: &[&str]) -> Arc<Self> {
        Arc::new(Self::with_rows(rows, fail_once, None))
    }

    fn with_rows(rows: Vec<Vec<i64>>, fail_once: &[&str], stall: Option<String>) -> Self {
        Self {
            session_streams: rows.len(),
            estimated_rows: rows.iter().map(Vec::len).sum(),
            streams: Mutex::new(
                rows.into_iter()
                    .enumerate()
                    .map(|(i, rows)| (Self::stream_name(i), rows))
                    .collect(),
            ),
            fail_once: Mutex::new(fail_once.iter().map(|s| s.to_string()).collect()),
            stall,
            ..Self::default()
        }
    }

    fn stream_name(index: usize) -> String {
        format!("projects/p/locations/us/sessions/s/streams/{index}")
    }
}

impl google_cloud_bigquery::stub::Read for FakeRead {
    async fn create_read_session(
        &self,
        req: CreateReadSessionRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<Response<ReadSession>> {
        let codec = req
            .read_session
            .as_ref()
            .and_then(|s| s.read_options.as_ref())
            .and_then(|o| o.arrow_serialization_options())
            .and_then(|o| match o.buffer_compression {
                CompressionCodec::Lz4Frame => Some(CompressionType::LZ4_FRAME),
                CompressionCodec::Zstd => Some(CompressionType::ZSTD),
                _ => None,
            });
        *self.codec.lock().unwrap() = codec;
        self.sessions.lock().unwrap().push(req);
        let streams = (0..self.session_streams)
            .map(|i| ReadStream::new().set_name(Self::stream_name(i)))
            .collect::<Vec<_>>();
        Ok(Response::from(
            ReadSession::new()
                .set_estimated_row_count(self.estimated_rows as i64)
                .set_estimated_total_bytes_scanned(self.estimated_rows as i64 * 16)
                .set_streams(streams)
                .set_arrow_schema(SessionSchema::new().set_serialized_schema(ipc_schema())),
        ))
    }

    async fn read_rows(
        &self,
        req: ReadRowsRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<ResponseStream<ReadRowsResponse>> {
        self.reads
            .lock()
            .unwrap()
            .push((req.read_stream.clone(), req.offset));
        let rows = self.streams.lock().unwrap()[&req.read_stream].clone();
        let codec = *self.codec.lock().unwrap();
        let fail = {
            let mut fail_once = self.fail_once.lock().unwrap();
            let found = fail_once.iter().position(|s| *s == req.read_stream);
            found.map(|i| fail_once.remove(i)).is_some()
        };
        let start = usize::try_from(req.offset).unwrap();
        let (sender, receiver) = mpsc::channel(rows.len() + 2);
        if start > rows.len() {
            let status = Status::default()
                .set_code(Code::OutOfRange)
                .set_message("past the end");
            sender.try_send(Err(Error::service(status))).unwrap();
            return Ok(ResponseStream::from(receiver));
        }
        let chunks: Vec<Vec<i64>> = rows[start..]
            .chunks(BATCH_ROWS as usize)
            .map(<[i64]>::to_vec)
            .collect();
        if self.stall.as_deref() == Some(req.read_stream.as_str()) {
            let split_done = Arc::clone(&self.split_done);
            let total = rows.len();
            tokio::spawn(async move {
                let mut read = start;
                for (n, chunk) in chunks.into_iter().enumerate() {
                    if n == 1 {
                        let _ =
                            tokio::time::timeout(Duration::from_secs(20), split_done.notified())
                                .await;
                    }
                    read += chunk.len();
                    if sender
                        .send(Ok(response(&chunk, read, total, codec)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            });
            return Ok(ResponseStream::from(receiver));
        }
        let mut read = start;
        for (n, chunk) in chunks.iter().map(Vec::as_slice).enumerate() {
            read += chunk.len();
            sender
                .try_send(Ok(response(chunk, read, rows.len(), codec)))
                .unwrap();
            if fail && n == 0 {
                let status = Status::default()
                    .set_code(Code::Unavailable)
                    .set_message("connection reset");
                sender.try_send(Err(Error::service(status))).unwrap();
                break;
            }
        }
        Ok(ResponseStream::from(receiver))
    }

    async fn split_read_stream(
        &self,
        req: SplitReadStreamRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<Response<SplitReadStreamResponse>> {
        self.splits
            .lock()
            .unwrap()
            .push((req.name.clone(), req.fraction));
        let mut streams = self.streams.lock().unwrap();
        let rows = streams[&req.name].clone();
        let at = (rows.len() as f64 * req.fraction).ceil() as usize;
        if at == 0 || at >= rows.len() {
            return Ok(Response::from(SplitReadStreamResponse::new()));
        }
        let (primary, remainder) = (format!("{}/p", req.name), format!("{}/r", req.name));
        streams.insert(primary.clone(), rows[..at].to_vec());
        streams.insert(remainder.clone(), rows[at..].to_vec());
        self.split_done.notify_waiters();
        Ok(Response::from(
            SplitReadStreamResponse::new()
                .set_primary_stream(ReadStream::new().set_name(primary))
                .set_remainder_stream(ReadStream::new().set_name(remainder)),
        ))
    }
}

fn response(
    chunk: &[i64],
    read: usize,
    total: usize,
    codec: Option<CompressionType>,
) -> ReadRowsResponse {
    ReadRowsResponse::new()
        .set_row_count(chunk.len() as i64)
        .set_arrow_record_batch(
            ArrowRecordBatch::new().set_serialized_record_batch(ipc_batch(chunk, codec)),
        )
        .set_stats(
            StreamStats::new()
                .set_progress(Progress::new().set_at_response_end(read as f64 / total as f64)),
        )
}

fn client(fake: &Arc<FakeRead>) -> Read {
    Read::from_stub::<FakeRead>(Arc::clone(fake))
}

async fn read_all(fake: &Arc<FakeRead>, options: ReadOptions) -> Vec<Row> {
    let schema = beam_schema(&arrow_schema()).unwrap();
    run_read(
        Pipeline::new(),
        ReadRows::new(options)
            .with_schema(schema)
            .with_client(client(fake)),
    )
    .await
}

async fn run_read(p: Pipeline, read: ReadRows) -> Vec<Row> {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    p.apply("Read", read).inspect("Capture", move |row: &Row| {
        sink.lock().unwrap().push(row.clone());
    });
    let result = p.run_with_runner(&PrismRunner::new()).await;
    assert!(result.is_ok(), "read pipeline failed: {result:?}");
    captured.lock().unwrap().clone()
}

fn ids(rows: &[Row]) -> Vec<i64> {
    let mut ids: Vec<i64> = rows
        .iter()
        .map(|r| r.get_i64("id").unwrap().unwrap())
        .collect();
    ids.sort_unstable();
    ids
}

#[tokio::test(flavor = "multi_thread")]
async fn every_row_of_every_stream_is_read_once() {
    let rows = vec![
        (0..10).collect(),
        (100..103).collect(),
        (200..217).collect(),
    ];
    let expected: Vec<i64> = rows.iter().flatten().copied().collect();
    let fake = FakeRead::new(rows, &[]);

    let read = read_all(&fake, ReadOptions::new("p.d.t")).await;
    assert_eq!(ids(&read), expected);
    let civil = read
        .iter()
        .find(|r| r.get_i64("id").unwrap() == Some(201))
        .unwrap();
    assert_eq!(
        civil.get_string("civil").unwrap(),
        Some("1970-01-01T00:03:21.000000")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_session_carries_the_read_options() {
    let fake = FakeRead::new(vec![vec![1]], &[]);
    let options = ReadOptions::new("bigquery-public-data:samples.wikipedia")
        .with_billing_project("billing")
        .with_selected_fields(["id", "civil"])
        .with_row_restriction("id > 0")
        .with_max_stream_count(8)
        .with_compression(ArrowCompression::Zstd);
    read_all(&fake, options).await;

    let sessions = fake.sessions.lock().unwrap();
    assert_eq!(sessions.len(), 1);
    let request = &sessions[0];
    assert_eq!(request.parent, "projects/billing");
    assert_eq!(request.max_stream_count, 8);
    let session = request.read_session.as_ref().unwrap();
    assert_eq!(
        session.table,
        "projects/bigquery-public-data/datasets/samples/tables/wikipedia"
    );
    assert_eq!(
        session.read_options,
        Some(
            TableReadOptions::new()
                .set_selected_fields(["id", "civil"])
                .set_row_restriction("id > 0")
                .set_arrow_serialization_options(
                    ArrowSerializationOptions::new().set_buffer_compression(CompressionCodec::Zstd)
                )
        )
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_read_resumes_after_the_rows_it_sent() {
    let rows: Vec<Vec<i64>> = vec![(0..10).collect(), (100..113).collect()];
    let expected: Vec<i64> = rows.iter().flatten().copied().collect();
    let failing = FakeRead::stream_name(1);
    let fake = FakeRead::new(rows, &[&failing]);

    let read = read_all(&fake, ReadOptions::new("p.d.t")).await;
    assert_eq!(ids(&read), expected, "no row is lost or read twice");

    let reads = fake.reads.lock().unwrap();
    let offsets: HashMap<&str, Vec<i64>> = reads.iter().fold(HashMap::new(), |mut acc, (s, o)| {
        acc.entry(s.as_str()).or_default().push(*o);
        acc
    });
    assert_eq!(offsets[failing.as_str()], vec![0, BATCH_ROWS]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_session_without_streams_reads_nothing() {
    let fake = FakeRead::new(Vec::new(), &[]);
    let read = read_all(&fake, ReadOptions::new("p.d.t")).await;
    assert!(read.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_stream_is_split_and_every_row_is_read_once() {
    let rows: Vec<Vec<i64>> = vec![(0..40).collect(), (100..104).collect()];
    let expected: Vec<i64> = rows.iter().flatten().copied().collect();
    let stalled = FakeRead::stream_name(0);
    let fake = Arc::new(FakeRead::with_rows(rows, &[], Some(stalled.clone())));

    let read = read_all(&fake, ReadOptions::new("p.d.t")).await;
    assert_eq!(ids(&read), expected, "no row is lost or read twice");

    let splits = fake.splits.lock().unwrap();
    assert!(
        splits.iter().any(|(stream, _)| *stream == stalled),
        "the runner did not split the stalled stream: {splits:?}"
    );
    let reads = fake.reads.lock().unwrap();
    let streams: Vec<&str> = reads.iter().map(|(s, _)| s.as_str()).collect();
    assert!(
        streams.contains(&format!("{stalled}/p").as_str()),
        "{streams:?}"
    );
    assert!(
        streams.contains(&format!("{stalled}/r").as_str()),
        "{streams:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn compressed_responses_decode_to_the_same_rows() {
    let rows: Vec<Vec<i64>> = vec![(0..10).collect(), (100..117).collect()];
    let expected: Vec<i64> = rows.iter().flatten().copied().collect();
    for compression in [ArrowCompression::Lz4Frame, ArrowCompression::Zstd] {
        let fake = FakeRead::new(rows.clone(), &[]);
        let options = ReadOptions::new("p.d.t").with_compression(compression);
        let read = read_all(&fake, options).await;
        assert_eq!(ids(&read), expected, "{compression:?}");
        assert!(fake.codec.lock().unwrap().is_some(), "{compression:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_schema_is_looked_up_when_none_is_given() {
    let fake = FakeRead::new(vec![(0..6).collect()], &[]);
    let read = run_read(
        Pipeline::new(),
        ReadRows::new(ReadOptions::new("p.d.t")).with_client(client(&fake)),
    )
    .await;
    assert_eq!(ids(&read), (0..6).collect::<Vec<_>>());
    let sessions = fake.sessions.lock().unwrap();
    assert_eq!(
        sessions.len(),
        2,
        "one session for the schema and one for the read"
    );
    assert_eq!(sessions[0].max_stream_count, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_pipeline_project_pays_when_no_billing_project_is_set() {
    let fake = FakeRead::new(vec![vec![1]], &[]);
    let options = PipelineOptions::default();
    options
        .set(GcpOptions {
            project: Some("from-options".to_string()),
            ..GcpOptions::default()
        })
        .unwrap();
    run_read(
        Pipeline::create(&options),
        ReadRows::new(ReadOptions::new("samples.wikipedia")).with_client(client(&fake)),
    )
    .await;

    let sessions = fake.sessions.lock().unwrap();
    for request in sessions.iter() {
        assert_eq!(request.parent, "projects/from-options");
        assert_eq!(
            request.read_session.as_ref().unwrap().table,
            "projects/from-options/datasets/samples/tables/wikipedia"
        );
    }
}

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Event {
    id: i64,
    civil: Option<String>,
}

#[tokio::test(flavor = "multi_thread")]
async fn a_typed_read_selects_the_fields_of_the_type() {
    let fake = FakeRead::new(vec![(0..3).collect()], &[]);
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let p = Pipeline::new();
    p.apply(
        "Read",
        ReadRows::new(ReadOptions::new("p.d.t"))
            .with_client(client(&fake))
            .typed::<Event>(),
    )
    .inspect("Capture", move |event: &Event| {
        sink.lock().unwrap().push(event.clone());
    });
    let result = p.run_with_runner(&PrismRunner::new()).await;
    assert!(result.is_ok(), "read pipeline failed: {result:?}");

    let mut events = captured.lock().unwrap().clone();
    events.sort_by_key(|e| e.id);
    assert_eq!(
        events,
        (0..3)
            .map(|id| Event {
                id,
                civil: Some(format!("1970-01-01T00:00:0{id}.000000")),
            })
            .collect::<Vec<_>>()
    );
    let sessions = fake.sessions.lock().unwrap();
    assert_eq!(sessions.len(), 1, "a typed read needs no schema lookup");
    let selected = &sessions[0]
        .read_session
        .as_ref()
        .unwrap()
        .read_options
        .as_ref()
        .unwrap()
        .selected_fields;
    assert_eq!(selected, &["id", "civil"]);
}

/// A BigQuery service for query reads: jobs, datasets and tables. It records each request. A
/// dry-run job returns the schema of [`arrow_schema`] and reads one table in `source_location`.
/// Other jobs are done at once.
#[derive(Debug, Default)]
struct FakeBigQuery {
    source_location: String,
    jobs: Mutex<Vec<Job>>,
    datasets: Mutex<Vec<Dataset>>,
    deleted_datasets: Mutex<Vec<DeleteDatasetRequest>>,
    deleted_tables: Mutex<Vec<DeleteTableRequest>>,
}

impl FakeBigQuery {
    fn new(source_location: &str) -> Arc<Self> {
        Arc::new(Self {
            source_location: source_location.to_string(),
            ..Self::default()
        })
    }

    fn dry_runs(&self) -> Vec<Job> {
        let jobs = self.jobs.lock().unwrap();
        jobs.iter().filter(|job| is_dry_run(job)).cloned().collect()
    }

    /// The configuration of the one query job that is not a dry run.
    fn query_job(&self) -> (JobReference, JobConfigurationQuery) {
        let jobs = self.jobs.lock().unwrap();
        let [job] = jobs
            .iter()
            .filter(|job| !is_dry_run(job))
            .collect::<Vec<_>>()[..]
        else {
            panic!("not one query job: {jobs:?}");
        };
        let config = job.configuration.clone().unwrap().query.unwrap();
        (job.job_reference.clone().unwrap(), config)
    }
}

fn is_dry_run(job: &Job) -> bool {
    job.configuration
        .as_ref()
        .and_then(|c| c.dry_run)
        .unwrap_or(false)
}

impl bigquery_v2::stub::JobService for FakeBigQuery {
    async fn insert_job(
        &self,
        req: InsertJobRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<Response<Job>> {
        let job = req.job.unwrap();
        self.jobs.lock().unwrap().push(job.clone());
        if !is_dry_run(&job) {
            return Ok(Response::from(
                job.set_status(JobStatus::new().set_state("DONE")),
            ));
        }
        let schema = TableSchema::new().set_fields([
            TableFieldSchema::new()
                .set_name("id")
                .set_type("INTEGER")
                .set_mode("REQUIRED"),
            TableFieldSchema::new()
                .set_name("civil")
                .set_type("DATETIME")
                .set_mode("NULLABLE"),
        ]);
        let source = TableReference::new()
            .set_project_id("src-proj")
            .set_dataset_id("src_ds")
            .set_table_id("src_tbl");
        Ok(Response::from(
            job.set_statistics(
                JobStatistics::new().set_query(
                    JobStatistics2::new()
                        .set_schema(schema)
                        .set_referenced_tables([source]),
                ),
            ),
        ))
    }
}

impl bigquery_v2::stub::DatasetService for FakeBigQuery {
    async fn get_dataset(
        &self,
        req: GetDatasetRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<Response<Dataset>> {
        assert_eq!(
            (req.project_id.as_str(), req.dataset_id.as_str()),
            ("src-proj", "src_ds")
        );
        Ok(Response::from(
            Dataset::new().set_location(&self.source_location),
        ))
    }

    async fn insert_dataset(
        &self,
        req: InsertDatasetRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<Response<Dataset>> {
        let dataset = req.dataset.unwrap();
        self.datasets.lock().unwrap().push(dataset.clone());
        Ok(Response::from(dataset))
    }

    async fn delete_dataset(
        &self,
        req: DeleteDatasetRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<Response<()>> {
        self.deleted_datasets.lock().unwrap().push(req);
        Ok(Response::from(()))
    }
}

impl bigquery_v2::stub::TableService for FakeBigQuery {
    async fn delete_table(
        &self,
        req: DeleteTableRequest,
        _options: RequestOptions,
    ) -> google_cloud_gax::Result<Response<()>> {
        self.deleted_tables.lock().unwrap().push(req);
        Ok(Response::from(()))
    }
}

fn query_read(
    fake_read: &Arc<FakeRead>,
    fake: &Arc<FakeBigQuery>,
    options: ReadOptions,
) -> ReadRows {
    ReadRows::new(options)
        .with_client(client(fake_read))
        .with_query_clients(
            JobService::from_stub::<FakeBigQuery>(Arc::clone(fake)),
            DatasetService::from_stub::<FakeBigQuery>(Arc::clone(fake)),
            TableService::from_stub::<FakeBigQuery>(Arc::clone(fake)),
        )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_query_read_reads_a_temp_dataset_in_the_source_location_and_deletes_it() {
    let rows: Vec<Vec<i64>> = vec![(0..8).collect(), (100..105).collect()];
    let expected: Vec<i64> = rows.iter().flatten().copied().collect();
    let fake_read = FakeRead::new(rows, &[]);
    let fake = FakeBigQuery::new("asia-northeast1");
    let sql = "SELECT id, civil FROM `src-proj.src_ds.src_tbl`";
    let kms_key = "projects/p/locations/asia-northeast1/keyRings/r/cryptoKeys/k";
    let options = ReadOptions::from_query(sql)
        .with_billing_project("billing-proj")
        .with_query_priority(QueryPriority::Interactive)
        .with_kms_key(kms_key)
        .with_use_legacy_sql(true)
        .with_flatten_results(false);

    let read = run_read(Pipeline::new(), query_read(&fake_read, &fake, options)).await;
    assert_eq!(ids(&read), expected);

    // The schema lookup and the location lookup each run a dry run.
    let dry_runs = fake.dry_runs();
    assert_eq!(dry_runs.len(), 2);
    let dry_run = dry_runs[0].configuration.clone().unwrap().query.unwrap();
    assert_eq!(dry_run.query, sql);
    assert_eq!(
        (dry_run.use_legacy_sql, dry_run.flatten_results),
        (Some(true), Some(false))
    );

    let datasets = fake.datasets.lock().unwrap();
    let [dataset] = &datasets[..] else {
        panic!("not one dataset: {datasets:?}");
    };
    let dataset_ref = dataset.dataset_reference.clone().unwrap();
    assert_eq!(dataset_ref.project_id, "billing-proj");
    assert!(
        dataset_ref
            .dataset_id
            .starts_with("temp_dataset_beam_bq_job_QUERY_")
    );
    assert_eq!(dataset.location, "asia-northeast1");
    assert_eq!(dataset.default_table_expiration_ms, Some(86_400_000));

    let (reference, query) = fake.query_job();
    assert_eq!(reference.project_id, "billing-proj");
    assert_eq!(reference.location.as_deref(), Some("asia-northeast1"));
    assert_eq!(query.priority, "INTERACTIVE");
    assert_eq!(
        (query.use_legacy_sql, query.flatten_results),
        (Some(true), Some(false))
    );
    assert_eq!(
        query
            .destination_encryption_configuration
            .unwrap()
            .kms_key_name
            .as_deref(),
        Some(kms_key)
    );
    let destination = query.destination_table.unwrap();
    assert_eq!(destination.project_id, "billing-proj");
    assert_eq!(destination.dataset_id, dataset_ref.dataset_id);

    let sessions = fake_read.sessions.lock().unwrap();
    assert_eq!(
        sessions[0].read_session.as_ref().unwrap().table,
        format!(
            "projects/billing-proj/datasets/{}/tables/{}",
            destination.dataset_id, destination.table_id
        )
    );

    // Deleting the temporary dataset deletes the temporary table in it.
    let deleted = fake.deleted_datasets.lock().unwrap();
    let [deleted] = &deleted[..] else {
        panic!("not one deleted dataset: {deleted:?}");
    };
    assert_eq!(deleted.dataset_id, dataset_ref.dataset_id);
    assert!(deleted.delete_contents);
    assert!(fake.deleted_tables.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_query_read_with_a_temp_dataset_deletes_only_its_table() {
    let fake_read = FakeRead::new(vec![vec![1, 2]], &[]);
    let fake = FakeBigQuery::new("unused");
    let options = ReadOptions::from_query("SELECT 1")
        .with_billing_project("billing-proj")
        .with_query_location("EU")
        .with_temp_dataset("dest-proj:existing_dataset");

    let read = run_read(Pipeline::new(), query_read(&fake_read, &fake, options)).await;
    assert_eq!(ids(&read), vec![1, 2]);

    // A location in the options needs no location lookup: one dry run, for the schema.
    assert_eq!(fake.dry_runs().len(), 1);
    assert!(fake.datasets.lock().unwrap().is_empty());
    assert!(fake.deleted_datasets.lock().unwrap().is_empty());

    let (reference, query) = fake.query_job();
    assert_eq!(reference.location.as_deref(), Some("EU"));
    let destination = query.destination_table.unwrap();
    assert_eq!(
        (
            destination.project_id.as_str(),
            destination.dataset_id.as_str()
        ),
        ("dest-proj", "existing_dataset")
    );
    let deleted = fake.deleted_tables.lock().unwrap();
    let [deleted] = &deleted[..] else {
        panic!("not one deleted table: {deleted:?}");
    };
    assert_eq!(deleted.table_id, destination.table_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_query_result_deletes_its_temp_dataset() {
    let fake_read = FakeRead::new(Vec::new(), &[]);
    let fake = FakeBigQuery::new("US");
    let options = ReadOptions::from_query("SELECT 1 WHERE FALSE").with_billing_project("p");

    let read = run_read(Pipeline::new(), query_read(&fake_read, &fake, options)).await;
    assert!(read.is_empty());
    assert_eq!(fake.deleted_datasets.lock().unwrap().len(), 1);
}

#[test]
fn invalid_query_reads_fail_at_expansion() {
    let p = Pipeline::new();
    let invalid = [
        ReadOptions::from_query(""),
        ReadOptions::from_query("SELECT 1")
            .with_billing_project("p")
            .with_row_restriction("id > 0"),
        ReadOptions::from_query("SELECT 1")
            .with_billing_project("p")
            .with_selected_fields(["id"]),
        // A query read has no table project to bill.
        ReadOptions::from_query("SELECT 1"),
    ];
    for options in invalid {
        let read = p.try_apply("Read", ReadRows::new(options.clone()));
        assert!(read.is_err(), "{options:?}");
    }
}
