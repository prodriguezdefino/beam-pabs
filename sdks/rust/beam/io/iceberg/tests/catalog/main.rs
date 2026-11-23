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

//! Tests of the REST catalog connection against a local HTTP server.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use beam::internals::{MetricsContainer, MetricsScope};
use beam::metrics::{
    MetricResults, RETRY_WAIT_COUNTER_NAME, THROTTLE_TIME_COUNTER_NAME, THROTTLE_TIME_NAMESPACE,
};
use beam::retry::Decision;
use beam_io_iceberg::Catalog;
use beam_io_iceberg::catalog::{
    CatalogClient, SERVICE, TOKEN_EXPIRES_AT, TableFileIo, catalog_decision, catalog_status,
    connect, parse_retry_after,
};
use beam_test_utils::request_counts;
use iceberg::{Catalog as _, ErrorKind, TableIdent};
use reqwest::header::{HeaderMap, HeaderValue, RETRY_AFTER};

/// One answer of the server: a path prefix, the HTTP status and the JSON body.
type Route = (&'static str, u16, &'static str);

/// Reads one request from `stream` and writes the answer of the first route that matches.
fn answer(mut stream: &TcpStream, routes: &[Route]) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    let mut header = String::new();
    while reader.read_line(&mut header)? > 2 {
        header.clear();
    }
    let path = request_line.split(' ').nth(1).unwrap_or_default();
    let (status, body) = routes
        .iter()
        .find(|(prefix, _, _)| path.starts_with(prefix))
        .map_or((404, "{}"), |(_, status, body)| (*status, *body));
    write!(
        stream,
        "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Connection: close\r\n\r\n{body}",
        body.len()
    )
}

/// Starts a server on 127.0.0.1 that answers with `routes`, and returns its URI.
fn serve(routes: &'static [Route]) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a free port");
    let uri = format!("http://{}", listener.local_addr().expect("an address"));
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let _answered = answer(&stream, routes);
        }
    });
    uri
}

const ROUTES: &[Route] = &[
    ("/v1/config", 200, r#"{"defaults":{},"overrides":{}}"#),
    (
        "/v1/namespaces/db/tables/t/credentials",
        200,
        r#"{"storage-credentials":[{"prefix":"gs://b/","config":{"k":"v"}}]}"#,
    ),
    (
        "/v1/namespaces/db/tables/missing",
        404,
        r#"{"error":{"message":"no table","type":"NoSuchTableException","code":404}}"#,
    ),
];

#[test]
fn catalog_calls_report_io_request_metrics() {
    let uri = serve(ROUTES);
    let catalog = Catalog::rest(&uri);
    let table = TableIdent::from_strs(["db", "t"]).expect("an identifier");
    let missing = TableIdent::from_strs(["db", "missing"]).expect("an identifier");
    let (credentials, loaded) = beam::internals::block_on_io(async {
        let connection = connect(&catalog).await.expect("a connection");
        let credentials = connection.credentials(&table, "gs://b/db/t").await;
        (credentials, connection.load_table(&missing).await)
    })
    .expect("the I/O runtime runs");

    let config = credentials.expect("the credentials of the table");
    assert_eq!(config.get("k").map(String::as_str), Some("v"));
    assert!(loaded.is_err(), "the table does not exist");
    let ok = vec![("ok".to_string(), 1)];
    assert_eq!(request_counts(&format!("{uri}/v1/config")), ok);
    assert_eq!(
        request_counts(&format!("{uri}/v1/namespaces/db/tables/t/credentials")),
        ok
    );
    assert_eq!(
        request_counts(&format!("{uri}/db.missing")),
        vec![("not_found".to_string(), 1)]
    );
    assert_eq!(SERVICE, "IcebergRestCatalog");
}

#[test]
fn catalog_errors_map_to_canonical_statuses() {
    let status = |kind| catalog_status(&iceberg::Error::new(kind, "x"));
    assert_eq!(status(ErrorKind::TableNotFound), "not_found");
    assert_eq!(status(ErrorKind::NamespaceAlreadyExists), "already_exists");
    assert_eq!(status(ErrorKind::CatalogCommitConflicts), "aborted");
    assert_eq!(status(ErrorKind::DataInvalid), "invalid_argument");
    assert_eq!(status(ErrorKind::Unexpected), "unknown");
    let throttled = iceberg::Error::new(ErrorKind::Unexpected, "rate limited")
        .with_context("status", "429 Too Many Requests");
    assert_eq!(catalog_status(&throttled), "resource_exhausted");
}

#[test]
fn catalog_decision_and_retry_after_classify_throttling() {
    let throttled = iceberg::Error::new(ErrorKind::Unexpected, "rate limited")
        .with_context("status", "429 Too Many Requests");
    assert_eq!(catalog_decision(&throttled), Decision::Throttled);

    let conflict = iceberg::Error::new(ErrorKind::CatalogCommitConflicts, "conflict");
    assert_eq!(catalog_decision(&conflict), Decision::Backoff);

    let unavailable = iceberg::Error::new(ErrorKind::Unexpected, "unavailable")
        .with_context("status", "503 Service Unavailable");
    assert_eq!(catalog_decision(&unavailable), Decision::Backoff);

    let missing = iceberg::Error::new(ErrorKind::TableNotFound, "missing");
    assert_eq!(catalog_decision(&missing), Decision::Stop);

    let mut headers = HeaderMap::new();
    assert_eq!(parse_retry_after(&headers), None);
    headers.insert(RETRY_AFTER, HeaderValue::from_static("2"));
    assert_eq!(parse_retry_after(&headers), Some(Duration::from_secs(2)));
    headers.insert(RETRY_AFTER, HeaderValue::from_static("0"));
    assert_eq!(parse_retry_after(&headers), Some(Duration::ZERO));
    headers.insert(RETRY_AFTER, HeaderValue::from_static("not-a-number"));
    assert_eq!(parse_retry_after(&headers), None);
}

#[test]
fn connection_send_retries_429_and_records_throttling() {
    let credential_calls = Arc::new(AtomicUsize::new(0));
    let calls_for_server = Arc::clone(&credential_calls);
    let listener = TcpListener::bind("127.0.0.1:0").expect("a free port");
    let uri = format!("http://{}", listener.local_addr().expect("an address"));
    thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let mut reader = BufReader::new(&stream);
            let mut request_line = String::new();
            if reader.read_line(&mut request_line).is_err() {
                continue;
            }
            let mut header = String::new();
            while reader.read_line(&mut header).unwrap_or(0) > 2 {
                header.clear();
            }
            let path = request_line.split(' ').nth(1).unwrap_or_default();
            let (status, body) = if path.starts_with("/v1/config") {
                (200, r#"{"defaults":{},"overrides":{}}"#)
            } else if path.starts_with("/v1/namespaces/db/tables/t/credentials") {
                if calls_for_server.fetch_add(1, Ordering::SeqCst) == 0 {
                    (429, r#"{"error":{"message":"slow down","code":429}}"#)
                } else {
                    (
                        200,
                        r#"{"storage-credentials":[{"prefix":"gs://b/","config":{"k":"v"}}]}"#,
                    )
                }
            } else {
                (404, "{}")
            };
            let _written = write!(
                stream,
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });

    let catalog = Catalog::rest(&uri);
    let table = TableIdent::from_strs(["db", "t"]).expect("an identifier");
    let container = Arc::new(MetricsContainer::new());
    let config = {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("IcebergCatalog"));
        beam::internals::block_on_io(async {
            let connection = connect(&catalog).await.expect("a connection");
            connection.credentials(&table, "gs://b/db/t").await
        })
        .expect("the I/O runtime runs")
        .expect("credentials after 429 retry")
    };

    assert_eq!(config.get("k").map(String::as_str), Some("v"));
    assert_eq!(credential_calls.load(Ordering::SeqCst), 2);

    let metrics = MetricResults::from_container(&container);
    assert!(
        metrics
            .counter_for_transform(
                "IcebergCatalog",
                THROTTLE_TIME_NAMESPACE,
                THROTTLE_TIME_COUNTER_NAME,
            )
            .unwrap_or(0)
            > 0
    );
    assert_eq!(
        metrics.counter_for_transform("IcebergCatalog", "iceberg", RETRY_WAIT_COUNTER_NAME),
        None
    );
}

/// Starts a local HTTP server that passes each `(method, path, body)` request to `handler`
/// and writes its `(status, body)` JSON response.
fn serve_requests(handler: impl Fn(&str, &str, &[u8]) -> (u16, String) + Send + 'static) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a free port");
    let uri = format!("http://{}", listener.local_addr().expect("an address"));
    thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let mut reader = BufReader::new(&stream);
            let mut request_line = String::new();
            if reader.read_line(&mut request_line).is_err() {
                continue;
            }
            let mut content_length = 0usize;
            let mut header = String::new();
            while reader.read_line(&mut header).unwrap_or(0) > 2 {
                if let Some((name, value)) = header.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    content_length = value.trim().parse().unwrap_or(0);
                }
                header.clear();
            }
            let mut body = vec![0u8; content_length];
            if content_length > 0 {
                let _ = reader.read_exact(&mut body);
            }
            let mut parts = request_line.split(' ');
            let method = parts.next().unwrap_or_default();
            let path = parts.next().unwrap_or_default();
            let (status, resp_body) = handler(method, path, &body);
            let _written = if status == 204 {
                write!(
                    stream,
                    "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"
                )
            } else {
                write!(
                    stream,
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{resp_body}",
                    resp_body.len()
                )
            };
        }
    });
    uri
}

/// Starts a local HTTP server that passes each request path to `handler` and writes its
/// `(status, body)` JSON response.
fn serve_with(handler: impl Fn(&str) -> (u16, String) + Send + 'static) -> String {
    serve_requests(move |_, path, _| handler(path))
}

#[test]
fn table_url_encodes_special_characters_and_connection_debug_hides_headers() {
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_for_server = Arc::clone(&seen);
    let uri = serve_with(move |path| {
        seen_for_server
            .lock()
            .expect("no test thread panics")
            .push(path.to_string());
        if path.starts_with("/v1/config") {
            (
                200,
                r#"{"defaults":{"prefix":"p/q"},"overrides":{}}"#.to_string(),
            )
        } else if path == "/v1/p/q/namespaces/a%1Fb/tables/my%20table%2F1/credentials" {
            (
                200,
                r#"{"storage-credentials":[{"prefix":"gs://b/","config":{"k":"v"}}]}"#.to_string(),
            )
        } else {
            (404, "{}".to_string())
        }
    });

    let catalog = Catalog::rest(&uri).with_header("x-api-key", "secret-header");
    let table = TableIdent::from_strs(["a", "b", "my table/1"]).expect("an identifier");
    let (debug, config) = beam::internals::block_on_io(async {
        let connection = connect(&catalog).await.expect("a connection");
        let debug = format!("{connection:?}");
        let config = connection
            .credentials(&table, "gs://b/t")
            .await
            .expect("credentials for encoded table URL");
        (debug, config)
    })
    .expect("the I/O runtime runs");

    assert_eq!(config.get("k").map(String::as_str), Some("v"));
    let paths = seen.lock().expect("no test thread panics");
    assert!(
        paths
            .iter()
            .any(|p| p == "/v1/p/q/namespaces/a%1Fb/tables/my%20table%2F1/credentials"),
        "{paths:?}"
    );
    assert!(
        paths.iter().all(|p| !p.contains("%251F")),
        "namespace separator must not double-encode: {paths:?}"
    );
    assert!(!debug.contains("secret-header"), "{debug}");
    assert!(debug.contains("x-api-key"), "{debug}");
    assert!(debug.contains(".."), "{debug}");
}

#[test]
fn config_prefix_is_fetched_once_across_credentials_and_metrics_calls() {
    let config_calls = Arc::new(AtomicUsize::new(0));
    let credential_calls = Arc::new(AtomicUsize::new(0));
    let metrics_calls = Arc::new(AtomicUsize::new(0));
    let config_for_server = Arc::clone(&config_calls);
    let cred_for_server = Arc::clone(&credential_calls);
    let metrics_for_server = Arc::clone(&metrics_calls);
    let uri = serve_with(move |path| {
        if path.starts_with("/v1/config") {
            config_for_server.fetch_add(1, Ordering::SeqCst);
            (200, r#"{"defaults":{},"overrides":{}}"#.to_string())
        } else if path == "/v1/namespaces/db/tables/t/credentials" {
            cred_for_server.fetch_add(1, Ordering::SeqCst);
            (
                200,
                r#"{"storage-credentials":[{"prefix":"gs://b/","config":{"k":"v"}}]}"#.to_string(),
            )
        } else if path == "/v1/namespaces/db/tables/t/metrics" {
            metrics_for_server.fetch_add(1, Ordering::SeqCst);
            (200, "{}".to_string())
        } else {
            (404, "{}".to_string())
        }
    });

    let catalog = Catalog::rest(&uri).with_metrics_reporting();
    let table = TableIdent::from_strs(["db", "t"]).expect("an identifier");
    let report = serde_json::json!({"table-name": "db.t"});
    let config_before = beam::internals::block_on_io(async {
        let connection = connect(&catalog).await.expect("a connection");
        let before = config_calls.load(Ordering::SeqCst);
        connection
            .credentials(&table, "gs://b/db/t")
            .await
            .expect("first credentials");
        connection
            .credentials(&table, "gs://b/db/t")
            .await
            .expect("second credentials");
        assert!(connection.report_metrics(&table, &report).await);
        assert!(connection.report_metrics(&table, &report).await);
        before
    })
    .expect("the I/O runtime runs");

    assert_eq!(config_calls.load(Ordering::SeqCst) - config_before, 1);
    assert_eq!(credential_calls.load(Ordering::SeqCst), 2);
    assert_eq!(metrics_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        request_counts(&format!("{uri}/v1/config")),
        vec![("ok".to_string(), 1)]
    );
}

#[test]
fn vended_token_expiry_handles_small_timestamps_and_short_ttls() {
    let table = TableIdent::from_strs(["db", "t"]).expect("an identifier");

    // Small epoch timestamp (< 300_000 ms) must not underflow and refreshes on the next call.
    let small_calls = Arc::new(AtomicUsize::new(0));
    let small_for_server = Arc::clone(&small_calls);
    let small_uri = serve_with(move |path| {
        if path.starts_with("/v1/config") {
            (200, r#"{"defaults":{},"overrides":{}}"#.to_string())
        } else if path == "/v1/namespaces/db/tables/t/credentials" {
            small_for_server.fetch_add(1, Ordering::SeqCst);
            let body = serde_json::json!({
                "storage-credentials": [{
                    "prefix": "gs://b/",
                    "config": { TOKEN_EXPIRES_AT: "1000" }
                }]
            })
            .to_string();
            (200, body)
        } else {
            (404, "{}".to_string())
        }
    });
    let mut small_io = TableFileIo::new(
        Catalog::rest(&small_uri).with_vended_credentials(),
        table.clone(),
    );
    small_io
        .get("gs://b/db/t")
        .expect("small timestamp does not underflow");
    small_io
        .get("gs://b/db/t")
        .expect("expired token refreshes");
    assert_eq!(small_calls.load(Ordering::SeqCst), 2);

    // Short TTL (60 s < 300 s) is cached for half its remaining lifetime instead of thrashing.
    let short_calls = Arc::new(AtomicUsize::new(0));
    let short_for_server = Arc::clone(&short_calls);
    let short_uri = serve_with(move |path| {
        if path.starts_with("/v1/config") {
            (200, r#"{"defaults":{},"overrides":{}}"#.to_string())
        } else if path == "/v1/namespaces/db/tables/t/credentials" {
            short_for_server.fetch_add(1, Ordering::SeqCst);
            let expires_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_millis()
                + 60_000;
            let body = serde_json::json!({
                "storage-credentials": [{
                    "prefix": "gs://b/",
                    "config": { TOKEN_EXPIRES_AT: expires_ms.to_string() }
                }]
            })
            .to_string();
            (200, body)
        } else {
            (404, "{}".to_string())
        }
    });
    let mut short_io = TableFileIo::new(Catalog::rest(&short_uri).with_vended_credentials(), table);
    short_io
        .get("gs://b/db/t")
        .expect("fetches short-lived token");
    short_io
        .get("gs://b/db/t")
        .expect("reuses cached short-lived token");
    assert_eq!(short_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn report_metrics_retries_transient_5xx_and_sends_commit_report() {
    let seen = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let seen_for_server = Arc::clone(&seen);
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_server = Arc::clone(&calls);
    let uri = serve_requests(move |method, path, body| {
        if path.starts_with("/v1/config") {
            (200, r#"{"defaults":{},"overrides":{}}"#.to_string())
        } else if method == "POST" && path == "/v1/namespaces/db/tables/t/metrics" {
            let payload: serde_json::Value =
                serde_json::from_slice(body).expect("valid commit report JSON");
            seen_for_server
                .lock()
                .expect("no test thread panics")
                .push(payload);
            match calls_for_server.fetch_add(1, Ordering::SeqCst) {
                0 => (503, r#"{"error":"unavailable"}"#.to_string()),
                1 => (500, r#"{"error":"internal"}"#.to_string()),
                _ => (204, String::new()),
            }
        } else {
            (404, "{}".to_string())
        }
    });

    let client = CatalogClient::Rest(Catalog::rest(&uri).with_metrics_reporting());
    let table = TableIdent::from_strs(["db", "t"]).expect("an identifier");
    let report = serde_json::json!({
        "report-type": "commit-report",
        "table-name": "db.t",
    });
    let container = Arc::new(MetricsContainer::new());
    let sent = {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("Commit"));
        beam::internals::block_on_io(client.report_metrics(&table, &report))
            .expect("the I/O runtime runs")
    };

    assert!(sent);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let payloads = seen.lock().expect("no test thread panics");
    assert_eq!(
        payloads.as_slice(),
        &[report.clone(), report.clone(), report]
    );

    let metrics = MetricResults::from_container(&container);
    assert!(
        metrics
            .counter_for_transform("Commit", "iceberg", RETRY_WAIT_COUNTER_NAME)
            .unwrap_or(0)
            > 0
    );
    assert_eq!(
        metrics.counter_for_transform("Commit", "iceberg", "metrics_report_failures"),
        None
    );
    assert_eq!(
        request_counts(&format!("{uri}/v1/namespaces/db/tables/t/metrics")),
        vec![
            ("internal".to_string(), 1),
            ("ok".to_string(), 1),
            ("unavailable".to_string(), 1),
        ]
    );
}

#[test]
fn report_metrics_counts_failures_when_retries_exhaust_or_4xx_rejects() {
    let transient_calls = Arc::new(AtomicUsize::new(0));
    let bad_calls = Arc::new(AtomicUsize::new(0));
    let transient_for_server = Arc::clone(&transient_calls);
    let bad_for_server = Arc::clone(&bad_calls);
    let uri = serve_requests(move |method, path, _| {
        if path.starts_with("/v1/config") {
            (200, r#"{"defaults":{},"overrides":{}}"#.to_string())
        } else if method == "POST" && path == "/v1/namespaces/db/tables/t/metrics" {
            transient_for_server.fetch_add(1, Ordering::SeqCst);
            (503, r#"{"error":"unavailable"}"#.to_string())
        } else if method == "POST" && path == "/v1/namespaces/db/tables/bad/metrics" {
            bad_for_server.fetch_add(1, Ordering::SeqCst);
            (400, r#"{"error":"bad report"}"#.to_string())
        } else {
            (404, "{}".to_string())
        }
    });

    let enabled = CatalogClient::Rest(Catalog::rest(&uri).with_metrics_reporting());
    let disabled = CatalogClient::Rest(Catalog::rest(&uri));
    let table = TableIdent::from_strs(["db", "t"]).expect("an identifier");
    let bad = TableIdent::from_strs(["db", "bad"]).expect("an identifier");
    let report = serde_json::json!({ "report-type": "commit-report" });
    let container = Arc::new(MetricsContainer::new());
    let (exhausted, rejected, skipped) = {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("Commit"));
        beam::internals::block_on_io(async {
            let exhausted = enabled.report_metrics(&table, &report).await;
            let rejected = enabled.report_metrics(&bad, &report).await;
            let skipped = disabled.report_metrics(&table, &report).await;
            (exhausted, rejected, skipped)
        })
        .expect("the I/O runtime runs")
    };

    assert!(!exhausted);
    assert!(!rejected);
    assert!(skipped);
    assert_eq!(transient_calls.load(Ordering::SeqCst), 5);
    assert_eq!(bad_calls.load(Ordering::SeqCst), 1);

    let metrics = MetricResults::from_container(&container);
    assert!(
        metrics
            .counter_for_transform("Commit", "iceberg", RETRY_WAIT_COUNTER_NAME)
            .unwrap_or(0)
            > 0
    );
    assert_eq!(
        metrics.counter_for_transform("Commit", "iceberg", "metrics_report_failures"),
        Some(2)
    );
}

#[test]
fn commit_fn_succeeds_and_counts_failure_when_metrics_endpoint_fails() {
    use std::collections::HashMap;

    use beam::coders::{BeamIterable, DefaultCoder, PaneInfo, WindowedHeader};
    use beam::internals::{HandlerContext, SideInputElements, SideInputError, SideInputReader};
    use beam::pipeline::Pipeline;
    use beam::schema::{Field, FieldType, Schema};
    use beam::transforms::{Create, DoFn};
    use beam::values::SideInputWindowing;
    use beam_io_iceberg::WriteOptions;
    use beam_io_iceberg::storage::FileSystemStorageFactory;
    use beam_io_iceberg::write::commit::{CommitFn, SnapshotInfo, commit};
    use beam_io_iceberg::write::files::TableWriter;
    use beam_io_iceberg::write::table::{TableInfo, prepare};
    use bytes::Bytes;
    use iceberg::CatalogBuilder;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat};
    use tempfile::TempDir;

    struct SingleSideInput {
        tag: String,
        encoded: Bytes,
    }

    impl SideInputReader for SingleSideInput {
        fn get_iterable<'a>(
            &'a self,
            tag: &str,
            _window: &[u8],
        ) -> Result<SideInputElements<'a>, SideInputError> {
            let items = if tag == self.tag {
                vec![Ok(self.encoded.clone())]
            } else {
                Vec::new()
            };
            Ok(Box::new(items.into_iter()))
        }

        fn get_multimap<'a>(
            &'a self,
            tag: &str,
            _window: &[u8],
            _key: &[u8],
        ) -> Result<SideInputElements<'a>, SideInputError> {
            Err(SideInputError::MissingReader {
                tag: tag.to_string(),
            })
        }

        fn get_multimap_keys<'a>(
            &'a self,
            tag: &str,
            _window: &[u8],
        ) -> Result<SideInputElements<'a>, SideInputError> {
            Err(SideInputError::MissingReader {
                tag: tag.to_string(),
            })
        }

        fn window_mapping(&self, tag: &str) -> Result<&SideInputWindowing, SideInputError> {
            Err(SideInputError::MissingReader {
                tag: tag.to_string(),
            })
        }
    }

    let warehouse = TempDir::new().expect("a temp dir");
    let file = DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path("file:///data/a.parquet".to_string())
        .file_format(DataFileFormat::Parquet)
        .record_count(10)
        .file_size_in_bytes(1000)
        .build()
        .expect("a data file");
    let (v1_info, metas, v1_body, v2_body, expected_snapshot_id) =
        beam::internals::block_on_io(async {
            let memory = MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(FileSystemStorageFactory))
                .load(
                    "test",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        format!("file://{}", warehouse.path().display()),
                    )]),
                )
                .await
                .expect("a memory catalog");
            let schema = Schema::new(vec![Field::new("id", FieldType::int64())]);
            let base_opts = WriteOptions::new(Catalog::rest("http://unused"), "db.t");
            let v1_info = prepare(&memory, &base_opts, &schema)
                .await
                .expect("table prepared");
            let writer = TableWriter::new(&v1_info, None).expect("a table writer");
            let metas = writer.metas(vec![file.clone()]).expect("data file metas");
            let ident = base_opts.table_ident().expect("a table ident");
            commit(&memory, &ident, vec![file])
                .await
                .expect("reference commit");
            let committed_table = memory.load_table(&ident).await.expect("committed table");
            let snapshot_id = committed_table
                .metadata()
                .current_snapshot()
                .expect("current snapshot")
                .snapshot_id();
            let v2_info = TableInfo::try_from(&committed_table).expect("v2 table info");
            let v1_meta: serde_json::Value =
                serde_json::from_slice(&v1_info.metadata).expect("v1 metadata json");
            let v2_meta: serde_json::Value =
                serde_json::from_slice(&v2_info.metadata).expect("v2 metadata json");
            let v1_body = serde_json::json!({
                "metadata-location": v1_info.metadata_location,
                "metadata": v1_meta,
            })
            .to_string();
            let v2_body = serde_json::json!({
                "metadata-location": v2_info.metadata_location,
                "metadata": v2_meta,
            })
            .to_string();
            (v1_info, metas, v1_body, v2_body, snapshot_id)
        })
        .expect("the I/O runtime runs");

    let seen_reports = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
    let seen_for_server = Arc::clone(&seen_reports);
    let metrics_calls = Arc::new(AtomicUsize::new(0));
    let calls_for_server = Arc::clone(&metrics_calls);
    let uri = serve_requests(move |method, path, body| match (method, path) {
        (_, p) if p.starts_with("/v1/config") => {
            (200, r#"{"defaults":{},"overrides":{}}"#.to_string())
        }
        ("GET", "/v1/namespaces/db/tables/t") => (200, v1_body.clone()),
        ("POST", "/v1/namespaces/db/tables/t") => (200, v2_body.clone()),
        ("POST", "/v1/namespaces/db/tables/t/metrics") => {
            let report: serde_json::Value =
                serde_json::from_slice(body).expect("commit report JSON");
            seen_for_server
                .lock()
                .expect("no test thread panics")
                .push(report);
            if calls_for_server.fetch_add(1, Ordering::SeqCst) == 0 {
                (503, r#"{"error":"unavailable"}"#.to_string())
            } else {
                (400, r#"{"error":"bad report"}"#.to_string())
            }
        }
        _ => (404, "{}".to_string()),
    });

    let rest_catalog = Catalog::rest(&uri).with_metrics_reporting();
    let opts = WriteOptions::new(rest_catalog.clone(), "db.t");
    let pipeline = Pipeline::new();
    let view = pipeline
        .apply("Info", Create::new([v1_info.clone()]))
        .as_singleton();
    let reader = SingleSideInput {
        tag: view.tag().to_string(),
        encoded: Bytes::from(v1_info.encode().expect("table info encodes")),
    };
    let mut step =
        CommitFn::new(view, CatalogClient::Rest(rest_catalog), &opts).expect("a commit fn");

    let mut raw: Vec<Vec<u8>> = Vec::new();
    let header = WindowedHeader::global(0, PaneInfo::NO_FIRING);
    let container = Arc::new(MetricsContainer::new());
    {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("Commit"));
        let mut handler_ctx = HandlerContext::new(&mut raw)
            .with_side_inputs(Some(&reader))
            .with_header(&header);
        let mut ctx = handler_ctx.as_process_context::<SnapshotInfo>();
        let input = ("db.t".to_string(), BeamIterable::from_vec(metas));
        step.process_element(input, &mut ctx)
            .expect("commit succeeds even when metrics endpoint fails");
    }

    assert_eq!(raw.len(), 1);
    let emitted = SnapshotInfo::decode(&raw[0]).expect("snapshot info decodes");
    assert_eq!(emitted.snapshot_id, expected_snapshot_id);
    assert_eq!(metrics_calls.load(Ordering::SeqCst), 2);

    let reports = seen_reports.lock().expect("no test thread panics");
    assert_eq!(reports.len(), 2);
    let report = &reports[0];
    assert_eq!(report["report-type"], "commit-report");
    assert_eq!(report["table-name"], "db.t");
    assert_eq!(report["snapshot-id"], expected_snapshot_id);
    assert_eq!(
        report["metrics"]["attempts"],
        serde_json::json!({ "unit": "count", "value": 1 })
    );
    assert_eq!(
        report["metrics"]["added-data-files"],
        serde_json::json!({ "unit": "count", "value": 1 })
    );
    assert_eq!(
        report["metrics"]["added-records"],
        serde_json::json!({ "unit": "count", "value": 10 })
    );
    assert_eq!(
        report["metrics"]["added-files-size-bytes"],
        serde_json::json!({ "unit": "bytes", "value": 1000 })
    );
    assert_eq!(
        report["metrics"]["total-data-files"],
        serde_json::json!({ "unit": "count", "value": 1 })
    );
    assert_eq!(
        report["metrics"]["total-records"],
        serde_json::json!({ "unit": "count", "value": 10 })
    );
    assert_eq!(
        report["metrics"]["total-files-size-bytes"],
        serde_json::json!({ "unit": "bytes", "value": 1000 })
    );

    let results = MetricResults::from_container(&container);
    assert_eq!(
        results.counter_for_transform("Commit", "iceberg", "snapshots_committed"),
        Some(1)
    );
    assert_eq!(
        results.counter_for_transform("Commit", "iceberg", "metrics_report_failures"),
        Some(1)
    );
    assert!(
        results
            .counter_for_transform("Commit", "iceberg", RETRY_WAIT_COUNTER_NAME)
            .unwrap_or(0)
            > 0
    );
}
