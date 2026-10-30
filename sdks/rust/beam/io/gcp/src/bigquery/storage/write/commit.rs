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

//! `BatchCommitWriteStreams` for many streams in one call. The Write client of
//! `google-cloud-bigquery` commits one stream at a time, so this module calls the RPC on the
//! gRPC transport of the Google Cloud clients with the vendored protos.

use std::io;

use beam::retry::with_retries;
use google_cloud_gax::error::Error;
use google_cloud_gax::options::RequestOptions;
use google_cloud_gax_internal::grpc::Client;
use google_cloud_gax_internal::routing_parameter;
use tokio::sync::OnceCell;
use tonic::codegen::http::uri::PathAndQuery;
use tonic::{Extensions, GrpcMethod};

use super::client::{CALL_RETRY, transient_decision};
use crate::grpc::{API_CLIENT, connect};
use beam_io_gcp_googleapis::google::cloud::bigquery::storage::v1::{
    BatchCommitWriteStreamsRequest, BatchCommitWriteStreamsResponse,
    storage_error::StorageErrorCode,
};

const ENDPOINT: &str = "https://bigquerystorage.googleapis.com";
const SERVICE: &str = "google.cloud.bigquery.storage.v1.BigQueryWrite";
const METHOD: &str = "BatchCommitWriteStreams";
const PATH: &str = "/google.cloud.bigquery.storage.v1.BigQueryWrite/BatchCommitWriteStreams";

/// The commit client of the process.
static CLIENT: OnceCell<CommitClient> = OnceCell::const_new();

/// Commits `streams` of `table` with the commit client of the process. Call it on the client
/// runtime only.
pub(super) async fn commit_streams(table: &str, streams: Vec<String>) -> io::Result<()> {
    CLIENT
        .get_or_try_init(|| CommitClient::connect(None))
        .await?
        .commit_streams(table, streams)
        .await
}

/// Client for `BatchCommitWriteStreams`. Cloning shares the connection.
#[derive(Clone, Debug)]
pub struct CommitClient {
    inner: Client,
}

impl CommitClient {
    /// Connects to `endpoint`, or to the BigQuery Storage endpoint when `None`. An `http://`
    /// endpoint gets no credentials. Call it on the client runtime only.
    pub async fn connect(endpoint: Option<&str>) -> io::Result<Self> {
        let inner = connect(endpoint.unwrap_or(ENDPOINT), ENDPOINT, 1).await?;
        Ok(Self { inner })
    }

    /// Commits `streams` of `table` (`projects/{p}/datasets/{d}/tables/{t}`) in one atomic
    /// call. Streams that a previous call committed count as committed. Call it on the client
    /// runtime only.
    pub async fn commit_streams(&self, table: &str, streams: Vec<String>) -> io::Result<()> {
        let routing = routing_parameter::format(&[Some(("parent", table))]);
        let request = BatchCommitWriteStreamsRequest {
            parent: table.to_string(),
            write_streams: streams,
        };
        let response = with_retries(CALL_RETRY, transient_decision, || {
            self.call(&request, &routing)
        })
        .await
        .map_err(io::Error::other)?;
        match response {
            BatchCommitWriteStreamsResponse {
                commit_time: Some(_),
                ..
            } => Ok(()),
            BatchCommitWriteStreamsResponse { stream_errors, .. }
                if !stream_errors.is_empty()
                    && stream_errors
                        .iter()
                        .all(|error| error.code() == StorageErrorCode::StreamAlreadyCommitted) =>
            {
                Ok(())
            }
            BatchCommitWriteStreamsResponse { stream_errors, .. } => {
                Err(io::Error::other(format!(
                    "BigQuery committed no stream: {}",
                    stream_errors
                        .iter()
                        .map(|error| format!("{}: {}", error.entity, error.error_message))
                        .collect::<Vec<_>>()
                        .join("; ")
                )))
            }
        }
    }

    async fn call(
        &self,
        request: &BatchCommitWriteStreamsRequest,
        routing: &str,
    ) -> Result<BatchCommitWriteStreamsResponse, Error> {
        let mut extensions = Extensions::new();
        extensions.insert(GrpcMethod::new(SERVICE, METHOD));
        // The transport takes the message of each attempt, so each retry sends a copy.
        let message = request.clone();
        self.inner
            .execute(
                extensions,
                PathAndQuery::from_static(PATH),
                message,
                RequestOptions::default(),
                &API_CLIENT,
                routing,
            )
            .await
            .map(tonic::Response::into_inner)
    }
}
