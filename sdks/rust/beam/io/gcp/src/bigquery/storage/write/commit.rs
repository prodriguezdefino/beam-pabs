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
//! `google-cloud-bigquery` commits one stream at a time and keeps its generated client private,
//! so this module calls the RPC with the client that `build.rs` generates from the vendored
//! `google.cloud.bigquery.storage.v1` protos.

use std::io;

use tokio::sync::OnceCell;
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::{Code, Request, Status};

use super::client::CALL_RETRY;
use googleapis::google::cloud::bigquery::storage::v1::{
    BatchCommitWriteStreamsRequest, BatchCommitWriteStreamsResponse,
    big_query_write_client::BigQueryWriteClient, storage_error::StorageErrorCode,
};

const ENDPOINT: &str = "https://bigquerystorage.googleapis.com";

/// The channel of the process. The calls authenticate with [`crate::auth`].
static CHANNEL: OnceCell<Channel> = OnceCell::const_new();

/// Call it on the client runtime only.
async fn channel() -> io::Result<Channel> {
    CHANNEL
        .get_or_try_init(|| async {
            Channel::from_static(ENDPOINT)
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .map(|endpoint| endpoint.connect_lazy())
                .map_err(io::Error::other)
        })
        .await
        .cloned()
}

/// Commits `streams` of `table` (`projects/{p}/datasets/{d}/tables/{t}`) in one atomic call.
/// Streams that a previous call committed count as committed. Call it on the client runtime
/// only.
pub(super) async fn commit_streams(table: &str, streams: Vec<String>) -> io::Result<()> {
    let channel = channel().await?;
    let message = BatchCommitWriteStreamsRequest {
        parent: table.to_string(),
        write_streams: streams,
    };
    let mut attempt = 0;
    let response = loop {
        match call(&channel, message.clone()).await {
            Err(status) if attempt + 1 < CALL_RETRY.attempts && is_transient(status.code()) => {
                tokio::time::sleep(CALL_RETRY.backoff(attempt)).await;
                attempt += 1;
            }
            result => break result.map_err(io::Error::other)?,
        }
    };
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
        BatchCommitWriteStreamsResponse { stream_errors, .. } => Err(io::Error::other(format!(
            "BigQuery committed no stream: {}",
            stream_errors
                .iter()
                .map(|error| format!("{}: {}", error.entity, error.error_message))
                .collect::<Vec<_>>()
                .join("; ")
        ))),
    }
}

async fn call(
    channel: &Channel,
    message: BatchCommitWriteStreamsRequest,
) -> Result<BatchCommitWriteStreamsResponse, Status> {
    let mut metadata = crate::auth::headers()
        .await
        .map_err(|e| Status::unauthenticated(e.to_string()))?
        .into_iter()
        .try_fold(MetadataMap::new(), |mut metadata, (name, value)| {
            let key = MetadataKey::from_bytes(name.as_bytes()).map_err(|e| e.to_string())?;
            let value = MetadataValue::try_from(value).map_err(|e| e.to_string())?;
            metadata.insert(key, value);
            Ok::<_, String>(metadata)
        })
        .map_err(Status::unauthenticated)?;
    let routing = MetadataValue::try_from(format!("parent={}", message.parent))
        .map_err(|e| Status::invalid_argument(e.to_string()))?;
    metadata.insert("x-goog-request-params", routing);
    let mut request = Request::new(message);
    *request.metadata_mut() = metadata;
    BigQueryWriteClient::new(channel.clone())
        .batch_commit_write_streams(request)
        .await
        .map(tonic::Response::into_inner)
}

fn is_transient(code: Code) -> bool {
    matches!(
        code,
        Code::Aborted
            | Code::Cancelled
            | Code::DeadlineExceeded
            | Code::Internal
            | Code::ResourceExhausted
            | Code::Unavailable
            | Code::Unknown
    )
}
