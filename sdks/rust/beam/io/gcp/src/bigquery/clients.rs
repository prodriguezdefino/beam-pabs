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

//! Process-wide BigQuery client singletons shared across reads and writes.
//!
//! Initialize and call every helper on [`crate::runtime::client_runtime`] only.

use std::io;
use std::sync::Arc;

use google_cloud_bigquery::client::{Read, Write};
use google_cloud_bigquery_v2::client::{DatasetService, JobService, TableService};
use tokio::sync::OnceCell;

use super::write::commit::CommitClient;

static READ_CLIENT: OnceCell<Read> = OnceCell::const_new();
static SPLIT_CLIENT: OnceCell<Read> = OnceCell::const_new();
static WRITE_CLIENT: OnceCell<Arc<Write>> = OnceCell::const_new();
static TABLE_CLIENT: OnceCell<TableService> = OnceCell::const_new();
static JOB_CLIENT: OnceCell<JobService> = OnceCell::const_new();
static DATASET_CLIENT: OnceCell<DatasetService> = OnceCell::const_new();
static COMMIT_CLIENT: OnceCell<CommitClient> = OnceCell::const_new();

/// Process-wide Storage Read client.
pub(super) async fn read_client() -> io::Result<Read> {
    READ_CLIENT
        .get_or_try_init(|| async { Read::builder().build().await.map_err(io::Error::other) })
        .await
        .cloned()
}

/// Process-wide Storage Read client for `SplitReadStream`, on a separate connection from
/// [`read_client`] so an active stream read never blocks a split RPC.
pub(super) async fn split_client() -> io::Result<Read> {
    SPLIT_CLIENT
        .get_or_try_init(|| async { Read::builder().build().await.map_err(io::Error::other) })
        .await
        .cloned()
}

/// Returns `given`, or the process-wide Storage Write client.
pub(super) async fn write_client(given: Option<Arc<Write>>) -> io::Result<Arc<Write>> {
    match given {
        Some(client) => Ok(client),
        None => WRITE_CLIENT
            .get_or_try_init(|| async {
                Write::builder()
                    .build()
                    .await
                    .map(Arc::new)
                    .map_err(io::Error::other)
            })
            .await
            .cloned(),
    }
}

/// Returns `given`, or the process-wide BigQuery v2 `TableService` client shared by query
/// reads and table writes.
pub(super) async fn table_client(given: Option<TableService>) -> io::Result<TableService> {
    match given {
        Some(client) => Ok(client),
        None => TABLE_CLIENT
            .get_or_try_init(|| async {
                TableService::builder()
                    .build()
                    .await
                    .map_err(io::Error::other)
            })
            .await
            .cloned(),
    }
}

/// Process-wide BigQuery v2 `JobService` client.
pub(super) async fn job_client() -> io::Result<JobService> {
    JOB_CLIENT
        .get_or_try_init(|| async {
            JobService::builder()
                .build()
                .await
                .map_err(io::Error::other)
        })
        .await
        .cloned()
}

/// Process-wide BigQuery v2 `DatasetService` client.
pub(super) async fn dataset_client() -> io::Result<DatasetService> {
    DATASET_CLIENT
        .get_or_try_init(|| async {
            DatasetService::builder()
                .build()
                .await
                .map_err(io::Error::other)
        })
        .await
        .cloned()
}

/// Process-wide `BatchCommitWriteStreams` client.
pub(super) async fn commit_client() -> io::Result<&'static CommitClient> {
    COMMIT_CLIENT
        .get_or_try_init(|| CommitClient::connect(None))
        .await
}
