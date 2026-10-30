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

//! The Storage Read client, the read session that divides a table into streams, and the
//! DoFns around it.

use std::io;

use beam::transforms::{DoFn, ProcessContext};
use beam::values::{AnySideInput, IterView};
use google_cloud_bigquery::client::Read;
use google_cloud_bigquery::model::arrow_serialization_options::CompressionCodec;
use google_cloud_bigquery::model::read_session::TableModifiers;
use google_cloud_bigquery::model::read_session::TableReadOptions;
use google_cloud_bigquery::model::{ArrowSerializationOptions, DataFormat, ReadSession};
use tokio::sync::OnceCell;

use super::query::{QueryClients, TempTable, delete_temp_table, run_query};
use super::restriction::StreamRestriction;
use super::{ArrowCompression, ReadOptions};
use crate::bigquery::table::TableRef;
use crate::runtime::block_on_async;

static READ_CLIENT: OnceCell<Read> = OnceCell::const_new();
static SPLIT_CLIENT: OnceCell<Read> = OnceCell::const_new();

/// The process-wide Storage Read client. Call it on the client runtime only.
pub(super) async fn read_client() -> io::Result<Read> {
    READ_CLIENT
        .get_or_try_init(|| async { Read::builder().build().await.map_err(io::Error::other) })
        .await
        .cloned()
}

/// The client for stream splits, on its own connection. A split holds the reader of the stream,
/// so the read connection stops taking data, and a split call on it would wait for the timeout.
pub(super) async fn split_client() -> io::Result<Read> {
    SPLIT_CLIENT
        .get_or_try_init(|| async { Read::builder().build().await.map_err(io::Error::other) })
        .await
        .cloned()
}

/// Creates a read session for the table `table_spec`. The column, filter and session settings
/// come from `options`.
pub(super) async fn create_session(
    table_spec: &str,
    options: &ReadOptions,
    client: Option<Read>,
) -> io::Result<ReadSession> {
    let table = TableRef::parse(table_spec)?;
    let billing = options.billing_project.as_deref();
    let resource = table.resource_name(billing)?;
    let parent = billing.or(table.project.as_deref()).unwrap_or_default();
    let mut read_options =
        TableReadOptions::new().set_selected_fields(options.selected_fields.clone());
    if let Some(filter) = &options.row_restriction {
        read_options = read_options.set_row_restriction(filter.clone());
    }
    let codec = match options.compression {
        ArrowCompression::None => None,
        ArrowCompression::Lz4Frame => Some(CompressionCodec::Lz4Frame),
        ArrowCompression::Zstd => Some(CompressionCodec::Zstd),
    };
    if let Some(codec) = codec {
        read_options = read_options.set_arrow_serialization_options(
            ArrowSerializationOptions::new().set_buffer_compression(codec),
        );
    }
    let modifiers = options
        .snapshot_timestamp()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
        .map(|time| TableModifiers::new().set_snapshot_time(time));
    let client = match client {
        Some(client) => client,
        None => read_client().await?,
    };
    client
        .create_read_session()
        .set_parent(format!("projects/{parent}"))
        .set_read_session(
            ReadSession::new()
                .set_table(resource)
                .set_data_format(DataFormat::Arrow)
                .set_read_options(read_options)
                .set_or_clear_table_modifiers(modifiers),
        )
        .set_max_stream_count(options.max_stream_count)
        .set_preferred_min_stream_count(options.preferred_min_stream_count)
        .send()
        .await
        .map_err(io::Error::other)
}

pub(super) fn session_schema(session: &ReadSession) -> beam::Result<Vec<u8>> {
    session
        .arrow_schema()
        .map(|schema| schema.serialized_schema.to_vec())
        .ok_or_else(|| beam::Error::msg("The BigQuery read session has no Arrow schema"))
}

/// Creates the read session and emits the restriction of each stream with the serialized
/// Arrow schema.
fn emit_streams(
    table_spec: &str,
    options: &ReadOptions,
    client: Option<Read>,
    ctx: &mut ProcessContext<'_, (StreamRestriction, Vec<u8>)>,
) -> beam::Result {
    let (spec, options) = (table_spec.to_string(), options.clone());
    let session = block_on_async(async move { create_session(&spec, &options, client).await })
        .map_err(|e| {
            beam::Error::from(e).context(format!(
                "Failed to create a BigQuery read session for '{table_spec}'"
            ))
        })?;
    if session.streams.is_empty() {
        return Ok(());
    }
    let schema = session_schema(&session)?;
    let streams = i64::try_from(session.streams.len())
        .unwrap_or(i64::MAX)
        .max(1);
    let rows = session.estimated_row_count.max(0);
    let estimated_rows = ((rows + streams - 1) / streams).max(1);
    let row_bytes = session.estimated_total_bytes_scanned.max(0) / rows.max(1);
    session.streams.iter().try_for_each(|stream| {
        ctx.emit((
            StreamRestriction::new(stream.name.clone(), estimated_rows, row_bytes),
            schema.clone(),
        ))
    })
}

/// Emits the streams of a table read.
#[derive(Clone)]
pub(super) struct CreateSessionFn {
    pub(super) table_spec: String,
    pub(super) options: ReadOptions,
    pub(super) client: Option<Read>,
}

impl DoFn for CreateSessionFn {
    type In = Vec<u8>;
    type Out = (StreamRestriction, Vec<u8>);

    fn process_element(
        &mut self,
        _impulse: Vec<u8>,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result {
        emit_streams(&self.table_spec, &self.options, self.client.clone(), ctx)
    }
}

/// Runs the query of a query read and emits its temporary table.
#[derive(Clone)]
pub(super) struct RunQueryFn {
    pub(super) query: String,
    pub(super) options: ReadOptions,
    pub(super) clients: Option<QueryClients>,
}

impl DoFn for RunQueryFn {
    type In = Vec<u8>;
    type Out = TempTable;

    fn process_element(
        &mut self,
        _impulse: Vec<u8>,
        ctx: &mut ProcessContext<'_, TempTable>,
    ) -> beam::Result {
        let temp = run_query(&self.options, &self.query, self.clients.clone())
            .map_err(|e| beam::Error::from(e).context("Failed to run the BigQuery query"))?;
        ctx.emit(temp)
    }
}

/// Emits the streams of the temporary table of a query read. A failed session keeps the
/// table, so a retry of the bundle can read it.
#[derive(Clone)]
pub(super) struct CreateQuerySessionFn {
    pub(super) options: ReadOptions,
    pub(super) client: Option<Read>,
}

impl DoFn for CreateQuerySessionFn {
    type In = TempTable;
    type Out = (StreamRestriction, Vec<u8>);

    fn process_element(
        &mut self,
        temp: TempTable,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result {
        emit_streams(&temp.table_spec(), &self.options, self.client.clone(), ctx)
    }
}

/// Deletes the temporary table of a query read. The side input `done` is ready only when
/// every stream is read, so the deletion waits for the readers.
#[derive(Clone)]
pub(super) struct DeleteTempTableFn {
    pub(super) done: IterView<i32>,
    pub(super) clients: Option<QueryClients>,
}

impl DoFn for DeleteTempTableFn {
    type In = TempTable;
    type Out = i32;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.done]
    }

    fn process_element(
        &mut self,
        temp: TempTable,
        ctx: &mut ProcessContext<'_, i32>,
    ) -> beam::Result {
        ctx.side_input(&self.done)?.to_vec()?;
        if let Err(e) = delete_temp_table(&temp, self.clients.clone()) {
            tracing::warn!(
                "Cannot delete the temporary table '{}': {e}",
                temp.table_spec()
            );
        }
        Ok(())
    }
}
