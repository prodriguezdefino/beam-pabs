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

//! The step that creates or checks the table once, before any row, and gives the writers its
//! column types.

use std::io;
use std::sync::Arc;

use beam::schema::{BeamRow, Schema};
use beam::transforms::{DoFn, ProcessContext};
use bytes::Bytes;
use google_cloud_bigquery_v2::client::TableService;
use google_cloud_bigquery_v2::model::{
    Clustering, EncryptionConfiguration, Table, TableReference, TableSchema,
};

use super::WriteOptions;
use super::client::table_client;
use super::encode::schema_bytes;
use crate::bigquery::storage::schema::{table_arrow_schema, table_fields};
use crate::bigquery::{CreateDisposition, WriteDisposition};
use crate::runtime::block_on_async;

/// The table that the writers write to: its resource name, and the Arrow schema that a read
/// session gives for its columns, in IPC format.
#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
pub struct Destination {
    pub table: String,
    pub arrow_schema: Bytes,
}

/// Creates the table if needed, checks the write disposition, and emits the [`Destination`].
/// It runs once for each write, so a table that the write fills is not checked again.
#[derive(Clone)]
pub(super) struct PrepareTableFn {
    /// `projects/{project}/datasets/{dataset}/tables/{table}`.
    pub(super) table: String,
    pub(super) schema: Arc<Schema>,
    pub(super) options: WriteOptions,
    pub(super) tables: Option<TableService>,
}

impl DoFn for PrepareTableFn {
    type In = Vec<u8>;
    type Out = Destination;

    fn process_element(
        &mut self,
        _impulse: Vec<u8>,
        ctx: &mut ProcessContext<'_, Destination>,
    ) -> beam::Result {
        let (table, schema, options, tables) = (
            self.table.clone(),
            Arc::clone(&self.schema),
            self.options.clone(),
            self.tables.clone(),
        );
        let destination = block_on_async(async move {
            let tables = table_client(tables).await?;
            prepare(&tables, &table, &schema, &options).await
        })
        .map_err(|e| {
            beam::Error::from(e).context(format!(
                "Cannot prepare the BigQuery table '{}'",
                self.table
            ))
        })?;
        ctx.emit(destination)
    }
}

async fn prepare(
    tables: &TableService,
    resource: &str,
    schema: &Schema,
    options: &WriteOptions,
) -> io::Result<Destination> {
    let reference = table_reference(resource)?;
    let table = match get(tables, &reference).await {
        Ok(table) => table,
        Err(e) if e.http_status_code() == Some(404) => match options.create_disposition {
            CreateDisposition::CreateIfNeeded => {
                create(tables, &reference, schema, options).await?
            }
            CreateDisposition::CreateNever => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "The table does not exist and the create disposition is CREATE_NEVER",
                ));
            }
        },
        Err(e) => return Err(io::Error::other(e)),
    };
    let rows = table.num_rows.unwrap_or_default();
    match options.write_disposition {
        WriteDisposition::WriteEmpty if rows > 0 => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("The table has {rows} rows and the write disposition is WRITE_EMPTY"),
            ));
        }
        WriteDisposition::WriteEmpty
        | WriteDisposition::WriteAppend
        | WriteDisposition::WriteTruncate => {}
    }
    let fields = table.schema.map(|schema| schema.fields).unwrap_or_default();
    let arrow_schema = table_arrow_schema(&fields)
        .and_then(|schema| schema_bytes(&schema))
        .map(Bytes::from)
        .map_err(|e| io::Error::other(e.to_string()))?;
    Ok(Destination {
        table: resource.to_string(),
        arrow_schema,
    })
}

async fn get(
    tables: &TableService,
    reference: &TableReference,
) -> google_cloud_bigquery_v2::Result<Table> {
    tables
        .get_table()
        .set_project_id(&reference.project_id)
        .set_dataset_id(&reference.dataset_id)
        .set_table_id(&reference.table_id)
        .send()
        .await
}

/// Creates the table. If another job creates it first, returns that table.
async fn create(
    tables: &TableService,
    reference: &TableReference,
    schema: &Schema,
    options: &WriteOptions,
) -> io::Result<Table> {
    let fields = table_fields(schema).map_err(|e| io::Error::other(e.to_string()))?;
    let mut table = Table::new()
        .set_table_reference(reference.clone())
        .set_schema(TableSchema::new().set_fields(fields));
    if !options.clustering_fields.is_empty() {
        table = table.set_clustering(
            Clustering::new().set_fields(options.clustering_fields.iter().map(String::as_str)),
        );
    }
    if let Some(key) = &options.kms_key {
        table = table
            .set_encryption_configuration(EncryptionConfiguration::new().set_kms_key_name(key));
    }
    let created = tables
        .insert_table()
        .set_project_id(&reference.project_id)
        .set_dataset_id(&reference.dataset_id)
        .set_table(table)
        .send()
        .await;
    match created {
        Err(e) if e.http_status_code() == Some(409) => {
            get(tables, reference).await.map_err(io::Error::other)
        }
        created => created.map_err(io::Error::other),
    }
}

/// Parses `projects/{project}/datasets/{dataset}/tables/{table}`.
fn table_reference(resource: &str) -> io::Result<TableReference> {
    match resource.split('/').collect::<Vec<_>>().as_slice() {
        ["projects", project, "datasets", dataset, "tables", table] => Ok(TableReference::new()
            .set_project_id(*project)
            .set_dataset_id(*dataset)
            .set_table_id(*table)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Invalid table resource '{resource}'"),
        )),
    }
}
