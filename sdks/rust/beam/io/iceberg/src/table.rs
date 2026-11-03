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

//! The step that loads or creates the table once, before any row, and gives the writers its
//! metadata.

use std::collections::HashMap;
use std::sync::Arc;

use beam::schema::{BeamRow, Schema};
use beam::transforms::{DoFn, ProcessContext};
use bytes::Bytes;
use iceberg::table::Table;
use iceberg::{Catalog, ErrorKind, TableCreation, TableIdent};

use crate::catalog::CatalogClient;
use crate::options::WriteOptions;
use crate::schema::{check_compatible, iceberg_schema, partition_spec};

/// The table that the writers write to: its name, the location of its metadata file, and
/// the metadata as Iceberg table metadata JSON.
#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
pub struct TableInfo {
    pub table: String,
    pub metadata_location: String,
    pub metadata: Bytes,
}

impl TryFrom<&Table> for TableInfo {
    type Error = beam::Error;

    fn try_from(table: &Table) -> beam::Result<Self> {
        let ident = table.identifier();
        Ok(Self {
            table: ident
                .namespace()
                .iter()
                .map(String::as_str)
                .chain([ident.name()])
                .collect::<Vec<_>>()
                .join("."),
            metadata_location: table.metadata_location().unwrap_or_default().to_string(),
            metadata: Bytes::from(serde_json::to_vec(table.metadata())?),
        })
    }
}

/// Loads or creates the table, checks the input schema against it, and emits the
/// [`TableInfo`]. It runs once for each write.
#[derive(Clone)]
pub struct PrepareTableFn {
    client: CatalogClient,
    options: WriteOptions,
    schema: Arc<Schema>,
}

impl PrepareTableFn {
    pub fn new(client: CatalogClient, options: WriteOptions, schema: Arc<Schema>) -> Self {
        Self {
            client,
            options,
            schema,
        }
    }
}

impl DoFn for PrepareTableFn {
    type In = Vec<u8>;
    type Out = TableInfo;

    fn process_element(
        &mut self,
        _impulse: Vec<u8>,
        ctx: &mut ProcessContext<'_, TableInfo>,
    ) -> beam::Result {
        let (client, options, schema) = (&self.client, &self.options, &self.schema);
        let info = beam::internals::block_on_io(async {
            prepare(client.catalog().await?.as_ref(), options, schema).await
        })?
        .map_err(|e| {
            e.context(format!(
                "Cannot prepare the Iceberg table '{}'",
                options.table
            ))
        })?;
        ctx.emit(info)
    }
}

/// Loads the table of `options`, or creates it, and checks that it can take rows of `schema`.
///
/// # Errors
///
/// Fails if the catalog call fails, or if the table schema does not match `schema`.
pub async fn prepare(
    catalog: &dyn Catalog,
    options: &WriteOptions,
    schema: &Schema,
) -> beam::Result<TableInfo> {
    let ident = options.table_ident()?;
    let table = match catalog.load_table(&ident).await {
        Ok(table) => table,
        Err(e)
            if matches!(
                e.kind(),
                ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound
            ) =>
        {
            create(catalog, &ident, options, schema).await?
        }
        Err(e) => return Err(e.into()),
    };
    check_compatible(schema, table.metadata().current_schema())?;
    TableInfo::try_from(&table)
}

/// Creates the namespace if needed, then the table. The table has no location, so the catalog
/// picks it. If another worker creates the table first, loads that table.
async fn create(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    options: &WriteOptions,
    schema: &Schema,
) -> beam::Result<Table> {
    let namespace = ident.namespace();
    if !catalog.namespace_exists(namespace).await? {
        match catalog.create_namespace(namespace, HashMap::new()).await {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::NamespaceAlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
    }
    let table_schema = iceberg_schema(schema)?;
    let spec = partition_spec(&options.partition_fields, &table_schema)?;
    let creation = TableCreation::builder()
        .name(ident.name().to_string())
        .schema(table_schema)
        .partition_spec(spec)
        .properties(options.table_properties.clone())
        .build();
    match catalog.create_table(namespace, creation).await {
        Ok(table) => Ok(table),
        Err(e) if e.kind() == ErrorKind::TableAlreadyExists => {
            Ok(catalog.load_table(ident).await?)
        }
        Err(e) => Err(e.into()),
    }
}
