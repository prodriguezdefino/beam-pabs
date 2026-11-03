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

//! A memory catalog with a table, data files that need no file, and a catalog that fails its
//! commits.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType, Schema,
    Type,
};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, Error, ErrorKind, Namespace, NamespaceIdent, TableCommit,
    TableCreation, TableIdent,
};
use iceberg_io::storage::FileSystemStorageFactory;
use tempfile::TempDir;

/// A memory catalog whose metadata files go to a temp directory through the SDK storage.
pub async fn memory_catalog(warehouse: &TempDir) -> MemoryCatalog {
    MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(FileSystemStorageFactory))
        .load(
            "test",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                format!("file://{}", warehouse.path().display()),
            )]),
        )
        .await
        .expect("a memory catalog")
}

/// Creates the unpartitioned table `db.metrics`.
pub async fn table(catalog: &MemoryCatalog) -> TableIdent {
    let namespace = NamespaceIdent::new("db".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("a namespace");
    let schema = Schema::builder()
        .with_fields([NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into()])
        .build()
        .expect("a schema");
    let creation = TableCreation::builder()
        .name("metrics".to_string())
        .schema(schema)
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("a table")
        .identifier()
        .clone()
}

/// A data file at `name` with `records` records of 100 bytes. The commit does not read it.
pub fn data_file(name: &str, records: u64) -> DataFile {
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(format!("file:///data/{name}"))
        .file_format(DataFileFormat::Parquet)
        .record_count(records)
        .file_size_in_bytes(records * 100)
        .build()
        .expect("a data file")
}

/// How [`Faulty`] fails a commit.
#[derive(Clone, Copy, Debug)]
pub enum Fault {
    /// The commit happens, then the reply fails.
    LostReply,
    /// The catalog rejects the commit.
    Reject,
}

/// A memory catalog whose `update_table` fails.
#[derive(Debug)]
pub struct Faulty {
    pub inner: MemoryCatalog,
    fault: Fault,
}

impl Faulty {
    pub fn new(inner: MemoryCatalog, fault: Fault) -> Self {
        Self { inner, fault }
    }
}

#[async_trait::async_trait]
impl Catalog for Faulty {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, ident: &TableIdent) -> iceberg::Result<Table> {
        self.inner.load_table(ident).await
    }

    async fn drop_table(&self, ident: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(ident).await
    }

    async fn purge_table(&self, ident: &TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(ident).await
    }

    async fn table_exists(&self, ident: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(ident).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        ident: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner.register_table(ident, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        match self.fault {
            Fault::LostReply => {
                self.inner.update_table(commit).await?;
                Err(Error::new(ErrorKind::Unexpected, "the reply was lost"))
            }
            Fault::Reject => Err(Error::new(ErrorKind::Unexpected, "the commit was rejected")),
        }
    }
}
