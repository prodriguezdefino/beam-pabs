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

//! Tests against a BigLake Iceberg REST catalog in vended-credentials mode.
//!
//! They are ignored by default. To run them, set `ICEBERG_IT_PROJECT` and
//! `ICEBERG_IT_CATALOG`, log in with Application Default Credentials, and run
//! `./gradlew :sdks:rust:test -Ppkg=apache-beam-io-iceberg -PtestArgs=--ignored`.
//! Each run creates a table in the namespace `beam_rust_it`, commits two data files to it, and
//! drops it at the end. The committed data files stay in the bucket.

use std::time::{SystemTime, UNIX_EPOCH};

use std::sync::Arc;

use arrow_io::SchemaRowCodec;
use beam::schema::{Field, FieldType, FieldValue, Row, RowElement, Schema, URN_MILLIS_INSTANT};
use bytes::Bytes;
use iceberg::spec::DataFile;
use iceberg::{Catalog as _, TableIdent};
use iceberg_io::catalog::{Connection, connect};
use iceberg_io::commit::{COMMIT_ID, Commit, commit, commit_id};
use iceberg_io::encode::Encoder;
use iceberg_io::storage::GCS_TOKEN;
use iceberg_io::table::{TableInfo, prepare};
use iceberg_io::write::TableWriter;
use iceberg_io::{Catalog, PartitionField, Transform, WriteOptions};

const BIGLAKE: &str = "https://biglake.googleapis.com/iceberg/v1/restcatalog";

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name} to run the live tests"))
}

fn device_metric() -> Schema {
    Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new(
            "timestamp_ms",
            FieldType::logical(URN_MILLIS_INSTANT, Vec::new(), FieldType::int64()),
        ),
        Field::new("device_id", FieldType::string()),
        Field::new("payload", FieldType::double()),
        Field::nullable("location", FieldType::string()),
    ])
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "needs a BigLake catalog and Application Default Credentials"]
async fn biglake_creates_a_table_and_vends_a_token_that_writes_to_its_location() {
    let (project, catalog_name) = (env("ICEBERG_IT_PROJECT"), env("ICEBERG_IT_CATALOG"));
    let catalog = Catalog::rest(BIGLAKE)
        .with_warehouse(format!("bl://projects/{project}/catalogs/{catalog_name}"))
        .with_google_auth()
        .with_header("x-goog-user-project", project.as_str())
        .with_vended_credentials();
    let connection = connect(&catalog).await.expect("a connection");
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after the epoch")
        .as_secs();
    let options = WriteOptions::new(catalog, format!("beam_rust_it.prepare_{seconds}"))
        .with_partition_fields([PartitionField::new("timestamp_ms", Transform::Hour)])
        .with_table_property("write.parquet.compression-codec", "zstd");
    let ident = options.table_ident().expect("a valid table name");

    let checked = check_table(&connection, &options, &ident).await;
    let dropped = connection.drop_table(&ident).await;
    checked.expect("the table checks pass");
    dropped.expect("the table is dropped");
}

/// Creates and loads the table, then reads and writes its files with the vended token.
async fn check_table(
    connection: &Connection,
    options: &WriteOptions,
    ident: &TableIdent,
) -> beam::Result {
    let info = prepare(connection, options, &device_metric()).await?;
    let again = prepare(connection, options, &device_metric()).await?;
    assert_eq!(info.metadata_location, again.metadata_location);

    let table = connection.load_table(ident).await?;
    let io = table.file_io();
    assert!(
        io.config().get(GCS_TOKEN).is_some(),
        "the catalog vends a GCS token"
    );
    let metadata = io.new_input(&info.metadata_location)?.read().await?;
    assert!(
        !metadata.is_empty(),
        "the vended token reads the metadata file"
    );

    let probe = format!(
        "{}/data/beam-rust-it-probe.txt",
        table.metadata().location()
    );
    io.new_output(&probe)?
        .write(Bytes::from_static(b"probe"))
        .await?;
    io.delete(&probe).await?;

    write_data_files(&info, io).await?;
    commit_files(connection, ident, &info, io).await
}

/// Writes rows of two hours to Parquet data files with the vended token, checks their sizes,
/// and deletes them. The table gets no snapshot.
async fn write_data_files(info: &TableInfo, io: &iceberg::io::FileIO) -> beam::Result {
    let files = write_rows(info, io, "beam-rust-it").await?;
    assert_eq!(files.len(), 2, "one file for each hour partition");
    assert_eq!(
        files.iter().map(|file| file.record_count()).sum::<u64>(),
        20
    );
    for file in &files {
        let size = io.new_input(file.file_path())?.metadata().await?.size;
        assert_eq!(size, file.file_size_in_bytes(), "{}", file.file_path());
        io.delete(file.file_path()).await?;
    }
    Ok(())
}

/// Commits new data files twice through the connection. The first commit makes one snapshot
/// with the commit id. The second finds that snapshot and makes none.
async fn commit_files(
    connection: &Connection,
    ident: &TableIdent,
    info: &TableInfo,
    io: &iceberg::io::FileIO,
) -> beam::Result {
    let files = write_rows(info, io, "beam-rust-it-commit").await?;
    let id = commit_id(&files).id;

    let Commit::New(snapshot) = commit(connection, ident, files.clone()).await? else {
        panic!("the first commit makes a snapshot");
    };
    let again = commit(connection, ident, files).await?;

    assert_eq!(
        snapshot.summary().additional_properties.get(COMMIT_ID),
        Some(&id)
    );
    assert!(matches!(again, Commit::Found(found) if found == snapshot));
    let table = connection.load_table(ident).await?;
    assert_eq!(table.metadata().snapshots().count(), 1);
    Ok(())
}

/// Writes 20 rows of two hours to new Parquet data files with the vended token.
async fn write_rows(
    info: &TableInfo,
    io: &iceberg::io::FileIO,
    prefix: &str,
) -> beam::Result<Vec<DataFile>> {
    let schema = Arc::new(device_metric());
    let writer = TableWriter::new(info, None)?;
    let encoder = Encoder::new(Arc::clone(&schema), writer.metadata().current_schema())?;
    let rows = (0..20)
        .map(|id| {
            Ok(RowElement::Row(Row::new(
                Arc::clone(&schema),
                vec![
                    Some(FieldValue::Int64(id)),
                    Some(FieldValue::Int64(1_700_000_000_000 + (id % 2) * 3_600_000)),
                    Some(FieldValue::String(format!("device-{id}"))),
                    Some(FieldValue::Double(0.5)),
                    Some(FieldValue::String("us-central1".to_string())),
                ],
            )?))
        })
        .collect::<beam::Result<Vec<_>>>()?;
    let encoded = encoder.encode(&SchemaRowCodec::new(Arc::clone(&schema)), &rows)?;
    let (_, ipc) = encoded
        .batch
        .ok_or_else(|| beam::Error::msg("the rows give no batch"))?;
    writer
        .write(io.clone(), prefix.to_string(), vec![writer.decode(ipc)?])
        .await
}
