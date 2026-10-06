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

//! Managed I/O Iceberg WordCount example.
//!
//! Round-trips word counts through an Apache Iceberg table with
//! `ManagedWrite::new("Managed Write(ICEBERG)", ICEBERG)` and
//! `ManagedRead::new("Managed Read(ICEBERG)", ICEBERG)` expand through Java's
//! `beam:transform:managed:v1`.
//!
//! - `--mode=write` (default): reads a text file, counts words in Rust, and writes
//!   `(word STRING, count INT64)` rows to `--table`. Iceberg creates the table from the row
//!   schema on first write. Every snapshot the write commits comes back on its `snapshots`
//!   output; the pipeline logs it and counts it as `snapshots_committed`.
//! - `--mode=read`: reads the table back and writes `word: count` lines to `--output`.
//!
//! The catalog is configured entirely from arguments: `--catalog_type` and `--warehouse`
//! set Iceberg's `type` and `warehouse` properties (default: a Hadoop catalog), and each
//! repeated `--catalog_property=key=value` adds or overrides one catalog property, which is
//! how REST catalogs, auth managers, `FileIO` implementations or headers are selected. The
//! warehouse must be reachable from wherever the Java transforms run.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use beam::external::ExpansionError;
use beam::io::managed::{self, ManagedRead, ManagedWrite};
use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue};
use clap::{Args as ClapArgs, ValueEnum};
use regex::Regex;
use serde::{Deserialize, Serialize};

/// Which direction the pipeline moves data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Count words from `--input` and write them to the Iceberg table.
    Write,
    /// Read counts from the Iceberg table and write them as text to `--output`.
    Read,
}

/// Command-line arguments for the Iceberg WordCount example.
#[derive(ClapArgs, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "iceberg_wordcount",
    about = "Apache Beam Rust SDK Managed I/O Iceberg WordCount example"
)]
pub struct Args {
    /// Whether to write word counts to Iceberg or read them back.
    #[arg(long, value_enum, default_value = "write")]
    pub mode: Mode,

    /// Text file to count words in (write mode).
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/shakespeare/kinglear.txt"
    )]
    pub input: String,

    /// Text file prefix to write `word: count` lines to (read mode).
    #[arg(long)]
    pub output: Option<String>,

    /// Warehouse location of the catalog, e.g. `gs://my-bucket/warehouse`.
    #[arg(long)]
    pub warehouse: String,

    /// Iceberg table identifier, `<namespace>.<table>`.
    #[arg(long, default_value = "rust_sdk.wordcount")]
    pub table: String,

    /// Catalog name, scoped to this pipeline.
    #[arg(long, alias = "catalogName", default_value = "rust_sdk")]
    pub catalog_name: String,

    /// Iceberg catalog type (`hadoop`, `rest`, `hive`...).
    #[arg(long, alias = "catalogType", default_value = "hadoop")]
    pub catalog_type: String,

    /// Extra catalog properties as `key=value`; may be repeated.
    #[arg(long = "catalog_property", value_parser = parse_key_value)]
    pub catalog_properties: Vec<(String, String)>,

    /// Optional expansion service address override (host:port).
    /// Defaults to the automated Java I/O expansion service if omitted.
    #[arg(long, alias = "expansionService")]
    pub expansion_service: Option<String>,
}

impl PipelineOptionGroup for Args {}

fn parse_key_value(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| format!("expected key=value, got '{s}'"))
}

static COUNT_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("word", FieldType::string()),
        Field::new("count", FieldType::int64()),
    ]))
});

/// Schema of the rows stored in the table: `(word STRING, count INT64)`.
pub fn count_schema() -> Arc<Schema> {
    Arc::clone(&COUNT_SCHEMA)
}

/// Builds a [`count_schema`] row.
pub fn count_row(word: &str, count: i64) -> Row {
    Row::builder(count_schema())
        .with_named("word", Some(word))
        .with_named("count", Some(count))
        .build()
        .expect("row matches count_schema")
}

/// Formats a table row as `word: count`, or `None` if it lacks either field.
pub fn format_count(row: &Row) -> Option<String> {
    let word = row.get_string("word").ok()??;
    let count = row.get_i64("count").ok()??;
    Some(format!("{word}: {count}"))
}

/// Splits a line into words. The separator is `[^\p{L}]+`: any run of non-letter characters.
pub fn extract_words(line: &str) -> impl Iterator<Item = String> + '_ {
    static SEPARATOR: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"[^\p{L}]+").expect("valid tokenizer pattern"));
    SEPARATOR
        .split(line.trim())
        .filter(|w| !w.is_empty())
        .map(str::to_string)
}

/// The catalog properties passed to Iceberg: `type` and `warehouse` from their flags, then
/// any `--catalog_property` entries, which take precedence.
pub fn catalog_properties(args: &Args) -> BTreeMap<String, String> {
    [
        ("type".to_string(), args.catalog_type.clone()),
        ("warehouse".to_string(), args.warehouse.clone()),
    ]
    .into_iter()
    .chain(args.catalog_properties.iter().cloned())
    .collect()
}

/// Configuration shared by the Iceberg read and write.
macro_rules! iceberg_config {
    ($builder:expr, $args:expr) => {{
        let builder = $builder
            .with_config_entry("table", &$args.table)
            .with_config_entry("catalog_name", &$args.catalog_name)
            .with_config_entry("catalog_properties", catalog_properties($args));
        match &$args.expansion_service {
            Some(service) => builder.with_expansion_service(service),
            None => builder,
        }
    }};
}

/// Namespace of the pipeline's user counters.
pub const METRICS_NAMESPACE: &str = "iceberg_wordcount";

/// Describes one row of the Iceberg write's `snapshots` output, e.g.
/// `rust_sdk.wordcount: append snapshot 123 (+4555 records)`.
pub fn describe_snapshot(row: &Row) -> Option<String> {
    let table = row.get_string("table").ok()??;
    let operation = row
        .get_string("operation")
        .ok()
        .flatten()
        .unwrap_or("commit");
    let snapshot_id = row.get_i64("snapshot_id").ok()??;
    let added = match row.get_value("summary") {
        Some(Some(FieldValue::Map(entries))) => entries.iter().find_map(|(k, v)| match (k, v) {
            (FieldValue::String(k), Some(FieldValue::String(v))) if k == "added-records" => {
                Some(v.clone())
            }
            _ => None,
        }),
        _ => None,
    };
    Some(match added {
        Some(n) => format!("{table}: {operation} snapshot {snapshot_id} (+{n} records)"),
        None => format!("{table}: {operation} snapshot {snapshot_id}"),
    })
}

/// Builds the pipeline for the selected [`Mode`].
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Result<Pipeline, ExpansionError> {
    let p = Pipeline::create(options);
    match args.mode {
        Mode::Write => build_write(&p, args)?,
        Mode::Read => build_read(&p, args),
    }
    Ok(p)
}

fn build_write(p: &Pipeline, args: &Args) -> Result<(), ExpansionError> {
    let written = p
        .apply(textio::Read::new("ReadLines", args.input.as_str()))
        .par_do_fn("ExtractWords", |line: String, out| {
            extract_words(&line).try_for_each(|word| out.emit(word))
        })
        .count_per_element("CountWords")
        .map("ToRows", |(word, count): (String, i64)| {
            count_row(&word, count)
        })
        .with_row_schema(&count_schema())
        .apply(
            iceberg_config!(
                ManagedWrite::new("Managed Write(ICEBERG)", managed::ICEBERG),
                args
            )
            .with_outputs(),
        );

    // Each commit comes back as one row of the `snapshots` output.
    written
        .expect(managed::SNAPSHOTS)?
        .inspect("LogSnapshots", |row: &Row| {
            Metrics::counter(METRICS_NAMESPACE, "snapshots_committed").inc();
            match describe_snapshot(row) {
                Some(snapshot) => tracing::info!("committed {snapshot}"),
                None => tracing::info!("committed snapshot {row:?}"),
            }
        });
    Ok(())
}

fn build_read(p: &Pipeline, args: &Args) {
    let output = args
        .output
        .as_deref()
        .expect("--output is required with --mode=read");
    p.apply(iceberg_config!(
        ManagedRead::new("Managed Read(ICEBERG)", managed::ICEBERG),
        args
    ))
    .flat_map("FormatCounts", |row: Row| format_count(&row))
    .apply(textio::Write::new("WriteLines", output));
}
