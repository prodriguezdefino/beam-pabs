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

//! Word counts stored in an Apache Iceberg table through Managed I/O.
//!
//! `ManagedWrite::new(ICEBERG)` and `ManagedRead::new(ICEBERG)` expand through the Java
//! `beam:transform:managed:v1` transform.
//!
//! - `--mode=write` (the default) counts the words of `--input` with `wordcount::CountWords`
//!   and writes [`WordCount`] rows to `--table`. Iceberg creates the table from the row
//!   schema on the first write. The pipeline logs each snapshot that the write commits.
//! - `--mode=read` reads the table and writes `word: count` lines to `--output`.
//!
//! `--catalog_type` and `--warehouse` set the Iceberg catalog properties `type` and
//! `warehouse`. Each `--catalog_property=key=value` adds or replaces one catalog property,
//! for example the `uri` of a REST catalog. The Java transforms must have access to the warehouse.

use std::collections::BTreeMap;

use beam::external::ExpansionError;
use beam::io::managed::{self, ManagedRead, ManagedWrite};
use beam::prelude::*;
use clap::{Args as ClapArgs, ValueEnum};
use serde::{Deserialize, Serialize};
use wordcount::CountWords;

/// The direction in which the pipeline moves the counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Count the words of `--input` and write the counts to Iceberg.
    Write,
    /// Read the counts from Iceberg and write them as text to `--output`.
    Read,
}

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(
    name = "iceberg_wordcount",
    about = "Apache Beam Managed Iceberg WordCount example"
)]
pub struct Args {
    /// Write the word counts to Iceberg, or read them back.
    #[arg(long, value_enum, default_value = "write")]
    pub mode: Mode,

    /// Text file to count the words of, with `--mode=write`.
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/shakespeare/kinglear.txt"
    )]
    pub input: String,

    /// Prefix of the text files to write the `word: count` lines to. Required with `--mode=read`.
    #[arg(long, required_if_eq("mode", "read"))]
    pub output: Option<String>,

    /// Warehouse location of the catalog, for example `gs://my-bucket/warehouse`.
    #[arg(long)]
    pub warehouse: String,

    /// Iceberg table identifier, `<namespace>.<table>`.
    #[arg(long, default_value = "rust_sdk.wordcount")]
    pub table: String,

    /// Name of the catalog in this pipeline.
    #[arg(long, default_value = "rust_sdk")]
    pub catalog_name: String,

    /// Iceberg catalog type (`hadoop`, `rest`, `hive`, ...).
    #[arg(long, default_value = "hadoop")]
    pub catalog_type: String,

    /// One more catalog property, as `key=value`. Repeat the flag for more properties.
    #[arg(long = "catalog_property", value_parser = parse_key_value)]
    pub catalog_properties: Vec<(String, String)>,

    /// Address (host:port) of an expansion service to use instead of the Java one that the SDK starts.
    #[arg(long)]
    pub expansion_service: Option<String>,
}

impl PipelineOptionGroup for Args {}

fn parse_key_value(s: &str) -> Result<(String, String), String> {
    s.split_once('=')
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .ok_or_else(|| format!("expected key=value, got '{s}'"))
}

/// One row of the Iceberg table.
#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
pub struct WordCount {
    pub word: String,
    pub count: i64,
}

/// The Managed configuration of the Iceberg read and write. The keys are the names on the
/// Managed I/O page.
#[derive(Debug, Serialize)]
pub struct IcebergConfig<'a> {
    pub table: &'a str,
    pub catalog_name: &'a str,
    pub catalog_properties: BTreeMap<&'a str, &'a str>,
}

impl<'a> From<&'a Args> for IcebergConfig<'a> {
    /// A `--catalog_property` replaces the value from `--catalog_type` or `--warehouse`.
    fn from(args: &'a Args) -> Self {
        let catalog_properties = [
            ("type", args.catalog_type.as_str()),
            ("warehouse", args.warehouse.as_str()),
        ]
        .into_iter()
        .chain(
            args.catalog_properties
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        )
        .collect();
        Self {
            table: &args.table,
            catalog_name: &args.catalog_name,
            catalog_properties,
        }
    }
}

/// Counts the words of text lines, as [`WordCount`] rows.
pub struct CountWordsToRows;

impl PTransform<PCollection<String>> for CountWordsToRows {
    type Output = PCollection<Row>;

    fn expand(&self, lines: &PCollection<String>) -> Self::Output {
        lines
            .apply("CountWords", CountWords)
            .par_do_fn("ToRows", |(word, count): (String, i64), out| {
                out.emit(WordCount { word, count }.to_row()?)
            })
            .with_row_schema(WordCount::beam_schema())
    }
}

/// Builds the pipeline for the [`Mode`] of `args`, run with `options`.
///
/// # Errors
///
/// Returns an error if the expansion fails, or if `--mode=read` has no `--output`.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Result<Pipeline, ExpansionError> {
    let p = Pipeline::create(options);
    match (args.mode, args.output.as_deref()) {
        (Mode::Write, _) => build_write(&p, args)?,
        (Mode::Read, Some(output)) => build_read(&p, args, output),
        (Mode::Read, None) => {
            return Err(ExpansionError::InvalidConfig(
                "--output is required with --mode=read".to_string(),
            ));
        }
    }
    Ok(p)
}

fn build_write(p: &Pipeline, args: &Args) -> Result<(), ExpansionError> {
    // The Iceberg write accepts `error_handling` only in its `merge-on-read` mode.
    let write = ManagedWrite::new(managed::ICEBERG).with_config(IcebergConfig::from(args));
    let write = match &args.expansion_service {
        Some(service) => write.with_expansion_service(service),
        None => write,
    };

    let written = p
        .apply("ReadLines", textio::Read::new(&args.input))
        .apply("CountWordsToRows", CountWordsToRows)
        .apply("WriteCounts", write.with_outputs());

    written
        .try_get(managed::SNAPSHOTS)?
        .inspect("LogSnapshots", |snapshot: &Row| {
            tracing::info!("committed snapshot {snapshot:?}");
        });
    Ok(())
}

fn build_read(p: &Pipeline, args: &Args, output: &str) {
    let read = ManagedRead::new(managed::ICEBERG).with_config(IcebergConfig::from(args));
    let read = match &args.expansion_service {
        Some(service) => read.with_expansion_service(service),
        None => read,
    };
    p.apply("ReadCounts", read)
        .par_do_fn("FormatCounts", |row: Row, out| {
            let WordCount { word, count } = WordCount::from_row(&row)?;
            out.emit(format!("{word}: {count}"))
        })
        .apply("WriteLines", textio::Write::new(output));
}
