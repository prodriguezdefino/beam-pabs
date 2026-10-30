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

//! BigQuery benchmark in a Rust pipeline, with the native Storage Read and Storage Write
//! connectors.
//!
//! - `count` reads `--table` (or `--query`), counts the rows, and adds the total to `e2e/rows`.
//! - `copy` reads `--table`, sends each row through a Rust `Touch` step that counts
//!   `e2e/rows`, and writes the rows to `--destination`.
//!
//! All steps run in one Rust SDK harness.

use beam::io::gcp::bigquery::storage::read::{ArrowCompression, ReadOptions, ReadRows};
use beam::io::gcp::bigquery::storage::write::{self, WriteOptions, WriteRows};
use beam::options::PipelineOptionGroup;
use beam::prelude::*;
use beam_benchmark_io::{Connector, RecordRowsFn, TouchFn};
use clap::{Args as ClapArgs, ValueEnum};
use serde::{Deserialize, Serialize};

/// The shape of the benchmark.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Shape {
    /// Reads the rows and counts them.
    Count,
    /// Reads the rows, counts each one in `Touch`, and writes them to a new table.
    Copy,
}

/// How many times a written row can arrive in the table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum Semantics {
    /// Each row arrives once.
    ExactlyOnce,
    /// A retried bundle can write its rows again.
    AtLeastOnce,
}

impl From<Semantics> for write::Semantics {
    fn from(semantics: Semantics) -> Self {
        match semantics {
            Semantics::ExactlyOnce => Self::ExactlyOnce,
            Semantics::AtLeastOnce => Self::AtLeastOnce,
        }
    }
}

/// Benchmark flags. The flags of the runner and `--project` come from the pipeline options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
pub struct Args {
    /// The connector of the read.
    #[arg(long, value_enum, default_value_t = Connector::Rust)]
    pub reader: Connector,
    /// The connector of the write, `copy` only.
    #[arg(long, value_enum)]
    pub writer: Option<Connector>,
    /// The shape of the benchmark.
    #[arg(long, value_enum, default_value_t = Shape::Count)]
    pub shape: Shape,
    /// The table to read: `[project:]dataset.table` or `project.dataset.table`.
    #[arg(long, conflicts_with = "query", required_unless_present = "query")]
    pub table: Option<String>,
    /// A GoogleSQL query to read instead of `--table`, `count` only.
    #[arg(long)]
    pub query: Option<String>,
    /// The columns to read, separated by commas. All columns if not set.
    #[arg(long, value_delimiter = ',')]
    pub selected_fields: Vec<String>,
    /// A filter on the rows of `--table`.
    #[arg(long)]
    pub row_restriction: Option<String>,
    /// Arrow buffer compression of the read: `NONE`, `LZ4_FRAME` or `ZSTD`.
    #[arg(long)]
    pub compression: Option<String>,
    /// The table that `copy` creates and writes.
    #[arg(long)]
    pub destination: Option<String>,
    /// The delivery guarantee of the `copy` write.
    #[arg(long, value_enum, default_value_t = Semantics::ExactlyOnce)]
    pub semantics: Semantics,
}

impl PipelineOptionGroup for Args {}

/// Builds the [`ReadOptions`] of the table or query of `args`.
///
/// # Errors
///
/// Fails if `args` gives both or neither of `--table` and `--query`, if the compression is
/// unknown, or if the options are not valid.
pub fn read_options(args: &Args) -> Result<ReadOptions> {
    let options = match (&args.table, &args.query) {
        (Some(table), None) => ReadOptions::new(table),
        (None, Some(query)) => ReadOptions::from_query(query),
        (Some(_), Some(_)) | (None, None) => {
            return Err(Error::msg("give exactly one of --table and --query"));
        }
    };
    let options = match args.selected_fields.as_slice() {
        [] => options,
        fields => options.with_selected_fields(fields),
    };
    let options = match &args.row_restriction {
        Some(filter) => options.with_row_restriction(filter),
        None => options,
    };
    let options = match args.compression.as_deref() {
        Some(name) => {
            options.with_compression(name.parse::<ArrowCompression>().map_err(Error::msg)?)
        }
        None => options,
    };
    options.validate()?;
    Ok(options)
}

/// Builds the [`WriteOptions`] of the `copy` write to `--destination`.
///
/// # Errors
///
/// Fails if `--destination` is not set, or if the options are not valid.
pub fn write_options(args: &Args) -> Result<WriteOptions> {
    let destination = args
        .destination
        .as_deref()
        .ok_or_else(|| Error::msg("copy needs --destination"))?;
    let options = WriteOptions::new(destination).with_semantics(args.semantics.into());
    options.validate()?;
    Ok(options)
}

/// Checks that the flags of `args` fit its shape.
///
/// # Errors
///
/// Fails if `count` gets a writer or a destination, or if `copy` misses them or reads a
/// query or selected fields.
pub fn check_shape(args: &Args) -> Result {
    match args.shape {
        Shape::Count if args.writer.is_some() || args.destination.is_some() => Err(Error::msg(
            "--writer and --destination apply to copy only, not to count",
        )),
        Shape::Count => Ok(()),
        Shape::Copy if args.writer.is_none() || args.destination.is_none() => {
            Err(Error::msg("copy needs --writer and --destination"))
        }
        Shape::Copy if args.query.is_some() || !args.selected_fields.is_empty() => Err(Error::msg(
            "copy reads whole rows of --table: no --query or --selected_fields",
        )),
        Shape::Copy => Ok(()),
    }
}

/// Builds the pipeline of the shape of `args`.
///
/// # Errors
///
/// Fails if the flags do not fit the shape, or if the read gives no row schema.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Result<Pipeline> {
    check_shape(args)?;
    let p = Pipeline::create(options);
    let rows = p.apply("Read", ReadRows::new(read_options(args)?));
    match args.shape {
        Shape::Count => {
            rows.apply("CountRows", CountGlobally::new())
                .par_do("RecordRows", RecordRowsFn::new());
        }
        Shape::Copy => {
            let schema = rows
                .row_schema()
                .ok_or_else(|| Error::msg("the read gives no row schema"))?;
            // The write sends the rows that BigQuery rejects to its error output. The table
            // check after the job finds them as missing rows.
            rows.par_do("Touch", TouchFn::<Row>::new())
                // A Row-to-Row ParDo does not keep the row schema of its input.
                .with_row_schema(&schema)
                .apply("Write", WriteRows::new(write_options(args)?));
        }
    }
    Ok(p)
}
