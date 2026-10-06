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

//! BigQuery Tornadoes example.
//!
//! The pipeline workflow:
//! - Reads weather station observations from BigQuery through the Java Storage Read API
//!   (defaults to `apache-beam-testing.samples.weather_stations`).
//! - Filters for records where a tornado was recorded (`tornado == true`).
//! - Extracts the month of occurrence.
//! - Counts tornadoes per month in Rust with `CountPerElement`.
//! - Formats counts into rows matching schema `(month: i64, tornado_count: i64)`.
//! - Writes results to BigQuery through the Java BigQuery Write transform.

use beam::io::gcp::bigquery::{
    BigQueryRead, BigQueryWrite, CreateDisposition, WriteDisposition, WriteMethod,
};
use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// Default public BigQuery table containing weather station observations.
pub const DEFAULT_INPUT_TABLE: &str = "apache-beam-testing.samples.weather_stations";

/// Output record representing monthly tornado counts.
#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
pub struct TornadoCount {
    pub month: i64,
    pub tornado_count: i64,
}

/// Prepares a [`Row`] containing the month and the number of tornadoes that occurred.
pub fn format_tornado_row(month: i64, count: i64) -> Row {
    TornadoCount {
        month,
        tornado_count: count,
    }
    .to_row()
    .expect("TornadoCount must convert to Row")
}

/// Examines a weather observation row and returns the month if a tornado was recorded.
pub fn extract_tornado_month(row: &Row) -> Option<i64> {
    row.get_bool("tornado")
        .ok()
        .flatten()
        .filter(|&tornado| tornado)
        .and_then(|_| row.get_i64("month").ok().flatten())
}

/// Composite transform computing tornado counts per month from weather rows.
pub struct CountTornadoes;

impl PTransform<PCollection<Row>> for CountTornadoes {
    type Output = PCollection<Row>;

    fn expand(&self, rows: &PCollection<Row>) -> Self::Output {
        count_tornadoes(rows)
    }
}

/// Computes monthly tornado counts from weather observation rows.
pub fn count_tornadoes(rows: &PCollection<Row>) -> PCollection<Row> {
    rows.flat_map("ExtractTornadoes", |row: Row| extract_tornado_month(&row))
        .count_per_element("CountTornadoes")
        .map("FormatCounts", |(month, count): (i64, i64)| {
            format_tornado_row(month, count)
        })
        .with_row_schema(TornadoCount::beam_schema())
}

/// Parses the command-line write method into a [`WriteMethod`] enum.
pub fn parse_write_method(method: &str) -> WriteMethod {
    match method.to_lowercase().as_str() {
        "storage_write_api" | "storage_api" => WriteMethod::StorageWriteApi,
        "file_loads" | "fileloads" => WriteMethod::FileLoads,
        "at_least_once" | "storage_api_at_least_once" => WriteMethod::StorageApiAtLeastOnce,
        _ => WriteMethod::Auto,
    }
}

/// Command-line arguments for the BigQuery Tornadoes example pipeline.
#[derive(ClapArgs, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "bigquery_tornadoes",
    about = "Apache Beam Rust SDK BigQuery Tornadoes Example"
)]
pub struct Args {
    /// BigQuery table with weather data to read from, as DATASET.TABLE or PROJECT:DATASET.TABLE.
    #[arg(
        long,
        alias = "input_table",
        alias = "input-table",
        alias = "inputTable",
        default_value = DEFAULT_INPUT_TABLE
    )]
    pub input: String,

    /// Custom Google Standard SQL query to read from BigQuery instead of a table.
    #[arg(long, alias = "inputQuery")]
    pub input_query: Option<String>,

    /// BigQuery table for results specified as DATASET.TABLE or PROJECT:DATASET.TABLE.
    #[arg(
        long,
        alias = "output_table",
        alias = "output-table",
        alias = "outputTable"
    )]
    pub output: Option<String>,

    /// BigQuery write method: storage_write_api, file_loads, at_least_once, or auto.
    #[arg(long, alias = "writeMethod", default_value = "storage_write_api")]
    pub write_method: String,

    /// Optional expansion service address override (host:port).
    /// Defaults to the automated Java expansion service if omitted.
    #[arg(long, alias = "expansionService")]
    pub expansion_service: Option<String>,
}

impl PipelineOptionGroup for Args {}

/// Builds the BigQuery Tornadoes pipeline according to the provided [`Args`].
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);

    // Read weather rows from BigQuery through the Java Storage Read API.
    let mut read_transform = match &args.input_query {
        Some(query) => BigQueryRead::new("BigQueryRead").with_query(query),
        None => BigQueryRead::new("BigQueryRead")
            .with_table(&args.input)
            .with_selected_fields(["month", "tornado"]),
    };
    if let Some(service) = &args.expansion_service {
        read_transform = read_transform.with_expansion_service(service);
    }

    let rows = p.apply(read_transform);

    // Count tornadoes per month.
    let counts = rows.apply(CountTornadoes);

    // Write results to BigQuery when an output table is specified.
    let output_table = args
        .output
        .as_deref()
        .filter(|s| !s.is_empty() && *s != "none");

    if let Some(table) = output_table {
        let mut write_transform = BigQueryWrite::new("BigQueryWrite", table)
            .with_create_disposition(CreateDisposition::CreateIfNeeded)
            .with_write_disposition(WriteDisposition::WriteAppend)
            .with_method(parse_write_method(&args.write_method));

        if let Some(service) = &args.expansion_service {
            write_transform = write_transform.with_expansion_service(service);
        }

        counts.apply(write_transform);
    }

    p
}
