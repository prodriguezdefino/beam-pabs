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

//! Counts tornadoes per month in BigQuery weather data.
//!
//! The pipeline reads weather station rows with the BigQuery Storage Read connector,
//! counts the rows with a tornado for each month, and writes `(month, tornado_count)` rows with
//! the Storage Write connector.

use beam::io::gcp::bigquery::read::{ReadOptions, ReadRows};
use beam::io::gcp::bigquery::write::{
    CreateDisposition, WriteDisposition, WriteOptions, WriteRows,
};
use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(
    name = "bigquery_tornadoes",
    about = "Apache Beam BigQuery Tornadoes example"
)]
pub struct Args {
    /// Weather table to read, as PROJECT:DATASET.TABLE or DATASET.TABLE.
    #[arg(long, default_value = "apache-beam-testing.samples.weather_stations")]
    pub input: String,

    /// Google Standard SQL query to read instead of `--input`.
    #[arg(long)]
    pub input_query: Option<String>,

    /// Table to write the counts to, as PROJECT:DATASET.TABLE or DATASET.TABLE.
    #[arg(long)]
    pub output: String,
}

impl PipelineOptionGroup for Args {}

/// Number of tornadoes in one month.
#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
pub struct TornadoCount {
    pub month: i64,
    pub tornado_count: i64,
}

/// Month of a weather row that recorded a tornado, or `None` for the other rows.
fn tornado_month(row: &Row) -> Option<i64> {
    row.get_bool("tornado")
        .ok()
        .flatten()
        .filter(|&tornado| tornado)
        .and_then(|_| row.get_i64("month").ok().flatten())
}

/// Counts the weather rows with a tornado for each month, as [`TornadoCount`] rows.
pub struct CountTornadoes;

impl PTransform<PCollection<Row>> for CountTornadoes {
    type Output = PCollection<Row>;

    fn expand(self, rows: &PCollection<Row>) -> Self::Output {
        rows.flat_map("ExtractTornadoMonths", |row: Row| tornado_month(&row))
            .count_per_element("CountPerMonth")
            .par_do_fn("FormatCounts", |(month, tornado_count): (i64, i64), out| {
                out.emit(
                    TornadoCount {
                        month,
                        tornado_count,
                    }
                    .to_row()?,
                )
            })
            .with_row_schema(&TornadoCount::beam_schema())
    }
}

/// Builds the pipeline from [`Args`], run with `options`.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);

    let read = match &args.input_query {
        Some(query) => ReadOptions::from_query(query),
        None => ReadOptions::new(&args.input).with_selected_fields(["month", "tornado"]),
    };
    let write = WriteOptions::new(&args.output)
        .with_create_disposition(CreateDisposition::CreateIfNeeded)
        .with_write_disposition(WriteDisposition::WriteAppend);

    p.apply("ReadWeather", ReadRows::new(read))
        .apply("CountTornadoes", CountTornadoes)
        .apply("WriteCounts", WriteRows::new(write));
    p
}
