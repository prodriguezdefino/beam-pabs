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

use beam::io::gcp::bigquery::{
    CreateDisposition, Read, ReadOptions, Write, WriteDisposition, WriteOptions,
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

/// Weather station columns read from BigQuery.
#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
pub struct WeatherStation {
    pub month: Option<i64>,
    pub tornado: Option<bool>,
}

/// Number of tornadoes in one month.
#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
pub struct TornadoCount {
    pub month: i64,
    pub tornado_count: i64,
}

/// Counts weather observations with a tornado for each month, as [`TornadoCount`] values.
pub struct CountTornadoes;

impl PTransform<PCollection<WeatherStation>> for CountTornadoes {
    type Output = PCollection<TornadoCount>;

    fn expand(self, rows: &PCollection<WeatherStation>) -> Self::Output {
        rows.flat_map("ExtractTornadoMonths", |row: WeatherStation| {
            row.tornado.filter(|&tornado| tornado).and(row.month)
        })
        .count_per_element("CountPerMonth")
        .map("FormatCounts", |(month, tornado_count): (i64, i64)| {
            TornadoCount {
                month,
                tornado_count,
            }
        })
    }
}

/// Builds the pipeline from [`Args`], run with `options`.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);

    let read = match &args.input_query {
        Some(query) => ReadOptions::from_query(query),
        None => ReadOptions::new(&args.input),
    };
    let write = WriteOptions::new(&args.output)
        .with_create_disposition(CreateDisposition::CreateIfNeeded)
        .with_write_disposition(WriteDisposition::WriteAppend);

    // `Read<WeatherStation>` selects only the fields of `WeatherStation`.
    p.apply("ReadWeather", Read::<WeatherStation>::new(read))
        .apply("CountTornadoes", CountTornadoes)
        .apply("WriteCounts", Write::<TornadoCount>::new(write));
    p
}
