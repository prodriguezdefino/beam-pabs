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

//! NYC Taxi & Rideshare trip statistics from Parquet files.
//!
//! Concepts:
//!
//! - Splittable Parquet reads with column projection through [`parquetio::Read`]
//! - Typed records with `#[derive(BeamRow)]`
//! - One keyed `Compose` combine that the runner can lift
//! - Sharded Parquet writes with ZSTD compression through [`parquetio::Write`]

use beam::io::parquet::parquetio::{self, Compression, ZstdLevel};
use beam::prelude::*;
use chrono::{DateTime, Datelike, Utc};
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(name = "nyc_taxi", about = "Apache Beam NYC Taxi Parquet example")]
pub struct Args {
    /// Parquet file or pattern to read.
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/nyc_trip/parquet/fhvhv_tripdata_2023-02.parquet"
    )]
    pub input: String,

    /// Prefix of the Parquet files to write.
    #[arg(long)]
    pub output: String,
}

impl PipelineOptionGroup for Args {}

/// High Volume For-Hire Vehicle trip, with the columns of the TLC schema that the example reads.
#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct NycTripRecord {
    pub hvfhs_license_num: Option<String>,
    pub pickup_datetime: Option<DateTime<Utc>>,
    pub dropoff_datetime: Option<DateTime<Utc>>,
    pub trip_miles: Option<f64>,
    pub trip_time: Option<i64>,
    pub base_passenger_fare: Option<f64>,
    pub tolls: Option<f64>,
    pub bcf: Option<f64>,
    pub sales_tax: Option<f64>,
    pub congestion_surcharge: Option<f64>,
    pub airport_fee: Option<f64>,
    pub tips: Option<f64>,
    pub driver_pay: Option<f64>,
}

impl NycTripRecord {
    /// Name of the service for the TLC license number.
    pub fn service_name(&self) -> &str {
        match self.hvfhs_license_num.as_deref() {
            Some("HV0002") => "Juno",
            Some("HV0003") => "Uber",
            Some("HV0004") => "Via",
            Some("HV0005") => "Lyft",
            Some(other) => other,
            None => "Unknown",
        }
    }

    /// Abbreviated day of the week of the pickup ("Mon", "Tue", ...).
    pub fn day_of_week(&self) -> String {
        self.pickup_datetime
            .map_or_else(|| "Unknown".to_string(), |dt| dt.weekday().to_string())
    }

    /// Sum of all the fare parts that the passenger pays.
    pub fn total_passenger_fare(&self) -> f64 {
        [
            self.base_passenger_fare,
            self.tolls,
            self.bcf,
            self.sales_tax,
            self.congestion_surcharge,
            self.airport_fee,
            self.tips,
        ]
        .into_iter()
        .flatten()
        .sum()
    }

    /// Driver pay plus tips.
    pub fn total_driver_pay(&self) -> f64 {
        self.driver_pay.unwrap_or(0.0) + self.tips.unwrap_or(0.0)
    }

    /// Trip distance in miles, 0 if absent.
    pub fn miles(&self) -> f64 {
        self.trip_miles.unwrap_or(0.0)
    }

    /// Trip duration in seconds, 0 if absent.
    pub fn seconds(&self) -> i64 {
        self.trip_time.unwrap_or(0)
    }
}

/// Statistics of one service on one day of the week, written to the output Parquet files.
#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct DailyServiceSummary {
    pub service: String,
    pub day_of_week: String,
    pub total_trips: i64,
    pub total_miles: f64,
    pub total_minutes: f64,
    pub total_passenger_fare: f64,
    pub total_driver_pay: f64,
    pub avg_fare_per_trip: f64,
    pub avg_fare_per_mile: f64,
    pub avg_driver_pay_per_trip: f64,
    pub avg_speed_mph: f64,
}

/// Totals of a group of trips: trips, miles, seconds, passenger fare and driver pay.
type TripTotals = (i64, f64, i64, f64, f64);

impl From<((String, String), TripTotals)> for DailyServiceSummary {
    fn from(
        ((service, day_of_week), (trips, miles, seconds, fare, pay)): (
            (String, String),
            TripTotals,
        ),
    ) -> Self {
        let round_2 = |v: f64| (v * 100.0).round() / 100.0;
        // SummarizeTrips keeps only trips with miles > 0 and seconds > 0, so no ratio divides by zero.
        let trips_f = trips as f64;
        let minutes = seconds as f64 / 60.0;
        Self {
            service,
            day_of_week,
            total_trips: trips,
            total_miles: round_2(miles),
            total_minutes: round_2(minutes),
            total_passenger_fare: round_2(fare),
            total_driver_pay: round_2(pay),
            avg_fare_per_trip: round_2(fare / trips_f),
            avg_fare_per_mile: round_2(fare / miles),
            avg_driver_pay_per_trip: round_2(pay / trips_f),
            avg_speed_mph: (miles / (minutes / 60.0) * 10.0).round() / 10.0,
        }
    }
}

/// Sums the metrics of a group of trips in one pass. Each part sums one metric, so the
/// runner can lift the combine and shuffle only the partial totals.
fn trip_totals() -> impl CombineFn<Input = NycTripRecord, Output = TripTotals> {
    Compose::new((
        Sum::new().on(|_: &NycTripRecord| 1i64),
        Sum::new().on(NycTripRecord::miles),
        Sum::new().on(NycTripRecord::seconds),
        Sum::new().on(NycTripRecord::total_passenger_fare),
        Sum::new().on(NycTripRecord::total_driver_pay),
    ))
}

/// Computes one [`DailyServiceSummary`] for each service and day of the week.
///
/// Trips without distance or duration are dropped.
pub struct SummarizeTrips;

impl PTransform<PCollection<NycTripRecord>> for SummarizeTrips {
    type Output = PCollection<DailyServiceSummary>;

    fn expand(&self, trips: &PCollection<NycTripRecord>) -> Self::Output {
        trips
            .filter("FilterValidTrips", |t: &NycTripRecord| {
                t.miles() > 0.0 && t.seconds() > 0
            })
            .map("KeyByServiceAndDay", |t: NycTripRecord| {
                ((t.service_name().to_string(), t.day_of_week()), t)
            })
            .combine_per_key("SumTrips", trip_totals())
            .map("FormatSummary", DailyServiceSummary::from)
    }
}

/// Builds the pipeline from [`Args`], run with `options`.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);
    p.apply(
        "ReadTrips",
        parquetio::Read::<NycTripRecord>::new(&args.input),
    )
    .apply("SummarizeTrips", SummarizeTrips)
    .apply(
        "WriteSummaries",
        parquetio::Write::new(&args.output)
            .with_compression(Compression::ZSTD(ZstdLevel::default())),
    );
    p
}
