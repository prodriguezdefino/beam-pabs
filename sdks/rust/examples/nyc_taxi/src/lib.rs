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

//! NYC Taxi & Rideshare Parquet Analytics Example.
//!
//! Demonstrates fluent, type-driven columnar data processing in Apache Beam:
//! - Splittable Parquet ingestion with column projection through [`parquetio::Read`].
//! - Strongly typed records derived with `#[derive(BeamRow)]`.
//! - Keyed combiners with partial aggregation and combiner lifting through `.combine_per_key()`.
//! - Sharded, distributed Parquet writing through [`parquetio::Write`].
//!
//! Input data can be read directly from Google Cloud Storage:
//! `gs://apache-beam-samples/nyc_trip/parquet/fhvhv_tripdata_2023-02.parquet`
//! or from local sample Parquet files.

use std::fs;
use std::path::Path;

use beam::io::file::FileSink;
use beam::io::parquet::parquetio::{self, ParquetSink};
use beam::prelude::*;
use chrono::{DateTime, Datelike, Utc};

/// Default public dataset path in Google Cloud Storage.
pub const DEFAULT_NYC_TRIP_PARQUET: &str =
    "gs://apache-beam-samples/nyc_trip/parquet/fhvhv_tripdata_2023-02.parquet";

/// High Volume For-Hire Vehicle (HVFHV) trip record matching NYC TLC data schema.
///
/// Only the projected fields needed for analytical metrics are decoded from Parquet.
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
    /// Maps TLC license numbers to recognizable rideshare / taxi service names.
    pub fn service_name(&self) -> String {
        match self.hvfhs_license_num.as_deref() {
            Some("HV0002") => "Juno".to_string(),
            Some("HV0003") => "Uber".to_string(),
            Some("HV0004") => "Via".to_string(),
            Some("HV0005") => "Lyft".to_string(),
            Some(other) => other.to_string(),
            None => "Unknown".to_string(),
        }
    }

    /// Extracts the abbreviated day of the week (e.g. "Mon", "Tue") from pickup time.
    pub fn day_of_week(&self) -> String {
        match self.pickup_datetime {
            Some(dt) => match dt.weekday() {
                chrono::Weekday::Mon => "Mon".to_string(),
                chrono::Weekday::Tue => "Tue".to_string(),
                chrono::Weekday::Wed => "Wed".to_string(),
                chrono::Weekday::Thu => "Thu".to_string(),
                chrono::Weekday::Fri => "Fri".to_string(),
                chrono::Weekday::Sat => "Sat".to_string(),
                chrono::Weekday::Sun => "Sun".to_string(),
            },
            None => "Unknown".to_string(),
        }
    }

    /// Computes the total fare paid by the passenger across fare components.
    pub fn total_passenger_fare(&self) -> f64 {
        let base = self.base_passenger_fare.unwrap_or(0.0);
        let tolls = self.tolls.unwrap_or(0.0);
        let bcf = self.bcf.unwrap_or(0.0);
        let tax = self.sales_tax.unwrap_or(0.0);
        let congestion = self.congestion_surcharge.unwrap_or(0.0);
        let airport = self.airport_fee.unwrap_or(0.0);
        let tips = self.tips.unwrap_or(0.0);
        base + tolls + bcf + tax + congestion + airport + tips
    }

    /// Computes total driver compensation (base pay + tips).
    pub fn total_driver_pay(&self) -> f64 {
        self.driver_pay.unwrap_or(0.0) + self.tips.unwrap_or(0.0)
    }

    /// Trip distance in miles, defaulting to 0.0 if absent.
    pub fn miles(&self) -> f64 {
        self.trip_miles.unwrap_or(0.0)
    }

    /// Trip duration in seconds, defaulting to 0 if absent.
    pub fn seconds(&self) -> i64 {
        self.trip_time.unwrap_or(0)
    }
}

/// Intermediate combiner accumulator for metric aggregations.
#[derive(Clone, Debug, Default, PartialEq, BeamRow)]
pub struct TripAccumulator {
    pub total_trips: i64,
    pub total_miles: f64,
    pub total_seconds: i64,
    pub total_passenger_fare: f64,
    pub total_driver_pay: f64,
}

/// Summary metrics computed per service provider and day of week.
#[derive(Clone, Debug, Default, PartialEq, BeamRow)]
pub struct TripStats {
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

/// Combiner aggregating trip records into statistical summaries per service and day.
#[derive(Clone, Debug, Default)]
pub struct TripAggregator;

impl CombineFn for TripAggregator {
    type Input = NycTripRecord;
    type Accum = TripAccumulator;
    type Output = TripStats;

    fn create_accumulator(&self) -> Self::Accum {
        TripAccumulator::default()
    }

    fn add_input(&self, mut accum: Self::Accum, trip: Self::Input) -> Self::Accum {
        accum.total_trips += 1;
        accum.total_miles += trip.miles();
        accum.total_seconds += trip.seconds();
        accum.total_passenger_fare += trip.total_passenger_fare();
        accum.total_driver_pay += trip.total_driver_pay();
        accum
    }

    fn merge_accumulators(&self, accumulators: Vec<Self::Accum>) -> Self::Accum {
        accumulators
            .into_iter()
            .fold(TripAccumulator::default(), |mut merged, acc| {
                merged.total_trips += acc.total_trips;
                merged.total_miles += acc.total_miles;
                merged.total_seconds += acc.total_seconds;
                merged.total_passenger_fare += acc.total_passenger_fare;
                merged.total_driver_pay += acc.total_driver_pay;
                merged
            })
    }

    fn extract_output(&self, accum: Self::Accum) -> Self::Output {
        let trips = accum.total_trips.max(1) as f64;
        let miles = if accum.total_miles > 0.0 {
            accum.total_miles
        } else {
            1.0
        };
        let minutes = (accum.total_seconds as f64) / 60.0;
        let hours = minutes / 60.0;
        let avg_speed_mph = if hours > 0.0 {
            accum.total_miles / hours
        } else {
            0.0
        };

        TripStats {
            total_trips: accum.total_trips,
            total_miles: (accum.total_miles * 100.0).round() / 100.0,
            total_minutes: (minutes * 100.0).round() / 100.0,
            total_passenger_fare: (accum.total_passenger_fare * 100.0).round() / 100.0,
            total_driver_pay: (accum.total_driver_pay * 100.0).round() / 100.0,
            avg_fare_per_trip: ((accum.total_passenger_fare / trips) * 100.0).round() / 100.0,
            avg_fare_per_mile: ((accum.total_passenger_fare / miles) * 100.0).round() / 100.0,
            avg_driver_pay_per_trip: ((accum.total_driver_pay / trips) * 100.0).round() / 100.0,
            avg_speed_mph: (avg_speed_mph * 10.0).round() / 10.0,
        }
    }
}

/// Final daily service summary written to output Parquet files.
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

/// Builds the fluent NYC Taxi analytics pipeline transformations.
///
/// Steps:
/// - Reads Parquet files with splittable reader and column projection.
/// - Filters out trips with non-positive distance or duration.
/// - Keys records by `(service, day_of_week)`.
/// - Aggregates metrics using [`TripAggregator`] with combiner lifting.
/// - Formats aggregated stats into typed [`DailyServiceSummary`] rows.
pub fn build_nyc_taxi_summary(
    pipeline: &Pipeline,
    input_path: &str,
) -> PCollection<DailyServiceSummary> {
    pipeline
        .apply(parquetio::Read::<NycTripRecord>::new(
            "ParquetIO.Read",
            input_path,
        ))
        .filter("FilterValidTrips", |t: &NycTripRecord| {
            t.miles() > 0.0 && t.seconds() > 0
        })
        .map("KeyByServiceAndDay", |t: NycTripRecord| {
            let key = format!("{}#{}", t.service_name(), t.day_of_week());
            (key, t)
        })
        .combine_per_key("AggregateDailyStats", TripAggregator)
        .map("FormatSummary", |(key, stats): (String, TripStats)| {
            let (service, day) = key.split_once('#').unwrap_or((key.as_str(), "Unknown"));
            DailyServiceSummary {
                service: service.to_string(),
                day_of_week: day.to_string(),
                total_trips: stats.total_trips,
                total_miles: stats.total_miles,
                total_minutes: stats.total_minutes,
                total_passenger_fare: stats.total_passenger_fare,
                total_driver_pay: stats.total_driver_pay,
                avg_fare_per_trip: stats.avg_fare_per_trip,
                avg_fare_per_mile: stats.avg_fare_per_mile,
                avg_driver_pay_per_trip: stats.avg_driver_pay_per_trip,
                avg_speed_mph: stats.avg_speed_mph,
            }
        })
}

/// Builds and executes the full NYC Taxi analytics pipeline, writing output Parquet files.
pub fn build_nyc_taxi_pipeline(
    pipeline: &Pipeline,
    input_path: &str,
    output_prefix: &str,
) -> PCollection<String> {
    let summary = build_nyc_taxi_summary(pipeline, input_path);
    summary.apply(parquetio::Write::new(
        "ParquetIO.Write",
        output_prefix,
        ParquetSink::new(),
    ))
}

/// Generates a curated set of sample trip records for testing and local demonstration.
pub fn sample_trips() -> Vec<NycTripRecord> {
    let monday = DateTime::parse_from_rfc3339("2023-02-06T08:30:00Z")
        .expect("valid datetime")
        .with_timezone(&Utc);
    let friday = DateTime::parse_from_rfc3339("2023-02-10T18:15:00Z")
        .expect("valid datetime")
        .with_timezone(&Utc);

    vec![
        // Uber Monday morning commute (5.0 miles, 20 mins, $25 fare, $18 pay)
        NycTripRecord {
            hvfhs_license_num: Some("HV0003".to_string()),
            pickup_datetime: Some(monday),
            dropoff_datetime: Some(monday + chrono::Duration::minutes(20)),
            trip_miles: Some(5.0),
            trip_time: Some(1200),
            base_passenger_fare: Some(20.0),
            tolls: Some(2.5),
            bcf: Some(0.5),
            sales_tax: Some(1.0),
            congestion_surcharge: Some(2.5),
            airport_fee: Some(0.0),
            tips: Some(3.0),
            driver_pay: Some(15.0),
        },
        // Uber Monday lunch ride (3.0 miles, 12 mins, $15 fare, $11 pay)
        NycTripRecord {
            hvfhs_license_num: Some("HV0003".to_string()),
            pickup_datetime: Some(monday + chrono::Duration::hours(4)),
            dropoff_datetime: Some(
                monday + chrono::Duration::hours(4) + chrono::Duration::minutes(12),
            ),
            trip_miles: Some(3.0),
            trip_time: Some(720),
            base_passenger_fare: Some(12.0),
            tolls: Some(0.0),
            bcf: Some(0.5),
            sales_tax: Some(0.5),
            congestion_surcharge: Some(2.5),
            airport_fee: Some(0.0),
            tips: Some(2.0),
            driver_pay: Some(9.0),
        },
        // Lyft Friday evening trip (10.0 miles, 30 mins, $45 fare, $32 pay)
        NycTripRecord {
            hvfhs_license_num: Some("HV0005".to_string()),
            pickup_datetime: Some(friday),
            dropoff_datetime: Some(friday + chrono::Duration::minutes(30)),
            trip_miles: Some(10.0),
            trip_time: Some(1800),
            base_passenger_fare: Some(35.0),
            tolls: Some(5.0),
            bcf: Some(1.0),
            sales_tax: Some(2.0),
            congestion_surcharge: Some(2.5),
            airport_fee: Some(1.5),
            tips: Some(5.0),
            driver_pay: Some(27.0),
        },
        // Invalid trip with zero miles and zero duration. The pipeline filters it out.
        NycTripRecord {
            hvfhs_license_num: Some("HV0003".to_string()),
            pickup_datetime: Some(monday),
            dropoff_datetime: Some(monday),
            trip_miles: Some(0.0),
            trip_time: Some(0),
            base_passenger_fare: Some(0.0),
            tolls: None,
            bcf: None,
            sales_tax: None,
            congestion_surcharge: None,
            airport_fee: None,
            tips: None,
            driver_pay: None,
        },
    ]
}

/// Helper that writes sample trips to a local Parquet file.
pub fn write_sample_parquet(path: &Path, trips: &[NycTripRecord]) -> Result {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let sink = ParquetSink::<NycTripRecord>::default();
    let file = fs::File::create(path)?;
    let mut writer = sink.open(Box::new(file))?;
    trips.iter().try_for_each(|trip| writer.write(trip))?;
    writer.finish()
}
