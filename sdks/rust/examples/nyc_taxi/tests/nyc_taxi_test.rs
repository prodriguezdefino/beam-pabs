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

use beam::io::parquet::parquetio;
use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use chrono::{DateTime, Duration, Utc};
use nyc_taxi::{Args, DailyServiceSummary, NycTripRecord, SummarizeTrips, build_pipeline};

fn trip(
    license: &str,
    pickup: &str,
    miles: f64,
    seconds: i64,
    fare: f64,
    tips: f64,
    pay: f64,
) -> NycTripRecord {
    let pickup = DateTime::parse_from_rfc3339(pickup)
        .expect("valid RFC 3339 time")
        .with_timezone(&Utc);
    NycTripRecord {
        hvfhs_license_num: Some(license.to_string()),
        pickup_datetime: Some(pickup),
        dropoff_datetime: Some(pickup + Duration::seconds(seconds)),
        trip_miles: Some(miles),
        trip_time: Some(seconds),
        base_passenger_fare: Some(fare),
        tolls: None,
        bcf: None,
        sales_tax: None,
        congestion_surcharge: None,
        airport_fee: None,
        tips: Some(tips),
        driver_pay: Some(pay),
    }
}

/// Two Uber trips on a Monday, one Lyft trip on a Friday, and one trip without distance.
fn trips() -> Vec<NycTripRecord> {
    vec![
        trip("HV0003", "2023-02-06T08:30:00Z", 5.0, 1200, 26.5, 3.0, 15.0),
        trip("HV0003", "2023-02-06T12:30:00Z", 3.0, 720, 15.5, 2.0, 9.0),
        trip(
            "HV0005",
            "2023-02-10T18:15:00Z",
            10.0,
            1800,
            47.0,
            5.0,
            27.0,
        ),
        trip("HV0003", "2023-02-06T09:00:00Z", 0.0, 0, 0.0, 0.0, 0.0),
    ]
}

fn expected() -> [DailyServiceSummary; 2] {
    [
        DailyServiceSummary {
            service: "Uber".to_string(),
            day_of_week: "Mon".to_string(),
            total_trips: 2,
            total_miles: 8.0,
            total_minutes: 32.0,
            total_passenger_fare: 47.0,
            total_driver_pay: 29.0,
            avg_fare_per_trip: 23.5,
            avg_fare_per_mile: 5.88,
            avg_driver_pay_per_trip: 14.5,
            avg_speed_mph: 15.0,
        },
        DailyServiceSummary {
            service: "Lyft".to_string(),
            day_of_week: "Fri".to_string(),
            total_trips: 1,
            total_miles: 10.0,
            total_minutes: 30.0,
            total_passenger_fare: 52.0,
            total_driver_pay: 32.0,
            avg_fare_per_trip: 52.0,
            avg_fare_per_mile: 5.2,
            avg_driver_pay_per_trip: 32.0,
            avg_speed_mph: 20.0,
        },
    ]
}

#[tokio::test]
async fn test_summarize_trips() {
    let p = TestPipeline::new();

    let summaries = p
        .apply("Create", Create::new(trips()))
        .apply("SummarizeTrips", SummarizeTrips);

    passert::that("AssertSummaries", &summaries).contains_in_any_order(expected());

    p.run().await.expect("SummarizeTrips assertions must pass");
}

#[tokio::test]
async fn test_parquet_round_trip() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let input = temp.path().join("trips").to_string_lossy().into_owned();
    let output = temp.path().join("stats").to_string_lossy().into_owned();

    let write_input = TestPipeline::new();
    write_input
        .apply("Create", Create::new(trips()))
        .apply("WriteTrips", parquetio::Write::new(&input));
    write_input
        .run()
        .await
        .expect("input files must be written");

    let output_files = format!("{output}*");
    let args = Args {
        input: format!("{input}*"),
        output,
    };
    build_pipeline(&PipelineOptions::default(), &args)
        .run()
        .await
        .expect("the example pipeline must succeed");

    let read_output = TestPipeline::new();
    let summaries = read_output.apply(
        "ReadSummaries",
        parquetio::Read::<DailyServiceSummary>::new(output_files),
    );
    passert::that("AssertSummaries", &summaries).contains_in_any_order(expected());
    read_output
        .run()
        .await
        .expect("output files must hold the summaries");
}
