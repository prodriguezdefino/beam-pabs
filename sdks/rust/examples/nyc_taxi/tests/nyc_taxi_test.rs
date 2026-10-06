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

//! End-to-end integration tests for the NYC Taxi Parquet analytics example.

use beam::io::parquet::parquetio;
use beam::testing::{TestPipeline, passert};
use nyc_taxi::{
    DailyServiceSummary, build_nyc_taxi_pipeline, build_nyc_taxi_summary, sample_trips,
    write_sample_parquet,
};

#[tokio::test]
async fn test_nyc_taxi_fluent_summary_in_memory() {
    let temp = tempfile::tempdir().unwrap();
    let input_path = temp.path().join("input_trips.parquet");
    let trips = sample_trips();
    write_sample_parquet(&input_path, &trips).unwrap();

    let p = TestPipeline::new();
    let summaries = build_nyc_taxi_summary(&p, input_path.to_str().unwrap());

    // The invalid trip is filtered out, so exactly 2 daily summaries remain.
    passert::that("AssertSummaries", &summaries).has_count(2);

    passert::that("AssertSummaries", &summaries).satisfies(|results: &[DailyServiceSummary]| {
        let uber = results
            .iter()
            .find(|s| s.service == "Uber" && s.day_of_week == "Mon")
            .ok_or_else(|| "Uber Monday summary missing".to_string())?;

        if uber.total_trips != 2 {
            return Err(format!("Expected 2 Uber trips, got {}", uber.total_trips).into());
        }
        if (uber.total_miles - 8.0).abs() >= 1e-4 {
            return Err(format!("Expected 8.0 miles, got {}", uber.total_miles).into());
        }
        if (uber.total_minutes - 32.0).abs() >= 1e-4 {
            return Err(format!("Expected 32.0 minutes, got {}", uber.total_minutes).into());
        }
        if (uber.total_passenger_fare - 47.0).abs() >= 1e-4 {
            return Err(format!("Expected 47.0 fare, got {}", uber.total_passenger_fare).into());
        }
        if (uber.total_driver_pay - 29.0).abs() >= 1e-4 {
            return Err(format!("Expected 29.0 pay, got {}", uber.total_driver_pay).into());
        }
        if (uber.avg_fare_per_trip - 23.5).abs() >= 1e-4 {
            return Err(format!("Expected 23.5 avg fare, got {}", uber.avg_fare_per_trip).into());
        }

        let lyft = results
            .iter()
            .find(|s| s.service == "Lyft" && s.day_of_week == "Fri")
            .ok_or_else(|| "Lyft Friday summary missing".to_string())?;

        if lyft.total_trips != 1 {
            return Err(format!("Expected 1 Lyft trip, got {}", lyft.total_trips).into());
        }
        if (lyft.total_miles - 10.0).abs() >= 1e-4 {
            return Err(format!("Expected 10.0 miles, got {}", lyft.total_miles).into());
        }
        if (lyft.total_minutes - 30.0).abs() >= 1e-4 {
            return Err(format!("Expected 30.0 minutes, got {}", lyft.total_minutes).into());
        }
        if (lyft.total_passenger_fare - 52.0).abs() >= 1e-4 {
            return Err(format!("Expected 52.0 fare, got {}", lyft.total_passenger_fare).into());
        }
        if (lyft.total_driver_pay - 32.0).abs() >= 1e-4 {
            return Err(format!("Expected 32.0 pay, got {}", lyft.total_driver_pay).into());
        }
        if (lyft.avg_speed_mph - 20.0).abs() >= 1e-4 {
            return Err(format!("Expected 20.0 mph, got {}", lyft.avg_speed_mph).into());
        }

        Ok(())
    });

    p.run().await.unwrap();
}

#[tokio::test]
async fn test_nyc_taxi_end_to_end_parquet_round_trip() {
    let temp = tempfile::tempdir().unwrap();
    let input_path = temp.path().join("input_trips.parquet");
    let output_prefix = temp.path().join("output/daily_stats");
    let trips = sample_trips();
    write_sample_parquet(&input_path, &trips).unwrap();

    // Read input Parquet and write sharded output Parquet files.
    let p1 = TestPipeline::new();
    let _ = build_nyc_taxi_pipeline(
        &p1,
        input_path.to_str().unwrap(),
        output_prefix.to_str().unwrap(),
    );
    p1.run().await.unwrap();

    // Verify generated output files are readable through parquetio::Read.
    let output_pattern = format!("{}*", output_prefix.to_str().unwrap());
    let p2 = TestPipeline::new();
    let read_back = p2.apply(parquetio::Read::<DailyServiceSummary>::new(
        "ParquetIO.Read",
        &output_pattern,
    ));

    passert::that("AssertReadBack", &read_back).has_count(2);
    passert::that("AssertReadBack", &read_back).satisfies(|rows: &[DailyServiceSummary]| {
        if rows.len() != 2 {
            return Err(format!("Expected 2 rows, got {}", rows.len()).into());
        }
        if !rows
            .iter()
            .any(|r| r.service == "Uber" && r.total_trips == 2)
        {
            return Err("Uber 2 trips summary missing".into());
        }
        if !rows
            .iter()
            .any(|r| r.service == "Lyft" && r.total_trips == 1)
        {
            return Err("Lyft 1 trip summary missing".into());
        }
        Ok(())
    });

    p2.run().await.unwrap();
}
