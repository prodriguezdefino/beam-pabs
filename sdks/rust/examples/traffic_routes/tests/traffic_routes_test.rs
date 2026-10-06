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

//! End-to-end integration and unit tests for the Traffic Routes Avro analytics example.

use std::fs;

use beam::io::avro::avroio;
use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use traffic_routes::{
    RouteAccumulator, RouteTrafficSummary, TrafficAggregator, TrafficRecord,
    build_traffic_pipeline, build_traffic_summary, format_route_corridor, parse_traffic_record,
};

const SAMPLE_CSV: &str = "\
Timestamp,Station ID,Freeway,Direction of Travel,Station Type,Samples,% Observed,Total Flow,Average Occupancy,Average Speed
01/01/2010 00:00:00,1108148,15,S,ML,10,100,63,0.0092,70.9
01/01/2010 00:05:00,1108148,15,S,ML,10,100,58,0.0088,72.1
01/01/2010 00:00:00,1108285,94,W,ML,10,100,37,0.0084,58.5
01/01/2010 00:05:00,1108285,94,W,ML,10,100,40,0.0090,57.5
01/01/2010 00:00:00,1100340,5,N,ML,10,100,120,0.2450,32.0
01/01/2010 00:05:00,1100340,5,N,ML,10,100,135,0.2680,28.5
01/01/2010 00:00:00,1100310,5,N,FR,10,0,-1,,,
";

#[test]
fn test_corridor_formatting() {
    assert_eq!(format_route_corridor(15, "S"), "I-15 S");
    assert_eq!(format_route_corridor(5, "n"), "I-5 N");
    assert_eq!(format_route_corridor(94, "W"), "CA-94 W");
    assert_eq!(format_route_corridor(52, "E"), "CA-52 E");
    assert_eq!(format_route_corridor(78, "W"), "Route-78 W");
    assert_eq!(format_route_corridor(15, ""), "I-15");
}

#[test]
fn test_parse_traffic_record() {
    let valid_line = "01/01/2010 00:00:00,1108148,15,S,ML,10,100,63,0.0092,70.9";
    let record = parse_traffic_record(valid_line).expect("must parse valid CSV line");
    assert_eq!(record.freeway, 15);
    assert_eq!(record.direction, "S");
    assert_eq!(record.total_flow, Some(63));
    assert_eq!(record.avg_speed, Some(70.9));

    let header_line = "Timestamp,Station,Freeway,Direction,Type,Length,Samples,%Observed,TotalFlow,AvgOccupancy,AvgSpeed";
    assert_eq!(parse_traffic_record(header_line), None);

    let invalid_line = "01/01/2010 00:00:00,1100310,5,N,FR,0.5,0,0,,,";
    assert_eq!(parse_traffic_record(invalid_line), None);
}

#[test]
fn test_functional_accumulator_and_summary() {
    let mut acc = RouteAccumulator::default();
    assert_eq!(acc.avg_speed(), None);
    assert_eq!(acc.avg_occupancy(), None);
    assert_eq!(acc.min_speed(), None);
    assert!(!acc.is_heavily_congested());

    let record1 = TrafficRecord {
        timestamp: "01/01/2010 00:00:00".to_string(),
        station_id: 1,
        freeway: 15,
        direction: "S".to_string(),
        station_type: "ML".to_string(),
        total_flow: Some(50),
        avg_occupancy: Some(0.02),
        avg_speed: Some(70.0),
    };
    let record2 = TrafficRecord {
        timestamp: "01/01/2010 00:05:00".to_string(),
        station_id: 1,
        freeway: 15,
        direction: "S".to_string(),
        station_type: "ML".to_string(),
        total_flow: Some(60),
        avg_occupancy: Some(0.04),
        avg_speed: Some(80.0),
    };

    acc.add_record(&record1);
    acc.add_record(&record2);

    assert_eq!(acc.avg_speed(), Some(75.0));
    assert_eq!(acc.avg_occupancy(), Some(0.03));
    assert_eq!(acc.min_speed(), Some(70.0));
    assert_eq!(acc.max_speed, 80.0);
    assert!(!acc.is_heavily_congested());

    let summary = RouteTrafficSummary::from_accumulator("I-15 S".to_string(), acc);
    assert_eq!(summary.route, "I-15 S");
    assert_eq!(summary.total_readings, 2);
    assert_eq!(summary.total_vehicles, 110);
    assert_eq!(summary.avg_speed_mph, 75.0);
    assert_eq!(summary.avg_occupancy_pct, 3.0);
    assert_eq!(summary.min_speed_mph, 70.0);
    assert_eq!(summary.max_speed_mph, 80.0);
    assert_eq!(summary.condition, "Free Flow");
}

#[test]
fn test_traffic_aggregator_combine_fn() {
    let aggregator = TrafficAggregator;
    let acc1 = aggregator.create_accumulator();
    let record = TrafficRecord {
        timestamp: "01/01/2010 00:00:00".to_string(),
        station_id: 1,
        freeway: 5,
        direction: "N".to_string(),
        station_type: "ML".to_string(),
        total_flow: Some(100),
        avg_occupancy: Some(0.20),
        avg_speed: Some(30.0),
    };

    let acc2 = aggregator.add_input(acc1, record);
    let merged = aggregator.merge_accumulators(vec![acc2.clone(), acc2]);
    let summary = RouteTrafficSummary::from(("I-5 N".to_string(), merged));

    assert_eq!(summary.total_readings, 2);
    assert_eq!(summary.total_vehicles, 200);
    assert_eq!(summary.avg_speed_mph, 30.0);
    assert_eq!(summary.congestion_incidents, 2);
    assert_eq!(summary.condition, "Congested");
}

#[tokio::test]
async fn test_traffic_fluent_summary_in_memory() {
    let temp = tempfile::tempdir().unwrap();
    let input_path = temp.path().join("input_traffic.csv");
    fs::write(&input_path, SAMPLE_CSV).unwrap();

    let p = TestPipeline::new();
    let summaries = build_traffic_summary(&p, input_path.to_str().unwrap());

    // Assert that exactly 3 route summaries are computed (invalid reading filtered out)
    passert::that("AssertSummaries", &summaries).has_count(3);

    passert::that("AssertSummaries", &summaries).satisfies(|results: &[RouteTrafficSummary]| {
        let i15 = results
            .iter()
            .find(|s| s.route == "I-15 S")
            .ok_or_else(|| "I-15 S summary missing".to_string())?;

        if i15.total_readings != 2 {
            return Err(format!("Expected 2 I-15 readings, got {}", i15.total_readings).into());
        }
        if i15.total_vehicles != 121 {
            return Err(format!("Expected 121 vehicles, got {}", i15.total_vehicles).into());
        }
        if (i15.avg_speed_mph - 71.5).abs() >= 1e-2 {
            return Err(format!("Expected ~71.5 mph, got {}", i15.avg_speed_mph).into());
        }
        if i15.condition != "Free Flow" {
            return Err(format!("Expected Free Flow, got {}", i15.condition).into());
        }

        let ca94 = results
            .iter()
            .find(|s| s.route == "CA-94 W")
            .ok_or_else(|| "CA-94 W summary missing".to_string())?;

        if ca94.total_readings != 2 {
            return Err(format!("Expected 2 CA-94 readings, got {}", ca94.total_readings).into());
        }
        if ca94.total_vehicles != 77 {
            return Err(format!("Expected 77 vehicles, got {}", ca94.total_vehicles).into());
        }
        if (ca94.avg_speed_mph - 58.0).abs() >= 1e-2 {
            return Err(format!("Expected ~58.0 mph, got {}", ca94.avg_speed_mph).into());
        }
        if ca94.condition != "Moderate" {
            return Err(format!("Expected Moderate, got {}", ca94.condition).into());
        }

        let i5 = results
            .iter()
            .find(|s| s.route == "I-5 N")
            .ok_or_else(|| "I-5 N summary missing".to_string())?;

        if i5.total_readings != 2 {
            return Err(format!("Expected 2 I-5 readings, got {}", i5.total_readings).into());
        }
        if i5.total_vehicles != 255 {
            return Err(format!("Expected 255 vehicles, got {}", i5.total_vehicles).into());
        }
        if (i5.avg_speed_mph - 30.25).abs() >= 1e-2 {
            return Err(format!("Expected ~30.25 mph, got {}", i5.avg_speed_mph).into());
        }
        if i5.congestion_incidents != 2 {
            return Err(format!(
                "Expected 2 congestion incidents, got {}",
                i5.congestion_incidents
            )
            .into());
        }
        if i5.condition != "Congested" {
            return Err(format!("Expected Congested, got {}", i5.condition).into());
        }

        Ok(())
    });

    p.run().await.unwrap();
}

#[tokio::test]
async fn test_traffic_end_to_end_avro_round_trip() {
    let temp = tempfile::tempdir().unwrap();
    let input_path = temp.path().join("input_traffic.csv");
    let output_prefix = temp.path().join("output/route_stats");
    fs::write(&input_path, SAMPLE_CSV).unwrap();

    let p1 = TestPipeline::new();
    let _ = build_traffic_pipeline(
        &p1,
        input_path.to_str().unwrap(),
        output_prefix.to_str().unwrap(),
    );
    p1.run().await.unwrap();

    let output_pattern = format!("{}*", output_prefix.to_str().unwrap());
    let p2 = TestPipeline::new();
    let read_back = p2.apply(avroio::Read::<RouteTrafficSummary>::new(
        "AvroIO.Read",
        &output_pattern,
    ));

    passert::that("AssertReadBack", &read_back).has_count(3);
    passert::that("AssertReadBack", &read_back).satisfies(|rows: &[RouteTrafficSummary]| {
        if rows.len() != 3 {
            return Err(format!("Expected 3 rows, got {}", rows.len()).into());
        }
        if !rows
            .iter()
            .any(|r| r.route == "I-15 S" && r.condition == "Free Flow")
        {
            return Err("I-15 S summary missing".into());
        }
        if !rows
            .iter()
            .any(|r| r.route == "CA-94 W" && r.condition == "Moderate")
        {
            return Err("CA-94 W summary missing".into());
        }
        if !rows
            .iter()
            .any(|r| r.route == "I-5 N" && r.condition == "Congested")
        {
            return Err("I-5 N summary missing".into());
        }
        Ok(())
    });

    p2.run().await.unwrap();
}
