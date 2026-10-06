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

//! Traffic Routes Avro Analytics Example.
//!
//! Ingests freeway sensor telemetry from Caltrans PeMS CSV datasets,
//! extracts corridor metrics, aggregates through keyed combiners, and
//! outputs sharded, Deflate-compressed Apache Avro container files.
//!
//! Input data defaults to the official public Apache Beam sample:
//! `gs://apache-beam-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15_test2.csv`
//! or local CSV files.

use beam::io::avro::avroio::{self, CompressionCodec};
use beam::prelude::*;

/// Default public dataset path in Google Cloud Storage (Caltrans PeMS freeway traffic telemetry).
pub const DEFAULT_TRAFFIC_CSV: &str =
    "gs://apache-beam-samples/traffic_sensor/Freeways-5Minaa2010-01-01_to_2010-02-15_test2.csv";

/// Freeway traffic sensor telemetry record.
#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct TrafficRecord {
    pub timestamp: String,
    pub station_id: i64,
    pub freeway: i64,
    pub direction: String,
    pub station_type: String,
    pub total_flow: Option<i64>,
    pub avg_occupancy: Option<f64>,
    pub avg_speed: Option<f64>,
}

/// Formats a freeway number and direction into a recognized corridor name.
pub fn format_route_corridor(freeway: i64, direction: &str) -> String {
    let prefix = match freeway {
        5 | 8 | 15 | 805 => format!("I-{freeway}"),
        52 | 94 | 125 | 163 => format!("CA-{freeway}"),
        other => format!("Route-{other}"),
    };
    let dir = direction.trim().to_uppercase();
    if dir.is_empty() {
        prefix
    } else {
        format!("{prefix} {dir}")
    }
}

/// Accumulator for route traffic analytics.
#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct RouteAccumulator {
    pub total_readings: i64,
    pub total_vehicles: i64,
    pub speed_sum: f64,
    pub speed_count: i64,
    pub min_speed: f64,
    pub max_speed: f64,
    pub occupancy_sum: f64,
    pub congestion_incidents: i64,
}

impl Default for RouteAccumulator {
    fn default() -> Self {
        Self {
            total_readings: 0,
            total_vehicles: 0,
            speed_sum: 0.0,
            speed_count: 0,
            min_speed: f64::MAX,
            max_speed: 0.0,
            occupancy_sum: 0.0,
            congestion_incidents: 0,
        }
    }
}

impl RouteAccumulator {
    /// Returns the average speed in mph if speed readings were recorded.
    pub fn avg_speed(&self) -> Option<f64> {
        (self.speed_count > 0).then(|| self.speed_sum / self.speed_count as f64)
    }

    /// Returns the average occupancy ratio (0.0 to 1.0) if readings were recorded.
    pub fn avg_occupancy(&self) -> Option<f64> {
        (self.total_readings > 0).then(|| self.occupancy_sum / self.total_readings as f64)
    }

    /// Returns the minimum recorded speed if at least one speed reading was observed.
    pub fn min_speed(&self) -> Option<f64> {
        (self.min_speed < f64::MAX).then_some(self.min_speed)
    }

    /// Evaluates whether the corridor experienced severe congestion (>= 33% congested readings).
    pub fn is_heavily_congested(&self) -> bool {
        self.total_readings > 0 && (self.congestion_incidents * 3) >= self.total_readings
    }

    pub fn add_record(&mut self, record: &TrafficRecord) {
        self.total_readings += 1;
        if let Some(flow) = record.total_flow.filter(|&f| f > 0) {
            self.total_vehicles += flow;
        }
        if let Some(speed) = record.avg_speed.filter(|&s| s > 0.0) {
            self.speed_sum += speed;
            self.speed_count += 1;
            self.min_speed = self.min_speed.min(speed);
            self.max_speed = self.max_speed.max(speed);
            if speed < 45.0 {
                self.congestion_incidents += 1;
            }
        }
        if let Some(occ) = record.avg_occupancy.filter(|&o| o > 0.0) {
            self.occupancy_sum += occ;
        }
    }

    pub fn merge(&mut self, other: &Self) {
        self.total_readings += other.total_readings;
        self.total_vehicles += other.total_vehicles;
        self.speed_sum += other.speed_sum;
        self.speed_count += other.speed_count;
        self.min_speed = self.min_speed.min(other.min_speed);
        self.max_speed = self.max_speed.max(other.max_speed);
        self.occupancy_sum += other.occupancy_sum;
        self.congestion_incidents += other.congestion_incidents;
    }
}

/// Aggregated traffic summary for a route corridor.
#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct RouteTrafficSummary {
    pub route: String,
    pub total_readings: i64,
    pub total_vehicles: i64,
    pub avg_speed_mph: f64,
    pub min_speed_mph: f64,
    pub max_speed_mph: f64,
    pub avg_occupancy_pct: f64,
    pub congestion_incidents: i64,
    pub condition: String,
}

impl RouteTrafficSummary {
    /// Constructs a traffic summary from a route name and its aggregated metrics using a functional pipeline.
    pub fn from_accumulator(route: String, acc: RouteAccumulator) -> Self {
        let round_2 = |v: f64| (v * 100.0).round() / 100.0;

        let avg_speed_mph = acc.avg_speed().map(round_2).unwrap_or(0.0);
        let avg_occupancy_pct = acc
            .avg_occupancy()
            .map(|occ| round_2(occ * 100.0))
            .unwrap_or(0.0);
        let min_speed_mph = acc.min_speed().map(round_2).unwrap_or(0.0);
        let max_speed_mph = round_2(acc.max_speed);

        let condition = match avg_speed_mph {
            speed if speed < 45.0 || acc.is_heavily_congested() => "Congested",
            speed if speed < 60.0 => "Moderate",
            _ => "Free Flow",
        }
        .to_string();

        Self {
            route,
            total_readings: acc.total_readings,
            total_vehicles: acc.total_vehicles,
            avg_speed_mph,
            min_speed_mph,
            max_speed_mph,
            avg_occupancy_pct,
            congestion_incidents: acc.congestion_incidents,
            condition,
        }
    }
}

impl From<(String, RouteAccumulator)> for RouteTrafficSummary {
    fn from((route, acc): (String, RouteAccumulator)) -> Self {
        Self::from_accumulator(route, acc)
    }
}

/// Combiner aggregating traffic records into a route accumulator.
#[derive(Clone, Debug, Default)]
pub struct TrafficAggregator;

impl CombineFn for TrafficAggregator {
    type Input = TrafficRecord;
    type Accum = RouteAccumulator;
    type Output = RouteAccumulator;

    fn create_accumulator(&self) -> Self::Accum {
        RouteAccumulator::default()
    }

    fn add_input(&self, mut accum: Self::Accum, record: Self::Input) -> Self::Accum {
        accum.add_record(&record);
        accum
    }

    fn merge_accumulators(&self, accumulators: Vec<Self::Accum>) -> Self::Accum {
        accumulators
            .into_iter()
            .fold(RouteAccumulator::default(), |mut acc, other| {
                acc.merge(&other);
                acc
            })
    }

    fn extract_output(&self, accum: Self::Accum) -> Self::Output {
        accum
    }
}

/// Parses a single Caltrans PeMS CSV line into a [`TrafficRecord`].
///
/// Skips header lines, empty records, and entries lacking valid speed or direction.
pub fn parse_traffic_record(line: &str) -> Option<TrafficRecord> {
    let parts: Vec<&str> = line.split(',').collect();
    if parts.len() < 10 {
        return None;
    }
    let timestamp = parts[0].trim().to_string();
    if timestamp.is_empty() || timestamp == "Timestamp" {
        return None;
    }
    let station_id: i64 = parts[1].trim().parse().ok()?;
    let freeway: i64 = parts[2].trim().parse().ok()?;
    let direction = parts[3].trim().to_string();
    let station_type = parts[4].trim().to_string();
    let total_flow: Option<i64> = parts[7].trim().parse().ok().filter(|&f| f >= 0);
    let avg_occupancy: Option<f64> = parts[8].trim().parse().ok();
    let avg_speed: Option<f64> = parts[9].trim().parse().ok().filter(|&s| s > 0.0);

    if avg_speed.is_none() || direction.is_empty() {
        return None;
    }

    Some(TrafficRecord {
        timestamp,
        station_id,
        freeway,
        direction,
        station_type,
        total_flow,
        avg_occupancy,
        avg_speed,
    })
}

/// Builds the core analytical PTransform aggregating CSV traffic records into RouteTrafficSummary.
pub fn build_traffic_summary(
    pipeline: &Pipeline,
    input_path: &str,
) -> PCollection<RouteTrafficSummary> {
    pipeline
        .apply(textio::Read::new("ReadLines", input_path))
        .flat_map("ParseTrafficRecords", |line: String| {
            parse_traffic_record(&line)
        })
        .key_by("KeyByRouteCorridor", |r: &TrafficRecord| {
            format_route_corridor(r.freeway, &r.direction)
        })
        .combine_per_key("AggregateRouteTraffic", TrafficAggregator)
        .map("FormatSummary", RouteTrafficSummary::from)
}

/// Builds the end-to-end traffic analytics pipeline reading CSV and writing Avro.
pub fn build_traffic_pipeline(
    pipeline: &Pipeline,
    input_path: &str,
    output_prefix: &str,
) -> PCollection<String> {
    build_traffic_summary(pipeline, input_path).apply(
        avroio::Write::new(
            "AvroIO.Write",
            output_prefix,
            avroio::AvroSink::<RouteTrafficSummary>::new(),
        )
        .with_compression(Some(CompressionCodec::Deflate)),
    )
}
