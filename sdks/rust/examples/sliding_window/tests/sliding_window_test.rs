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

use std::sync::{Arc, Mutex};

use beam::prelude::*;
use sliding_window::{SensorReading, build_sliding_window_graph, parse_sensor_reading_line};

#[test]
fn test_parse_sensor_reading_line() {
    let line = "sensor_alpha,24.5,1700000000000";
    let rec = parse_sensor_reading_line(line).expect("CSV line must parse");
    assert_eq!(
        rec,
        SensorReading {
            sensor_id: "sensor_alpha".to_string(),
            reading: 24.5,
            timestamp_ms: 1700000000000,
        }
    );

    let invalid = "invalid,format";
    assert!(parse_sensor_reading_line(invalid).is_none());
}

#[tokio::test]
async fn test_sliding_window_on_prism_runner() {
    let p = Pipeline::new();
    let results = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&results);

    // Window size = 20s (20,000 ms), period = 10s (10,000 ms)
    // Events with timestamps >= 10,000 so all window starts are >= 0
    // sensorA at 12,000 (val=20.0) -> windows [0, 20000), [10000, 30000)
    // sensorA at 18,000 (val=40.0) -> windows [0, 20000), [10000, 30000)
    let csv_lines = p.apply(Create::new(
        "Create",
        vec![
            "sensorA,20.0,12000".to_string(),
            "sensorA,40.0,18000".to_string(),
        ],
    ));

    let averages = build_sliding_window_graph(&csv_lines, 20, 10);
    averages.inspect("CaptureAveragesPrism", move |s: &String| {
        captured.lock().unwrap().push(s.clone());
    });

    p.run()
        .await
        .expect("sliding window pipeline on prism runner must succeed");

    let mut items = results.lock().unwrap().clone();
    items.sort();
    assert_eq!(
        items,
        vec![
            "AVG|sensorA|[0..20000)|count=2|avg=30.00|min=20.00|max=40.00".to_string(),
            "AVG|sensorA|[10000..30000)|count=2|avg=30.00|min=20.00|max=40.00".to_string(),
        ]
    );
}
