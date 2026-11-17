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

//! Graph and coder tests of `WithTimestamps`.

use std::time::Duration;

use beam::Instant;
use beam::pipeline::{Pipeline, URN_PAR_DO};
use beam::schema::{FieldType, FieldValue, Row, Schema};
use beam::transforms::display_data::{DisplayDataItem, DisplayDataValue};
use beam::transforms::{Create, WithTimestamps};
use beam::windowing::{FixedWindows, WindowInto};

use crate::support::graph;

#[test]
fn with_timestamps_is_one_par_do() {
    let p = Pipeline::new();
    let millis = p.apply("Millis", Create::new(vec![1_000_i64, 2_000]));
    let _ = millis.apply(
        "Timestamps",
        WithTimestamps::new(|ms: &i64| Instant::from_millis(*ms)),
    );

    let pipeline = p.to_proto();
    let transform = graph::transform(&pipeline, "Timestamps");
    assert_eq!(graph::urn(transform), URN_PAR_DO);
    assert!(transform.subtransforms.is_empty());
    let inputs: Vec<&str> = transform.inputs.values().map(String::as_str).collect();
    assert_eq!(inputs, [millis.id()]);
    let display_data: Vec<(String, DisplayDataValue)> = transform
        .display_data
        .iter()
        .map(|item| DisplayDataItem::from_proto(item).expect("display data item"))
        .map(|item| (item.key, item.value))
        .collect();
    assert!(
        display_data.contains(&(
            "transform".to_string(),
            DisplayDataValue::Text("WithTimestamps".to_string()),
        )),
        "{display_data:?}"
    );
}

#[test]
fn with_timestamps_keeps_the_coder_and_the_windowing_strategy_of_the_input() {
    let p = Pipeline::new();
    let windowed = p
        .apply("Millis", Create::new(vec![1_000_i64, 2_000]))
        .apply(
            "Windows",
            WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
        );
    let timestamped = windowed.apply(
        "Timestamps",
        WithTimestamps::new(|ms: &i64| Instant::from_millis(*ms))
            .with_allowed_timestamp_skew(Duration::from_secs(1)),
    );

    assert_eq!(timestamped.coder_id(), windowed.coder_id());
    assert_eq!(
        timestamped.windowing_strategy_id(),
        windowed.windowing_strategy_id()
    );
    let pipeline = p.to_proto();
    let pcollection = &graph::components(&pipeline).pcollections[timestamped.id()];
    assert_eq!(pcollection.coder_id, windowed.coder_id());
}

#[test]
fn with_timestamps_keeps_the_schema_of_a_row() {
    let schema = Schema::builder().field("ms", FieldType::int64()).build();
    let row = |ms| Row::new(schema.clone().into(), vec![Some(FieldValue::Int64(ms))]).unwrap();
    let p = Pipeline::new();
    let rows = p.apply("Rows", Create::new(vec![row(1_000), row(2_000)]));
    let timestamped = rows.apply("Timestamps", WithTimestamps::new(|_: &Row| Instant::EPOCH));

    assert_eq!(timestamped.coder_id(), rows.coder_id());
    assert_eq!(timestamped.row_schema().as_deref(), Some(&schema));
    p.validate().expect("the row coder has a schema");
}
