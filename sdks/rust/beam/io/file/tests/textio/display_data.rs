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

use std::collections::HashMap;

use beam::pipeline::Pipeline;
use beam::prelude::*;
use beam::transforms::display_data::DisplayDataItem;
use file::textio;
use model::pipeline as proto;

fn display_data_for(
    components: &proto::Components,
    transform_name: &str,
) -> HashMap<String, String> {
    let transform = components
        .transforms
        .values()
        .find(|t| t.unique_name == transform_name)
        .unwrap_or_else(|| panic!("transform '{transform_name}' not found in proto"));

    transform
        .display_data
        .iter()
        .map(|d| DisplayDataItem::from_proto(d).expect("display data must decode"))
        .map(|item| (item.key, item.value))
        .collect()
}

#[test]
fn test_text_write_reports_its_destination() {
    let p = Pipeline::new();
    p.apply(Create::new("Create", vec!["line".to_string()]))
        .apply(textio::Write::new(
            "TextIO.Write",
            "/tmp/beam_display_data_test.txt",
        ));

    let components = p.to_proto().components.expect("components present");
    let items = display_data_for(&components, "TextIO.Write");

    assert_eq!(
        items.get("transform").map(String::as_str),
        Some("TextIO.Write")
    );
    assert_eq!(
        items.get("filenamePrefix").map(String::as_str),
        Some("/tmp/beam_display_data_test.txt")
    );
}

#[test]
fn test_text_read_reports_pattern_and_split_size() {
    let p = Pipeline::new();
    p.apply(textio::Read::new("TextIO.Read", "/data/in-*.txt").with_split_size(1234));

    let components = p.to_proto().components.expect("components present");
    let items = display_data_for(&components, "TextIO.Read");
    let expected: HashMap<String, String> = [
        ("filePattern", "/data/in-*.txt"),
        ("splitSize", "1234"),
        ("transform", "TextIO.Read"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    assert_eq!(items, expected);
}

#[test]
fn test_text_read_with_filename_reports_pattern_and_split_size() {
    let p = Pipeline::new();
    p.apply(
        textio::ReadWithFilename::new("TextIO.ReadWithFilename", "/data/*.log").with_split_size(77),
    );

    let components = p.to_proto().components.expect("components present");
    let items = display_data_for(&components, "TextIO.ReadWithFilename");
    let expected: HashMap<String, String> = [
        ("filePattern", "/data/*.log"),
        ("splitSize", "77"),
        ("transform", "TextIO.ReadWithFilename"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    assert_eq!(items, expected);
}
