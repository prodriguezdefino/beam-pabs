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

//! Integration tests for the Unified Display Data architecture in `beam-core`.
//!
//! Tests verify display data collection, typed values, protocol buffer conversion,
//! round-trip decoding and transform integration. `options_snapshot_test.rs` tests the
//! display data of pipeline options.

use std::collections::HashMap;

use beam::pipeline::Pipeline;
use beam::prelude::*;
use beam::transforms::display_data::{
    DisplayDataBuilder, DisplayDataItem, HasDisplayData, URN_DISPLAY_DATA_LABELLED,
};
use beam::transforms::{Filter, FlatMap, Inspect, Map};
use model::pipeline as proto;

#[test]
fn test_builder_typed_items() {
    let mut builder = DisplayDataBuilder::with_namespace("test_ns");
    builder
        .add_text("str_key", "hello")
        .add_text_with_label("str_labeled", "world", "World Label")
        .add_integer("int_key", 42)
        .add_integer_with_label("int_labeled", 100, "Century")
        .add_boolean("bool_key", true)
        .add_boolean_with_label("bool_labeled", false, "Flag")
        .add_float("float_key", 12.5)
        .add_float_with_label("float_labeled", 99.5, "CustomLabel");

    let items = builder.build();
    assert_eq!(items.len(), 8);

    assert_eq!(items[0].key, "str_key");
    assert_eq!(items[0].namespace, "test_ns");
    assert_eq!(items[0].item_type, "STRING");
    assert_eq!(items[0].value, "hello");
    assert_eq!(items[0].label, None);

    assert_eq!(items[1].key, "str_labeled");
    assert_eq!(items[1].label.as_deref(), Some("World Label"));

    assert_eq!(items[2].key, "int_key");
    assert_eq!(items[2].item_type, "INTEGER");
    assert_eq!(items[2].value, "42");

    assert_eq!(items[4].key, "bool_key");
    assert_eq!(items[4].item_type, "BOOLEAN");
    assert_eq!(items[4].value, "true");

    assert_eq!(items[6].key, "float_key");
    assert_eq!(items[6].item_type, "FLOAT");
    assert_eq!(items[6].value, "12.5");
}

#[test]
fn test_proto_roundtrip_labelled() {
    let original = vec![
        DisplayDataItem::text("k1", "ns1", "val1").with_label("Label 1"),
        DisplayDataItem::integer("k2", "ns1", 999),
        DisplayDataItem::boolean("k3", "ns2", true),
        DisplayDataItem::float("k4", "ns2", 1.25),
    ];

    for item in original {
        let proto = item.to_proto();
        assert_eq!(proto.urn, URN_DISPLAY_DATA_LABELLED);

        let decoded = DisplayDataItem::from_proto(&proto).expect("failed to decode proto");
        assert_eq!(decoded.key, item.key);
        assert_eq!(decoded.namespace, item.namespace);
        assert_eq!(decoded.item_type, item.item_type);
        assert_eq!(decoded.value, item.value);
        assert_eq!(decoded.label, item.label);
    }
}

#[test]
fn test_proto_non_labelled_urn_fallback() {
    let custom_proto = proto::DisplayData {
        urn: "beam:display_data:custom:v1".to_string(),
        payload: b"custom payload".to_vec(),
    };

    let item = DisplayDataItem::from_proto(&custom_proto).expect("decode fallback");
    assert_eq!(item.key, "beam:display_data:custom:v1");
    assert_eq!(item.namespace, "urn");
    assert_eq!(item.value, "custom payload");
}

struct CustomTransformWithoutDisplayData;

impl PTransform<PCollection<i64>> for CustomTransformWithoutDisplayData {
    type Output = PCollection<i64>;

    fn expand(&self, input: &PCollection<i64>) -> Self::Output {
        input.apply(Map::new("Double", |x: i64| x * 2))
    }
}

struct CustomTransformWithDisplayData {
    multiplier: i64,
}

impl HasDisplayData for CustomTransformWithDisplayData {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_integer("multiplier", self.multiplier);
    }
}

impl PTransform<PCollection<i64>> for CustomTransformWithDisplayData {
    type Output = PCollection<i64>;

    fn expand(&self, input: &PCollection<i64>) -> Self::Output {
        let mult = self.multiplier;
        input.apply(Map::new("Multiply", move |x: i64| x * mult))
    }
}

#[test]
fn test_display_data_optionality_on_custom_transforms() {
    let p = Pipeline::new();
    let col = p.apply(Create::new("Create", vec![1i64, 2, 3]));

    // Transforms without `HasDisplayData` compile without display data.
    let res1 = col.apply(CustomTransformWithoutDisplayData);
    assert!(!res1.id().is_empty());

    // Transforms with `HasDisplayData` populate display data.
    let res2 = col.apply(CustomTransformWithDisplayData { multiplier: 10 });
    assert!(!res2.id().is_empty());
}

/// Decodes display data for the named transform in the pipeline protocol buffer.
fn display_data_for(components: &proto::Components, unique_name: &str) -> HashMap<String, String> {
    let transform = components
        .transforms
        .values()
        .find(|t| t.unique_name == unique_name)
        .unwrap_or_else(|| {
            let present: Vec<&str> = components
                .transforms
                .values()
                .map(|t| t.unique_name.as_str())
                .collect();
            panic!("no transform named '{unique_name}'; pipeline has {present:?}")
        });

    transform
        .display_data
        .iter()
        .map(|d| DisplayDataItem::from_proto(d).expect("display data must decode"))
        .map(|item| (item.key, item.value))
        .collect()
}

/// Transforms expanding to a `ParDo` must attach display data directly to the `ParDo` node.
#[test]
fn test_par_do_transforms_attach_display_data_to_their_proto_node() {
    let p = Pipeline::new();
    let col = p.apply(Create::new(
        "Create",
        vec!["hello".to_string(), "world".to_string()],
    ));
    let filtered = col.apply(Filter::new("FilterNonEmpty", |s: &String| !s.is_empty()));
    let split = filtered.apply(FlatMap::new("SplitChars", |s: String| {
        s.chars().map(|c| c.to_string()).collect::<Vec<_>>()
    }));
    let mapped = split.apply(Map::new("Upper", |s: String| s.to_uppercase()));
    let _inspected = mapped.apply(Inspect::new("InspectElements", |_: &String| {}));

    let components = p.to_proto().components.expect("components present");

    for (unique_name, kind) in [
        ("FilterNonEmpty", "Filter"),
        ("SplitChars", "FlatMap"),
        ("Upper", "Map"),
        ("InspectElements", "Inspect"),
    ] {
        let items = display_data_for(&components, unique_name);

        assert_eq!(
            items.get("transform").map(String::as_str),
            Some(kind),
            "'{unique_name}' should report its transform kind; got {items:?}"
        );
        assert_eq!(
            items.get("transform_name").map(String::as_str),
            Some(unique_name),
            "'{unique_name}' should report its name; got {items:?}"
        );
    }
}

#[test]
fn test_display_data_typed_json_serialization() {
    let int_item = DisplayDataItem::integer("num_workers", "beam:option:worker:v1", 4);
    let int_json = serde_json::to_value(&int_item).expect("Serialize to JSON");
    assert_eq!(int_json["type"], "INTEGER");
    assert_eq!(int_json["value"], serde_json::json!(4)); // Integer value must remain numeric.

    let bool_item = DisplayDataItem::boolean("streaming", "beam:option:core:v1", true);
    let bool_json = serde_json::to_value(&bool_item).expect("Serialize to JSON");
    assert_eq!(bool_json["type"], "BOOLEAN");
    assert_eq!(bool_json["value"], serde_json::json!(true)); // Boolean value must remain boolean.

    let float_item = DisplayDataItem::float("threshold", "custom:ns", 12.5);
    let float_json = serde_json::to_value(&float_item).expect("Serialize to JSON");
    assert_eq!(float_json["type"], "FLOAT");
    assert_eq!(float_json["value"], serde_json::json!(12.5));

    let str_item = DisplayDataItem::text("name", "custom:ns", "val");
    let str_json = serde_json::to_value(&str_item).expect("Serialize to JSON");
    assert_eq!(str_json["type"], "STRING");
    assert_eq!(str_json["value"], serde_json::json!("val"));

    // Verify deserialization back to `DisplayDataItem`.
    let roundtripped_int: DisplayDataItem =
        serde_json::from_value(int_json).expect("Deserialize int item");
    assert_eq!(roundtripped_int.value, "4");
    assert_eq!(roundtripped_int.item_type, "INTEGER");
}
