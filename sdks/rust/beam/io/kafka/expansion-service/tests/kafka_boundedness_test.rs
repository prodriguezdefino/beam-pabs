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

//! The boundedness of the PCollections that the Kafka providers add. No test connects to a
//! broker.

use std::collections::{BTreeSet, HashMap};

use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, FieldType, Schema};
use beam::values::IsBounded;
use expansion::{PCollectionId, SchemaTransformProvider, TransformInputs};
use kafka_expansion::{
    ErrorHandling, KafkaReadConfig, KafkaReadProvider, KafkaWriteConfig, KafkaWriteProvider,
};

fn errors(output: Option<&str>) -> Option<ErrorHandling> {
    output.map(|output| ErrorHandling {
        output: output.to_string(),
    })
}

/// The boundedness of every PCollection of `p`, by id.
fn marks(p: &Pipeline) -> HashMap<String, IsBounded> {
    p.to_proto()
        .components
        .unwrap_or_default()
        .pcollections
        .iter()
        .map(|(id, pcoll)| (id.clone(), IsBounded::from(pcoll.is_bounded())))
        .collect()
}

/// The outputs of the transform named `*{suffix}` and every PCollection derived from them.
fn downstream_of(p: &Pipeline, suffix: &str) -> BTreeSet<String> {
    let components = p.to_proto().components.unwrap_or_default();
    let mut reached: BTreeSet<String> = components
        .transforms
        .values()
        .filter(|t| t.unique_name.ends_with(suffix))
        .flat_map(|t| t.outputs.values().cloned())
        .collect();
    loop {
        let next: BTreeSet<String> = components
            .transforms
            .values()
            .filter(|t| t.inputs.values().any(|id| reached.contains(id)))
            .flat_map(|t| t.outputs.values().cloned())
            .filter(|id| !reached.contains(id))
            .collect();
        if next.is_empty() {
            return reached;
        }
        reached.extend(next);
    }
}

fn expand_read(config: KafkaReadConfig) -> (Pipeline, HashMap<String, PCollectionId>) {
    let mut p = Pipeline::new();
    let outputs = KafkaReadProvider
        .build_transform(
            config.to_row().expect("the read config encodes"),
            TransformInputs::default(),
            &mut p,
        )
        .expect("the read expands");
    (p, outputs)
}

fn read_config(max_read_time_seconds: Option<i32>, output: Option<&str>) -> KafkaReadConfig {
    KafkaReadConfig {
        format: "RAW".to_string(),
        topic: "events".to_string(),
        bootstrap_servers: "broker:9092".to_string(),
        max_read_time_seconds,
        error_handling: errors(output),
        ..Default::default()
    }
}

#[test]
fn every_pcollection_after_read_partitions_is_unbounded() {
    [None, Some("failures")].into_iter().for_each(|output| {
        let (p, outputs) = expand_read(read_config(None, output));
        let marks = marks(&p);
        let after = downstream_of(&p, "/ReadPartitions");
        assert!(!after.is_empty(), "{output:?}: no ReadPartitions output");
        let bounded: Vec<&String> = after
            .iter()
            .filter(|id| marks[*id] != IsBounded::Unbounded)
            .collect();
        assert_eq!(bounded, Vec::<&String>::new(), "{output:?}");
        outputs.values().for_each(|id| {
            assert!(
                after.contains(id.as_str()),
                "{output:?}: output {id} not after ReadPartitions"
            );
        });
    });
}

#[test]
fn a_read_with_a_max_read_time_is_bounded() {
    let (p, _) = expand_read(read_config(Some(60), None));
    let unbounded: Vec<(String, IsBounded)> = marks(&p)
        .into_iter()
        .filter(|(_, mark)| *mark != IsBounded::Bounded)
        .collect();
    assert_eq!(unbounded, []);
}

#[test]
fn the_write_follows_the_boundedness_of_the_caller_input() {
    let schema = Schema::builder()
        .field("value", FieldType::string())
        .build();
    let cases = [
        (IsBounded::Unbounded, None),
        (IsBounded::Unbounded, Some("failures")),
        (IsBounded::Bounded, None),
        (IsBounded::Bounded, Some("failures")),
    ];
    cases.into_iter().for_each(|(input_mark, output)| {
        let mut p = Pipeline::new();
        let coder = p.register_row_coder(&schema);
        let input = p.add_pcollection::<beam::schema::Row>("caller_input", &coder, input_mark);
        let config = KafkaWriteConfig {
            format: "JSON".to_string(),
            topic: "events".to_string(),
            bootstrap_servers: "broker:9092".to_string(),
            error_handling: errors(output),
            ..Default::default()
        };
        let inputs: TransformInputs = [("input".to_string(), PCollectionId::from(input.id()))]
            .into_iter()
            .collect();
        let outputs = KafkaWriteProvider
            .build_transform(
                config.to_row().expect("the write config encodes"),
                inputs,
                &mut p,
            )
            .expect("the write expands");
        let marks = marks(&p);
        assert!(marks.len() > 1, "the write adds no PCollection");
        let other: Vec<(String, IsBounded)> = marks
            .into_iter()
            .filter(|(_, mark)| *mark != input_mark)
            .collect();
        assert_eq!(other, [], "{input_mark:?} {output:?}");
        assert_eq!(outputs.len(), 1);
    });
}
