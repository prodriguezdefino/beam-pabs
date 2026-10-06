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

use beam::io::gcp::pubsub::{URN_PUBSUB_READ, raw_bytes_row};
use beam::prelude::*;
use pubsub_ouroboros::{
    Args, OuroborosMessage, build_pipeline, evolve_ouroboros, message_to_row, ouroboros_schema,
    process_raw_message_row, row_to_message,
};
use testutils::MockExpansionService;

#[test]
fn test_ouroboros_message_serde() {
    let msg = OuroborosMessage::new("serpent-alpha", 4);
    assert_eq!(msg.iteration, 0);
    assert_eq!(msg.state, "egg");
    assert!(!msg.is_complete());

    let json_bytes = msg.to_json_bytes();
    let decoded = OuroborosMessage::from_json_slice(&json_bytes).expect("valid deserialization");
    assert_eq!(decoded, msg);
}

#[test]
fn test_ouroboros_evolution_cycle() {
    let mut msg = OuroborosMessage::new("ouroboros-1", 4);

    // Iteration 1
    msg = evolve_ouroboros(&msg);
    assert_eq!(msg.iteration, 1);
    assert_eq!(msg.state, "shedding_skin");
    assert!(!msg.is_complete());

    // Iteration 2
    msg = evolve_ouroboros(&msg);
    assert_eq!(msg.iteration, 2);
    assert_eq!(msg.state, "growing_coils");
    assert!(!msg.is_complete());

    // Iteration 3
    msg = evolve_ouroboros(&msg);
    assert_eq!(msg.iteration, 3);
    assert_eq!(msg.state, "devouring_tail");
    assert!(!msg.is_complete());

    // Iteration 4 (max reached)
    msg = evolve_ouroboros(&msg);
    assert_eq!(msg.iteration, 4);
    assert_eq!(msg.state, "ouroboros_ascended");
    assert!(msg.is_complete());
}

#[test]
fn test_ouroboros_message_row_conversion() {
    let msg = OuroborosMessage {
        cycle_id: "test-cycle".to_string(),
        iteration: 2,
        max_iterations: 5,
        state: "growing_coils".to_string(),
        history: "born -> shedding_skin#1 -> growing_coils#2".to_string(),
    };

    let schema = ouroboros_schema();
    assert_eq!(schema.num_fields(), 5);

    let row = message_to_row(&msg).expect("build row");
    let recovered = row_to_message(&row).expect("recover message");
    assert_eq!(recovered, msg);
}

#[test]
fn test_process_raw_message_row_routing() {
    let initial = OuroborosMessage::new("raw-cycle", 3);
    let raw_in = raw_bytes_row(initial.to_json_bytes());

    let raw_out = process_raw_message_row(&raw_in).expect("processed row");
    let out_bytes = raw_out
        .get_bytes("payload")
        .unwrap()
        .expect("bytes payload");
    let evolved = OuroborosMessage::from_json_slice(out_bytes).expect("evolved message");

    assert_eq!(evolved.iteration, 1);
    assert_eq!(evolved.state, "shedding_skin");

    let completed = OuroborosMessage {
        cycle_id: "completed-cycle".to_string(),
        iteration: 4,
        max_iterations: 4,
        state: "ouroboros_ascended".to_string(),
        history: "born -> ascended#4".to_string(),
    };
    let completed_raw = raw_bytes_row(completed.to_json_bytes());
    assert!(process_raw_message_row(&completed_raw).is_none());
}

#[test]
fn test_pubsub_ouroboros_pipeline_expansion() {
    let server = MockExpansionService::new()
        .with_source(URN_PUBSUB_READ, false)
        .start();

    let args = Args {
        input_subscription: Some("projects/test-project/subscriptions/test-sub".to_string()),
        input_topic: None,
        loop_topic: Some("projects/test-project/topics/test-loop".to_string()),
        expansion_service: server.endpoint().to_string(),
        max_cycles: 5,
    };

    let p = build_pipeline(&PipelineOptions::default(), &args);
    let lock = p.lock();
    let transforms = &lock.components.transforms;

    let pubsub_read = transforms
        .values()
        .find(|t| t.unique_name == "PubsubRead")
        .expect("Pipeline must contain expanded PubsubRead source transform");
    let pubsub_read_out = pubsub_read
        .outputs
        .get("output")
        .expect("PubsubRead must produce output PCollection");

    let seed = transforms
        .values()
        .find(|t| t.unique_name == "SeedOuroborosMessage")
        .expect("Pipeline must contain SeedOuroborosMessage transform");
    let seed_out = seed
        .outputs
        .values()
        .next()
        .expect("SeedOuroborosMessage must produce output PCollection");

    let merge = transforms
        .values()
        .find(|t| t.unique_name == "MergeIncomingAndSeed")
        .expect("Pipeline must contain MergeIncomingAndSeed flatten transform");
    assert!(
        merge
            .inputs
            .values()
            .any(|in_pcoll| in_pcoll == pubsub_read_out),
        "MergeIncomingAndSeed must consume PubsubRead output"
    );
    assert!(
        merge.inputs.values().any(|in_pcoll| in_pcoll == seed_out),
        "MergeIncomingAndSeed must consume SeedOuroborosMessage output"
    );
    let merge_out = merge
        .outputs
        .values()
        .next()
        .expect("MergeIncomingAndSeed must produce output PCollection");

    let evolve = transforms
        .values()
        .find(|t| t.unique_name == "EvolveOuroborosState")
        .expect("Pipeline must contain native Rust EvolveOuroborosState transform");
    assert!(
        evolve.inputs.values().any(|in_pcoll| in_pcoll == merge_out),
        "EvolveOuroborosState must consume MergeIncomingAndSeed output"
    );
    let evolve_out = evolve
        .outputs
        .values()
        .next()
        .expect("EvolveOuroborosState must produce output PCollection");

    let pubsub_write = transforms
        .values()
        .find(|t| t.unique_name == "PubsubWrite")
        .expect("Pipeline must contain expanded PubsubWrite sink transform");
    assert!(
        pubsub_write
            .inputs
            .values()
            .any(|in_pcoll| in_pcoll == evolve_out),
        "PubsubWrite must consume EvolveOuroborosState output"
    );
}
