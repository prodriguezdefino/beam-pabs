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

use beam::io::kafka::{payload_bytes, raw_bytes_row};
use kafka_ouroboros::{
    Args, OuroborosMessage, READ_ERRORS, Step, WRITE_ERRORS, build_pipeline, process_record,
    seed_messages, step,
};

/// Workers rebuild the pipeline without an expansion service; both Kafka error outputs
/// must still exist there for the error counters to attach to.
#[test]
fn test_pipeline_builds_on_a_worker_with_error_outputs() {
    let (mut options, args) = beam::options::parse_from::<Args, _, _>([
        "kafka_ouroboros",
        "--bootstrap_servers=broker:9092",
        "--loop_topic=loop",
    ]);
    options.harness.worker = true;

    let p = build_pipeline(&options, &args).expect("worker-side build needs no expansion service");
    let lock = p.lock();
    let outputs_of = |name: &str| {
        lock.components
            .transforms
            .values()
            .find(|t| t.unique_name == name)
            .map(|t| t.outputs.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_else(|| panic!("{name} in the graph"))
    };
    assert!(outputs_of("KafkaRead").contains(&READ_ERRORS.to_string()));
    assert!(outputs_of("KafkaWrite").contains(&WRITE_ERRORS.to_string()));
}

#[test]
fn test_seed_messages() {
    let seeds = seed_messages(3, 4);
    let ids: Vec<_> = seeds.iter().map(|m| m.cycle_id.as_str()).collect();
    assert_eq!(ids, ["ouroboros-0", "ouroboros-1", "ouroboros-2"]);
    assert!(
        seeds
            .iter()
            .all(|m| m.iteration == 0 && m.max_iterations == 4)
    );
    assert!(seed_messages(-1, 4).is_empty());
}

#[test]
fn test_full_cycle_loops_then_ascends() {
    let mut payload = OuroborosMessage::new("c", 3).to_json_bytes();
    let mut states = Vec::new();
    loop {
        match step(&payload) {
            Step::Continue(next) => {
                states.push(next.state.clone());
                payload = next.to_json_bytes();
            }
            Step::Ascended(done) => {
                states.push(done.state.clone());
                assert_eq!(
                    done.history,
                    "born -> shedding_skin#1 -> growing_coils#2 -> ouroboros_ascended#3"
                );
                break;
            }
            other => panic!("unexpected step {other:?}"),
        }
    }
    assert_eq!(
        states,
        ["shedding_skin", "growing_coils", "ouroboros_ascended"]
    );
}

#[test]
fn test_process_record_routing() {
    let fresh = raw_bytes_row(OuroborosMessage::new("c", 5).to_json_bytes());
    let out = process_record(&fresh).expect("continues around the loop");
    let next = OuroborosMessage::from_json_slice(payload_bytes(&out).unwrap()).unwrap();
    assert_eq!(next.iteration, 1);

    let last_hop = OuroborosMessage {
        iteration: 4,
        ..OuroborosMessage::new("c", 5)
    };
    assert_eq!(
        process_record(&raw_bytes_row(last_hop.to_json_bytes())),
        None
    );

    let replayed = OuroborosMessage {
        iteration: 5,
        ..OuroborosMessage::new("c", 5)
    };
    assert_eq!(step(&replayed.to_json_bytes()), Step::AlreadyComplete);

    assert_eq!(step(b"not json"), Step::Malformed);
    assert_eq!(process_record(&raw_bytes_row(b"not json".to_vec())), None);
}
