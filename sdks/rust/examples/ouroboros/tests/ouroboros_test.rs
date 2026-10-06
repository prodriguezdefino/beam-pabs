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

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use ouroboros::{Ouroboros, OuroborosMessage, Stage, Step, step};

fn message(cycle_id: &str, iteration: i64, stage: Stage) -> OuroborosMessage {
    OuroborosMessage {
        cycle_id: cycle_id.to_string(),
        iteration,
        max_iterations: 3,
        stage,
    }
}

fn payload(message: &OuroborosMessage) -> Vec<u8> {
    serde_json::to_vec(message).expect("a message serializes to JSON")
}

#[test]
fn test_step() {
    let cases = [
        (
            message("c", 0, Stage::Egg),
            Step::Continue(message("c", 1, Stage::SheddingSkin)),
        ),
        (
            message("c", 1, Stage::SheddingSkin),
            Step::Continue(message("c", 2, Stage::GrowingCoils)),
        ),
        (
            message("c", 2, Stage::GrowingCoils),
            Step::Ascended(message("c", 3, Stage::Ascended)),
        ),
        (message("c", 3, Stage::Ascended), Step::AlreadyAscended),
    ];
    for (input, expected) in cases {
        assert_eq!(step(&payload(&input)).expect("valid payload"), expected);
    }
    assert!(step(b"not json").is_err());
}

#[test]
fn test_evolve_cycles_through_the_stages() {
    let stages: Vec<Stage> = std::iter::successors(Some(OuroborosMessage::new("c", 6)), |m| {
        (m.stage != Stage::Ascended).then(|| m.clone().evolve())
    })
    .map(|m| m.stage)
    .collect();
    assert_eq!(
        stages,
        [
            Stage::Egg,
            Stage::SheddingSkin,
            Stage::GrowingCoils,
            Stage::DevouringTail,
            Stage::Regenerating,
            Stage::SheddingSkin,
            Stage::Ascended,
        ]
    );
}

/// The seeds join the loop, a message on its last pass leaves it, and malformed or
/// ascended payloads are dropped.
#[tokio::test]
async fn test_ouroboros_pass() {
    let p = TestPipeline::new();

    let incoming = vec![
        payload(&message("loop", 1, Stage::SheddingSkin)),
        payload(&message("last", 2, Stage::GrowingCoils)),
        payload(&message("done", 3, Stage::Ascended)),
        b"not json".to_vec(),
    ];
    let outgoing = p.apply("Create", Create::new(incoming)).apply(
        "Ouroboros",
        Ouroboros {
            num_seeds: 2,
            max_cycles: 3,
        },
    );

    passert::that("AssertOutgoing", &outgoing).contains_in_any_order([
        payload(&message("loop", 2, Stage::GrowingCoils)),
        payload(&message("ouroboros-0", 1, Stage::SheddingSkin)),
        payload(&message("ouroboros-1", 1, Stage::SheddingSkin)),
    ]);

    p.run().await.expect("Ouroboros assertions must pass");
}
