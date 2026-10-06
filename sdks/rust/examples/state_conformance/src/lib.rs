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

//! Cross-bundle `MapState` and `SetState` conformance comparison across execution runners.
//!
//! A mutation applied in the same bundle that wrote the state proves nothing: the SDK
//! answers later reads from its own in-bundle buffer and the runner is never asked.
//! So every check here is staged across three bundles using event-time timers:
//!
//! - `process_element` seeds the cells.
//! - The `mutate` timer fires with those writes committed, so the mutation it issues has
//!   to reach the runner to have any effect.
//! - The `verify` timer reads the cells back and compares them against the canonical Beam model.
//!
//! # Conformance Scenarios
//!
//! This example evaluates how execution runners handle cross-bundle multimap state mutations:
//!
//! | Scenario | Staged Mutation | Expected Model Result |
//! | :--- | :--- | :--- |
//! | `clear_all` | `scores.clear()`, `players.clear()` | `keys=[]`, `members=[]` |
//! | `remove_present` | `remove("alice")` | `keys=[bob]`, `members=[bob]` |
//! | `remove_absent` | `remove("ghost")` | `keys=[carol]`, `members=[carol]` |
//!
//! ### Why the Checks Span Bundles
//!
//! An SDK that buffers state mutations in memory during a bundle answers same-bundle
//! reads from that buffer. A test that mutates and reads in one bundle does not send
//! the request to the runner, so it cannot detect runner divergences across bundle
//! boundaries.
//!
//! By default, this example records the comparison into `--output` without failing
//! the pipeline, allowing side-by-side comparison between runners. Pass `--strict`
//! to fail the pipeline on any runner divergence.
//!
//! # Running the Comparison
//!
//! To run on Prism:
//! ```text
//! ./gradlew :sdks:rust:prism -Pexample=state_conformance -PextraArgs="--output=/tmp/prism_state.txt"
//! cat /tmp/prism_state.txt
//! ```
//!
//! To run on Google Cloud Dataflow:
//! ```text
//! ./gradlew :sdks:rust:dataflow -Pexample=state_conformance -PmaxNumWorkers=1
//! gcloud storage cat "gs://<your-bucket>/temp/output/state_conformance_output.txt"
//! ```
//!

use std::collections::BTreeSet;

use beam::prelude::*;
use clap::Args;
use serde::{Deserialize, Serialize};

/// How a scenario mutates its cells in the second bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mutation {
    /// Clear the entire map and set.
    ClearAll,
    /// Remove a single key, which may or may not be present.
    Remove(&'static str),
}

/// One end-to-end check: what to write, what to do to it, and what must survive.
#[derive(Debug, Clone, Copy)]
pub struct Scenario {
    /// Key that isolates this scenario's state cells from every other scenario's.
    pub team: &'static str,
    /// Entries written during the first bundle.
    pub seed: &'static [(&'static str, i64)],
    /// Mutation applied during the second bundle.
    pub mutation: Mutation,
    /// Entries the third bundle must find, in sorted order.
    pub expected: &'static [&'static str],
}

/// The checks this example performs.
///
/// Each isolates a single way a runner can mishandle multimap state: dropping a whole-cell
/// clear, retaining a key that was removed, or materialising a key that never existed.
pub const SCENARIOS: &[Scenario] = &[
    Scenario {
        team: "clear_all",
        seed: &[("alice", 10), ("bob", 20)],
        mutation: Mutation::ClearAll,
        expected: &[],
    },
    Scenario {
        team: "remove_present",
        seed: &[("alice", 10), ("bob", 20)],
        mutation: Mutation::Remove("alice"),
        expected: &["bob"],
    },
    Scenario {
        team: "remove_absent",
        seed: &[("carol", 30)],
        mutation: Mutation::Remove("ghost"),
        expected: &["carol"],
    },
];

/// Looks up the scenario that owns a state key.
pub fn scenario_for(team: &str) -> Result<&'static Scenario> {
    SCENARIOS
        .iter()
        .find(|s| s.team == team)
        .ok_or_else(|| format!("no scenario is registered for key '{team}'").into())
}

/// The seed elements, derived from [`SCENARIOS`] so the input cannot drift from the checks.
pub fn seed_elements() -> Vec<(String, (String, i64))> {
    SCENARIOS
        .iter()
        .flat_map(|s| {
            s.seed
                .iter()
                .map(move |(player, points)| (s.team.to_string(), (player.to_string(), *points)))
        })
        .collect()
}

/// Renders a key collection as a stable, comparable string.
pub fn render(keys: impl IntoIterator<Item = String>) -> String {
    keys.into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join("+")
}

/// Compares an observed key collection against the model, describing the gap if they differ.
pub fn mismatch(label: &str, team: &str, expected: &[&str], observed: &str) -> Option<String> {
    let want = render(expected.iter().map(|s| (*s).to_string()));
    (want != observed)
        .then(|| format!("{team}: {label} is '{observed}' but the model says '{want}'"))
}

/// Command-line arguments for the state conformance pipeline.
#[derive(Args, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "state_conformance",
    about = "Apache Beam Rust Cross-Bundle Map And Set State Conformance Example",
    version
)]
pub struct StateConformanceArgs {
    /// Output file path for the diagnostic report.
    #[arg(long, default_value = "/tmp/state_conformance_output.txt")]
    pub output: String,

    /// Fail the pipeline with an error if any runner divergence is detected.
    #[arg(long, default_value_t = false)]
    pub strict: bool,
}

impl PipelineOptionGroup for StateConformanceArgs {}

#[derive(State)]
struct StateConformanceState {
    scores: MapState<String, i64>,
    players: SetState<String>,
}

#[derive(Timers)]
enum StateConformanceTimer {
    #[timer(event_time, id = "mutate")]
    Mutate,
    #[timer(event_time, id = "verify")]
    Verify,
}

/// Seeds, mutates and verifies one pair of state cells per key.
#[derive(Clone)]
struct StateConformanceDoFn {
    strict: bool,
}

impl StatefulDoFn for StateConformanceDoFn {
    type Key = String;
    type Value = (String, i64);
    type Out = String;
    type State = StateConformanceState;
    type Timer = StateConformanceTimer;

    fn process_element(
        &mut self,
        _team: &Self::Key,
        (player, points): Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        state.scores.put(player.clone(), points)?;
        state.players.insert(player)?;
        ctx.timer(StateConformanceTimer::Mutate)
            .at(Instant::from_millis(1_000))?;
        Ok(())
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        team: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> Result {
        let scenario = scenario_for(team)?;
        let observed_keys = render(state.scores.keys()?);

        match timer {
            StateConformanceTimer::Mutate => {
                // Nothing downstream is meaningful if the seed writes never landed, so this is
                // checked before the mutation rather than being folded into the final compare.
                let seeded: Vec<&str> = scenario.seed.iter().map(|(p, _)| *p).collect();
                if let Some(gap) = mismatch("seeded state", team, &seeded, &observed_keys) {
                    return Err(format!("first bundle's writes did not persist -- {gap}").into());
                }

                match scenario.mutation {
                    Mutation::ClearAll => {
                        state.scores.clear()?;
                        state.players.clear()?;
                    }
                    Mutation::Remove(key) => {
                        let key = key.to_string();
                        state.scores.remove(&key)?;
                        state.players.remove(&key)?;
                    }
                }

                ctx.timer(StateConformanceTimer::Verify)
                    .at(Instant::from_millis(2_000))?;
                Ok(())
            }
            StateConformanceTimer::Verify => {
                let observed_players = render(state.players.read()?);
                let failures: Vec<String> = [
                    mismatch("map keys", team, scenario.expected, &observed_keys),
                    mismatch("set members", team, scenario.expected, &observed_players),
                ]
                .into_iter()
                .flatten()
                .collect();

                if !failures.is_empty() {
                    if self.strict {
                        return Err(format!(
                            "{:?} did not take effect across bundles -- {}",
                            scenario.mutation,
                            failures.join("; ")
                        )
                        .into());
                    }
                    let want = render(scenario.expected.iter().map(|s| (*s).to_string()));
                    ctx.emit(format!(
                        "DIVERGENCE|team={team}|mutation={:?}|observed_keys={observed_keys}|expected_keys={want}|observed_members={observed_players}|expected_members={want}|issue={}",
                        scenario.mutation,
                        failures.join("; ")
                    ))?;
                } else {
                    ctx.emit(format!(
                        "MATCH|team={team}|mutation={:?}|keys={observed_keys}|members={observed_players}",
                        scenario.mutation
                    ))?;
                }
                Ok(())
            }
        }
    }
}

/// Builds the conformance pipeline graph over an already-created seed collection.
pub fn build_conformance_graph(
    events: &PCollection<(String, (String, i64))>,
    strict: bool,
) -> PCollection<String> {
    events.apply(ParDo::new(
        "SeedMutateVerify",
        StateConformanceDoFn { strict },
    ))
}

/// Constructs the complete state conformance pipeline.
pub fn build_pipeline(options: &PipelineOptions, args: &StateConformanceArgs) -> Pipeline {
    let p = Pipeline::create(options);
    let events = p.apply(Create::new("SeedScores", seed_elements()));
    build_conformance_graph(&events, args.strict)
        .apply(textio::Write::new("WriteLines", &args.output));
    p
}
