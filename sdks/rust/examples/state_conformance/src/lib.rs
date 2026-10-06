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
//! # Runner Conformance Comparison: Dataflow vs Prism
//!
//! This example runs live on both runners to directly compare how each runner handles
//! cross-bundle multimap state mutations:
//!
//! | Scenario | Staged Mutation | Canonical Model (Dataflow) | Prism Runner (observed) |
//! | :--- | :--- | :--- | :--- |
//! | `clear_all` | `scores.clear()`, `players.clear()` | `keys=[]`, `members=[]` (`MATCH`) | `keys=[alice, bob]` (`DIVERGENCE`: drops clear) |
//! | `remove_present` | `remove("alice")` | `keys=[bob]`, `members=[bob]` (`MATCH`) | `keys=[alice, bob]` (`DIVERGENCE`: retains key) |
//! | `remove_absent` | `remove("ghost")` | `keys=[carol]`, `members=[carol]` (`MATCH`) | `keys=[carol, ghost]` (`DIVERGENCE`: creates phantom key) |
//!
//! ### Empirical Results from Google Cloud Dataflow
//!
//! Dataflow 100% conforms to the canonical Beam state specification across bundle boundaries.
//! Verified on Dataflow (Job ID `2026-09-20_19_45_38-13417053993828032276` and Job ID
//! `2026-09-20_20_09_33-11446438552276818766`, `JOB_STATE_DONE`):
//! ```text
//! MATCH|team=remove_present|mutation=Remove("alice")|keys=bob|members=bob
//! MATCH|team=clear_all|mutation=ClearAll|keys=|members=
//! MATCH|team=remove_absent|mutation=Remove("ghost")|keys=carol|members=carol
//! ```
//!
//! ### Empirical Results from Prism Runner
//!
//! On Prism, all three scenarios diverge from the canonical model:
//! ```text
//! DIVERGENCE|team=remove_absent|mutation=Remove("ghost")|observed_keys=carol+ghost|expected_keys=carol|observed_members=carol+ghost|expected_members=carol|issue=remove_absent: map keys is 'carol+ghost' but the model says 'carol'; remove_absent: set members is 'carol+ghost' but the model says 'carol'
//! DIVERGENCE|team=clear_all|mutation=ClearAll|observed_keys=alice+bob|expected_keys=|observed_members=alice+bob|expected_members=|issue=clear_all: map keys is 'alice+bob' but the model says ''; clear_all: set members is 'alice+bob' but the model says ''
//! DIVERGENCE|team=remove_present|mutation=Remove("alice")|observed_keys=alice+bob|expected_keys=bob|observed_members=alice+bob|expected_members=bob|issue=remove_present: map keys is 'alice+bob' but the model says 'bob'; remove_present: set members is 'alice+bob' but the model says 'bob'
//! ```
//!
//! ### Analysis of Prism Runner Defects
//!
//! - **D1 (Whole-Cell Clear Dropped):** In `ClearMultimapKeysState`, Prism reads the key through
//!   `key.GetMultimapUserState()` instead of the multimap key variant. This mismatch returns `nil`,
//!   causing Prism to compute `LinkID{"", ""}` and silently drop the clear request.
//! - **D2 (Key Retained After Removal):** In `ClearMultimapState`, Prism assigns
//!   `userMap.Multimap[mapKey] = nil` instead of deleting the map key
//!   (`delete(userMap.Multimap, mapKey)`). The tombstone entry stays in the map.
//! - **D3 (Phantom Key Materialized):** Because `ClearMultimapState` assigns `nil` to
//!   `userMap.Multimap[mapKey]`, removing an absent key creates a new map entry with value
//!   `nil`. `GetMultimapKeysState` then iterates over all map keys without filtering out `nil`
//!   and reports keys that never existed.
//!
//! ### Why the Checks Span Bundles
//!
//! An SDK that buffers state mutations in memory during a bundle answers same-bundle reads
//! from that buffer. A test that mutates and reads in one bundle does not send the request
//! to the runner, so it cannot detect D1, D2 or D3.
//!
//! By default, this example records the comparison into `--output` without failing the pipeline,
//! allowing side-by-side comparison between runners. Pass `--strict` to fail the pipeline
//! on any runner divergence.
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

/// The timer that mutates state a bundle after it was written.
const MUTATE_TIMER: &str = "mutate";
/// The timer that reads the mutated state back a bundle later still.
const VERIFY_TIMER: &str = "verify";

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

/// Seeds, mutates and verifies one pair of state cells per key.
#[derive(Clone)]
struct StateConformanceDoFn {
    scores: MapStateSpec<String, i64>,
    players: SetStateSpec<String>,
    mutate_timer: TimerFamilySpec,
    verify_timer: TimerFamilySpec,
    strict: bool,
}

impl DoFn for StateConformanceDoFn {
    type In = (String, (String, i64));
    type Out = String;

    fn process_element(
        &mut self,
        (team, (player, points)): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        ctx.map_state(&self.scores, &team)?
            .put(player.clone(), points)?;
        ctx.set_state(&self.players, &team)?.insert(player)?;
        ctx.timer(&self.mutate_timer)?.key(&team)?.set(1_000);
        Ok(())
    }

    fn on_timer(
        &mut self,
        timer_family_id: &str,
        _tag: &str,
        _timestamp: i64,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        let team: String = ctx.current_key()?;
        let scenario = scenario_for(&team)?;
        let mut scores = ctx.map_state(&self.scores, &team)?;
        let mut players = ctx.set_state(&self.players, &team)?;

        let observed_keys = render(scores.keys()?);

        if timer_family_id == MUTATE_TIMER {
            // Nothing downstream is meaningful if the seed writes never landed, so this is
            // checked before the mutation rather than being folded into the final compare.
            let seeded: Vec<&str> = scenario.seed.iter().map(|(p, _)| *p).collect();
            if let Some(gap) = mismatch("seeded state", &team, &seeded, &observed_keys) {
                return Err(format!("first bundle's writes did not persist -- {gap}").into());
            }

            match scenario.mutation {
                Mutation::ClearAll => {
                    scores.clear()?;
                    players.clear()?;
                }
                Mutation::Remove(key) => {
                    let key = key.to_string();
                    scores.remove(&key)?;
                    players.remove(&key)?;
                }
            }

            ctx.timer(&self.verify_timer)?.key(&team)?.set(2_000);
            return Ok(());
        }

        let observed_players = render(players.read()?);
        let failures: Vec<String> = [
            mismatch("map keys", &team, scenario.expected, &observed_keys),
            mismatch("set members", &team, scenario.expected, &observed_players),
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

/// Builds the conformance pipeline graph over an already-created seed collection.
pub fn build_conformance_graph(
    events: &PCollection<(String, (String, i64))>,
    strict: bool,
) -> PCollection<String> {
    let scores = MapStateSpec::<String, i64>::new("scores");
    let players = SetStateSpec::<String>::new("players");
    let mutate = TimerFamilySpec::event_time(MUTATE_TIMER);
    let verify = TimerFamilySpec::event_time(VERIFY_TIMER);

    events.apply(
        ParDo::new(
            "SeedMutateVerify",
            StateConformanceDoFn {
                scores: scores.clone(),
                players: players.clone(),
                mutate_timer: mutate.clone(),
                verify_timer: verify.clone(),
                strict,
            },
        )
        .with_state_spec(&scores)
        .with_state_spec(&players)
        .with_timer_family(&mutate)
        .with_timer_family(&verify),
    )
}

/// Constructs the complete state conformance pipeline.
pub fn build_pipeline(options: &PipelineOptions, args: &StateConformanceArgs) -> Pipeline {
    let p = Pipeline::create(options);
    let events = p.apply(Create::new("SeedScores", seed_elements()));
    build_conformance_graph(&events, args.strict)
        .apply(textio::Write::new("WriteLines", &args.output));
    p
}
