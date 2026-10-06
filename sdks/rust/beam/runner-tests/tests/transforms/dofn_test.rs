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
//! Tests for user-defined [`DoFn`] implementations.

use beam::prelude::*;
use beam::transforms::ProcessContext;
use beam_runner_prism::PrismRunner;

/// Fails on a specific element.
#[derive(Clone)]
struct FailOn {
    bad: i64,
}

impl DoFn for FailOn {
    type In = i64;
    type Out = i64;

    fn process_element(&mut self, n: i64, out: &mut ProcessContext<i64>) -> Result {
        if n == self.bad {
            return Err(format!("refusing to process {n}").into());
        }
        out.emit(n)
    }
}

#[tokio::test]
async fn test_custom_dofn_error_fails_the_pipeline() {
    let p = Pipeline::new();

    p.apply("Create", Create::new(vec![1i64, 2, 3]))
        .apply("FailOnTwo", ParDo::new(FailOn { bad: 2 }));

    let err = p
        .run_with_runner(&PrismRunner::new())
        .await
        .expect_err("an error returned from DoFn::process_element must fail the pipeline");
    let message = format!("{err:?}");
    assert!(
        message.contains("refusing to process 2"),
        "the job error must carry the DoFn's own error: {message}"
    );
}

/// The fluent `par_do` runs a `StatefulDoFn` with its state and timers.
#[tokio::test]
async fn test_fluent_par_do_runs_a_stateful_dofn() {
    use beam::testing::{TestPipeline, passert};
    use beam_runner_tests::dofns::StatefulTeamMilestoneDoFn;

    let p = TestPipeline::new();
    let milestones = p
        .apply(
            "ScoreEvents",
            Create::new(vec![
                ("red".to_string(), ("alice".to_string(), 60i64)),
                ("red".to_string(), ("bob".to_string(), 50i64)),
                ("blue".to_string(), ("carol".to_string(), 40i64)),
            ]),
        )
        .par_do("StatefulTeamMilestone", StatefulTeamMilestoneDoFn);
    passert::that("AssertMilestones", &milestones).contains_in_any_order(
        [
            "milestone:red:110:alice+bob",
            "timer_flush:blue:40",
            "timer_flush:red:110",
        ]
        .map(String::from),
    );

    p.run_with(&PrismRunner::new())
        .await
        .expect("a stateful DoFn through the fluent par_do must succeed");
}
