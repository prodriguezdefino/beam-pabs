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

//! Tests for the operator chain's handler-instance slice (`Instances` / `Downstream`)
//! and the per-transform execution-time sampler.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beam::internals::HandlerContext;
use beam::internals::{BundleHandler, HandlerInstance};
use harness::bundle_processor::{BundleError, ExecutionSampler, Instances};

/// Records lifecycle calls in a log shared with the test; fails setup if asked to.
#[derive(Clone)]
struct Probe {
    name: &'static str,
    fail_setup: bool,
    log: Arc<Mutex<Vec<String>>>,
}

impl Probe {
    fn record(&self, event: &str) {
        self.log
            .lock()
            .expect("log lock")
            .push(format!("{event}:{}", self.name));
    }
}

impl BundleHandler for Probe {
    fn setup(&mut self) -> Result<(), String> {
        self.record("setup");
        if self.fail_setup {
            Err("boom".into())
        } else {
            Ok(())
        }
    }

    fn process(&mut self, _: &[u8], _: &mut HandlerContext<'_>) -> Result<(), String> {
        Ok(())
    }

    fn teardown(&mut self) -> Result<(), String> {
        self.record("teardown");
        Ok(())
    }

    fn instantiate(&self) -> HandlerInstance {
        Box::new(self.clone())
    }
}

fn instances(fail_at: Option<usize>) -> (Instances, Arc<Mutex<Vec<String>>>) {
    let log = Arc::new(Mutex::new(Vec::new()));
    let handlers = ["a", "b", "c"]
        .into_iter()
        .enumerate()
        .map(|(i, name)| -> HandlerInstance {
            Box::new(Probe {
                name,
                fail_setup: fail_at == Some(i),
                log: Arc::clone(&log),
            })
        })
        .collect();
    (Instances::new(handlers), log)
}

fn ids() -> Vec<String> {
    ["a", "b", "c"].map(String::from).to_vec()
}

fn take(log: &Mutex<Vec<String>>) -> Vec<String> {
    std::mem::take(&mut *log.lock().expect("log lock"))
}

#[test]
fn split_lends_only_the_operators_after_the_invoked_one() {
    let (mut instances, log) = instances(None);
    let mut all = instances.all();

    // Each split lends the probe at the requested position, named by its log entry.
    let (first, mut after_first) = all.split(0).expect("operator 0 is reachable");
    first.teardown().unwrap();
    let (second, mut after_second) = after_first.split(1).expect("operator 1 is downstream of 0");
    second.teardown().unwrap();
    let (third, mut after_third) = after_second
        .split(2)
        .expect("operator 2 is downstream of 1");
    third.teardown().unwrap();
    assert_eq!(take(&log), ["teardown:a", "teardown:b", "teardown:c"]);

    // Nothing is downstream of the last operator.
    assert!(matches!(
        after_third.split(2),
        Err(BundleError::InvalidGraph(_))
    ));
}

#[test]
fn split_can_skip_ahead_and_reborrow() {
    let (mut instances, log) = instances(None);
    let mut all = instances.all();

    // Operator 0 pushing straight to operator 2 skips 1.
    {
        let mut again = all.reborrow();
        let (handler, _) = again
            .split(2)
            .expect("operator 2 is reachable from the root");
        handler.teardown().unwrap();
    }
    // The reborrow ended, so the root view still lends operator 1.
    let (handler, _) = all.split(1).expect("root still lends operator 1");
    handler.teardown().unwrap();
    assert_eq!(take(&log), ["teardown:c", "teardown:b"]);
}

#[test]
fn split_rejects_the_invoking_operator_and_those_before_it() {
    let (mut instances, _) = instances(None);
    let mut all = instances.all();
    let (_, mut after_second) = all.split(1).expect("operator 1 is reachable");
    for upstream in [0, 1] {
        let err = after_second
            .split(upstream)
            .err()
            .expect("upstream is rejected");
        assert!(
            matches!(&err, BundleError::InvalidGraph(m)
                if m == &format!("operator {upstream} is not downstream of the operator invoking it (operators 2 onwards are)")),
            "{err}"
        );
    }
    assert!(matches!(
        after_second.split(3),
        Err(BundleError::InvalidGraph(_))
    ));
}

#[test]
fn setup_and_teardown_run_in_topological_order() {
    let (mut instances, log) = instances(None);
    instances.setup(&ids()).expect("all set up");
    instances.teardown(&ids());
    assert_eq!(
        take(&log),
        [
            "setup:a",
            "setup:b",
            "setup:c",
            "teardown:a",
            "teardown:b",
            "teardown:c"
        ]
    );
}

#[test]
fn failed_setup_tears_down_only_the_instances_already_set_up() {
    let (mut instances, log) = instances(Some(1));
    let result = instances.setup(&ids());
    assert!(
        matches!(&result, Err(BundleError::Setup(m)) if m == "transform 'b': boom"),
        "{:?}",
        result.err()
    );
    assert_eq!(take(&log), ["setup:a", "setup:b", "teardown:a"]);
}

#[test]
fn sampler_charges_time_to_the_innermost_transform() {
    let ids = vec!["outer".to_string(), "inner".to_string()];
    let sampler = ExecutionSampler::new("inst", &ids);

    let outer = sampler.enter(0);
    let inner = sampler.enter(1);
    std::thread::sleep(Duration::from_millis(60));
    sampler.exit(inner);
    sampler.exit(outer);

    let msecs: HashMap<_, _> = sampler.msecs(&ids).collect();
    assert!(msecs.get("inner").copied().unwrap_or(0) >= 20, "{msecs:?}");
    assert!(!msecs.contains_key("outer"), "{msecs:?}");
}

#[test]
fn sampler_stops_charging_once_the_transform_exits() {
    let ids = vec!["t".to_string()];
    let sampler = ExecutionSampler::new("inst", &ids);

    // Prove that the sampler runs, so the idle check cannot pass only because nothing is sampled.
    let previous = sampler.enter(0);
    std::thread::sleep(Duration::from_millis(60));
    sampler.exit(previous);
    let charged: HashMap<_, _> = sampler.msecs(&ids).collect();
    let charged = charged
        .get("t")
        .copied()
        .expect("the running transform was charged");
    assert!(charged >= 20, "{charged}");

    // Idle time after exit is charged to nobody.
    std::thread::sleep(Duration::from_millis(60));
    let after: HashMap<_, _> = sampler.msecs(&ids).collect();
    assert_eq!(after.get("t").copied(), Some(charged));
}

#[test]
fn sampler_never_charges_more_than_the_wall_time_elapsed() {
    // A tick may charge time from just before `enter`, and load can stretch it past 10ms.
    const SLACK_MS: u128 = 250;
    let ids = vec!["t".to_string()];
    let started = std::time::Instant::now();
    let sampler = ExecutionSampler::new("inst", &ids);

    let previous = sampler.enter(0);
    std::thread::sleep(Duration::from_millis(60));
    sampler.exit(previous);
    // Let any tick already in progress land before reading.
    std::thread::sleep(Duration::from_millis(30));
    let wall_ms = started.elapsed().as_millis();

    let msecs: HashMap<_, _> = sampler.msecs(&ids).collect();
    let charged = msecs
        .get("t")
        .copied()
        .expect("the running transform was charged");
    assert!(
        u128::try_from(charged).unwrap() <= wall_ms + SLACK_MS,
        "charged {charged}ms in {wall_ms}ms of wall time"
    );
}

#[test]
fn time_in_current_call_tracks_a_call_that_does_not_progress() {
    let ids = vec!["outer".to_string(), "inner".to_string()];
    let sampler = ExecutionSampler::new("inst", &ids);
    assert_eq!(sampler.time_in_current_call(), None, "idle");

    let outer = sampler.enter(0);
    std::thread::sleep(Duration::from_millis(80));
    let stuck = sampler
        .time_in_current_call()
        .expect("inside a transform call");
    assert!(stuck >= Duration::from_millis(40), "{stuck:?}");

    // Entering another call is progress, so the count starts over.
    let inner = sampler.enter(1);
    std::thread::sleep(Duration::from_millis(30));
    let fresh = sampler
        .time_in_current_call()
        .expect("inside a transform call");
    assert!(fresh < stuck, "{fresh:?} >= {stuck:?}");

    sampler.exit(inner);
    sampler.exit(outer);
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(sampler.time_in_current_call(), None, "idle again");
}
