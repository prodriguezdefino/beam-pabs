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

//! Tests for `passert::assertion_status` and `verify_assertions`: how the `PAssert`
//! counters that a runner reports give the status of the assertions of a pipeline.

use beam::metrics::{MetricKey, MetricPhase, MetricReading, MetricResults, MetricValue};
use beam::runners::PipelineResult;
use testing::passert::{
    AssertionStatus, FAILURE_COUNTER, PASSERT_NAMESPACE, SUCCESS_COUNTER, assertion_status,
    failure_counter_name, success_counter_name, verify_assertions,
};

/// Committed counters `(namespace, name, value)`, all reported by one step.
fn counters(cells: &[(&str, &str, i64)]) -> MetricResults {
    cells
        .iter()
        .map(|&(namespace, name, value)| MetricReading {
            key: MetricKey::new("step", namespace, name),
            phase: MetricPhase::Committed,
            value: MetricValue::Counter(value),
        })
        .collect()
}

fn pending(passed: usize, missing: &[&str]) -> AssertionStatus {
    AssertionStatus::Pending {
        passed,
        missing: missing.iter().map(ToString::to_string).collect(),
    }
}

#[test]
fn assertion_status_decides_from_per_assertion_counters() {
    let ns = PASSERT_NAMESPACE;
    let ok_a = success_counter_name("A");
    let ok_b = success_counter_name("B");
    let fail_b = failure_counter_name("B");
    let cases = [
        (
            "all passed",
            counters(&[(ns, &ok_a, 1), (ns, &ok_b, 2)]),
            AssertionStatus::AllPassed(2),
        ),
        (
            "partially passed, though the aggregate count matches",
            counters(&[(ns, SUCCESS_COUNTER, 2), (ns, &ok_a, 2)]),
            pending(1, &["B"]),
        ),
        ("nothing reported", counters(&[]), pending(0, &["A", "B"])),
        (
            "aggregate counter only",
            counters(&[(ns, SUCCESS_COUNTER, 2)]),
            pending(0, &["A", "B"]),
        ),
        (
            "zero is not a pass",
            counters(&[(ns, &ok_a, 0), (ns, &ok_b, 1)]),
            pending(1, &["A"]),
        ),
        (
            "named failure",
            counters(&[(ns, &ok_a, 1), (ns, &fail_b, 1), (ns, FAILURE_COUNTER, 1)]),
            AssertionStatus::Failed {
                failed: vec!["B".into()],
                total: 1,
            },
        ),
        (
            "aggregate failure only",
            counters(&[(ns, &ok_a, 1), (ns, &ok_b, 1), (ns, FAILURE_COUNTER, 1)]),
            AssertionStatus::Failed {
                failed: vec![],
                total: 1,
            },
        ),
        (
            "other namespaces are ignored",
            counters(&[(ns, &ok_a, 1), ("user", &ok_b, 1)]),
            pending(1, &["B"]),
        ),
        (
            "names are matched exactly, not case-insensitively",
            counters(&[(ns, &ok_a, 1), (ns, "passertsuccess/B", 1)]),
            pending(1, &["B"]),
        ),
        (
            "the namespace is matched exactly",
            counters(&[(ns, &ok_a, 1), ("passert", &ok_b, 1), ("", &ok_b, 1)]),
            pending(1, &["B"]),
        ),
        (
            "a name that only starts like a counter is not one",
            counters(&[(ns, &ok_a, 1), (ns, "PAssertSuccessB", 1)]),
            pending(1, &["B"]),
        ),
    ];
    // Duplicate and unsorted names are accepted: the status lists each name once, sorted.
    for (case, metrics, expected) in cases {
        assert_eq!(
            assertion_status(&metrics, &["B", "A", "A"]),
            expected,
            "{case}"
        );
    }
}

#[test]
fn an_assertion_name_may_contain_a_slash() {
    let metrics = counters(&[(PASSERT_NAMESPACE, &success_counter_name("Outer/Inner"), 1)]);
    assert_eq!(
        assertion_status(&metrics, &["Outer/Inner"]),
        AssertionStatus::AllPassed(1)
    );
}

#[test]
fn per_assertion_counters_are_summed_across_steps() {
    let metrics: MetricResults = [("s1", -1), ("s2", 1)]
        .into_iter()
        .map(|(step, value)| MetricReading {
            key: MetricKey::new(step, PASSERT_NAMESPACE, failure_counter_name("A")),
            phase: MetricPhase::Attempted,
            value: MetricValue::Counter(value),
        })
        .collect();
    assert_eq!(assertion_status(&metrics, &["A"]), pending(0, &["A"]));
}

#[test]
fn verify_assertions_reports_each_outcome() {
    let result = |cells: &[(&str, &str, i64)]| {
        PipelineResult::new("job", "DONE").with_metrics(counters(cells))
    };
    let ns = PASSERT_NAMESPACE;
    let ok_a = success_counter_name("A");

    assert_eq!(
        verify_assertions(&result(&[(ns, &ok_a, 1)]), &["A"]),
        Ok(())
    );
    assert_eq!(
        verify_assertions(&result(&[(ns, &failure_counter_name("A"), 1)]), &["A"]),
        Err(r#"PAssert assertion(s) failed: ["A"]"#.to_string())
    );
    assert_eq!(
        verify_assertions(&result(&[(ns, FAILURE_COUNTER, 2)]), &["A"]),
        Err("2 PAssert assertion(s) failed".to_string())
    );
    let never_ran =
        verify_assertions(&result(&[(ns, &ok_a, 1)]), &["A", "B"]).expect_err("B never ran");
    assert!(
        never_ran.starts_with(r#"1 of 2 PAssert assertion(s) never ran: ["B"]."#),
        "{never_ran}"
    );
    let double_ok =
        verify_assertions(&result(&[(ns, &ok_a, 2)]), &["A", "B"]).expect_err("B never ran");
    assert!(
        double_ok.starts_with(r#"1 of 2 PAssert assertion(s) never ran: ["B"]"#),
        "{double_ok}"
    );
    assert_eq!(
        verify_assertions(&PipelineResult::new("job", "DONE"), &["A"]),
        Err("the runner reported no metrics for this pipeline".to_string())
    );
}
