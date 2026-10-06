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
use partition::{GradeStudents, Student};

/// Scores at the tier limits (80 and 50) go to the higher tier.
#[tokio::test]
async fn grades_students_by_tier() {
    let p = TestPipeline::new();
    let students = ["Alice,95", "Bob,80", "Carol,79", "Dave,50", "Erin,49"]
        .map(|line| line.parse::<Student>().expect("valid test line"));

    let grades = p
        .apply("Students", Create::new(students.to_vec()))
        .apply("GradeStudents", GradeStudents);

    passert::that("AssertGrades", &grades).contains_in_any_order(
        [
            "honours: Alice (95) with distinction",
            "honours: Bob (80) with distinction",
            "pass: Carol (79)",
            "pass: Dave (50)",
            "support: Erin (49), needs a tutor",
        ]
        .map(String::from),
    );

    p.run().await.expect("pipeline and assertions must pass");
}
