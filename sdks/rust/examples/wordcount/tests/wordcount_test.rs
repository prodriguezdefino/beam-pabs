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
use wordcount::CountWords;

/// `CountWords` splits on every non-letter, keeps the case and ignores empty lines.
#[tokio::test]
async fn count_words_counts_each_word() {
    let p = TestPipeline::new();
    let lines = ["hi there", "hi, sue's bob!", "", "  ", "Hi 42 bob"].map(String::from);

    let counts = p
        .apply("Create", Create::new(lines.to_vec()))
        .apply("CountWords", CountWords);

    passert::that("AssertCounts", &counts).contains_in_any_order(
        [
            ("hi", 2),
            ("Hi", 1),
            ("there", 1),
            ("sue", 1),
            ("s", 1),
            ("bob", 2),
        ]
        .map(|(word, count)| (word.to_string(), count)),
    );

    p.run().await.expect("pipeline and assertions must pass");
}
