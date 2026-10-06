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
use side_inputs::CountLongWords;

/// The mean length is 33 / 8: words of 5 letters or more stay, except the stopword.
#[tokio::test]
async fn counts_words_longer_than_the_mean_without_stopwords() {
    let p = TestPipeline::new();
    let lines = ["the king, the KING!", "should never wander", "be"].map(String::from);

    let counts = p.apply("Lines", Create::new(lines.to_vec())).apply(
        "CountLongWords",
        CountLongWords {
            stopwords: vec!["should".to_string()],
        },
    );

    passert::that("AssertCounts", &counts)
        .contains_in_any_order([("never".to_string(), 1), ("wander".to_string(), 1)]);

    p.run().await.expect("pipeline and assertions must pass");
}
