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

//! Fold combinators and state-backed iterable validation example.
//!
//! Demonstrates and contrasts two reduction models:
//!
//! - **Combiner-Backed Fold (`fold_per_key`)**:
//!   Uses an associative and commutative merge function. The runner applies combiner lifting,
//!   which pre-combines locally before the shuffle. This reduces the shuffle volume.
//!
//! - **GroupByKey-Backed Fold (`fold_values`)**:
//!   Folds grouped values in sequence without requiring an associative merge
//!   function. All raw elements cross the shuffle into `GroupByKey`. When a key is heavily
//!   skewed, the iterable exceeds the in-memory shuffle buffer, causing the runner to page
//!   elements through continuation tokens over the Fn API State channel
//!   (`beam:coder:state_backed_iterable:v1`).
//!
//! This example runs both branches on the same synthetic skewed data, joins the results, and
//! verifies exact numerical equivalence across both execution modes.
//!
//! # Running the Example
//!
//! On Prism, the default runner:
//! ```text
//! cargo run -p fold_state_backed -- --output=/tmp/fold_output.txt
//! ```
//!
//! On Prism through Gradle:
//! ```text
//! ./gradlew :sdks:rust:prism -Pexample=fold_state_backed -PextraArgs="--output=/tmp/prism_fold.txt"
//! ```
//!
//! On Google Cloud Dataflow:
//! ```text
//! ./gradlew :sdks:rust:dataflow -Pexample=fold_state_backed -PextraArgs="--num_elements=50000"
//! ```

use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// Options for the fold state-backed iterable pipeline.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(
    name = "fold_state_backed",
    about = "Apache Beam Rust SDK Fold Combinators and State-Backed Iterable Example",
    version
)]
pub struct Args {
    /// Number of elements to generate under the primary hot key.
    ///
    /// Values above 10,000 typically trigger Dataflow's state-backed continuation tokens.
    #[arg(long, alias = "numElements", default_value_t = 50000)]
    pub num_elements: i64,

    /// Byte size of the dummy string payload attached to each element.
    ///
    /// Exceeding 2MB total shuffled payload (e.g. 50,000 * 128 bytes = 6.4MB) guarantees
    /// Dataflow's in-memory shuffle buffer is exceeded, triggering state-backed continuation tokens.
    #[arg(long, alias = "payloadBytes", default_value_t = 128)]
    pub payload_bytes: usize,

    /// Destination path for the verification output.
    #[arg(long, default_value = "/tmp/fold_state_backed_output.txt")]
    pub output: String,
}

impl PipelineOptionGroup for Args {}

/// Accumulator tracking element count and sum of values.
pub type FoldStats = (i64, i64);

/// Formats the side-by-side comparison of combiner fold and state-backed fold.
///
/// Returns `Err` when the two branches disagree, or when neither matches the closed-form
/// expected sum. Returning an error fails the bundle and the job: a verification
/// that only logged its verdict would let the pipeline exit 0 having proven nothing.
pub fn format_verification(
    key: &str,
    combiner_stats: FoldStats,
    state_backed_stats: FoldStats,
) -> Result<String> {
    let (c_count, c_sum) = combiner_stats;
    let (s_count, s_sum) = state_backed_stats;

    // Gauss summation for 0..count-1.
    let expected_sum = if c_count > 0 {
        (c_count - 1) * c_count / 2
    } else {
        0
    };

    let is_correct = c_count == s_count && c_sum == s_sum && c_sum == expected_sum;
    let report = format!(
        "{status}|key={key}|elements={c_count}|combiner_sum={c_sum}|state_backed_sum={s_sum}|expected_sum={expected_sum}",
        status = if is_correct { "MATCH" } else { "MISMATCH" }
    );

    if !is_correct {
        return Err(format!("State-backed fold verification failed: {report}").into());
    }
    tracing::info!("Verification result: {report}");
    Ok(report)
}

/// Constructs the fold state-backed iterable pipeline from [`Args`].
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);

    // Seed keys: 1 large hot key to test state-backed paging, plus normal small keys.
    let seed_keys = p.apply(Create::new(
        "SeedKeys",
        vec![
            "hot_key".to_string(),
            "normal_key_1".to_string(),
            "normal_key_2".to_string(),
        ],
    ));

    // Generate elements per key with optional dummy string payload to inflate shuffle volume.
    let num_elements = args.num_elements;
    let padding = "x".repeat(args.payload_bytes);
    let elements = seed_keys.flat_map("GenerateElements", move |key: String| {
        let count = if key == "hot_key" {
            num_elements
        } else if key == "normal_key_1" {
            10
        } else {
            100
        };
        let pad = padding.clone();
        (0..count)
            .map(move |i| (key.clone(), (i, pad.clone())))
            .collect::<Vec<_>>()
    });

    // Combiner-backed fold with associative merge.
    let combiner_fold = elements.fold_per_key(
        "CombinerFold",
        (0i64, 0i64),
        |(count, sum), (val, _): (i64, String)| (count + 1, sum + val),
        |(c1, s1), (c2, s2)| (c1 + c2, s1 + s2),
    );

    // GroupByKey-backed fold with sequential reduction.
    let state_backed_fold = elements.fold_values(
        "StateBackedFold",
        (0i64, 0i64),
        |(count, sum), (val, _): (i64, String)| (count + 1, sum + val),
    );

    // Join both results on key and verify equivalence.
    let comparison = combiner_fold.inner_join("CompareFolds", &state_backed_fold);

    let formatted = comparison.par_do_fn(
        "FormatResults",
        |(key, (combiner_stats, state_backed_stats)): (String, (FoldStats, FoldStats)), ctx| {
            ctx.emit(format_verification(
                &key,
                combiner_stats,
                state_backed_stats,
            )?)
        },
    );

    formatted.apply(textio::Write::new("WriteLines", &args.output));

    p
}
