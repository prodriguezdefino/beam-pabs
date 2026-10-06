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

//! A minimal example demonstrating Side Inputs and Broadcast Joins using `beam-fluent`.
//!
//! This pipeline:
//!
//! - Reads lines of Shakespeare's *King Lear* from GCS (`textio::Read`).
//! - Uses an **Iterable Side Input** (`as_iter`) to filter out common stopwords.
//! - Uses a **Singleton Side Input** (`as_singleton`) to keep only words meeting a
//!   dynamically supplied minimum length threshold.
//! - Counts word occurrences (`count_per_element`) and enriches known character names
//!   with their dramatic role using a shuffle-free **Broadcast Left Join**
//!   (`broadcast_left_join`).
//! - Writes the formatted results to `--output`.
//!
//! Run on Prism, the default runner:
//!
//! ```text
//! cargo run -p side_inputs -- --output=/tmp/side_inputs.txt
//! ```
//!
//! To run it on Google Cloud Dataflow:
//!
//! ```text
//! cargo run -p side_inputs -- \
//!   --runner=dataflow \
//!   --project=my-gcp-project \
//!   --region=us-central1 \
//!   --temp_location=gs://my-bucket/temp \
//!   --output=gs://my-bucket/side_inputs.txt
//! ```

use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(
    name = "side_inputs",
    about = "Apache Beam side inputs and broadcast join example",
    version
)]
struct Args {
    /// Path of the file to read from.
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/shakespeare/kinglear.txt"
    )]
    input: String,

    /// Path of the file to write to.
    #[arg(long, default_value = "/tmp/side_inputs.txt")]
    output: String,
}

impl PipelineOptionGroup for Args {}

/// Words too common to be interesting, supplied to the filter as an iterable side input.
const STOPWORDS: [&str; 5] = ["should", "would", "though", "before", "within"];

/// A small lookup table broadcast to every worker instead of being shuffled.
const CHARACTER_ROLES: [(&str, &str); 4] = [
    ("cordelia", "Princess of Britain"),
    ("gloucester", "Earl of Gloucester"),
    ("edmund", "Gloucester's Son"),
    ("goneril", "Lear's Eldest Daughter"),
];

/// Splits a line of text into lowercased alphabetic words.
fn extract_words(line: String) -> Vec<String> {
    line.split(|c: char| !c.is_alphabetic())
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect()
}

/// Builds the side input example pipeline.
fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);

    // Singleton side input: a minimum word length computed elsewhere in the pipeline.
    let min_len = p
        .apply(Create::new("MinWordLength", vec![6_i64]))
        .as_singleton();

    // Iterable side input: the stopword list to exclude.
    let stopwords = p
        .apply(Create::new(
            "Stopwords",
            STOPWORDS.map(str::to_string).to_vec(),
        ))
        .as_iter();

    // Broadcast side of a shuffle-free join.
    let character_roles = p.apply(Create::new(
        "CharacterRoles",
        CHARACTER_ROLES
            .map(|(name, role)| (name.to_string(), role.to_string()))
            .to_vec(),
    ));

    let (min_len_ref, stopwords_ref) = (min_len.clone(), stopwords.clone());

    p.apply(textio::Read::new("ReadLines", &args.input))
        .flat_map("ExtractWords", extract_words)
        // Both side inputs are read through the same `ProcessContext`.
        .apply(
            ParDo::from_fn("FilterByLengthAndStopwords", move |word: String, ctx| {
                let min_len = ctx.side_input(&min_len_ref)? as usize;
                let stopwords = ctx.side_input_iter(&stopwords_ref)?;
                if word.len() >= min_len && !stopwords.contains(&word) {
                    ctx.emit(word)
                } else {
                    Ok(())
                }
            })
            .with_side_input(&min_len)
            .with_side_input(&stopwords),
        )
        .count_per_element("CountWords")
        // Enrich with character roles without a GroupByKey shuffle.
        .broadcast_left_join("EnrichCharacterRoles", &character_roles)
        .map("FormatResults", |(word, (count, role))| match role {
            Some(role) => format!("{word} [{role}]: {count}"),
            None => format!("{word}: {count}"),
        })
        .apply(textio::Write::new("WriteLines", &args.output));

    p
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::harness::init_logging();

    let (options, args) = beam::options::parse::<Args>();

    let pipeline = build_pipeline(&options, &args);
    pipeline.run().await?;

    Ok(())
}
