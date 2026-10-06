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

//! WordCount example library for the Apache Beam Rust SDK.
//!
//! Exposes pipeline definitions, custom options, and transform functions
//! used by the WordCount example binary and integration tests.

use beam::prelude::*;
use clap::Args as ClapArgs;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(name = "wordcount", about = "Apache Beam WordCount example", version)]
pub struct Args {
    /// Path of the file to read from.
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/shakespeare/kinglear.txt"
    )]
    pub input: String,

    /// Path of the file to write to.
    #[arg(long, default_value = "/tmp/output.txt")]
    pub output: String,
}

impl PipelineOptionGroup for Args {}

/// Splits a line of text into words.
///
/// Words are separated by `[^\p{L}]+`: any run of characters that are not Unicode letters.
pub fn extract_words(line: &str) -> impl Iterator<Item = String> + '_ {
    static SEPARATOR: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"[^\p{L}]+").expect("valid tokenizer pattern"));

    Metrics::counter("wordcount", "line_count").inc();
    Metrics::distribution("wordcount", "line_len_dist").update(line.len() as i64);

    let trimmed = line.trim();
    if trimmed.is_empty() {
        Metrics::counter("wordcount", "empty_lines").inc();
    }

    SEPARATOR
        .split(trimmed)
        .filter(|w| !w.is_empty())
        .map(str::to_string)
}

/// Formats a word and its count as a single output line.
pub fn format_counts(word: &str, count: i64) -> String {
    Metrics::counter("wordcount", "unique_words").inc();
    Metrics::distribution("wordcount", "word_frequency_dist").update(count);
    Metrics::gauge("wordcount", "last_word_frequency").set(count);
    format!("{word}: {count}")
}

/// A composite transform counting the occurrences of each word.
///
/// Bundling the counting logic into a composite keeps it reusable and, more
/// importantly, directly testable.
pub struct CountWords;

impl PTransform<PCollection<String>> for CountWords {
    type Output = PCollection<(String, i64)>;

    fn expand(&self, lines: &PCollection<String>) -> Self::Output {
        lines
            .par_do_fn("ExtractWords", |line: String, out| {
                extract_words(&line).try_for_each(|word| out.emit(word))
            })
            .count_per_element("CountWords")
    }
}

/// Builds the WordCount pipeline from [`Args`], run with `options`.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);

    let lines = p.apply(textio::Read::new("ReadLines", &args.input));
    let counted = lines.apply(CountWords);
    let formatted = counted.map("FormatCounts", |(word, count): (String, i64)| {
        format_counts(&word, count)
    });
    formatted.apply(textio::Write::new("WriteLines", &args.output));

    p
}
