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

//! Minimal WordCount example.
//!
//! The first of four examples, each with more detail than the one before. This is the
//! simplest pipeline that counts words in Shakespeare. Paths are hardcoded. It has no
//! pipeline options, no composite transforms and no tests.
//! See `wordcount` for a complete example with CLI options.
//!
//! Concepts:
//! - Creating a pipeline
//! - Reading text files
//! - Applying element-wise transforms
//! - Counting elements in a PCollection
//! - Writing text files
//!
//! Reading from `gs://` uses the Google Cloud Storage filesystem from the default `gcs`
//! feature of `beam` (crate `apache-beam-io-gcp`).

use beam::prelude::*;

const INPUT: &str = "gs://apache-beam-samples/shakespeare/kinglear.txt";
const OUTPUT: &str = "/tmp/minimal_wordcount.txt";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let p = Pipeline::new();

    // Read lines, count word occurrences, and format one "word: count" line per word.
    let counts = p
        .apply(textio::Read::new("ReadLines", INPUT))
        .flat_map("ExtractWords", |line: String| {
            line.split_whitespace()
                .map(str::to_lowercase)
                .filter(|w| !w.is_empty())
                .collect::<Vec<_>>()
        })
        .count_per_element("CountWords")
        .map("FormatCounts", |(word, count): (String, i64)| {
            format!("{word}: {count}")
        });

    counts.apply(textio::Write::new("WriteLines", OUTPUT));

    p.run().await?;

    Ok(())
}
