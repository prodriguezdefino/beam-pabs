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

//! Word counts stored in Bigtable through the cross-language Bigtable transforms.
//!
//! - `--mode=write` (the default) counts the words of `--input` with `wordcount::CountWords`.
//!   It writes one Bigtable row for each word through the Java `bigtable_write`
//!   SchemaTransform. The row key is the word, and `<column_family>:count` holds the count
//!   as a decimal string.
//! - `--mode=read` reads the table through the Java `bigtable_read` SchemaTransform. It
//!   writes `word: count` lines to `--output`.
//!
//! Create the table and its column family before a write:
//!
//! ```text
//! cbt -project my-project -instance my-instance createtable wordcount families=counts
//! ```

use beam::io::gcp::bigtable::{
    BigtableColumn, BigtableMutation, BigtableRead, BigtableWrite, mutation_schema,
};
use beam::pipeline::PipelineError;
use beam::prelude::*;
use clap::{Args as ClapArgs, ValueEnum};
use serde::{Deserialize, Serialize};
use wordcount::CountWords;

/// Column qualifier of the count in the row of each word.
pub const COUNT_QUALIFIER: &str = "count";

/// The direction in which the pipeline moves the counts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Count the words of `--input` and write the counts to Bigtable.
    Write,
    /// Read the counts from Bigtable and write them as text to `--output`.
    Read,
}

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(
    name = "bigtable_wordcount",
    about = "Apache Beam Bigtable WordCount example"
)]
pub struct Args {
    /// Write the word counts to Bigtable, or read them back.
    #[arg(long, value_enum, default_value = "write")]
    pub mode: Mode,

    /// Text file to count the words of, with `--mode=write`.
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/shakespeare/kinglear.txt"
    )]
    pub input: String,

    /// Prefix of the text files to write the `word: count` lines to. Required with `--mode=read`.
    #[arg(long, required_if_eq("mode", "read"))]
    pub output: Option<String>,

    /// Google Cloud project of the Bigtable instance.
    #[arg(long)]
    pub bigtable_project: String,

    /// Bigtable instance ID.
    #[arg(long)]
    pub bigtable_instance: String,

    /// Bigtable table ID.
    #[arg(long)]
    pub bigtable_table: String,

    /// Column family of the counts. It must exist in the table.
    #[arg(long, default_value = "counts")]
    pub column_family: String,

    /// Address (host:port) of an expansion service to use instead of the Java one that the SDK starts.
    #[arg(long)]
    pub expansion_service: Option<String>,
}

impl PipelineOptionGroup for Args {}

/// The mutation that stores `count` for `word`.
pub fn count_mutation(family: &str, word: &str, count: i64) -> BigtableMutation {
    BigtableMutation::set_cell(
        word.as_bytes(),
        family,
        COUNT_QUALIFIER.as_bytes(),
        count.to_string().into_bytes(),
    )
}

/// `(word, count)` of a column that the Bigtable read returns.
///
/// Returns `None` for a column other than `<family>:count`, or for a value that is not a number.
pub fn decode_count(family: &str, column: BigtableColumn) -> Option<(String, i64)> {
    if column.family_name != family || column.column_qualifier != COUNT_QUALIFIER.as_bytes() {
        return None;
    }
    let count = std::str::from_utf8(column.latest_value()?)
        .ok()?
        .parse()
        .ok()?;
    let word = String::from_utf8(column.key).ok()?;
    Some((word, count))
}

/// Counts the words of text lines, as one Bigtable mutation for each word.
pub struct CountWordsToMutations {
    /// Column family of the counts.
    pub column_family: String,
}

impl PTransform<PCollection<String>> for CountWordsToMutations {
    type Output = PCollection<Row>;

    fn expand(&self, lines: &PCollection<String>) -> Self::Output {
        // The closure must own its data, because the runner calls it after `expand` returns.
        let family = self.column_family.clone();
        lines
            .apply("CountWords", CountWords)
            .map("ToMutations", move |(word, count): (String, i64)| {
                Row::from(count_mutation(&family, &word, count))
            })
            .with_row_schema(&mutation_schema())
    }
}

/// Builds the pipeline for the [`Mode`] of `args`, run with `options`.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);
    match (args.mode, args.output.as_deref()) {
        (Mode::Write, _) => build_write(&p, args),
        (Mode::Read, Some(output)) => build_read(&p, args, output),
        (Mode::Read, None) => p.record_construction_error(PipelineError::InvalidConstruction {
            transform_id: "WriteLines".to_string(),
            reason: "--output is required with --mode=read".to_string(),
        }),
    }
    p
}

fn build_write(p: &Pipeline, args: &Args) {
    let write = BigtableWrite::new(
        &args.bigtable_project,
        &args.bigtable_instance,
        &args.bigtable_table,
    );
    let write = match &args.expansion_service {
        Some(service) => write.with_expansion_service(service),
        None => write,
    };
    p.apply("ReadLines", textio::Read::new(&args.input))
        .apply(
            "CountWordsToMutations",
            CountWordsToMutations {
                column_family: args.column_family.clone(),
            },
        )
        .apply("WriteCounts", write);
}

fn build_read(p: &Pipeline, args: &Args, output: &str) {
    let read = BigtableRead::new(
        &args.bigtable_project,
        &args.bigtable_instance,
        &args.bigtable_table,
    );
    let read = match &args.expansion_service {
        Some(service) => read.with_expansion_service(service),
        None => read,
    };
    // The closure must own its data, because the runner calls it after the pipeline is built.
    let family = args.column_family.clone();
    p.apply("ReadCounts", read)
        .flat_map("DecodeCounts", move |row: Row| {
            BigtableColumn::try_from(row)
                .ok()
                .and_then(|column| decode_count(&family, column))
                .map(|(word, count)| format!("{word}: {count}"))
        })
        .apply("WriteLines", textio::Write::new(output));
}
