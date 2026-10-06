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

//! Cross-language Bigtable WordCount example.
//!
//! Two modes exercise both halves of the connector:
//!
//! - `--mode=write` (default): reads a text file, counts words in Rust, and writes
//!   one Bigtable row per word through the Java `bigtable_write` SchemaTransform. The row
//!   key is the word; the count is stored as a decimal string in
//!   `<column_family>:count`.
//! - `--mode=read`: reads the table back through the Java `bigtable_read` SchemaTransform,
//!   decodes each column in Rust, and writes `word: count` lines to `--output`.
//!
//! The table and its column family must exist before a write, e.g.:
//!
//! ```text
//! cbt -project my-project -instance my-instance createtable wordcount families=counts
//! ```

use std::sync::LazyLock;

use beam::io::gcp::bigtable::{
    BigtableColumn, BigtableMutation, BigtableRead, BigtableWrite, mutation_schema,
};
use beam::prelude::*;
use clap::{Args as ClapArgs, ValueEnum};
use regex::Regex;
use serde::{Deserialize, Serialize};

/// Column qualifier holding the count in each word's row.
pub const COUNT_QUALIFIER: &str = "count";

/// Which direction the pipeline moves data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Count words from `--input` and write them to Bigtable.
    Write,
    /// Read counts from Bigtable and write them as text to `--output`.
    Read,
}

/// Command-line arguments for the Bigtable WordCount example.
#[derive(ClapArgs, Serialize, Deserialize, Debug, Clone)]
#[command(
    name = "bigtable_wordcount",
    about = "Apache Beam Rust SDK cross-language Bigtable WordCount example"
)]
pub struct Args {
    /// Whether to write word counts to Bigtable or read them back.
    #[arg(long, value_enum, default_value = "write")]
    pub mode: Mode,

    /// Text file to count words in (write mode).
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/shakespeare/kinglear.txt"
    )]
    pub input: String,

    /// Text file prefix to write `word: count` lines to (read mode).
    #[arg(long)]
    pub output: Option<String>,

    /// Google Cloud project containing the Bigtable instance.
    #[arg(long, alias = "bigtableProject")]
    pub bigtable_project: String,

    /// Bigtable instance ID.
    #[arg(long, alias = "bigtableInstance")]
    pub bigtable_instance: String,

    /// Bigtable table ID.
    #[arg(long, alias = "bigtableTable")]
    pub bigtable_table: String,

    /// Column family the counts are stored in. Must already exist in the table.
    #[arg(long, alias = "columnFamily", default_value = "counts")]
    pub column_family: String,

    /// Optional expansion service address override (host:port).
    /// Defaults to the automated Java expansion service if omitted.
    #[arg(long, alias = "expansionService")]
    pub expansion_service: Option<String>,
}

impl PipelineOptionGroup for Args {}

/// Splits a line into words. The separator is `[^\p{L}]+`: any run of non-letter characters.
pub fn extract_words(line: &str) -> impl Iterator<Item = String> + '_ {
    static SEPARATOR: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"[^\p{L}]+").expect("valid tokenizer pattern"));
    SEPARATOR
        .split(line.trim())
        .filter(|w| !w.is_empty())
        .map(str::to_string)
}

/// Builds the mutation storing `count` for `word`.
pub fn count_mutation(family: &str, word: &str, count: i64) -> BigtableMutation {
    BigtableMutation::set_cell(
        word.as_bytes(),
        family,
        COUNT_QUALIFIER.as_bytes(),
        count.to_string().into_bytes(),
    )
}

/// Decodes a column produced by a flattened Bigtable read into `(word, count)`.
///
/// Returns `None` for columns other than `<family>:count` or with unparsable values.
pub fn decode_count(family: &str, column: &BigtableColumn) -> Option<(String, i64)> {
    if column.family_name != family || column.column_qualifier != COUNT_QUALIFIER.as_bytes() {
        return None;
    }
    let word = String::from_utf8(column.key.clone()).ok()?;
    let count = std::str::from_utf8(column.latest_value()?)
        .ok()?
        .parse()
        .ok()?;
    Some((word, count))
}

/// Configures an expansion service override on a builder, if one was given.
macro_rules! with_service {
    ($builder:expr, $args:expr) => {
        match &$args.expansion_service {
            Some(service) => $builder.with_expansion_service(service),
            None => $builder,
        }
    };
}

/// Builds the pipeline for the selected [`Mode`].
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);
    match args.mode {
        Mode::Write => build_write(&p, args),
        Mode::Read => build_read(&p, args),
    }
    p
}

fn build_write(p: &Pipeline, args: &Args) {
    let family = args.column_family.clone();
    p.apply(textio::Read::new("ReadLines", args.input.as_str()))
        .par_do_fn("ExtractWords", |line: String, out| {
            extract_words(&line).try_for_each(|word| out.emit(word))
        })
        .count_per_element("CountWords")
        .map("ToMutations", move |(word, count): (String, i64)| {
            count_mutation(&family, &word, count).to_row()
        })
        .with_row_schema(&mutation_schema())
        .apply(with_service!(
            BigtableWrite::new(
                "BigtableWrite",
                &args.bigtable_project,
                &args.bigtable_instance,
                &args.bigtable_table
            ),
            args
        ));
}

fn build_read(p: &Pipeline, args: &Args) {
    let output = args
        .output
        .as_deref()
        .expect("--output is required with --mode=read");
    let family = args.column_family.clone();
    p.apply(with_service!(
        BigtableRead::new(
            "BigtableRead",
            &args.bigtable_project,
            &args.bigtable_instance,
            &args.bigtable_table
        ),
        args
    ))
    .flat_map("DecodeCounts", move |row: Row| {
        BigtableColumn::from_row(&row)
            .and_then(|column| decode_count(&family, &column))
            .map(|(word, count)| format!("{word}: {count}"))
    })
    .apply(textio::Write::new("WriteLines", output));
}
