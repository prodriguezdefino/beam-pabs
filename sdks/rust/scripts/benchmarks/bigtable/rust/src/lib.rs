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

//! Cloud Bigtable benchmark in a Rust pipeline: the `bt_to_gcs` shape with the native
//! Bigtable connector.
//!
//! The pipeline reads `--bigtable_table` with `ReadRows`, sends each row through a Rust
//! `Touch` step that counts `e2e/rows` and formats the row as a line of text, and writes the
//! lines to `--output` with `textio::Write`. All steps run in one Rust SDK harness.

use std::borrow::Cow;

use beam::io::gcp::bigtable::BigtableColumn;
use beam::io::gcp::bigtable::read::{ReadOptions, ReadRows};
use beam::io::gcp::bigtable::target::ReadTarget;
use beam::metrics::MetricResults;
use beam::options::PipelineOptionGroup;
use beam::prelude::*;
use beam::schema::{FieldValue, MapEntry};
use clap::{Args as ClapArgs, ValueEnum};
use serde::{Deserialize, Serialize};

/// The namespace of the metrics of the `Touch` step.
pub const NAMESPACE: &str = "e2e";

/// The Bigtable connector of the read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum Connector {
    /// The native Cloud Bigtable connector of the Rust SDK.
    Rust,
}

/// The shape of the benchmark.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum Shape {
    /// Reads a Bigtable table and writes formatted text lines to Cloud Storage.
    BtToGcs,
}

/// Benchmark flags. The flags of the runner come from the pipeline options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
pub struct Args {
    /// The connector of the read.
    #[arg(long, value_enum, default_value_t = Connector::Rust)]
    pub reader: Connector,
    /// The shape of the benchmark.
    #[arg(long, value_enum, default_value_t = Shape::BtToGcs)]
    pub shape: Shape,
    /// Google Cloud project ID that holds the Bigtable instance.
    #[arg(long)]
    pub bigtable_project: String,
    /// Cloud Bigtable instance ID.
    #[arg(long)]
    pub bigtable_instance: String,
    /// Cloud Bigtable table ID. Give this or `--bigtable_materialized_view`.
    #[arg(
        long,
        conflicts_with = "bigtable_materialized_view",
        required_unless_present = "bigtable_materialized_view"
    )]
    pub bigtable_table: Option<String>,
    /// Cloud Bigtable materialized view ID to read instead of a table.
    #[arg(long)]
    pub bigtable_materialized_view: Option<String>,
    /// Optional Bigtable app profile ID, such as a Data Boost profile.
    #[arg(long)]
    pub app_profile: Option<String>,
    /// Emit one row per column qualifier (`true`) or one row per Bigtable row (`false`).
    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "true",
        default_value_t = false,
        action = clap::ArgAction::Set
    )]
    pub flatten: bool,
    /// Output file prefix for `bt_to_gcs`.
    #[arg(long)]
    pub output: String,
    /// Optional cap on the initial key-range bundles produced at sampling time.
    #[arg(long, value_parser = parse_positive_usize)]
    pub max_initial_bundles: Option<usize>,
    /// Sub-split sampled tablet ranges when the tablet count is below the target bundle count.
    #[arg(long, action = clap::ArgAction::Set)]
    pub sub_split_tablets: Option<bool>,
}

impl PipelineOptionGroup for Args {}

/// Parses a positive `usize` flag value.
///
/// # Errors
///
/// Fails if `value` is not a positive integer.
pub fn parse_positive_usize(value: &str) -> std::result::Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|err| format!("'{value}' is not a valid positive integer: {err}"))?;
    if parsed == 0 {
        return Err("value must be greater than zero".to_string());
    }
    Ok(parsed)
}

/// Builds the [`ReadOptions`] for the table or materialized view of `args`.
pub fn read_options(args: &Args) -> Result<ReadOptions> {
    let Args {
        bigtable_project: project,
        bigtable_instance: instance,
        ..
    } = args;
    let target = match (&args.bigtable_table, &args.bigtable_materialized_view) {
        (None, Some(view)) => ReadTarget::materialized_view(project, instance, view),
        (Some(table), None) => ReadTarget::table(project, instance, table),
        (Some(_), Some(_)) | (None, None) => {
            return Err(Error::msg(
                "give exactly one of --bigtable_table and --bigtable_materialized_view",
            ));
        }
    };
    let mut options = ReadOptions::new(target).with_flattened(args.flatten);
    if let Some(profile) = args.app_profile.as_deref().filter(|s| !s.is_empty()) {
        options = options.with_app_profile_id(profile);
    }
    if let Some(max_bundles) = args.max_initial_bundles {
        options = options.with_max_initial_bundles(max_bundles);
    }
    if let Some(sub_split) = args.sub_split_tablets {
        options = options.with_sub_split_tablets(sub_split);
    }
    options.validate().map_err(Error::msg)?;
    Ok(options)
}

/// Formats one emitted [`Row`] into a tab-separated line for `bt_to_gcs`.
///
/// A nested row gives `<key>\t<family>:<qualifier>=<value>...`. If the values do not have
/// the nested schema order, the key and the families come from their field names.
pub fn format_row(row: &Row, flatten: bool) -> String {
    if flatten {
        match row.values() {
            [
                Some(FieldValue::Bytes(key)),
                Some(FieldValue::String(family)),
                Some(FieldValue::Bytes(qualifier)),
                Some(FieldValue::Array(cells)),
            ] => {
                let key = String::from_utf8_lossy(key);
                let qualifier = String::from_utf8_lossy(qualifier);
                let value = first_cell_value(cells);
                format!("{key}\t{family}:{qualifier}\t{value}")
            }
            _ => match BigtableColumn::try_from(row) {
                Ok(col) => {
                    let key = String::from_utf8_lossy(&col.key);
                    let qualifier = String::from_utf8_lossy(&col.column_qualifier);
                    let value = col
                        .latest_value()
                        .map(|bytes| String::from_utf8_lossy(bytes))
                        .unwrap_or_default();
                    format!("{key}\t{}:{qualifier}\t{value}", col.family_name)
                }
                Err(_) => String::new(),
            },
        }
    } else {
        let (key, families) = match row.values() {
            [
                Some(FieldValue::Bytes(key)),
                Some(FieldValue::Map(families)),
            ] => (key.as_slice(), families.as_slice()),
            _ => (
                row.get_bytes("key").ok().flatten().unwrap_or_default(),
                row.get_map("column_families")
                    .ok()
                    .flatten()
                    .unwrap_or_default(),
            ),
        };
        format_nested(key, families)
    }
}

/// Formats a key and its `family -> qualifier -> cells` map. Skips entries of another type.
fn format_nested(key: &[u8], families: &[MapEntry]) -> String {
    let mut line = String::with_capacity(key.len() + 128);
    line.push_str(&String::from_utf8_lossy(key));
    families
        .iter()
        .filter_map(|family| match family {
            (FieldValue::String(name), Some(FieldValue::Map(columns))) => Some((name, columns)),
            _ => None,
        })
        .flat_map(|(family, columns)| {
            columns.iter().filter_map(move |column| match column {
                (FieldValue::String(qualifier), cells) => Some((family, qualifier, cells)),
                _ => None,
            })
        })
        .fold(line, |mut line, (family, qualifier, cells)| {
            let value = match cells {
                Some(FieldValue::Array(cells)) => first_cell_value(cells),
                _ => Cow::Borrowed(""),
            };
            line.extend([
                "\t",
                family.as_str(),
                ":",
                qualifier.as_str(),
                "=",
                value.as_ref(),
            ]);
            line
        })
}

/// Value of the first cell (the latest one), or an empty string.
fn first_cell_value(cells: &[Option<FieldValue>]) -> Cow<'_, str> {
    match cells.first() {
        Some(Some(FieldValue::Row(cell))) => match cell.values() {
            [Some(FieldValue::Bytes(bytes)), _] => String::from_utf8_lossy(bytes),
            _ => Cow::Borrowed(""),
        },
        _ => Cow::Borrowed(""),
    }
}

/// Counts `e2e/rows` and formats each [`Row`] into a text line for Cloud Storage.
#[derive(Clone)]
pub struct TouchFn {
    rows: Counter,
    flatten: bool,
}

impl TouchFn {
    /// Creates a [`TouchFn`] for flattened (`true`) or nested (`false`) rows.
    pub fn new(flatten: bool) -> Self {
        Self {
            rows: Metrics::counter(NAMESPACE, "rows"),
            flatten,
        }
    }
}

impl DoFn for TouchFn {
    type In = Row;
    type Out = String;

    fn process_element(&mut self, row: Row, ctx: &mut ProcessContext<'_, String>) -> Result {
        self.rows.inc();
        ctx.emit(format_row(&row, self.flatten))
    }
}

/// Builds the `bt_to_gcs` pipeline: `Read`, `Touch`, and `Write`.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Result<Pipeline> {
    let Args {
        reader: Connector::Rust,
        shape: Shape::BtToGcs,
        flatten,
        output,
        ..
    } = args;
    let p = Pipeline::create(options);
    p.apply("Read", ReadRows::new(read_options(args)?))
        .par_do("Touch", TouchFn::new(*flatten))
        .apply("Write", textio::Write::new(output.as_str()));
    Ok(p)
}

/// Prints the `e2e` metrics of a run.
pub fn print_metrics(metrics: &MetricResults) {
    let rows = metrics.counter(NAMESPACE, "rows").unwrap_or(0);
    println!("e2e/rows={rows}");
}
