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

//! Cloud Bigtable benchmark in a Rust pipeline: `bt_to_gcs` and `gcs_to_bt` with the
//! Bigtable read and write connectors.
//!
//! - `bt_to_gcs` reads `--bigtable_table` with `ReadRows`, counts `e2e/rows`, formats each row
//!   as a tab-separated line, and writes the lines to `--output` with `textio::Write`.
//! - `gcs_to_bt` reads tab-separated lines from `--input` with `textio::Read`, parses each line
//!   into a batched mutation row, counts `e2e/rows`, and writes to `--bigtable_table` with
//!   `WriteMutations`.

use std::borrow::Cow;
use std::sync::Arc;

use beam::io::gcp::bigtable::read::{ReadOptions, ReadRows};
use beam::io::gcp::bigtable::write::{WriteMutations, WriteOptions};
use beam::io::gcp::bigtable::{
    BigtableColumn, BigtableReadShape, MUTATION_SET_CELL, ReadTarget, WriteTarget,
    batched_mutation_schema,
};
use beam::options::PipelineOptionGroup;
use beam::prelude::*;
use beam::schema::{FieldValue, MapEntry, Schema};
use beam_benchmark_io::{Connector, NAMESPACE};
use clap::{Args as ClapArgs, ValueEnum};
use serde::{Deserialize, Serialize};

/// Benchmark shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub enum Shape {
    /// Reads a Bigtable table and writes formatted text lines to Cloud Storage.
    BtToGcs,
    /// Reads formatted text lines from Cloud Storage and mutates a Bigtable table.
    GcsToBt,
}

/// Benchmark flags. Runner flags come from [`PipelineOptions`].
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
pub struct Args {
    /// Read connector (`bt_to_gcs`).
    #[arg(long, value_enum)]
    pub reader: Option<Connector>,
    /// Write connector (`gcs_to_bt`).
    #[arg(long, value_enum)]
    pub writer: Option<Connector>,
    /// Benchmark shape.
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
    /// Optional Bigtable app profile ID.
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
    /// Input file pattern for `gcs_to_bt`.
    #[arg(long)]
    pub input: Option<String>,
    /// Output file prefix for `bt_to_gcs`.
    #[arg(long)]
    pub output: Option<String>,
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

/// Builds the [`ReadOptions`] for the table or materialized view in `args`.
///
/// # Errors
///
/// Fails if neither or both of `--bigtable_table` and `--bigtable_materialized_view` are set,
/// or if the resulting options fail validation.
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
    let shape = if args.flatten {
        BigtableReadShape::Flattened
    } else {
        BigtableReadShape::Nested
    };
    let mut options = ReadOptions::new(target).with_shape(shape);
    if let Some(profile) = args.app_profile.as_deref().filter(|s| !s.is_empty()) {
        options = options.with_app_profile_id(profile);
    }
    if let Some(max_bundles) = args.max_initial_bundles {
        options = options.with_max_initial_bundles(max_bundles);
    }
    if let Some(sub_split) = args.sub_split_tablets {
        options = options.with_sub_split_tablets(sub_split);
    }
    options.validate()?;
    Ok(options)
}

/// Builds the [`WriteOptions`] for the destination table in `args`.
///
/// # Errors
///
/// Fails if `--bigtable_table` is missing or if the options fail validation.
pub fn write_options(args: &Args) -> Result<WriteOptions> {
    if args.bigtable_materialized_view.is_some() {
        return Err(Error::msg(
            "gcs_to_bt writes to --bigtable_table, not --bigtable_materialized_view",
        ));
    }
    let table = args
        .bigtable_table
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::msg("gcs_to_bt needs --bigtable_table"))?;
    let mut options = WriteOptions::new(WriteTarget::table(
        &args.bigtable_project,
        &args.bigtable_instance,
        table,
    ));
    if let Some(profile) = args.app_profile.as_deref().filter(|s| !s.is_empty()) {
        options = options.with_app_profile_id(profile);
    }
    options.validate()?;
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

fn set_cell_map_entry(family: &str, qualifier: &str, value: &str) -> Option<FieldValue> {
    if family.is_empty() {
        return None;
    }
    Some(FieldValue::Map(vec![
        (
            FieldValue::String("type".to_string()),
            Some(FieldValue::Bytes(MUTATION_SET_CELL.as_bytes().to_vec())),
        ),
        (
            FieldValue::String("family_name".to_string()),
            Some(FieldValue::Bytes(family.as_bytes().to_vec())),
        ),
        (
            FieldValue::String("column_qualifier".to_string()),
            Some(FieldValue::Bytes(qualifier.as_bytes().to_vec())),
        ),
        (
            FieldValue::String("value".to_string()),
            Some(FieldValue::Bytes(value.as_bytes().to_vec())),
        ),
    ]))
}

/// Parses one flattened (`<key>\t<family>:<qualifier>\t<value>`) or nested
/// (`<key>\t<family>:<qualifier>=<value>...`) text line into a [`batched_mutation_schema`] row.
pub fn parse_line(line: &str, schema: &Arc<Schema>) -> Option<Row> {
    let mut parts = line.split('\t');
    let key = parts.next().filter(|k| !k.is_empty())?;
    let rest: Vec<&str> = parts.collect();
    let mutations: Vec<Option<FieldValue>> = match rest.as_slice() {
        [col, val] if !col.contains('=') => {
            let (family, qualifier) = col.split_once(':')?;
            vec![Some(set_cell_map_entry(family, qualifier, val)?)]
        }
        fields => fields
            .iter()
            .filter_map(|field| {
                let (col, val) = field.split_once('=')?;
                let (family, qualifier) = col.split_once(':')?;
                set_cell_map_entry(family, qualifier, val).map(Some)
            })
            .collect(),
    };
    if mutations.is_empty() {
        return None;
    }
    Row::new(
        Arc::clone(schema),
        vec![
            Some(FieldValue::Bytes(key.as_bytes().to_vec())),
            Some(FieldValue::Array(mutations)),
        ],
    )
    .ok()
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

/// Parses text lines into batched Bigtable mutation rows and counts `e2e/rows`.
#[derive(Clone)]
pub struct ParseMutationsFn {
    rows: Counter,
    schema: Arc<Schema>,
}

impl ParseMutationsFn {
    /// Creates a [`ParseMutationsFn`] for [`batched_mutation_schema`].
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            rows: Metrics::counter(NAMESPACE, "rows"),
            schema,
        }
    }
}

impl DoFn for ParseMutationsFn {
    type In = String;
    type Out = Row;

    fn process_element(&mut self, line: String, ctx: &mut ProcessContext<'_, Row>) -> Result {
        if let Some(row) = parse_line(&line, &self.schema) {
            self.rows.inc();
            ctx.emit(row)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
struct FailOnRejectedRowFn;

impl DoFn for FailOnRejectedRowFn {
    type In = Row;
    type Out = i32;

    fn process_element(&mut self, row: Row, _ctx: &mut ProcessContext<'_, i32>) -> Result {
        let message = row
            .get_string("error_message")
            .ok()
            .flatten()
            .unwrap_or("unknown error");
        Err(Error::msg(format!(
            "Bigtable rejected a mutation row: {message}"
        )))
    }
}

/// Builds the `bt_to_gcs` or `gcs_to_bt` pipeline.
///
/// # Errors
///
/// Fails if required flags for the selected shape are missing or invalid.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Result<Pipeline> {
    let p = Pipeline::create(options);
    match args.shape {
        Shape::BtToGcs => {
            if args.writer.is_some() {
                return Err(Error::msg("bt_to_gcs does not accept --writer"));
            }
            if args.input.is_some() {
                return Err(Error::msg("bt_to_gcs does not accept --input"));
            }
            let Connector::Rust = args.reader.unwrap_or(Connector::Rust);
            let output = args
                .output
                .as_deref()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| Error::msg("bt_to_gcs needs --output"))?;
            p.apply("Read", ReadRows::new(read_options(args)?))
                .par_do("Touch", TouchFn::new(args.flatten))
                .apply("Write", textio::Write::new(output));
        }
        Shape::GcsToBt => {
            if args.reader.is_some() {
                return Err(Error::msg("gcs_to_bt does not accept --reader"));
            }
            if args.output.is_some() {
                return Err(Error::msg("gcs_to_bt does not accept --output"));
            }
            let Connector::Rust = args.writer.unwrap_or(Connector::Rust);
            let input = args
                .input
                .as_deref()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| Error::msg("gcs_to_bt needs --input"))?;
            let schema = batched_mutation_schema();
            p.apply("Read", textio::Read::new(input))
                .par_do("Touch", ParseMutationsFn::new(Arc::clone(&schema)))
                .with_row_schema(&schema)
                .apply("Write", WriteMutations::new(write_options(args)?))
                .par_do("FailOnError", FailOnRejectedRowFn);
        }
    }
    Ok(p)
}
