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

//! SchemaTransform providers for the Apache Iceberg connector of the Rust SDK.
//!
//! [`IcebergWriteConfig`] and [`IcebergReadConfig`] have the field names and types of the
//! Iceberg SchemaTransforms of the Managed API, so a caller can send the same configuration
//! to either expansion service. Options that only this connector has are extra nullable fields.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

mod filter;
mod read;

pub use filter::parse_filter;
pub use read::{IcebergReadConfig, IcebergReadProvider, URN_ICEBERG_READ};

use beam::io::iceberg::{Catalog, PartitionField, PartitionTransform, WriteOptions, WriteRows};
use beam::io::managed::urns;
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Row, Schema};
use beam::transforms::{DoFn, ParDo, ProcessContext};
use beam::values::IsBounded;
use beam_expansion::{
    PCollectionId, SchemaTransformProvider, TransformInputs, register_schema_transform,
};

/// Identifier of [`IcebergWriteProvider`].
pub const URN_ICEBERG_WRITE: &str = "beam:schematransform:org.apache.beam:rust:iceberg_write:v1";

/// Output tag of the committed snapshots.
const SNAPSHOTS_TAG: &str = "snapshots";

/// Default output tag name advertised for rejected rows.
const ERRORS_TAG: &str = "errors";

/// Java class name of the Google auth manager for REST catalogs.
const GOOGLE_AUTH_MANAGER: &str = "org.apache.iceberg.gcp.auth.GoogleAuthManager";

/// Header that asks a REST catalog for vended storage credentials.
const ACCESS_DELEGATION_HEADER: &str = "X-Iceberg-Access-Delegation";

/// Where the rows that the write rejects go: the output tag of the transform.
#[derive(Clone, Debug, Default, PartialEq, BeamRow)]
pub struct ErrorHandling {
    pub output: String,
}

/// Configuration of [`IcebergWriteProvider`]. An empty string counts as unset.
#[derive(Clone, Debug, Default, PartialEq, BeamRow)]
pub struct IcebergWriteConfig {
    pub table: String,
    pub catalog_name: Option<String>,
    pub catalog_properties: Option<BTreeMap<String, String>>,
    pub config_properties: Option<BTreeMap<String, String>>,
    /// Commit interval of an unbounded write, in seconds. A bounded input ignores it.
    pub triggering_frequency_seconds: Option<i32>,
    pub direct_write_byte_limit: Option<i32>,
    /// Write mode. Only `append` is supported.
    pub mode: Option<String>,
    pub keep: Option<Vec<String>>,
    pub drop: Option<Vec<String>>,
    pub only: Option<String>,
    pub partition_fields: Option<Vec<String>>,
    pub table_properties: Option<BTreeMap<String, String>>,
    pub num_shards: Option<i32>,
    /// Byte size after which a writer rolls to a new Parquet data file.
    pub target_file_bytes: Option<i64>,
    pub error_handling: Option<ErrorHandling>,
}

impl IcebergWriteConfig {
    /// Returns the write options of this configuration for an input that is `unbounded` or
    /// bounded, or why they are not valid.
    pub fn write_options(self, unbounded: bool) -> Result<WriteOptions, String> {
        let Self {
            table,
            catalog_name: _,
            catalog_properties,
            config_properties,
            triggering_frequency_seconds,
            direct_write_byte_limit,
            mode,
            keep,
            drop,
            only,
            partition_fields,
            table_properties,
            num_shards,
            target_file_bytes,
            error_handling: _,
        } = self;

        if table.trim().is_empty() {
            return Err("Set table".to_string());
        }
        if config_properties.as_ref().is_some_and(|p| !p.is_empty()) {
            return Err(
                "config_properties is not supported: the Rust Iceberg write uses no Hadoop \
                 configuration"
                    .to_string(),
            );
        }
        if direct_write_byte_limit.is_some() {
            return Err("direct_write_byte_limit is not supported".to_string());
        }
        if let Some(mode) = non_empty(mode.as_deref())
            && !mode.eq_ignore_ascii_case("append")
        {
            return Err(format!(
                "Unsupported mode '{mode}': only 'append' is supported"
            ));
        }
        let keep = keep.filter(|fields| !fields.is_empty());
        let drop = drop.filter(|fields| !fields.is_empty());
        let only = only.filter(|field| !field.is_empty());

        let mut merged_table_props = table_properties.unwrap_or_default();
        let catalog = parse_catalog(
            catalog_properties.unwrap_or_default(),
            &mut merged_table_props,
        )?;
        let parsed_partitions = partition_fields
            .unwrap_or_default()
            .iter()
            .map(|spec| parse_partition_field(spec))
            .collect::<Result<Vec<_>, _>>()?;

        let options = merged_table_props.into_iter().fold(
            WriteOptions::new(catalog, table).with_partition_fields(parsed_partitions),
            |opts, (key, value)| opts.with_table_property(key, value),
        );
        let mut options = match (keep, drop, only) {
            (None, None, None) => options,
            (Some(keep), None, None) => options.with_keep(keep),
            (None, Some(drop), None) => options.with_drop(drop),
            (None, None, Some(only)) => options.with_only(only),
            (_, _, _) => return Err("Set at most one of keep, drop and only".to_string()),
        };

        if let Some(seconds) = triggering_frequency_seconds {
            let seconds = u64::try_from(seconds)
                .ok()
                .filter(|s| *s > 0)
                .ok_or_else(|| {
                    format!("triggering_frequency_seconds must be positive, got {seconds}")
                })?;
            if unbounded {
                options = options.with_triggering_frequency(Duration::from_secs(seconds));
            }
        }
        if let Some(shards) = num_shards {
            let shards = usize::try_from(shards)
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| format!("num_shards must be positive, got {shards}"))?;
            options = options.with_num_shards(shards);
        }
        if let Some(bytes) = target_file_bytes {
            let bytes = usize::try_from(bytes)
                .ok()
                .filter(|b| *b > 0)
                .ok_or_else(|| format!("target_file_bytes must be positive, got {bytes}"))?;
            options = options.with_target_file_bytes(bytes);
        }

        options.table_ident().map_err(|e| e.to_string())?;
        Ok(options)
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|s| !s.is_empty())
}

/// Builds a [`Catalog`] from `props` and adds `table-default.<key>` entries to `table_props`
/// when `table_props` does not already set `<key>`.
fn parse_catalog(
    mut props: BTreeMap<String, String>,
    table_props: &mut BTreeMap<String, String>,
) -> Result<Catalog, String> {
    if let Some(catalog_type) = props.remove("type")
        && !catalog_type.is_empty()
        && !catalog_type.eq_ignore_ascii_case("rest")
    {
        return Err(format!(
            "Unsupported catalog type '{catalog_type}': only 'rest' is supported"
        ));
    }

    let uri = props
        .remove("uri")
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "Set catalog_properties.uri for the REST catalog".to_string())?;
    let mut catalog = Catalog::rest(uri);

    if let Some(warehouse) = props.remove("warehouse").filter(|s| !s.is_empty()) {
        catalog = catalog.with_warehouse(warehouse);
    }

    let token = props.remove("token").filter(|s| !s.is_empty());
    let auth_type = props.remove("rest.auth.type").filter(|s| !s.is_empty());
    catalog = match auth_type.as_deref() {
        None => match token {
            Some(token) => catalog.with_token(token),
            None => catalog,
        },
        Some(auth)
            if auth.eq_ignore_ascii_case("google")
                || auth.eq_ignore_ascii_case(GOOGLE_AUTH_MANAGER) =>
        {
            catalog.with_google_auth()
        }
        Some(auth) if auth.eq_ignore_ascii_case("none") => catalog,
        Some(auth) if auth.eq_ignore_ascii_case("oauth2") => {
            let token = token.ok_or_else(|| {
                "rest.auth.type 'oauth2' requires catalog_properties.token".to_string()
            })?;
            catalog.with_token(token)
        }
        Some(other) => {
            return Err(format!(
                "Unsupported rest.auth.type '{other}': expected 'google', \
                 '{GOOGLE_AUTH_MANAGER}', 'oauth2' or 'none'"
            ));
        }
    };

    if let Some(metrics) = props
        .remove("rest-metrics-reporting-enabled")
        .filter(|s| !s.is_empty())
    {
        if metrics.eq_ignore_ascii_case("true") {
            catalog = catalog.with_metrics_reporting();
        } else if !metrics.eq_ignore_ascii_case("false") {
            return Err(format!(
                "Invalid rest-metrics-reporting-enabled '{metrics}': expected 'true' or 'false'"
            ));
        }
    }

    // Java FileIO implementation class name; the Rust connector uses the SDK file systems.
    props.remove("io-impl");

    for (key, value) in props {
        if let Some(name) = key.strip_prefix("header.") {
            if name.is_empty() {
                return Err("Empty header name in catalog_properties ('header.')".to_string());
            }
            if name.eq_ignore_ascii_case(ACCESS_DELEGATION_HEADER) {
                if parse_access_delegation(&value)? {
                    catalog = catalog.with_vended_credentials();
                }
            } else {
                catalog = catalog.with_header(name, value);
            }
        } else if let Some(prop) = key.strip_prefix("table-default.") {
            if prop.is_empty() {
                return Err(
                    "Empty property name in catalog_properties ('table-default.')".to_string(),
                );
            }
            table_props.entry(prop.to_string()).or_insert(value);
        } else {
            catalog = catalog.with_property(key, value);
        }
    }

    Ok(catalog)
}

fn parse_access_delegation(value: &str) -> Result<bool, String> {
    if value.trim().is_empty() {
        return Ok(false);
    }
    let mut vended = false;
    for mode in value.split(',').map(str::trim).filter(|m| !m.is_empty()) {
        if mode.eq_ignore_ascii_case("vended-credentials") {
            vended = true;
        } else {
            return Err(format!(
                "Unsupported {ACCESS_DELEGATION_HEADER} mode '{mode}': only 'vended-credentials' \
                 is supported"
            ));
        }
    }
    Ok(vended)
}

fn valid_column(name: &str) -> Option<&str> {
    let trimmed = name.trim();
    let has_delimiter = trimmed
        .chars()
        .any(|c| matches!(c, '(' | ')' | '[' | ']' | ',') || c.is_whitespace());
    (!trimmed.is_empty() && !has_delimiter).then_some(trimmed)
}

fn parse_positive_u32(raw: &str, spec: &str) -> Result<u32, String> {
    raw.trim()
        .parse::<u32>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| {
            format!("Partition transform parameter '{raw}' in '{spec}' must be a positive integer")
        })
}

/// Parses one partition field expression, such as `col`, `hour(col)`, `bucket(col, 16)` or
/// `bucket[16](col)`.
fn parse_partition_field(spec: &str) -> Result<PartitionField, String> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return Err("Partition field specification cannot be empty".to_string());
    }
    let Some((head, rest)) = trimmed.split_once('(') else {
        let col = valid_column(trimmed)
            .ok_or_else(|| format!("Invalid partition field specification '{spec}'"))?;
        return Ok(PartitionField::new(col, PartitionTransform::Identity));
    };
    let Some(args) = rest.strip_suffix(')') else {
        return Err(format!("Malformed partition field specification '{spec}'"));
    };
    if args.contains(['(', ')']) {
        return Err(format!("Malformed partition field specification '{spec}'"));
    }

    let head = head.trim();
    if let Some((name, bracketed)) = head.split_once('[') {
        let Some(param) = bracketed.strip_suffix(']') else {
            return Err(format!("Malformed partition field specification '{spec}'"));
        };
        let col = valid_column(args)
            .ok_or_else(|| format!("Invalid column name in partition field '{spec}'"))?;
        let value = parse_positive_u32(param, spec)?;
        return match name.trim().to_ascii_lowercase().as_str() {
            "bucket" => Ok(PartitionField::new(col, PartitionTransform::Bucket(value))),
            "truncate" => Ok(PartitionField::new(
                col,
                PartitionTransform::Truncate(value),
            )),
            "void" => Err(format!(
                "Partition transform 'void' in '{spec}' is not supported"
            )),
            other => Err(format!(
                "Unsupported parameterized partition transform '{other}' in '{spec}'"
            )),
        };
    }

    let lower = head.to_ascii_lowercase();
    match lower.as_str() {
        "identity" | "hour" | "day" | "month" | "year" => {
            let col = valid_column(args)
                .ok_or_else(|| format!("Invalid column name in partition field '{spec}'"))?;
            let transform = match lower.as_str() {
                "identity" => PartitionTransform::Identity,
                "hour" => PartitionTransform::Hour,
                "day" => PartitionTransform::Day,
                "month" => PartitionTransform::Month,
                _ => PartitionTransform::Year,
            };
            Ok(PartitionField::new(col, transform))
        }
        "bucket" | "truncate" => {
            let (col_raw, param_raw) = args.split_once(',').ok_or_else(|| {
                format!("Partition transform '{head}' in '{spec}' requires (column, N)")
            })?;
            let col = valid_column(col_raw)
                .ok_or_else(|| format!("Invalid column name in partition field '{spec}'"))?;
            let value = parse_positive_u32(param_raw, spec)?;
            let transform = if lower == "bucket" {
                PartitionTransform::Bucket(value)
            } else {
                PartitionTransform::Truncate(value)
            };
            Ok(PartitionField::new(col, transform))
        }
        "void" => Err(format!(
            "Partition transform 'void' in '{spec}' is not supported"
        )),
        other => Err(format!("Unknown partition transform '{other}' in '{spec}'")),
    }
}

/// Appends Beam rows to an Apache Iceberg table in a REST catalog. See
/// [`IcebergWriteConfig`] for the configuration.
///
/// Committed snapshots go to the `snapshots` output. With `error_handling`, rows that do not
/// convert go to the output that it names, with the fields `failed_row` and `error_message`.
/// Without `error_handling`, a rejected row fails the bundle.
#[derive(Default)]
pub struct IcebergWriteProvider;

impl SchemaTransformProvider for IcebergWriteProvider {
    fn identifier(&self) -> &'static str {
        URN_ICEBERG_WRITE
    }

    fn description(&self) -> &'static str {
        "Appends Beam rows as Parquet data files to an Apache Iceberg table in a REST catalog, \
         and outputs committed snapshots."
    }

    fn config_schema(&self) -> Schema {
        Arc::unwrap_or_clone(IcebergWriteConfig::beam_schema())
    }

    fn input_tags(&self) -> Vec<String> {
        vec!["input".to_string()]
    }

    fn output_tags(&self) -> Vec<String> {
        vec![SNAPSHOTS_TAG.to_string(), ERRORS_TAG.to_string()]
    }

    fn managed_identifiers(&self) -> &'static [&'static str] {
        &[urns::ICEBERG_WRITE]
    }

    fn build_transform(
        &self,
        config: Row,
        inputs: TransformInputs,
        pipeline: &mut Pipeline,
    ) -> beam::Result<HashMap<String, PCollectionId>> {
        let mut config = IcebergWriteConfig::from_row(&config).map_err(beam::Error::msg)?;
        let error_output = config
            .error_handling
            .take()
            .map(|ErrorHandling { output }| output)
            .filter(|output| !output.is_empty());
        if error_output.as_deref() == Some(SNAPSHOTS_TAG) {
            return Err(beam::Error::msg(format!(
                "error_handling.output cannot be '{SNAPSHOTS_TAG}'"
            )));
        }
        let rows = inputs
            .get::<Row>(pipeline, "input")
            .map_err(beam::Error::msg)?;
        let options = config
            .write_options(rows.is_bounded() == IsBounded::Unbounded)
            .map_err(beam::Error::msg)?;
        let written = rows.try_apply("WriteToIceberg", WriteRows::new(options))?;
        let mut outputs =
            HashMap::from([(SNAPSHOTS_TAG.to_string(), written.snapshots.id().into())]);
        match error_output {
            Some(output) => {
                outputs.insert(output, written.failures.id().into());
            }
            None => {
                written
                    .failures
                    .apply("FailOnError", ParDo::new(FailOnErrorFn));
            }
        }
        Ok(outputs)
    }
}

register_schema_transform!(IcebergWriteProvider);

/// Fails the bundle on the first rejected row, so its output has no elements.
#[derive(Clone)]
struct FailOnErrorFn;

impl DoFn for FailOnErrorFn {
    type In = Row;
    type Out = i32;

    fn process_element(&mut self, row: Row, _ctx: &mut ProcessContext<'_, i32>) -> beam::Result {
        let reason = row
            .get_string("error_message")
            .ok()
            .flatten()
            .unwrap_or_default();
        Err(beam::Error::msg(format!(
            "Iceberg write rejected a row, and the write has no error_handling output: {reason}"
        )))
    }
}
