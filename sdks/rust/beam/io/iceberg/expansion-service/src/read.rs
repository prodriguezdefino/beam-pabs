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

//! SchemaTransform provider for the Apache Iceberg batch read of the Rust SDK.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use beam::io::iceberg::{ReadOptions, ReadRows};
use beam::io::managed::urns;
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Row, Schema};
use beam_expansion::{
    PCollectionId, SchemaTransformProvider, TransformInputs, register_schema_transform,
};

use crate::filter::parse_filter;
use crate::{ErrorHandling, parse_catalog};

/// Identifier of [`IcebergReadProvider`].
pub const URN_ICEBERG_READ: &str = "beam:schematransform:org.apache.beam:rust:iceberg_read:v1";

const OUTPUT_TAG: &str = "output";
const ERRORS_TAG: &str = "errors";

/// Configuration of [`IcebergReadProvider`]: the fields of the Iceberg read SchemaTransform of
/// the Managed API, and the snapshot and batch options of the Rust read. An empty string
/// counts as unset.
#[derive(Clone, Debug, Default, PartialEq, BeamRow)]
pub struct IcebergReadConfig {
    pub table: String,
    pub catalog_name: Option<String>,
    pub catalog_properties: Option<BTreeMap<String, String>>,
    pub config_properties: Option<BTreeMap<String, String>>,
    pub keep: Option<Vec<String>>,
    pub drop: Option<Vec<String>>,
    /// A SQL boolean expression, such as `id > 10 AND name LIKE 'a%'`.
    pub filter: Option<String>,
    pub snapshot_id: Option<i64>,
    pub as_of_timestamp_millis: Option<i64>,
    pub batch_size: Option<i32>,
    pub error_handling: Option<ErrorHandling>,
}

impl IcebergReadConfig {
    /// The read options of this configuration, or why they are not valid.
    pub fn read_options(self) -> Result<ReadOptions, String> {
        let Self {
            table,
            catalog_name: _,
            catalog_properties,
            config_properties,
            keep,
            drop,
            filter,
            snapshot_id,
            as_of_timestamp_millis,
            batch_size,
            error_handling: _,
        } = self;
        if table.trim().is_empty() {
            return Err("Set table".to_string());
        }
        if config_properties.as_ref().is_some_and(|p| !p.is_empty()) {
            return Err(
                "config_properties is not supported: the Rust Iceberg read uses no Hadoop \
                 configuration"
                    .to_string(),
            );
        }
        let catalog = parse_catalog(catalog_properties.unwrap_or_default(), &mut BTreeMap::new())?;
        let options = ReadOptions::new(catalog, table);
        let options = match (
            keep.filter(|k| !k.is_empty()),
            drop.filter(|d| !d.is_empty()),
        ) {
            (None, None) => options,
            (Some(keep), None) => options.with_keep(keep),
            (None, Some(drop)) => options.with_drop(drop),
            (Some(_), Some(_)) => return Err("Set at most one of keep and drop".to_string()),
        };
        let options = match (snapshot_id, as_of_timestamp_millis) {
            (None, None) => options,
            (Some(id), None) => options.with_snapshot_id(id),
            (None, Some(millis)) => options.with_as_of_timestamp_millis(millis),
            (Some(_), Some(_)) => {
                return Err("Set at most one of snapshot_id and as_of_timestamp_millis".to_string());
            }
        };
        let options = match filter.filter(|f| !f.trim().is_empty()) {
            Some(filter) => options.with_filter(parse_filter(&filter)?),
            None => options,
        };
        let options = match batch_size {
            Some(rows) => options.with_batch_size(
                usize::try_from(rows)
                    .ok()
                    .filter(|rows| *rows > 0)
                    .ok_or_else(|| format!("batch_size must be positive, got {rows}"))?,
            ),
            None => options,
        };
        options.check().map_err(|e| e.to_string())?;
        Ok(options)
    }
}

/// Reads one snapshot of an Apache Iceberg table in a REST catalog into Beam rows. See
/// [`IcebergReadConfig`] for the configuration.
///
/// The rows go to the `output` output. With `error_handling`, the rows that do not convert go
/// to the output that it names, with the fields `failed_row`, `error_message` and
/// `data_file`. Without `error_handling`, such a row fails the bundle.
#[derive(Default)]
pub struct IcebergReadProvider;

impl SchemaTransformProvider for IcebergReadProvider {
    fn identifier(&self) -> &'static str {
        URN_ICEBERG_READ
    }

    fn description(&self) -> &'static str {
        "Reads a snapshot of an Apache Iceberg table in a REST catalog, and outputs Beam rows."
    }

    fn config_schema(&self) -> Schema {
        Arc::unwrap_or_clone(IcebergReadConfig::beam_schema())
    }

    fn input_tags(&self) -> Vec<String> {
        Vec::new()
    }

    fn output_tags(&self) -> Vec<String> {
        vec![OUTPUT_TAG.to_string(), ERRORS_TAG.to_string()]
    }

    fn managed_identifiers(&self) -> &'static [&'static str] {
        &[urns::ICEBERG_READ]
    }

    fn build_transform(
        &self,
        config: Row,
        _inputs: TransformInputs,
        pipeline: &mut Pipeline,
    ) -> beam::Result<HashMap<String, PCollectionId>> {
        let mut config = IcebergReadConfig::from_row(&config).map_err(beam::Error::msg)?;
        let error_output = config
            .error_handling
            .take()
            .map(|ErrorHandling { output }| output)
            .filter(|output| !output.is_empty());
        if error_output.as_deref() == Some(OUTPUT_TAG) {
            return Err(beam::Error::msg(format!(
                "error_handling.output cannot be '{OUTPUT_TAG}'"
            )));
        }
        let read =
            ReadRows::new(config.read_options().map_err(beam::Error::msg)?).with_encoded_rows();
        let begin = pipeline.begin();
        match error_output {
            Some(errors) => {
                let read = begin.try_apply("ReadFromIceberg", read.with_failures())?;
                Ok(HashMap::from([
                    (OUTPUT_TAG.to_string(), read.output.id().into()),
                    (errors, read.failures.id().into()),
                ]))
            }
            None => {
                let rows = begin.try_apply("ReadFromIceberg", read)?;
                Ok(HashMap::from([(OUTPUT_TAG.to_string(), rows.id().into())]))
            }
        }
    }
}

register_schema_transform!(IcebergReadProvider);
