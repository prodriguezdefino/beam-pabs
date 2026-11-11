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

//! The catalog, the table and the options of an Iceberg write.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use beam::schema::Schema;
use beam::transforms::display_data::DisplayDataBuilder;
use iceberg::TableIdent;

use crate::schema::{iceberg_schema, partition_spec};

/// An Iceberg REST catalog and how to reach it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Catalog {
    pub(crate) uri: String,
    pub(crate) warehouse: Option<String>,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) properties: BTreeMap<String, String>,
    pub(crate) auth: Auth,
    pub(crate) vended_credentials: bool,
    pub(crate) metrics_reporting: bool,
}

/// How the catalog client authenticates.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) enum Auth {
    None,
    Token(String),
    /// An access token of the Application Default Credentials, taken for each catalog request.
    #[cfg(feature = "gcp")]
    Google,
}

/// Hides the token, so that logs and errors do not show it.
impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::Token(_) => f.write_str("Token(..)"),
            #[cfg(feature = "gcp")]
            Self::Google => f.write_str("Google"),
        }
    }
}

impl Catalog {
    /// A REST catalog at `uri`, for example `https://biglake.googleapis.com/iceberg/v1/restcatalog`.
    pub fn rest(uri: impl Into<String>) -> Self {
        Self {
            uri: uri.into(),
            warehouse: None,
            headers: BTreeMap::new(),
            properties: BTreeMap::new(),
            auth: Auth::None,
            vended_credentials: false,
            metrics_reporting: false,
        }
    }

    /// The warehouse that the catalog serves, for example `bl://projects/<p>/catalogs/<c>`.
    pub fn with_warehouse(mut self, warehouse: impl Into<String>) -> Self {
        self.warehouse = Some(warehouse.into());
        self
    }

    /// Sends the HTTP header `name: value` with each catalog request.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(name.into(), value.into());
        self
    }

    /// Sends the catalog property `key = value` to the catalog client.
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Authenticates with a static bearer token.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.auth = Auth::Token(token.into());
        self
    }

    /// Authenticates with an access token of the Google Application Default Credentials, as
    /// the BigLake catalog needs.
    #[cfg(feature = "gcp")]
    pub fn with_google_auth(mut self) -> Self {
        self.auth = Auth::Google;
        self
    }

    /// Asks the catalog for short-lived storage credentials of each table, and writes the
    /// table files with them. A catalog in vended-credentials mode rejects requests without
    /// this option.
    pub fn with_vended_credentials(mut self) -> Self {
        self.vended_credentials = true;
        self
    }

    /// Sends a commit report to the `metrics` endpoint of the table after each commit. A
    /// report that fails is logged and does not fail the commit.
    pub fn with_metrics_reporting(mut self) -> Self {
        self.metrics_reporting = true;
        self
    }

    /// Returns the lineage segments of `ident` in this catalog.
    pub(crate) fn lineage_segments<'a>(&'a self, ident: &'a TableIdent) -> Vec<&'a str> {
        let root = self
            .warehouse
            .as_deref()
            .filter(|w| !w.is_empty())
            .or_else(|| (!self.uri.is_empty()).then_some(self.uri.as_str()));
        root.into_iter()
            .chain(ident.namespace().as_ref().iter().map(String::as_str))
            .chain([ident.name()])
            .collect()
    }
}

/// A partition transform of the Iceberg specification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    /// Hash into this number of buckets.
    Bucket(u32),
    /// Truncate to this width.
    Truncate(u32),
}

impl From<PartitionTransform> for iceberg::spec::Transform {
    fn from(transform: PartitionTransform) -> Self {
        match transform {
            PartitionTransform::Identity => Self::Identity,
            PartitionTransform::Year => Self::Year,
            PartitionTransform::Month => Self::Month,
            PartitionTransform::Day => Self::Day,
            PartitionTransform::Hour => Self::Hour,
            PartitionTransform::Bucket(n) => Self::Bucket(n),
            PartitionTransform::Truncate(width) => Self::Truncate(width),
        }
    }
}

/// A partition field of a new table: a transform of one top-level input field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionField {
    pub(crate) source: String,
    pub(crate) transform: PartitionTransform,
}

impl PartitionField {
    pub fn new(source: impl Into<String>, transform: PartitionTransform) -> Self {
        Self {
            source: source.into(),
            transform,
        }
    }
}

/// Where and how to write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteOptions {
    pub(crate) catalog: Catalog,
    pub(crate) table: String,
    pub(crate) partition_fields: Vec<PartitionField>,
    pub(crate) table_properties: BTreeMap<String, String>,
    pub(crate) triggering_frequency: Option<Duration>,
    pub(crate) target_file_bytes: Option<usize>,
    pub(crate) num_shards: Option<usize>,
}

impl WriteOptions {
    /// Writes to the table `table` of `catalog`. `table` is `namespace.name`, and a nested
    /// namespace has more parts: `a.b.name`.
    pub fn new(catalog: Catalog, table: impl Into<String>) -> Self {
        Self {
            catalog,
            table: table.into(),
            partition_fields: Vec::new(),
            table_properties: BTreeMap::new(),
            triggering_frequency: None,
            target_file_bytes: None,
            num_shards: None,
        }
    }

    /// The partition spec of the table, if the write creates it. An existing table keeps its
    /// own spec.
    pub fn with_partition_fields(
        mut self,
        fields: impl IntoIterator<Item = PartitionField>,
    ) -> Self {
        self.partition_fields = fields.into_iter().collect();
        self
    }

    /// A property of the table, if the write creates it. The write does not change the
    /// properties of an existing table.
    pub fn with_table_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.table_properties.insert(key.into(), value.into());
        self
    }

    /// How often a write of an unbounded input commits a snapshot. Default: 60 seconds. The
    /// boundedness of the input selects the shape of the write, not this option. A bounded
    /// input commits once at the end, so the expansion fails if this is set.
    pub fn with_triggering_frequency(mut self, frequency: Duration) -> Self {
        self.triggering_frequency = Some(frequency);
        self
    }

    /// The size after which a writer starts a new data file. Default: the table property
    /// `write.target-file-size-bytes`, else 512 MiB.
    pub fn with_target_file_bytes(mut self, bytes: usize) -> Self {
        self.target_file_bytes = Some(bytes);
        self
    }

    /// The number of writers for each table. Default: one for each worker thread that encodes
    /// rows, so the runner scales the writers with the input.
    pub fn with_num_shards(mut self, shards: usize) -> Self {
        self.num_shards = Some(shards);
        self
    }

    /// The identifier of the table.
    ///
    /// # Errors
    ///
    /// Fails if the table has no namespace or has an empty part.
    pub fn table_ident(&self) -> beam::Result<TableIdent> {
        let parts: Vec<&str> = self.table.split('.').collect();
        match parts.as_slice() {
            [_, _, ..] if parts.iter().all(|part| !part.is_empty()) => {
                Ok(TableIdent::from_strs(parts)?)
            }
            _ => Err(beam::Error::msg(format!(
                "The table '{}' is not of the form 'namespace.name'",
                self.table
            ))),
        }
    }

    /// Checks the table name, the input schema and the partition fields against `schema`, the
    /// schema of the rows to write.
    pub fn validate(&self, schema: &Schema) -> beam::Result {
        self.table_ident()?;
        let zero = [
            (
                "triggering frequency",
                self.triggering_frequency.is_some_and(|d| d.is_zero()),
            ),
            ("target file size", self.target_file_bytes == Some(0)),
            ("shard count", self.num_shards == Some(0)),
        ];
        if let Some((name, _)) = zero.into_iter().find(|(_, zero)| *zero) {
            return Err(beam::Error::msg(format!(
                "The {name} of the write must be greater than zero"
            )));
        }
        let table_schema = iceberg_schema(schema)?;
        partition_spec(&self.partition_fields, &table_schema)?;
        Ok(())
    }

    pub(crate) fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("catalog_uri", &self.catalog.uri);
        if let Some(warehouse) = &self.catalog.warehouse {
            builder.add_text("warehouse", warehouse);
        }
        builder.add_text("table", &self.table);
        if let Some(frequency) = self.triggering_frequency {
            builder.add_text("triggering_frequency", format!("{frequency:?}"));
        }
        if let Some(bytes) = self.target_file_bytes {
            builder.add_integer(
                "target_file_bytes",
                i64::try_from(bytes).unwrap_or(i64::MAX),
            );
        }
        if let Some(shards) = self.num_shards {
            builder.add_integer("num_shards", i64::try_from(shards).unwrap_or(i64::MAX));
        }
    }
}
