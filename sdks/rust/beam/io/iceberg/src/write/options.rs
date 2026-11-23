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

//! The table and the options of an Iceberg write.

use std::collections::BTreeMap;
use std::time::Duration;

use beam::schema::Schema;
use beam::transforms::display_data::DisplayDataBuilder;
use beam_io_arrow::FieldSelection;
use iceberg::TableIdent;

use super::schema::{iceberg_schema, partition_spec};
use crate::catalog::{Catalog, table_ident};

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
    pub(crate) fields: FieldSelection,
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
            fields: FieldSelection::All,
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

    /// Writes only these fields of the input rows. Replaces an earlier field selection.
    pub fn with_keep<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.fields = FieldSelection::Keep(fields.into_iter().map(Into::into).collect());
        self
    }

    /// Writes all fields of the input rows except these. Replaces an earlier field selection.
    pub fn with_drop<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.fields = FieldSelection::Drop(fields.into_iter().map(Into::into).collect());
        self
    }

    /// Writes the fields of the row field `field` of the input rows. Replaces an earlier field
    /// selection.
    pub fn with_only(mut self, field: impl Into<String>) -> Self {
        self.fields = FieldSelection::Only(field.into());
        self
    }

    /// The schema of the fields that the write takes from rows of `input`. A table that the
    /// write creates has these columns.
    ///
    /// # Errors
    ///
    /// Fails if the field selection names a field that `input` does not have, or selects no
    /// field.
    pub fn written_schema(&self, input: &Schema) -> beam::Result<Schema> {
        let (_, written) = self.fields.projection(input)?;
        if written.fields.is_empty() {
            return Err(beam::Error::msg(
                "The field selection leaves no fields to write",
            ));
        }
        Ok(written)
    }

    /// The identifier of the table.
    ///
    /// # Errors
    ///
    /// Fails if the table has no namespace or has an empty part.
    pub fn table_ident(&self) -> beam::Result<TableIdent> {
        table_ident(&self.table)
    }

    /// Checks the table name, the field selection, and the partition fields against `schema`,
    /// the schema of the input rows.
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
        let table_schema = iceberg_schema(&self.written_schema(schema)?)?;
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
        let selection = match &self.fields {
            FieldSelection::All => None,
            FieldSelection::Keep(fields) => Some(("keep", fields.join(","))),
            FieldSelection::Drop(fields) => Some(("drop", fields.join(","))),
            FieldSelection::Only(field) => Some(("only", field.clone())),
        };
        if let Some((key, value)) = selection {
            builder.add_text(key, value);
        }
    }
}
